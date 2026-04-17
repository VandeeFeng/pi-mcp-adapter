// server-manager.ts - MCP connection management (stdio + HTTP)
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import type { McpTool, McpResource, ServerDefinition, Transport } from "./types.js";
import { getStoredTokens } from "./oauth-handler.js";
import { resolveNpxBinary } from "./npx-resolver.js";

interface ServerConnection {
  client: Client;
  transport: Transport;
  definition: ServerDefinition;
  tools: McpTool[];
  resources: McpResource[];
  lastUsedAt: number;
  inFlight: number;
  status: "connected" | "closed";
}

export class McpServerManager {
  private connections = new Map<string, ServerConnection>();
  private connectPromises = new Map<string, Promise<ServerConnection>>();
  private connectAbortControllers = new Map<string, AbortController>();

  /**
   * Cancel an in-progress connection attempt.
   * Does nothing if no connection is pending for this server.
   */
  cancelConnect(name: string): void {
    const controller = this.connectAbortControllers.get(name);
    if (controller) {
      controller.abort();
      this.connectAbortControllers.delete(name);
    }
  }

  async connect(name: string, definition: ServerDefinition, signal?: AbortSignal): Promise<ServerConnection> {
    // Dedupe concurrent connection attempts
    if (this.connectPromises.has(name)) {
      return this.connectPromises.get(name)!;
    }

    // Reuse existing connection if healthy
    const existing = this.connections.get(name);
    if (existing?.status === "connected") {
      existing.lastUsedAt = Date.now();
      return existing;
    }

    // Create abort controller only if no external signal provided
    // (external signals are managed by the caller)
    const controller = signal ? undefined : new AbortController();
    const effectiveSignal = signal ?? controller!.signal;
    if (controller) {
      this.connectAbortControllers.set(name, controller);
    }

    const promise = this.createConnection(name, definition, effectiveSignal);
    this.connectPromises.set(name, promise);

    try {
      const connection = await promise;
      this.connections.set(name, connection);
      return connection;
    } catch (error) {
      // Clean up abort controller on error (including abort)
      if (controller) {
        this.connectAbortControllers.delete(name);
      }
      throw error;
    } finally {
      this.connectPromises.delete(name);
    }
  }

  private async createConnection(
    name: string,
    definition: ServerDefinition,
    signal: AbortSignal
  ): Promise<ServerConnection> {
    // Check if already aborted before starting
    if (signal.aborted) {
      throw new Error(`Connection to ${name} cancelled`);
    }

    const client = new Client({ name: `pi-mcp-${name}`, version: "1.0.0" });

    let transport: Transport;

    if (definition.command) {
      let command = definition.command;
      let args = definition.args ?? [];

      if (command === "npx" || command === "npm") {
        const resolved = await resolveNpxBinary(command, args, signal);
        if (resolved) {
          command = resolved.isJs ? "node" : resolved.binPath;
          args = resolved.isJs ? [resolved.binPath, ...resolved.extraArgs] : resolved.extraArgs;
          console.log(`MCP: ${name} resolved to ${resolved.binPath} (skipping npm parent)`);
        }
        if (signal?.aborted) {
          throw new Error(`Connection to ${name} cancelled`);
        }
      }

      transport = new StdioClientTransport({
        command,
        args,
        env: resolveEnv(definition.env),
        cwd: definition.cwd,
        stderr: definition.debug ? "inherit" : "ignore",
      });
    } else if (definition.url) {
      // HTTP transport with fallback
      transport = await this.createHttpTransport(definition, name, signal);
    } else {
      throw new Error(`Server ${name} has no command or url`);
    }

    const cleanupTransport = () => {
      const cleanup = () => {
        try { client.close(); } catch { /* ignore */ }
        try { transport.close(); } catch { /* ignore */ }
      };
      if (typeof queueMicrotask === "function") {
        queueMicrotask(cleanup);
      } else {
        setTimeout(cleanup, 0);
      }
    };

    const abortHandler = () => {
      if (signal.aborted) return;
      cleanupTransport();
    };
    signal.addEventListener("abort", abortHandler, { once: true });

    try {
      await client.connect(transport, { signal });

      // Check if aborted during connect
      if (signal.aborted) {
        throw new Error(`Connection to ${name} cancelled`);
      }

      // Discover tools and resources
      const [tools, resources] = await Promise.all([
        this.fetchAllTools(client, signal),
        this.fetchAllResources(client, signal),
      ]);

      // Check if aborted during tool/resource fetching
      if (signal.aborted) {
        throw new Error(`Connection to ${name} cancelled`);
      }
      return {
        client,
        transport,
        definition,
        tools,
        resources,
        lastUsedAt: Date.now(),
        inFlight: 0,
        status: "connected",
      };
    } catch (error) {
      // Non-blocking cleanup - fire and forget
      cleanupTransport();
      throw error;
    }
  }
  
  private async createHttpTransport(definition: ServerDefinition, serverName?: string, signal?: AbortSignal): Promise<Transport> {
    const url = new URL(definition.url!);
    const headers = resolveHeaders(definition.headers) ?? {};

    // Add bearer token if configured
    if (definition.auth === "bearer") {
      const token = definition.bearerToken
        ?? (definition.bearerTokenEnv ? process.env[definition.bearerTokenEnv] : undefined);
      if (token) {
        headers["Authorization"] = `Bearer ${token}`;
      }
    }

    // Handle OAuth auth - use stored tokens
    if (definition.auth === "oauth") {
      if (!serverName) {
        throw new Error("Server name required for OAuth authentication");
      }
      const tokens = getStoredTokens(serverName);
      if (!tokens) {
        throw new Error(
          `No OAuth tokens found for "${serverName}". Run /mcp-auth ${serverName} to authenticate.`
        );
      }
      headers["Authorization"] = `Bearer ${tokens.access_token}`;
    }

    const requestInit = Object.keys(headers).length > 0 ? { headers } : undefined;

    // Check abort before starting
    if (signal?.aborted) {
      throw new Error("Connection cancelled");
    }

    // Try StreamableHTTP first (modern MCP servers)
    const streamableTransport = new StreamableHTTPClientTransport(url, { requestInit });

    try {
      // Create a test client to verify the transport works (with abort support)
      const testClient = new Client({ name: "pi-mcp-probe", version: "1.0.0" });
      await testClient.connect(streamableTransport, { signal });
      await testClient.close().catch(() => {});
      // Close probe transport before creating fresh one
      await streamableTransport.close().catch(() => {});

      // StreamableHTTP works - create fresh transport for actual use
      return new StreamableHTTPClientTransport(url, { requestInit });
    } catch (error) {
      // StreamableHTTP failed, close and try SSE fallback (non-blocking)
      streamableTransport.close().catch(() => {});

      // Check abort before trying SSE
      if (signal?.aborted) {
        throw new Error("Connection cancelled");
      }

      // SSE is the legacy transport
      return new SSEClientTransport(url, { requestInit });
    }
  }
  
  private async fetchAllTools(client: Client, signal?: AbortSignal): Promise<McpTool[]> {
    const allTools: McpTool[] = [];
    let cursor: string | undefined;

    do {
      if (signal?.aborted) throw new Error("Connection cancelled");
      const result = await client.listTools(cursor ? { cursor } : undefined, { signal });
      allTools.push(...(result.tools ?? []));
      cursor = result.nextCursor;
    } while (cursor && !signal?.aborted);

    return allTools;
  }

  private async fetchAllResources(client: Client, signal?: AbortSignal): Promise<McpResource[]> {
    try {
      const allResources: McpResource[] = [];
      let cursor: string | undefined;

      do {
        if (signal?.aborted) throw new Error("Connection cancelled");
        const result = await client.listResources(cursor ? { cursor } : undefined, { signal });
        allResources.push(...(result.resources ?? []));
        cursor = result.nextCursor;
      } while (cursor && !signal?.aborted);

      return allResources;
    } catch {
      // Server may not support resources
      return [];
    }
  }
  
  async close(name: string): Promise<void> {
    const connection = this.connections.get(name);
    if (!connection) return;
    
    // Delete from map BEFORE async cleanup to prevent a race where a
    // concurrent connect() creates a new connection that our deferred
    // delete() would then remove, orphaning the new server process.
    connection.status = "closed";
    this.connections.delete(name);
    await connection.client.close().catch(() => {});
    await connection.transport.close().catch(() => {});
  }
  
  async closeAll(): Promise<void> {
    const names = [...this.connections.keys()];
    await Promise.all(names.map(name => this.close(name)));
  }
  
  getConnection(name: string): ServerConnection | undefined {
    return this.connections.get(name);
  }
  
  getAllConnections(): Map<string, ServerConnection> {
    return new Map(this.connections);
  }

  touch(name: string): void {
    const connection = this.connections.get(name);
    if (connection) {
      connection.lastUsedAt = Date.now();
    }
  }

  incrementInFlight(name: string): void {
    const connection = this.connections.get(name);
    if (connection) {
      connection.inFlight = (connection.inFlight ?? 0) + 1;
    }
  }

  decrementInFlight(name: string): void {
    const connection = this.connections.get(name);
    if (connection && connection.inFlight) {
      connection.inFlight--;
    }
  }

  isIdle(name: string, timeoutMs: number): boolean {
    const connection = this.connections.get(name);
    if (!connection || connection.status !== "connected") return false;
    if (connection.inFlight && connection.inFlight > 0) return false;
    return (Date.now() - connection.lastUsedAt) > timeoutMs;
  }
}

/**
 * Resolve environment variables with interpolation.
 */
function resolveEnv(env?: Record<string, string>): Record<string, string> {
  // Copy process.env, filtering out undefined values
  const resolved: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) {
      resolved[key] = value;
    }
  }
  
  if (!env) return resolved;
  
  for (const [key, value] of Object.entries(env)) {
    // Support ${VAR} and $env:VAR interpolation
    resolved[key] = value
      .replace(/\$\{(\w+)\}/g, (_, name) => process.env[name] ?? "")
      .replace(/\$env:(\w+)/g, (_, name) => process.env[name] ?? "");
  }
  
  return resolved;
}

/**
 * Resolve headers with environment variable interpolation.
 */
function resolveHeaders(headers?: Record<string, string>): Record<string, string> | undefined {
  if (!headers) return undefined;
  
  const resolved: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    resolved[key] = value
      .replace(/\$\{(\w+)\}/g, (_, name) => process.env[name] ?? "")
      .replace(/\$env:(\w+)/g, (_, name) => process.env[name] ?? "");
  }
  return resolved;
}
