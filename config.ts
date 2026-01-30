// config.ts - Config loading with import support
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import type { McpConfig, ServerEntry, McpSettings, ImportKind } from "./types.js";

const DEFAULT_CONFIG_PATH = join(homedir(), ".pi", "agent", "mcp.json");
const PROJECT_CONFIG_NAME = ".pi/mcp.json";

// Import source paths for other tools
const IMPORT_PATHS: Record<ImportKind, string> = {
  "cursor": join(homedir(), ".cursor", "mcp.json"),
  "claude-code": join(homedir(), ".claude", "claude_desktop_config.json"),
  "claude-desktop": join(homedir(), "Library", "Application Support", "Claude", "claude_desktop_config.json"),
  "codex": join(homedir(), ".codex", "config.json"),
  "windsurf": join(homedir(), ".windsurf", "mcp.json"),
  "vscode": ".vscode/mcp.json", // Relative to project
};

export function loadMcpConfig(overridePath?: string): McpConfig {
  const configPath = overridePath ? resolve(overridePath) : DEFAULT_CONFIG_PATH;
  
  // Load base config
  let config: McpConfig = { mcpServers: {} };
  
  if (existsSync(configPath)) {
    try {
      const raw = JSON.parse(readFileSync(configPath, "utf-8"));
      config = validateConfig(raw);
    } catch (error) {
      console.warn(`Failed to load MCP config from ${configPath}:`, error);
    }
  }
  
  // Process imports from other tools
  if (config.imports?.length) {
    for (const importKind of config.imports) {
      const importPath = IMPORT_PATHS[importKind];
      if (!importPath) continue;
      
      const fullPath = importPath.startsWith(".") 
        ? resolve(process.cwd(), importPath) 
        : importPath;
      
      if (!existsSync(fullPath)) continue;
      
      try {
        const imported = JSON.parse(readFileSync(fullPath, "utf-8"));
        const servers = extractServers(imported, importKind);
        
        // Merge - local config takes precedence over imports
        for (const [name, def] of Object.entries(servers)) {
          if (!config.mcpServers[name]) {
            config.mcpServers[name] = def;
          }
        }
      } catch (error) {
        console.warn(`Failed to import MCP config from ${importKind}:`, error);
      }
    }
  }
  
  // Check for project-local config (skip if it's the same as the main config)
  const projectPath = resolve(process.cwd(), PROJECT_CONFIG_NAME);
  if (existsSync(projectPath) && projectPath !== configPath) {
    try {
      const projectConfig = JSON.parse(readFileSync(projectPath, "utf-8"));
      const validated = validateConfig(projectConfig);
      
      // Project config overrides everything
      config.mcpServers = { ...config.mcpServers, ...validated.mcpServers };
      if (validated.settings) {
        config.settings = { ...config.settings, ...validated.settings };
      }
    } catch (error) {
      console.warn(`Failed to load project MCP config:`, error);
    }
  }
  
  return config;
}

function validateConfig(raw: unknown): McpConfig {
  if (!raw || typeof raw !== "object") {
    return { mcpServers: {} };
  }
  
  const obj = raw as Record<string, unknown>;
  const servers = obj.mcpServers ?? obj["mcp-servers"] ?? {};
  
  // Must be a plain object, not an array or null
  if (typeof servers !== "object" || servers === null || Array.isArray(servers)) {
    return { mcpServers: {} };
  }
  
  return {
    mcpServers: servers as Record<string, ServerEntry>,
    imports: Array.isArray(obj.imports) ? obj.imports as ImportKind[] : undefined,
    settings: obj.settings as McpSettings | undefined,
  };
}

function extractServers(config: unknown, kind: ImportKind): Record<string, ServerEntry> {
  if (!config || typeof config !== "object") return {};
  
  const obj = config as Record<string, unknown>;
  
  let servers: unknown;
  switch (kind) {
    case "claude-desktop":
    case "claude-code":
    case "codex":
      servers = obj.mcpServers;
      break;
    case "cursor":
    case "windsurf":
    case "vscode":
      servers = obj.mcpServers ?? obj["mcp-servers"];
      break;
    default:
      return {};
  }
  
  if (!servers || typeof servers !== "object" || Array.isArray(servers)) {
    return {};
  }
  
  return servers as Record<string, ServerEntry>;
}
