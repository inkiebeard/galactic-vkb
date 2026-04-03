/**
 * Structured logger for vkb.
 *
 * All output goes to stderr (never stdout, which is reserved for MCP JSON-RPC).
 * When an MCP server is registered via setMcpLogTarget(), log calls are also
 * forwarded as MCP notifications/message events so Claude Desktop and other
 * MCP clients can display them in their log views.
 *
 * Log level is controlled by the LOG_LEVEL env var (debug|info|warn|error).
 * Default: info.
 */

export type Level = 'debug' | 'info' | 'warn' | 'error';

// Syslog-style levels expected by the MCP spec
type McpLevel =
  | 'debug' | 'info' | 'notice' | 'warning'
  | 'error' | 'critical' | 'alert' | 'emergency';

const RANK: Record<Level, number> = { debug: 0, info: 1, warn: 2, error: 3 };

const TO_MCP: Record<Level, McpLevel> = {
  debug: 'debug',
  info:  'info',
  warn:  'warning',
  error: 'error',
};

// Minimal duck-typed interface — avoids importing the full MCP SDK here
interface McpLogTarget {
  sendLoggingMessage(params: { level: McpLevel; logger?: string; data: unknown }): void;
}

let _mcpTarget: McpLogTarget | null = null;

/** Register the active MCP server so log entries are forwarded as notifications. */
export function setMcpLogTarget(target: McpLogTarget): void {
  _mcpTarget = target;
}

function minRank(): number {
  const raw = (process.env.LOG_LEVEL ?? 'info').toLowerCase() as Level;
  return RANK[raw] ?? RANK.info;
}

function normalizeArg(a: unknown): unknown {
  if (a instanceof Error) return { message: a.message, stack: a.stack };
  return a;
}

function emit(level: Level, ns: string, args: unknown[]): void {
  if (RANK[level] < minRank()) return;

  // Human-readable line to stderr
  const label = level === 'warn' ? 'WARN ' : level.toUpperCase().padEnd(5);
  const text = args
    .map(a => {
      if (typeof a === 'string') return a;
      if (a instanceof Error) {
        // Prefer stack (includes message). For AggregateError also list sub-errors.
        const base = a.stack ?? a.message ?? String(a);
        const subErrors = (a as { errors?: unknown[] }).errors;
        if (Array.isArray(subErrors) && subErrors.length > 0) {
          const sub = subErrors
            .map(e => e instanceof Error ? (e.stack ?? e.message) : String(e))
            .join('\n    ');
          return `${base}\n  [errors]: ${sub}`;
        }
        return base;
      }
      return JSON.stringify(a);
    })
    .join(' ');
  process.stderr.write(`[${label}] [${ns}] ${text}\n`);

  // Structured notification to the connected MCP client (best-effort)
  try {
    _mcpTarget?.sendLoggingMessage({
      level: TO_MCP[level],
      logger: ns,
      data: args.length === 1 ? normalizeArg(args[0]) : args.map(normalizeArg),
    });
  } catch {
    // Never let MCP forwarding crash the caller
  }
}

export interface Logger {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
}

/** Create a namespaced logger. Cheap — just closes over the namespace string. */
export function createLogger(namespace: string): Logger {
  return {
    debug: (...args) => emit('debug', namespace, args),
    info:  (...args) => emit('info',  namespace, args),
    warn:  (...args) => emit('warn',  namespace, args),
    error: (...args) => emit('error', namespace, args),
  };
}
