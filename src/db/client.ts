import { Pool } from 'pg';
import { config } from '../config.js';
import { createLogger } from '../logger.js';

const log = createLogger('db');

let _pool: Pool | null = null;

export function getPool(): Pool {
  if (!_pool) {
    _pool = new Pool({
      connectionString: config.DATABASE_URL,
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });

    _pool.on('error', (err) => {
      log.error('Unexpected pool error:', err);
    });
  }
  return _pool;
}

/**
 * Probe the database connection, retrying until it succeeds or attempts are
 * exhausted.  Call this at startup before running migrations so a slow-starting
 * Docker container doesn't crash the process immediately.
 */
export async function waitForDb(
  maxAttempts = 10,
  delayMs     = 3_000,
): Promise<void> {
  const pool = getPool();
  let lastErr: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const client = await pool.connect();
      client.release();
      if (attempt > 1) log.info(`Database ready (attempt ${attempt})`);
      return;
    } catch (err) {
      lastErr = err;
      log.warn(
        `Database not ready (attempt ${attempt}/${maxAttempts}) — retrying in ${delayMs / 1000}s…`,
        err,
      );
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
  throw lastErr;
}

/**
 * Returns true when the error is a PostgreSQL / network connectivity failure —
 * i.e. the database process is unreachable, not an application-level query
 * error.  Handles AggregateError (Node's multi-address connect) recursively.
 */
export function isDbConnectionError(err: unknown): boolean {
  if (err == null) return false;
  // AggregateError — check every sub-error
  if (err instanceof Error && Array.isArray((err as { errors?: unknown[] }).errors)) {
    return (err as { errors: unknown[] }).errors.some(isDbConnectionError);
  }
  if (err instanceof Error) {
    const code = (err as NodeJS.ErrnoException).code ?? '';
    const msg  = err.message ?? '';
    const networkCodes = ['ECONNREFUSED', 'ENOTFOUND', 'ETIMEDOUT', 'ECONNRESET', 'EPIPE'];
    if (networkCodes.includes(code)) return true;
    if (networkCodes.some(c => msg.includes(c))) return true;
    // pg-pool / pg messages when connection drops mid-session
    if (msg.includes('Connection terminated') ||
        msg.includes('Client was closed') ||
        msg.includes('terminating connection') ||
        msg.includes('Connection timeout')) return true;
  }
  return false;
}

export async function closePool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}

/** Parse pgvector text format "[1.0,2.0,...]" → number[] */
export function parseVector(v: string | number[] | null | undefined): number[] | null {
  if (v == null) return null;
  if (Array.isArray(v)) return v as number[];
  try {
    return JSON.parse(v as string) as number[];
  } catch {
    return null;
  }
}

/** Serialize number[] → pgvector text format "[1.0,2.0,...]" */
export function serializeVector(v: number[]): string {
  return '[' + v.join(',') + ']';
}
