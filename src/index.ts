import 'dotenv/config';
import { config } from './config.js';
import { getPool, closePool, waitForDb } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { detectAndHandleDrift } from './db/drift.js';
import { getAdapters } from './adapters/registry.js';
import { startObsServer } from './http/server.js';
import { startMcpStdio, startMcpHttp } from './mcp/server.js';
import { startWorkerPool, shutdownWorkers } from './coordinator.js';
import { createLogger } from './logger.js';

const log = createLogger('vkb');

async function main(): Promise<void> {
  log.info('Starting up…');

  // ── Database ──────────────────────────────────────────────────────────────
  const db = getPool();

  await waitForDb();
  await runMigrations();
  log.info('Migrations complete');

  await detectAndHandleDrift(db);
  log.info('Drift check complete');

  // ── Adapters ──────────────────────────────────────────────────────────────
  const adapters = getAdapters();

  // ── Observability server ──────────────────────────────────────────────────
  startObsServer(adapters);

  // ── MCP server ────────────────────────────────────────────────────────────
  await startWorkerPool();
  if (config.MCP_PORT === 0) {
    await startMcpStdio(adapters); // blocks — stdio transport
  } else {
    await startMcpHttp(adapters, config.MCP_PORT);
    log.info('Ready');
  }
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function shutdown(signal: string): Promise<void> {
  log.info(`Received ${signal} — shutting down…`);
  shutdownWorkers();
  await closePool();
  process.exit(0);
}

process.on('SIGTERM', () => { void shutdown('SIGTERM'); });
process.on('SIGINT',  () => { void shutdown('SIGINT');  });

main().catch(err => {
  log.error('Fatal startup error:', err);
  process.exit(1);
});
