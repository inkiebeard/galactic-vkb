// ── Crash visibility — must be first so startup errors appear in logs ─────────
process.on('uncaughtException',  (err)    => { process.stderr.write('UNCAUGHT: '            + (err?.stack ?? String(err)) + '\n'); process.exit(1); });
process.on('unhandledRejection', (reason) => { process.stderr.write('UNHANDLED REJECTION: ' + String(reason)              + '\n'); process.exit(1); });

import './env.js';
import { config } from './config.js';
import { getPool, closePool, waitForDb } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { detectAndHandleDrift } from './db/drift.js';
import { getAdapters } from './adapters/registry.js';
import { startObsServer } from './http/server.js';
import { startMcpStdio, startMcpHttp, setReady } from './mcp/server.js';
import { startWorkerPool, shutdownWorkers } from './coordinator.js';
import { createLogger } from './logger.js';

const log = createLogger('vkb');

// ── CLI flags ─────────────────────────────────────────────────────────────────
if (process.argv.includes('--debug')) {
  process.env.LOG_LEVEL = 'debug';
}

async function main(): Promise<void> {
  log.info('Starting up…');

  // ── MCP transport — connect early so Claude Desktop's `initialize` ────────
  // handshake succeeds immediately. Tool calls are gated by `isReady` inside
  // the server and will return a "not ready" error until DB init completes.
  // stdio and HTTP transports are independent — both can run simultaneously
  // (e.g. Claude connects via stdio while external tools use the HTTP endpoint).
  if (config.MCP_STDIO) {
    console.error('STEP: startMcpStdio');
    try {
      await startMcpStdio();
    } catch(err: any) {
      log.error(`Couldn't start stdio MCP: ${err?.message ?? 'unknown error'}`)
    }
  }

  // ── Database ──────────────────────────────────────────────────────────────
  const db = getPool();

  console.error('STEP: waitForDb');
  await waitForDb();
  console.error('STEP: runMigrations');
  await runMigrations();
  log.info('Migrations complete');

  console.error('STEP: detectAndHandleDrift');
  await detectAndHandleDrift(db);
  log.info('Drift check complete');

  // ── Adapters & workers ────────────────────────────────────────────────────
  const adapters = getAdapters();

  // ── Observability server ──────────────────────────────────────────────────
  startObsServer(adapters);

  console.error('STEP: startWorkerPool');
  await startWorkerPool();

  console.error('STEP: setReady');
  // Unblock MCP tool calls now that everything is initialised.
  setReady(adapters);

  // ── HTTP MCP server (started after DB is ready) ───────────────────────────
  if (config.MCP_PORT !== 0) {
    console.error('STEP: startMcpHttp');
    try {
      await startMcpHttp(config.MCP_PORT);
      log.info('Ready');
    } catch(err: any) {
      log.error(`Couldn't start http MCP: ${err?.message ?? 'unknown error'}`)
    }
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

log.info(`Starting VKB Server: ${new Date().toISOString()} => ${JSON.stringify(process.env)}`)
main().catch(err => {
  log.error('Fatal startup error:', err);
  console.error(`Server Shutdown due to: ${err.message ?? 'no reason given'}`)
  process.exit(1);
});
