import { ChildProcess, spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { getPool, isDbConnectionError } from './db/client.js';
import { config } from './config.js';
import { broadcastEvent } from './http/server.js';
import type { WorkerMessage } from './types.js';
import { createLogger } from './logger.js';

const log = createLogger('coordinator');

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ── Worker state ──────────────────────────────────────────────────────────────
const ingestWorkers: Map<number, ChildProcess> = new Map();

// Tracks whether the last heartbeat saw a DB connectivity failure so we
// broadcast a recovery event when the connection is restored.
let _dbDown = false;

// ── Helpers ───────────────────────────────────────────────────────────────────
function getWorkerPath(name: string): string {
  const isTs = __filename.endsWith('.ts');
  const ext   = isTs ? '.ts' : '.js';
  return path.resolve(__dirname, `workers/${name}-worker${ext}`);
}

function spawnWorker(workerPath: string): ChildProcess {
  const isTs = workerPath.endsWith('.ts');
  const args = isTs
    ? ['--import', 'tsx', workerPath]
    : [workerPath];
  return spawn(process.execPath, args, {
    stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    env: process.env,
  });
}

function attachHandlers(worker: ChildProcess, name: string): void {
  worker.stdout?.on('data', (d: Buffer) =>
    process.stderr.write(`[${name}] ${d}`),
  );
  worker.stderr?.on('data', (d: Buffer) =>
    process.stderr.write(`[${name}] ${d}`),
  );

  worker.on('message', (msg: WorkerMessage) => {
    broadcastEvent(msg);
  });

  worker.on('exit', (code, signal) => {
    if (worker.pid !== undefined) {
      ingestWorkers.delete(worker.pid);
    }
    if (code !== 0 && code !== null) {
      log.error(`Worker ${worker.pid} (${name}) exited code=${code} signal=${signal}. Respawning in 5 s…`);
      broadcastEvent({ type: 'worker_crash', name, code, signal, ts: Date.now() });
      setTimeout(() => spawnIngestWorker(), 5000);
    }
  });
}

function spawnIngestWorker(): void {
  const workerPath = getWorkerPath('ingest');
  const worker     = spawnWorker(workerPath);
  if (worker.pid) {
    ingestWorkers.set(worker.pid, worker);
    attachHandlers(worker, `ingest#${worker.pid}`);
    log.info(`Spawned ingest worker pid=${worker.pid}`);
  }
}

// ── Heartbeat — reset stale jobs owned by dead PIDs ───────────────────────────
async function heartbeatCheck(): Promise<void> {
  const db = getPool();
  try {
    const { rows } = await db.query<{ id: string; worker_pid: number; stage: string }>(
      `SELECT id, worker_pid, stage FROM job
       WHERE stage NOT IN ('done','error') AND worker_pid IS NOT NULL`,
    );

    // DB responded — broadcast recovery if we were previously down
    if (_dbDown) {
      _dbDown = false;
      broadcastEvent({ type: 'db_available', ts: Date.now() });
      log.info('Database connection restored');
    }

    for (const job of rows) {
      let alive = false;
      try { process.kill(job.worker_pid, 0); alive = true; } catch { /* not alive */ }

      if (!alive) {
        await db.query(
          `UPDATE job
           SET stage       = 'queued',
               worker_pid  = NULL,
               progress    = jsonb_set(
                               progress, '{retry_count}',
                               (COALESCE((progress->>'retry_count')::int, 0) + 1)::text::jsonb
                             )
           WHERE id = $1
             AND (progress->>'retry_count')::int < $2
             AND stage NOT IN ('done','error')`,
          [job.id, config.INGEST_MAX_RETRIES],
        );
        await db.query(
          `UPDATE job
           SET stage        = 'error',
               completed_at = NOW()
           WHERE id = $1
             AND (progress->>'retry_count')::int >= $2
             AND stage NOT IN ('done','error')`,
          [job.id, config.INGEST_MAX_RETRIES],
        );
      }
    }
  } catch (e) {
    if (isDbConnectionError(e)) {
      if (!_dbDown) {
        _dbDown = true;
        broadcastEvent({ type: 'db_unavailable', ts: Date.now() });
        log.error('Database unavailable — workers will pause until connection is restored:', e);
      }
    } else {
      log.error('Heartbeat check error:', e);
    }
  }
}

// ── Retune scheduler ──────────────────────────────────────────────────────────
async function triggerScheduledRetune(): Promise<void> {
  const db = getPool();
  // Only schedule if no retune job is already queued or processing
  const { rows } = await db.query<{ cnt: string }>(
    `SELECT COUNT(*) AS cnt FROM job WHERE kind = 'retune' AND stage NOT IN ('done','error')`,
  );
  if (parseInt(rows[0]?.cnt ?? '0', 10) > 0) return;

  await db.query(
    `INSERT INTO job (kind, stage, progress, expires_at)
     VALUES ('retune', 'queued', '{"retry_count":0}', NOW() + INTERVAL '7 days')`,
  );
  spawnRetuneWorker();
  broadcastEvent({ type: 'retune_scheduled', ts: Date.now() });
  log.info('Scheduled retune sweep');
}

export function spawnRetuneWorker(scope?: string, force?: boolean): ChildProcess {
  const workerPath = getWorkerPath('retune');
  const env = {
    ...process.env,
    ...(scope ? { RETUNE_SCOPE: scope } : {}),
    ...(force ? { RETUNE_FORCE: 'true' } : {}),
  };
  const isTs = workerPath.endsWith('.ts');
  const args = isTs ? ['--import', 'tsx', workerPath] : [workerPath];
  const worker = spawn(process.execPath, args, {
    stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    env,
  });
  attachHandlers(worker, `retune#${worker.pid ?? 'x'}`);
  log.info(`Spawned retune worker pid=${worker.pid}`);
  return worker;
}

export function spawnFinetuneWorker(): ChildProcess {
  const workerPath = getWorkerPath('finetune');
  const isTs = workerPath.endsWith('.ts');
  const args = isTs ? ['--import', 'tsx', workerPath] : [workerPath];
  const worker = spawn(process.execPath, args, {
    stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    env: process.env,
  });
  attachHandlers(worker, `finetune#${worker.pid ?? 'x'}`);
  log.info(`Spawned finetune worker pid=${worker.pid}`);
  return worker;
}

// ── Public ────────────────────────────────────────────────────────────────────
export async function startWorkerPool(): Promise<void> {
  const concurrency = config.WORKER_CONCURRENCY;
  for (let i = 0; i < concurrency; i++) {
    spawnIngestWorker();
    // Stagger spawns
    await new Promise(r => setTimeout(r, 200));
  }

  // Single finetune worker (LLM-heavy, no benefit from concurrency)
  spawnFinetuneWorker();

  // Periodic heartbeat check
  setInterval(() => { void heartbeatCheck(); }, 30_000);

  // Retune scheduler
  if (config.RETUNE_INTERVAL_HOURS > 0) {
    setInterval(() => { void triggerScheduledRetune(); },
      config.RETUNE_INTERVAL_HOURS * 3_600_000);
  }

  log.info(`Worker pool started (concurrency=${concurrency})`);
}

export function shutdownWorkers(): void {
  for (const [pid, worker] of ingestWorkers) {
    log.info(`Shutting down ingest worker pid=${pid}`);
    worker.kill('SIGTERM');
  }
  ingestWorkers.clear();
}
