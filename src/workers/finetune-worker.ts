/**
 * Finetune worker — long-running polling loop (mirrors ingest-worker pattern).
 * Claims queued finetune jobs one at a time using SELECT FOR UPDATE SKIP LOCKED.
 * Spawned once at startup by coordinator.
 */
import { getPool, isDbConnectionError } from '../db/client.js';
import { getAdapters } from '../adapters/registry.js';
import { runFinetunePipeline } from '../pipeline/finetune.js';
import { createLogger } from '../logger.js';

const log = createLogger('finetune-worker');

const POLL_INTERVAL_MS   = 5_000;
const ERROR_BACKOFF_MS   = 5_000;
const DB_DOWN_BACKOFF_MS = 30_000;

async function claimJob(): Promise<string | null> {
  const db = getPool();
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ id: string }>(
      `SELECT id FROM job
       WHERE kind = 'finetune' AND stage = 'queued'
       ORDER BY created_at ASC
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
    );
    if (rows.length === 0) {
      await client.query('ROLLBACK');
      return null;
    }
    const jobId = rows[0].id;
    await client.query(
      `UPDATE job SET worker_pid = $1, stage = 'extracting' WHERE id = $2`,
      [process.pid, jobId],
    );
    await client.query('COMMIT');
    return jobId;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {/* ignore */});
    throw e;
  } finally {
    client.release();
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  log.info(`PID ${process.pid} started`);

  const heartbeat = setInterval(() => {
    if (process.send) process.send({ type: 'heartbeat', job_id: '', pid: process.pid });
  }, 10_000);

  const adapters = getAdapters();

  while (true) {
    try {
      const jobId = await claimJob();
      if (!jobId) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }
      log.info(`Claimed finetune job ${jobId}`);
      await runFinetunePipeline(jobId, adapters);
    } catch (e) {
      if (isDbConnectionError(e)) {
        log.warn('Database unavailable — pausing 30 s before retry…');
        await sleep(DB_DOWN_BACKOFF_MS);
      } else {
        log.error('Unexpected error:', e);
        await sleep(ERROR_BACKOFF_MS);
      }
    }
  }

  // eslint-disable-next-line no-unreachable
  clearInterval(heartbeat);
}

main().catch(e => {
  log.error('Fatal:', e);
  process.exit(1);
});
