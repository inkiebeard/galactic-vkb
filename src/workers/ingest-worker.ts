/**
 * Ingest worker — runs as a child process.
 * Polls the job table for queued ingest jobs using SELECT FOR UPDATE SKIP LOCKED.
 * Processes one job at a time, then loops.
 */
import { getPool, isDbConnectionError } from '../db/client.js';
import { getAdapters } from '../adapters/registry.js';
import { runIngestPipeline } from '../pipeline/ingest.js';
import { createLogger } from '../logger.js';

const log = createLogger('ingest-worker');

const POLL_INTERVAL_MS     = 2_000;
const ERROR_BACKOFF_MS     = 5_000;
const DB_DOWN_BACKOFF_MS   = 30_000;

async function claimJob(): Promise<string | null> {
  const db = getPool();
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ id: string }>(
      `SELECT id FROM job
       WHERE kind = 'ingest' AND stage = 'queued'
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
      `UPDATE job SET worker_pid = $1, stage = 'fetching' WHERE id = $2`,
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

async function main(): Promise<void> {
  log.info(`PID ${process.pid} started`);

  // Emit heartbeats so coordinator knows we're alive
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
      log.info(`Claimed job ${jobId}`);
      await runIngestPipeline(jobId, adapters);
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

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

main().catch(e => {
  log.error('Fatal:', e);
  process.exit(1);
});
