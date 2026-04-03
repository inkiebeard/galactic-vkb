/**
 * Retune worker — runs as a child process.
 * Claims a single retune job (kind=retune, stage=queued) using SELECT FOR UPDATE.
 * Only one retune runs at a time — enforced by this lock.
 */
import { getPool } from '../db/client.js';
import { getAdapters } from '../adapters/registry.js';
import { runRetunePipeline } from '../pipeline/retune.js';
import { createLogger } from '../logger.js';

const log = createLogger('retune-worker');

interface RetuneJobMeta {
  scope?: string;
  force?: boolean;
}

async function claimAndRun(): Promise<boolean> {
  const db = getPool();
  const client = await db.connect();

  try {
    await client.query('BEGIN');
    const { rows } = await client.query<{ id: string; progress: RetuneJobMeta }>(
      `SELECT id, progress FROM job
       WHERE kind = 'retune' AND stage = 'queued'
       ORDER BY created_at ASC
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
    );

    if (rows.length === 0) {
      await client.query('ROLLBACK');
      return false;
    }

    const job = rows[0];
    await client.query(
      `UPDATE job SET worker_pid = $1, stage = 'fetching' WHERE id = $2`,
      [process.pid, job.id],
    );
    await client.query('COMMIT');

  log.info(`Claimed job ${job.id}`);
  const opts: { scope?: string; force?: boolean } = {
      scope: (job.progress as Record<string, unknown>).scope as string | undefined,
      force: (job.progress as Record<string, unknown>).force as boolean | undefined,
    };
    await runRetunePipeline(job.id, getAdapters(), opts);
    return true;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {/* ignore */});
    throw e;
  } finally {
    client.release();
  }
}

async function main(): Promise<void> {
  log.info(`PID ${process.pid} started`);

  // Emit heartbeats
  const heartbeat = setInterval(() => {
    if (process.send) process.send({ type: 'heartbeat', job_id: '', pid: process.pid });
  }, 10_000);

  try {
    const ran = await claimAndRun();
    if (!ran) {
      log.info('No retune job found, exiting');
    }
  } catch (e) {
    log.error('Fatal:', e);
    process.exit(1);
  }

  clearInterval(heartbeat);
  process.exit(0);
}

main().catch(e => {
  log.error('Fatal:', e);
  process.exit(1);
});
