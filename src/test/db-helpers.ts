/**
 * Database helpers for integration tests.
 *
 * Usage pattern (in each integration test file):
 *
 *   import { openTestDb, truncateAll, waitForEntityReady } from '../test/db-helpers.js';
 *
 *   let db: Pool;
 *   beforeAll(async () => { db = await openTestDb(); });
 *   afterAll(async () => { await db.end(); });
 *   beforeEach(async () => { await truncateAll(db); });
 */

import { Pool } from 'pg';
import { forcePool, closePool } from '../db/client.js';
import { runMigrations } from '../db/migrate.js';

/**
 * The test database URL.  Set TEST_DATABASE_URL in your environment (or a
 * .env.test file) to point at a postgres instance with the pgvector extension.
 *
 * Defaults to the docker-compose dev database (different DB name to avoid
 * clobbering real data).
 *
 *   postgres://vkb:vkb@localhost:5433/vkb_test
 *
 * One-time setup: the test database must exist before running integration tests:
 *   docker exec -it galactic-vkb-postgres-1 createdb -U vkb vkb_test
 */
export const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://vkb:vkb@localhost:5433/vkb_test';

/**
 * Open a pool connected to the test database, inject it as the module-level
 * pool singleton (via forcePool), and run all migrations.
 *
 * Call once in `beforeAll`. Call `db.end()` in `afterAll`.
 */
export async function openTestDb(): Promise<Pool> {
  // Close any existing pool (handles re-runs within the same worker process)
  await closePool();

  const db = new Pool({
    connectionString: TEST_DATABASE_URL,
    max: 5,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 5_000,
  });

  // Verify connectivity before running migrations
  const client = await db.connect();
  client.release();

  // Inject as the shared singleton — all pipeline code using getPool() will
  // now talk to the test database.
  forcePool(db);

  // Idempotent: migrations use IF NOT EXISTS / DO $$ blocks
  await runMigrations();

  return db;
}

/**
 * Truncate all vkb tables in reverse dependency order and reset sequences.
 * Call in `beforeEach` to give each test a clean slate.
 */
export async function truncateAll(db: Pool): Promise<void> {
  await db.query(`
    TRUNCATE TABLE
      lint_finding,
      relation,
      chunk,
      job,
      entity
    RESTART IDENTITY CASCADE
  `);
}

/**
 * Poll the `entity` table until the given entity reaches `status='ready'`
 * (or 'error'), then return the final row.
 *
 * Use after calling `runIngestPipeline` directly in tests — the pipeline is
 * synchronous but this guard makes the polling intent explicit.
 */
export async function waitForEntityReady(
  db: Pool,
  entityId: string,
  timeoutMs = 10_000,
): Promise<{ id: string; status: string; summary: string | null; raw_store_key: string | null }> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await db.query<{
      id: string; status: string; summary: string | null; raw_store_key: string | null;
    }>(
      `SELECT id, status, summary, raw_store_key FROM entity WHERE id = $1`,
      [entityId],
    );
    if (rows[0] && (rows[0].status === 'ready' || rows[0].status === 'error')) {
      return rows[0];
    }
    await new Promise(r => setTimeout(r, 50));
  }
  throw new Error(`Entity ${entityId} did not reach ready/error within ${timeoutMs}ms`);
}
