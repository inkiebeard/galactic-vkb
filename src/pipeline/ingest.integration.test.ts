/**
 * Integration tests for the ingest pipeline.
 *
 * These tests connect to a real PostgreSQL database (with pgvector) and
 * exercise the full ingest path — from handleIngest() queueing a job to
 * runIngestPipeline() producing a ready entity with chunks, embeddings,
 * summary, and relations.
 *
 * Requirements:
 *   - Postgres running with pgvector extension enabled
 *   - TEST_DATABASE_URL env var (default: postgres://vkb:vkb@localhost:5433/vkb_test)
 *   - The test database must exist: createdb -U vkb vkb_test
 *
 * Run with:
 *   npm run test:integration
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { mkdtemp, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import type { Pool } from 'pg';

import { openTestDb, truncateAll, waitForEntityReady } from '../test/db-helpers.js';
import { makeStubAdapters } from '../test/stub-adapters.js';
import { setAdapters } from '../adapters/registry.js';
import { runIngestPipeline } from './ingest.js';
import {
  handleIngest, handleGet, handleRelate, handleNeighbors,
  handleQuery, handleIngestBulk,
} from '../mcp/tools.js';
import type { Adapters } from '../adapters/registry.js';

let db: Pool;
let rawstoreDir: string;
let adapters: Adapters;

beforeAll(async () => {
  db = await openTestDb();
  rawstoreDir = await mkdtemp(join(tmpdir(), 'vkb-ingest-test-'));
  adapters = makeStubAdapters(rawstoreDir);
  setAdapters(adapters);
}, 30_000);

afterAll(async () => {
  await db.end();
  await rm(rawstoreDir, { recursive: true, force: true });
});

beforeEach(async () => {
  await truncateAll(db);
});

// ── helpers ───────────────────────────────────────────────────────────────────

/** Queue and immediately run ingest for a document. */
async function ingestAndRun(text: string, type = 'doc', ref?: string): Promise<{
  entity_id: string; job_id: string | null;
}> {
  const result = await handleIngest({ type, text, ref });
  if (result.skipped) return { entity_id: result.entity_id, job_id: null };
  await runIngestPipeline(result.job_id!, adapters);
  return { entity_id: result.entity_id, job_id: result.job_id };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('ingest pipeline — basic flow', () => {
  it('creates a ready entity with summary, chunks, and raw_store_key', async () => {
    const text = 'Machine learning is a subset of artificial intelligence that enables systems to learn from data.';
    const { entity_id } = await ingestAndRun(text);

    const row = await waitForEntityReady(db, entity_id);
    expect(row.status).toBe('ready');
    expect(row.summary).toBeTruthy();
    expect(row.raw_store_key).toBeTruthy();

    const { rows: chunks } = await db.query(
      'SELECT id FROM chunk WHERE entity_id = $1',
      [entity_id],
    );
    expect(chunks.length).toBeGreaterThan(0);
  });

  it('stores embeddings for each chunk', async () => {
    const { entity_id } = await ingestAndRun(
      'Natural language processing allows computers to understand human language.',
    );
    await waitForEntityReady(db, entity_id);

    const { rows } = await db.query(
      `SELECT embedding FROM chunk WHERE entity_id = $1 AND embedding IS NOT NULL`,
      [entity_id],
    );
    expect(rows.length).toBeGreaterThan(0);
  });

  it('handleGet returns the ingested entity', async () => {
    const { entity_id } = await ingestAndRun('A brief document about databases.');
    await waitForEntityReady(db, entity_id);

    // handleGet returns entity fields spread directly (no .entity wrapper)
    const result = await handleGet(entity_id);
    expect(result.id).toBe(entity_id);
    expect(result.status).toBe('ready');
    expect(result.summary).toBeTruthy();
  });
});

describe('ingest pipeline — deduplication', () => {
  it('skips re-ingest of identical inline text (content hash dedup)', async () => {
    const text = 'Unique document content for dedup testing.';

    const first = await handleIngest({ type: 'doc', text });
    if (!first.skipped) await runIngestPipeline(first.job_id!, adapters);

    // Second ingest of identical text should be skipped
    const second = await handleIngest({ type: 'doc', text });
    expect(second.skipped).toBe(true);
    expect(second.entity_id).toBe(first.entity_id);
  });

  it('links previous_version_id when same ref is re-ingested', async () => {
    // Use a unique ref for this test. The stub fetch adapter returns the same
    // content for any ref, so we only test the version-linking logic here —
    // not the content-change detection (that is a pipeline internals test).
    const ref = `test://versioned-${Date.now()}`;

    // Ingest v1 and run the pipeline to completion so it reaches 'ready'.
    const v1 = await handleIngest({ type: 'doc', text: 'Placeholder (ref-based, text ignored).', ref });
    await runIngestPipeline(v1.job_id!, adapters);
    await waitForEntityReady(db, v1.entity_id!);

    // Queue v2 with the same ref. handleIngest should link it to v1.
    // We do NOT run the v2 pipeline — the stub fetch would return the same
    // content as v1 (same ref), which would trigger the content-unchanged
    // path and delete v2. The linkage test only requires handleIngest to have
    // correctly looked up the previous ready entity.
    const v2 = await handleIngest({ type: 'doc', text: 'Placeholder v2.', ref });
    expect(v2.previous_version_id).toBe(v1.entity_id);

    // The entity that was created for v2 should be in the DB at this point
    // (pipeline not run yet — it will delete it later if content unchanged).
    const { rows } = await db.query(
      `SELECT id, previous_version_id FROM entity WHERE id = $1`,
      [v2.entity_id],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].previous_version_id).toBe(v1.entity_id);
  });

  it('deletes the new entity and marks job skipped when ref content is unchanged', async () => {
    // The stub fetch adapter returns identical content for any ref, so two
    // ingests with the same ref always produce the same content hash — the
    // "content unchanged" pipeline path fires on the second run.
    const ref = `test://stable-${Date.now()}`;

    const v1 = await handleIngest({ type: 'doc', text: 'Placeholder (ignored for ref).', ref });
    await runIngestPipeline(v1.job_id!, adapters);
    await waitForEntityReady(db, v1.entity_id!);

    // Second ingest — same ref, stub returns same content → content_hash matches v1.
    const v2 = await handleIngest({ type: 'doc', text: 'Placeholder.', ref });
    // Pipeline detects hash match: deletes v2 entity, sets job.progress.skipped=true.
    await runIngestPipeline(v2.job_id!, adapters);

    // v2 entity should be deleted by the pipeline
    const { rows: v2Rows } = await db.query(
      `SELECT id FROM entity WHERE id = $1`,
      [v2.entity_id],
    );
    expect(v2Rows).toHaveLength(0);

    // v2 job should be marked done with skipped=true
    const { rows: jobRows } = await db.query<{ stage: string; progress: Record<string, unknown> }>(
      `SELECT stage, progress FROM job WHERE id = $1`,
      [v2.job_id],
    );
    expect(jobRows[0].stage).toBe('done');
    expect(jobRows[0].progress.skipped).toBe(true);
    expect(jobRows[0].progress.duplicate_of).toBe(v1.entity_id);

    // v1 should still be ready
    await waitForEntityReady(db, v1.entity_id!);
  });
});

describe('ingest pipeline — relations and graph traversal', () => {
  it('handleRelate creates a relation visible via handleNeighbors', async () => {
    const [a, b] = await Promise.all([
      ingestAndRun('Document A about machine learning.'),
      ingestAndRun('Document B about neural networks.'),
    ]);
    await Promise.all([
      waitForEntityReady(db, a.entity_id),
      waitForEntityReady(db, b.entity_id),
    ]);

    await handleRelate(a.entity_id, b.entity_id, 'relates_to', 0.9);

    const result = await handleNeighbors(a.entity_id);
    // nodes includes the seed + neighbours; filter to neighbour entities only
    const entityNodeIds = result.nodes
      .filter((n: { kind: string; id: string; hop: number }) => n.kind === 'entity' && n.hop > 0)
      .map((n: { id: string }) => n.id);
    expect(entityNodeIds).toContain(b.entity_id);
  });

  it('handleNeighbors returns only the seed node for an isolated entity', async () => {
    const { entity_id } = await ingestAndRun('A lonely document with no relations.');
    await waitForEntityReady(db, entity_id);

    const result = await handleNeighbors(entity_id);
    // The seed entity itself is always included (hop=0). An isolated entity
    // has no hop>0 neighbours.
    const hop0 = result.nodes.filter((n: { hop: number }) => n.hop === 0);
    const hopN = result.nodes.filter((n: { hop: number }) => n.hop > 0);
    expect(hop0).toHaveLength(1);
    expect(hopN).toHaveLength(0);
  });
});

describe('ingest pipeline — query', () => {
  it('handleQuery returns ingested entities via embedding similarity', async () => {
    // Ingest two documents and wait for both to be ready
    const { entity_id } = await ingestAndRun(
      'PostgreSQL is a powerful open-source relational database system.',
    );
    await waitForEntityReady(db, entity_id);

    const result = await handleQuery(
      { text: 'relational database', k: 10 },
      adapters,
    );
    const entityIds = result.results.map((r: { entity_id: string }) => r.entity_id);
    expect(entityIds).toContain(entity_id);
  });
});

describe('ingest pipeline — bulk ingest', () => {
  it('handleIngestBulk queues multiple entities and reports counts', async () => {
    const result = await handleIngestBulk([
      { type: 'doc', text: 'Bulk document one about databases.' },
      { type: 'doc', text: 'Bulk document two about machine learning.' },
      { type: 'doc', text: 'Bulk document three about distributed systems.' },
    ]);

    expect(result.queued).toBe(3);
    expect(result.failed).toBe(0);
    expect(result.results).toHaveLength(3);
    expect(result.results.every((r: { entity_id: string }) => !!r.entity_id)).toBe(true);

    // Run each pipeline job
    for (const r of result.results) {
      if (r.job_id) await runIngestPipeline(r.job_id, adapters);
    }

    // All should be ready
    for (const r of result.results) {
      const row = await waitForEntityReady(db, r.entity_id);
      expect(row.status).toBe('ready');
    }
  });

  it('handleIngestBulk deduplicates identical content', async () => {
    const text = 'Identical bulk content to be deduplicated.';

    // First bulk to create the entity
    await handleIngestBulk([{ type: 'doc', text }]);
    // Run all jobs
    const { rows: jobs } = await db.query<{ id: string; entity_id: string }>(
      `SELECT id, entity_id FROM job WHERE kind = 'ingest' AND stage = 'queued'`,
    );
    for (const job of jobs) {
      await runIngestPipeline(job.id, adapters);
    }

    // Second bulk with same text — should skip
    const second = await handleIngestBulk([{ type: 'doc', text }]);
    expect(second.skipped).toBe(1);
    expect(second.queued).toBe(0);
  });
});
