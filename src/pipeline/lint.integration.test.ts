/**
 * Integration tests for the lint pipeline.
 *
 * Tests the three lint checks against a real PostgreSQL database:
 *   1. Orphan detection — pure SQL, no LLM required
 *   2. Summary faithfulness — stub LLM returns 'high' by default; one test
 *      overrides the stub to return a faithfulness issue
 *   3. Contradiction detection — stub LLM returns no contradictions by default;
 *      one test confirms a finding is created when the stub flags one
 *
 * Also tests handleLintFindings() query filtering.
 *
 * Requirements: same as ingest.integration.test.ts — real postgres with pgvector.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { mkdtemp, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';
import { v4 as uuid } from 'uuid';
import type { Pool } from 'pg';

import { openTestDb, truncateAll, waitForEntityReady } from '../test/db-helpers.js';
import { makeStubAdapters } from '../test/stub-adapters.js';
import { setAdapters } from '../adapters/registry.js';
import { runIngestPipeline } from './ingest.js';
import { runLintPipeline } from './lint.js';
import { handleIngest, handleLint, handleLintFindings } from '../mcp/tools.js';
import type { Adapters } from '../adapters/registry.js';
import type { LLMAdapter } from '../adapters/interfaces.js';

let db: Pool;
let rawstoreDir: string;
let adapters: Adapters;

beforeAll(async () => {
  db = await openTestDb();
  rawstoreDir = await mkdtemp(join(tmpdir(), 'vkb-lint-test-'));
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

/** Queue and run ingest, return the ready entity id. */
async function ingestDoc(text: string, type = 'doc'): Promise<string> {
  const r = await handleIngest({ type, text });
  if (!r.skipped) await runIngestPipeline(r.job_id!, adapters);
  await waitForEntityReady(db, r.entity_id!);
  return r.entity_id!;
}

/** Insert a ready entity directly (fast path, no pipeline). */
async function insertReadyEntity(
  summary = 'Direct stub summary for testing.',
  rawStoreKey?: string,
): Promise<string> {
  const id = uuid();
  await db.query(
    `INSERT INTO entity (id, type, status, summary, raw_store_key, meta)
     VALUES ($1, 'test_doc', 'ready', $2, $3, '{}')`,
    [id, summary, rawStoreKey ?? null],
  );
  return id;
}

/** Insert a lint job so runLintPipeline has something to claim. */
async function createLintJob(checks?: string[]): Promise<string> {
  const { rows } = await db.query<{ id: string }>(
    `INSERT INTO job (kind, stage, progress, expires_at)
     VALUES ('lint', 'queued', $1::jsonb, NOW() + INTERVAL '1 day')
     RETURNING id`,
    [JSON.stringify({ retry_count: 0, lint_checks: checks ?? null })],
  );
  return rows[0].id;
}

// ── STAGE 1: Orphan detection ─────────────────────────────────────────────────

describe('lint — orphan detection', () => {
  it('flags a ready entity with no relations as an orphan', async () => {
    const entityId = await insertReadyEntity();
    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const { rows } = await db.query(
      `SELECT * FROM lint_finding WHERE kind = 'orphan' AND entity_id = $1`,
      [entityId],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].severity).toBe('low');
    expect(rows[0].status).toBe('open');
  });

  it('does not flag an entity that has a relation', async () => {
    const a = await insertReadyEntity('Entity A summary.');
    const b = await insertReadyEntity('Entity B summary.');

    await db.query(
      `INSERT INTO relation (id, source_id, target_id, source_kind, target_kind, rel_type, origin, weight, confidence)
       VALUES ($1, $2, $3, 'entity', 'entity', 'relates_to', 'asserted', 1.0, 1.0)`,
      [uuid(), a, b],
    );

    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const { rows } = await db.query(
      `SELECT * FROM lint_finding WHERE kind = 'orphan' AND entity_id IN ($1, $2)`,
      [a, b],
    );
    expect(rows).toHaveLength(0);
  });

  it('ignores entities that are not ready', async () => {
    const id = uuid();
    await db.query(
      `INSERT INTO entity (id, type, status, meta) VALUES ($1, 'test_doc', 'pending', '{}')`,
      [id],
    );

    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const { rows } = await db.query(
      `SELECT * FROM lint_finding WHERE kind = 'orphan' AND entity_id = $1`,
      [id],
    );
    expect(rows).toHaveLength(0);
  });

  it('flags multiple orphans in a single job', async () => {
    await insertReadyEntity('Alpha summary');
    await insertReadyEntity('Beta summary');
    await insertReadyEntity('Gamma summary');

    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const { rows } = await db.query(
      `SELECT COUNT(*) AS cnt FROM lint_finding WHERE kind = 'orphan' AND job_id = $1`,
      [jobId],
    );
    expect(parseInt(rows[0].cnt, 10)).toBe(3);
  });
});

// ── STAGE 2: Summary faithfulness ─────────────────────────────────────────────

describe('lint — summary faithfulness', () => {
  it('produces no finding when stub LLM returns high faithfulness', async () => {
    // Write raw content to rawstore so the pipeline can read it
    const entityId = uuid();
    const rawKey = `entities/${entityId}/entity.md`;
    await adapters.rawstore.write(rawKey, 'The actual raw source content for the entity.');
    await db.query(
      `INSERT INTO entity (id, type, status, summary, raw_store_key, meta)
       VALUES ($1, 'test_doc', 'ready', 'Accurate summary matching source.', $2, '{}')`,
      [entityId, rawKey],
    );

    const jobId = await createLintJob(['faithfulness']);
    await runLintPipeline(jobId, adapters);

    const { rows } = await db.query(
      `SELECT * FROM lint_finding WHERE kind = 'unfaithful_summary' AND entity_id = $1`,
      [entityId],
    );
    expect(rows).toHaveLength(0);
  });

  it('skips entities without raw_store_key', async () => {
    const entityId = await insertReadyEntity('Summary with no raw content.', /* rawStoreKey */ undefined);

    const jobId = await createLintJob(['faithfulness']);
    await runLintPipeline(jobId, adapters);

    const { rows } = await db.query(
      `SELECT * FROM lint_finding WHERE kind = 'unfaithful_summary' AND entity_id = $1`,
      [entityId],
    );
    // No finding — entity has no raw_store_key so it's skipped
    expect(rows).toHaveLength(0);
  });

  it('creates a finding when stub LLM returns low faithfulness', async () => {
    // Replace the LLM adapter with one that always returns a faithfulness issue
    const unfaithfulLLM: LLMAdapter = {
      async complete(system: string): Promise<string> {
        if (system.toLowerCase().includes('fact-checking') || system.toLowerCase().includes('faithfulness')) {
          return JSON.stringify({
            faithfulness: 'low',
            issues: [
              { claim: 'Revenue doubled', issue: 'unsupported', detail: 'Source never mentions revenue.' },
            ],
          });
        }
        return 'Stub summary.';
      },
    };

    const entityId = uuid();
    const rawKey = `entities/${entityId}/entity.md`;
    await adapters.rawstore.write(rawKey, 'Raw source: the company operates in three regions.');
    await db.query(
      `INSERT INTO entity (id, type, status, summary, raw_store_key, meta)
       VALUES ($1, 'test_doc', 'ready', 'Revenue doubled and the company operates globally.', $2, '{}')`,
      [entityId, rawKey],
    );

    const unfaithfulAdapters = { ...adapters, llm: unfaithfulLLM };
    const jobId = await createLintJob(['faithfulness']);
    await runLintPipeline(jobId, unfaithfulAdapters);

    const { rows } = await db.query(
      `SELECT * FROM lint_finding WHERE kind = 'unfaithful_summary' AND entity_id = $1`,
      [entityId],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].severity).toBe('high'); // low faithfulness → high severity
    expect(rows[0].detail.issue_count).toBe(1);
  });
});

// ── STAGE 3: Contradiction detection ─────────────────────────────────────────

describe('lint — contradiction detection', () => {
  it('asserts lint:contradicts relation when stub LLM flags a contradiction', async () => {
    // Use full pipeline to get real embeddings for proximity ranking
    const idA = await ingestDoc('The table has exactly 2 million rows as of Q3.');
    const idB = await ingestDoc('The table contains 10 million rows as of Q3.');

    // Replace LLM with one that always flags contradictions
    const contradictingLLM: LLMAdapter = {
      async complete(system: string): Promise<string> {
        if (system.toLowerCase().includes('contradiction')) {
          return JSON.stringify({
            contradicts: true,
            severity: 'high',
            description: 'Entry A says 2M rows; Entry B says 10M rows.',
          });
        }
        return 'Stub summary.';
      },
    };

    const contradictingAdapters = { ...adapters, llm: contradictingLLM };
    const jobId = await createLintJob(['contradiction']);
    await runLintPipeline(jobId, contradictingAdapters);

    // Expect a lint:contradicts relation between the two entities
    const { rows } = await db.query(
      `SELECT * FROM relation
       WHERE rel_type = 'lint:contradicts'
         AND ((source_id = $1 AND target_id = $2) OR (source_id = $2 AND target_id = $1))`,
      [idA, idB],
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].origin).toBe('asserted');

    // And a finding in lint_finding
    const { rows: findings } = await db.query(
      `SELECT * FROM lint_finding
       WHERE kind = 'contradiction'
         AND ((entity_id = $1 AND related_entity_id = $2)
           OR (entity_id = $2 AND related_entity_id = $1))`,
      [idA, idB],
    );
    expect(findings).toHaveLength(1);
    expect(findings[0].severity).toBe('high');
  });
});

// ── handleLintFindings query filtering ────────────────────────────────────────

describe('handleLintFindings', () => {
  it('returns open findings by default', async () => {
    await insertReadyEntity('Orphan alpha');
    await insertReadyEntity('Orphan beta');

    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const { findings, total } = await handleLintFindings({});
    expect(total).toBe(2);
    expect(findings.every((f: { status: string }) => f.status === 'open')).toBe(true);
  });

  it('filters by kind', async () => {
    await insertReadyEntity('Just an orphan');
    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const result = await handleLintFindings({ kind: 'orphan' });
    expect(result.findings.every((f: { kind: string }) => f.kind === 'orphan')).toBe(true);

    const noContradictions = await handleLintFindings({ kind: 'contradiction' });
    expect(noContradictions.total).toBe(0);
  });

  it('filters by severity', async () => {
    await insertReadyEntity('Low-severity orphan');
    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const lowFindings = await handleLintFindings({ severity: 'low' });
    expect(lowFindings.total).toBeGreaterThan(0);
    expect(lowFindings.findings.every((f: { severity: string }) => f.severity === 'low')).toBe(true);

    const highFindings = await handleLintFindings({ severity: 'high' });
    expect(highFindings.total).toBe(0);
  });

  it('filters by entity_id', async () => {
    const targetId = await insertReadyEntity('Target orphan');
    await insertReadyEntity('Other orphan');

    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const result = await handleLintFindings({ entity_id: targetId });
    expect(result.total).toBe(1);
    expect(result.findings[0].entity_id).toBe(targetId);
  });

  it('filters by job_id', async () => {
    await insertReadyEntity('Job-filtered orphan');
    const jobId = await createLintJob(['orphan']);
    await runLintPipeline(jobId, adapters);

    const result = await handleLintFindings({ job_id: jobId });
    expect(result.total).toBeGreaterThan(0);
    expect(result.findings.every((f: { job_id: string | null }) => f.job_id === jobId)).toBe(true);
  });

  it('handleLint queues a lint job and returns a job_id', async () => {
    const { job_id } = await handleLint(['orphan']);
    expect(typeof job_id).toBe('string');
    expect(job_id.length).toBeGreaterThan(0);

    const { rows } = await db.query(
      `SELECT kind, stage FROM job WHERE id = $1`,
      [job_id],
    );
    expect(rows[0].kind).toBe('lint');
    expect(rows[0].stage).toBe('queued');
  });
});
