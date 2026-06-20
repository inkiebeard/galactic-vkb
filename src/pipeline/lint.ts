/**
 * Lint pipeline — validates the integrity of the knowledge graph.
 *
 * Three checks, run in order:
 *
 *   Stage 1 — ORPHAN DETECTION  (SQL, fast)
 *     Entities with no inbound or outbound relations. Low severity. These are
 *     valid knowledge nodes but they're unreachable by graph traversal, which
 *     means vkb_neighbors will never surface them.
 *
 *   Stage 2 — SUMMARY FAITHFULNESS  (LLM, slow)
 *     Compares each entity's stored summary against its raw source text from
 *     the rawstore. Flags claims in the summary that cannot be supported by or
 *     contradict the raw content. Prevents hallucinated summaries from becoming
 *     ground truth that propagates through downstream relation extraction.
 *
 *   Stage 3 — CONTRADICTION DETECTION  (LLM, slow)
 *     For each entity, finds its top-K nearest neighbours by embedding cosine
 *     distance. Pairs that have no existing relation and whose summaries make
 *     contradictory factual claims are flagged. Confirmed contradictions also
 *     get a `lint:contradicts` relation asserted between them so graph
 *     traversals surface the conflict.
 *
 * Progress fields stored in job.progress:
 *   lint_checks?: string[]     — ['orphan','faithfulness','contradiction'] subset to run
 *   lint_entity_ids?: string[] — if set, limit faithfulness + contradiction to these entities
 *   orphans_found?: number
 *   faithfulness_total?: number
 *   faithfulness_done?: number
 *   faithfulness_issues?: number
 *   contradiction_pairs?: number
 *   contradiction_done?: number
 *   contradictions_found?: number
 *   findings_total?: number
 */

import { v4 as uuid } from 'uuid';
import { Pool } from 'pg';
import { getPool } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import { prompts } from './prompts.js';
import { createLogger } from '../logger.js';
import { pMap } from '../util/pmap.js';

const log = createLogger('lint');

// Maximum raw-content characters sent to the LLM for faithfulness checks.
// Keeps prompts within typical context limits while covering most content.
const MAX_RAW_CHARS = 4_000;

// Top-K nearest neighbours considered per entity for contradiction detection.
const CONTRADICTION_NEIGHBORS = 5;

// Maximum total pairs to check for contradictions (avoids O(n²) cost for
// large corpora). Pairs are ordered by proximity so the most likely
// contradictions are checked first.
const CONTRADICTION_MAX_PAIRS = 200;

function emit(msg: object): void {
  if (process.send) process.send(msg);
}

async function patchProgress(db: Pool, jobId: string, patch: object): Promise<void> {
  await db.query(
    `UPDATE job SET progress = progress || $1::jsonb WHERE id = $2`,
    [JSON.stringify(patch), jobId],
  );
}

async function insertFinding(
  db: Pool,
  jobId: string,
  opts: {
    kind: 'orphan' | 'unfaithful_summary' | 'contradiction';
    severity: 'high' | 'medium' | 'low';
    entityId: string | null;
    relatedEntityId?: string | null;
    description: string;
    detail?: Record<string, unknown>;
  },
): Promise<void> {
  await db.query(
    `INSERT INTO lint_finding
       (id, kind, severity, entity_id, related_entity_id, description, detail, status, job_id)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'open', $8)
     ON CONFLICT DO NOTHING`,
    [
      uuid(),
      opts.kind,
      opts.severity,
      opts.entityId,
      opts.relatedEntityId ?? null,
      opts.description,
      JSON.stringify(opts.detail ?? {}),
      jobId,
    ],
  );
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 1 — ORPHAN DETECTION
// ════════════════════════════════════════════════════════════════════════════

async function checkOrphans(db: Pool, jobId: string): Promise<number> {
  emit({ type: 'stage_change', job_id: jobId, stage: 'checking_orphans' });
  await db.query(`UPDATE job SET stage = 'checking_orphans' WHERE id = $1`, [jobId]);

  const { rows } = await db.query<{ id: string; type: string; summary: string | null }>(
    `SELECT e.id, e.type, e.summary
     FROM entity e
     WHERE e.status = 'ready'
       AND NOT EXISTS (
         SELECT 1 FROM relation r
         WHERE r.source_id = e.id OR r.target_id = e.id
       )
     ORDER BY e.created_at`,
  );

  let found = 0;
  for (const entity of rows) {
    const desc = entity.summary
      ? `Orphan entity (${entity.type}): has no inbound or outbound relations. Summary: "${entity.summary.slice(0, 120)}…"`
      : `Orphan entity (${entity.type}): has no inbound or outbound relations and no summary.`;

    await insertFinding(db, jobId, {
      kind: 'orphan',
      severity: 'low',
      entityId: entity.id,
      description: desc,
      detail: { entity_type: entity.type },
    });
    found++;
  }

  await patchProgress(db, jobId, { orphans_found: found });
  log.info(`Orphan check: ${found} orphan(s)`);
  return found;
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 2 — SUMMARY FAITHFULNESS
// ════════════════════════════════════════════════════════════════════════════

interface FaithfulnessIssue {
  claim: string;
  issue: 'unsupported' | 'contradicts';
  detail: string;
}

interface FaithfulnessResponse {
  faithfulness: 'high' | 'medium' | 'low';
  issues: FaithfulnessIssue[];
}

export function parseFaithfulnessResponse(raw: string): FaithfulnessResponse | null {
  // Strip markdown code fences if present
  const cleaned = raw.replace(/^```(?:json)?\s*/m, '').replace(/\s*```\s*$/m, '').trim();
  try {
    const parsed = JSON.parse(cleaned) as unknown;
    if (
      parsed &&
      typeof parsed === 'object' &&
      !Array.isArray(parsed) &&
      typeof (parsed as Record<string, unknown>).faithfulness === 'string' &&
      Array.isArray((parsed as Record<string, unknown>).issues)
    ) {
      return parsed as FaithfulnessResponse;
    }
  } catch {
    // ignore parse error — LLM may have returned non-JSON
  }
  return null;
}

async function checkFaithfulness(
  db: Pool,
  jobId: string,
  adapters: Adapters,
  entityIds: string[] | null,
): Promise<number> {
  emit({ type: 'stage_change', job_id: jobId, stage: 'checking_faithfulness' });
  await db.query(`UPDATE job SET stage = 'checking_faithfulness' WHERE id = $1`, [jobId]);

  const where: string[] = [
    "e.status = 'ready'",
    'e.summary IS NOT NULL',
    'e.raw_store_key IS NOT NULL',
  ];
  const params: unknown[] = [];

  if (entityIds?.length) {
    params.push(entityIds);
    where.push(`e.id = ANY($${params.length}::uuid[])`);
  }

  const { rows: entities } = await db.query<{
    id: string; type: string; summary: string; raw_store_key: string;
  }>(
    `SELECT e.id, e.type, e.summary, e.raw_store_key
     FROM entity e
     WHERE ${where.join(' AND ')}
     ORDER BY e.created_at`,
    params,
  );

  await patchProgress(db, jobId, { faithfulness_total: entities.length, faithfulness_done: 0, faithfulness_issues: 0 });

  let done = 0;
  let issues = 0;

  await pMap(entities, 2, async (entity) => {
    try {
      const rawFull = await adapters.rawstore.read(entity.raw_store_key);
      const rawText = rawFull.slice(0, MAX_RAW_CHARS);

      const rawResponse = await adapters.llm.complete(
        prompts.summaryFaithfulness,
        `SOURCE CONTENT:\n${rawText}\n\nGENERATED SUMMARY:\n${entity.summary}`,
      );

      const result = parseFaithfulnessResponse(rawResponse);
      if (!result) {
        log.warn(`Faithfulness: could not parse LLM response for entity ${entity.id}`);
      } else if (result.faithfulness !== 'high') {
        const severity = result.faithfulness === 'low' ? 'high' : 'medium';
        const issueCount = result.issues.length;
        const preview = result.issues
          .slice(0, 3)
          .map(i => `"${i.claim}" (${i.issue})`)
          .join('; ');

        await insertFinding(db, jobId, {
          kind: 'unfaithful_summary',
          severity,
          entityId: entity.id,
          description: `Summary faithfulness ${result.faithfulness}: ${issueCount} unsupported/contradicted claim(s). E.g.: ${preview}`,
          detail: {
            faithfulness: result.faithfulness,
            issue_count: issueCount,
            issues: result.issues,
            entity_type: entity.type,
          },
        });
        issues++;
      }
    } catch (e) {
      log.warn(`Faithfulness check failed for entity ${entity.id}:`, (e as Error).message);
    }

    done++;
    if (done % 5 === 0 || done === entities.length) {
      await patchProgress(db, jobId, { faithfulness_done: done, faithfulness_issues: issues });
    }
  });

  log.info(`Faithfulness check: ${issues} issue(s) across ${done} entities`);
  return issues;
}

// ════════════════════════════════════════════════════════════════════════════
// STAGE 3 — CONTRADICTION DETECTION
// ════════════════════════════════════════════════════════════════════════════

interface ContradictionResponse {
  contradicts: boolean;
  severity: 'high' | 'medium' | 'low';
  description: string;
}

export function parseContradictionResponse(raw: string): ContradictionResponse | null {
  const cleaned = raw.replace(/^```(?:json)?\s*/m, '').replace(/\s*```\s*$/m, '').trim();
  try {
    const parsed = JSON.parse(cleaned) as unknown;
    if (
      parsed &&
      typeof parsed === 'object' &&
      !Array.isArray(parsed) &&
      typeof (parsed as Record<string, unknown>).contradicts === 'boolean'
    ) {
      return parsed as ContradictionResponse;
    }
  } catch {
    // ignore
  }
  return null;
}

async function checkContradictions(
  db: Pool,
  jobId: string,
  adapters: Adapters,
  entityIds: string[] | null,
): Promise<number> {
  emit({ type: 'stage_change', job_id: jobId, stage: 'checking_contradictions' });
  await db.query(`UPDATE job SET stage = 'checking_contradictions' WHERE id = $1`, [jobId]);

  // Build the set of candidate entities
  const where: string[] = ["e.status = 'ready'", 'e.summary IS NOT NULL'];
  const params: unknown[] = [];
  if (entityIds?.length) {
    params.push(entityIds);
    where.push(`e.id = ANY($${params.length}::uuid[])`);
  }

  const { rows: entities } = await db.query<{ id: string; summary: string }>(
    `SELECT e.id, e.summary FROM entity e WHERE ${where.join(' AND ')} ORDER BY e.created_at`,
    params,
  );

  // Collect candidate pairs: (entityA, entityB, distanceScore) ordered by proximity.
  // For each entity we find its CONTRADICTION_NEIGHBORS nearest neighbours.
  // We deduplicate pairs (A,B) = (B,A) and skip pairs that already have a relation.
  const pairSet = new Set<string>(); // canonical key "minId:maxId"
  const pairs: Array<{ a: { id: string; summary: string }; b: { id: string; summary: string } }> = [];

  for (const entity of entities) {
    if (pairs.length >= CONTRADICTION_MAX_PAIRS) break;

    const { rows: neighbours } = await db.query<{ id: string; summary: string }>(
      `SELECT e.id, e.summary
       FROM entity e
       WHERE e.id != $1
         AND e.status = 'ready'
         AND e.summary IS NOT NULL
         AND NOT EXISTS (
           SELECT 1 FROM relation r
           WHERE (r.source_id = $1 AND r.target_id = e.id)
              OR (r.source_id = e.id AND r.target_id = $1)
         )
       ORDER BY (
         SELECT MIN(c.embedding <=> (
           SELECT embedding FROM chunk WHERE entity_id = $1 ORDER BY seq LIMIT 1
         ))
         FROM chunk c WHERE c.entity_id = e.id
       ) ASC
       LIMIT $2`,
      [entity.id, CONTRADICTION_NEIGHBORS],
    );

    for (const neighbour of neighbours) {
      if (pairs.length >= CONTRADICTION_MAX_PAIRS) break;
      const key = [entity.id, neighbour.id].sort().join(':');
      if (pairSet.has(key)) continue;
      pairSet.add(key);
      pairs.push({ a: entity, b: neighbour });
    }
  }

  await patchProgress(db, jobId, { contradiction_pairs: pairs.length, contradiction_done: 0, contradictions_found: 0 });

  let done = 0;
  let found = 0;

  await pMap(pairs, 2, async (pair) => {
    try {
      const rawResponse = await adapters.llm.complete(
        prompts.contradictionCheck,
        `ENTRY A:\n${pair.a.summary}\n\nENTRY B:\n${pair.b.summary}`,
      );

      const result = parseContradictionResponse(rawResponse);
      if (!result) {
        log.warn(`Contradiction: could not parse LLM response for pair ${pair.a.id}:${pair.b.id}`);
      } else if (result.contradicts) {
        await insertFinding(db, jobId, {
          kind: 'contradiction',
          severity: result.severity,
          entityId: pair.a.id,
          relatedEntityId: pair.b.id,
          description: result.description || 'Factual contradiction detected between two knowledge base entries.',
          detail: { contradiction_severity: result.severity },
        });

        // Assert a lint:contradicts relation so vkb_neighbors surfaces the conflict.
        await db.query(
          `INSERT INTO relation
             (id, source_id, target_id, source_kind, target_kind, rel_type, origin, weight, confidence)
           VALUES ($1, $2, $3, 'entity', 'entity', 'lint:contradicts', 'asserted', 1.0, 1.0)
           ON CONFLICT (source_id, target_id, rel_type) DO UPDATE
             SET last_seen_at = NOW()`,
          [uuid(), pair.a.id, pair.b.id],
        );

        found++;
      }
    } catch (e) {
      log.warn(`Contradiction check failed for pair ${pair.a.id}:${pair.b.id}:`, (e as Error).message);
    }

    done++;
    if (done % 5 === 0 || done === pairs.length) {
      await patchProgress(db, jobId, { contradiction_done: done, contradictions_found: found });
    }
  });

  log.info(`Contradiction check: ${found} contradiction(s) across ${done} pairs`);
  return found;
}

// ════════════════════════════════════════════════════════════════════════════
// MAIN ENTRY POINT
// ════════════════════════════════════════════════════════════════════════════

export async function runLintPipeline(jobId: string, adapters: Adapters): Promise<void> {
  const db = getPool();

  try {
    const { rows: jobRows } = await db.query<{
      progress: {
        lint_checks?: string[] | null;
        lint_entity_ids?: string[] | null;
        retry_count: number;
      };
    }>(
      'SELECT progress FROM job WHERE id = $1',
      [jobId],
    );
    if (!jobRows[0]) throw new Error(`Job not found: ${jobId}`);

    const { lint_checks, lint_entity_ids } = jobRows[0].progress;

    // Default: run all checks
    const checks = new Set<string>(
      lint_checks?.length ? lint_checks : ['orphan', 'faithfulness', 'contradiction'],
    );
    const entityIds: string[] | null = lint_entity_ids?.length ? lint_entity_ids : null;

    let findingsTotal = 0;

    if (checks.has('orphan')) {
      findingsTotal += await checkOrphans(db, jobId);
    }

    if (checks.has('faithfulness')) {
      findingsTotal += await checkFaithfulness(db, jobId, adapters, entityIds);
    }

    if (checks.has('contradiction')) {
      findingsTotal += await checkContradictions(db, jobId, adapters, entityIds);
    }

    await patchProgress(db, jobId, { findings_total: findingsTotal });
    await db.query(`UPDATE job SET stage = 'done', completed_at = NOW() WHERE id = $1`, [jobId]);
    emit({ type: 'complete', job_id: jobId });
    log.info(`Lint job ${jobId} complete — ${findingsTotal} finding(s)`);

  } catch (err) {
    const msg = (err as Error).message;
    log.error(`Lint job ${jobId} error:`, msg);
    await db.query(
      `UPDATE job SET stage = 'error', completed_at = NOW(),
       progress = progress || jsonb_build_object('error_detail', $1::text)
       WHERE id = $2`,
      [msg, jobId],
    );
    emit({ type: 'error', job_id: jobId, payload: msg });
  }
}
