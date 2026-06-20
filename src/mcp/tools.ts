import { Pool } from 'pg';
import { v4 as uuid } from 'uuid';
import { resolve as resolvePath } from 'path';
import { createHash } from 'crypto';
import { config } from '../config.js';
import { getPool, serializeVector } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import type {
  IngestPayload, QueryPayload, QueryResultItem,
  RelationRef, RelationKind, RelationOrigin, SourceContext, LintFinding,
} from '../types.js';
import { createLogger } from '../logger.js';
import { pMapSettled } from '../util/pmap.js';
import { parseOkfBundle, okfRelType, normaliseTags } from '../adapters/okf.js';

const log = createLogger('query');

// ── Envelope helper ───────────────────────────────────────────────────────────
function ok<T>(data: T)  { return { ok: true  as const, data }; }
function err(e: unknown) { return { ok: false as const, error: String(e) }; }

// ── Tool: vkb_ingest ──────────────────────────────────────────────────────────
/** Resolve a ref to its canonical form:
 * - http/https URLs are kept as-is
 * - local file paths are resolved to absolute using the process CWD
 */
function canonicaliseRef(ref: string): string {
  if (/^https?:\/\//i.test(ref)) return ref;
  return resolvePath(ref);
}

/** Infer a default SourceContext from the ref/type when none is provided. */
function inferSourceContext(type: string, ref?: string): import('../types.js').SourceContext {
  if (ref) {
    // URLs and epub files default to external
    if (/^https?:\/\//i.test(ref) || /\.epub$/i.test(ref)) return 'external';
  }
  // All other cases also default to external unless explicitly overridden
  return 'external';
}

/**
 * Normalise tag fields in meta before persisting to the DB.
 * Both `tag` and `tags` may arrive as comma-separated strings from callers
 * (e.g. MCP clients, manual ingestion). Convert them to string arrays so the
 * JSONB column is consistently typed and queryable.
 */
function normaliseMetaTags(meta: Record<string, unknown>): Record<string, unknown> {
  const result = { ...meta };
  for (const key of ['tag', 'tags'] as const) {
    if (typeof result[key] === 'string') {
      result[key] = (result[key] as string)
        .split(',')
        .map(t => t.trim())
        .filter(t => t.length > 0);
    }
  }
  return result;
}

/** SHA-256 of the raw text body, lower-hex. Used for content-aware dedup. */
export function computeContentHash(text: string): string {
  return createHash('sha256').update(text, 'utf8').digest('hex');
}

export async function handleIngest(payload: IngestPayload) {
  if (!payload || typeof payload !== 'object') throw new Error('Request body is missing or not JSON');
  const db = getPool();
  const { type, text, source_context, meta = {} } = payload;
  const ref = payload.ref ? canonicaliseRef(payload.ref) : undefined;
  if (!type) throw new Error('Field "type" is required (e.g. "url", "doc", "note")');
  if (!text && !ref) throw new Error('At least one of text or ref is required');

  const resolvedContext = source_context ?? inferSourceContext(type, ref);
  const normalisedMeta  = normaliseMetaTags(meta);
  const ttl = config.JOB_TTL_DAYS;

  // ── Dedup: text-only ingests (no ref) ──────────────────────────────────────
  // Hash the inline text immediately. If an identical ready entity exists, skip
  // the entire pipeline and return the existing entity.
  let inlineContentHash: string | null = null;
  if (text && !ref) {
    inlineContentHash = computeContentHash(text);
    const { rows: dupRows } = await db.query<{ id: string }>(
      `SELECT id FROM entity WHERE content_hash = $1 AND status = 'ready' LIMIT 1`,
      [inlineContentHash],
    );
    if (dupRows[0]) {
      return { entity_id: dupRows[0].id, job_id: null, skipped: true, reason: 'content_unchanged' };
    }
  }

  // ── Version linking: ref-based ingests ────────────────────────────────────
  // If a ready entity already exists for this ref, record it as the previous
  // version. The pipeline will fetch the new content, compare hashes, and
  // either skip (same content) or proceed as a new version (content changed).
  let previousVersionId: string | null = null;
  if (ref) {
    const { rows: priorRows } = await db.query<{ id: string }>(
      `SELECT id FROM entity WHERE ref = $1 AND status = 'ready' ORDER BY created_at DESC LIMIT 1`,
      [ref],
    );
    if (priorRows[0]) {
      previousVersionId = priorRows[0].id;
    }
  }

  const entityId = uuid();
  const jobId    = uuid();

  await db.query(
    `INSERT INTO entity (id, type, ref, source_context, meta, status, content_hash, previous_version_id)
     VALUES ($1,$2,$3,$4,$5,'pending',$6,$7)`,
    [entityId, type, ref ?? null, resolvedContext, JSON.stringify(normalisedMeta),
     inlineContentHash, previousVersionId],
  );
  await db.query(
    `INSERT INTO job (id, entity_id, kind, stage, expires_at)
     VALUES ($1,$2,'ingest','queued', NOW() + ($3 || ' days')::interval)`,
    [jobId, entityId, ttl],
  );

  // If inline text, stash it in rawstore for the worker to pick up
  if (text && !ref) {
    const { getAdapters } = await import('../adapters/registry.js');
    const adapters = getAdapters();
    await adapters.rawstore.write(`staging/${entityId}.txt`, text);
  }

  return { job_id: jobId, entity_id: entityId, previous_version_id: previousVersionId };
}

// ── Tool: vkb_ingest_bulk ────────────────────────────────────────────────────
export async function handleIngestBulk(items: IngestPayload[]): Promise<{
  queued: number;
  skipped: number;
  failed: number;
  results: Array<{ index: number; entity_id: string; job_id: string | null; skipped?: boolean; reason?: string; error?: string }>;
}> {
  // Cap at 10 (== DB pool size) so we never saturate the connection pool.
  // Each handleIngest does ~4 DB round-trips (dedup check, version lookup,
  // entity insert, job insert); beyond 10 concurrent callers you're just
  // queuing work inside pg-pool and adding latency for no throughput gain.
  const settled = await pMapSettled(items, 10, (item, i) =>
    handleIngest(item).then(result => ({ index: i, ...result })),
  );

  let queued  = 0;
  let skipped = 0;
  let failed  = 0;
  const results: Array<{ index: number; entity_id: string; job_id: string | null; skipped?: boolean; reason?: string; error?: string }> = [];

  for (let i = 0; i < settled.length; i++) {
    const outcome = settled[i];
    if (outcome.status === 'fulfilled') {
      const r = outcome.value;
      results.push(r);
      if ((r as any).skipped) skipped++; else queued++;
    } else {
      results.push({ index: i, entity_id: '', job_id: null, error: String(outcome.reason) });
      failed++;
    }
  }

  return { queued, skipped, failed, results };
}

// ── Tool: vkb_reingest ───────────────────────────────────────────────────
export async function handleReingest(entityId?: string, force = false) {
  const db = getPool();
  const ttl = config.JOB_TTL_DAYS;

  // If force=true we allow reingesting entities that have no rawstore key yet
  // (e.g. failed before Phase 1 completed), so drop the raw_store_key filter.
  const { rows: entities } = await db.query<{ id: string }>(
    entityId
      ? force
        ? `SELECT id FROM entity WHERE id = $1`
        : `SELECT id FROM entity WHERE id = $1 AND raw_store_key IS NOT NULL`
      : `SELECT id FROM entity WHERE raw_store_key IS NOT NULL`,
    entityId ? [entityId] : [],
  );

  if (entities.length === 0) {
    throw new Error(entityId
      ? `Entity ${entityId} not found${force ? '' : ' or has no rawstore data'}`
      : 'No entities with rawstore data found');
  }

  const jobs: Array<{ job_id: string; entity_id: string }> = [];

  for (const entity of entities) {
    const jobId = uuid();

    // Cancel any in-progress jobs for this entity
    await db.query(
      `UPDATE job SET stage = 'error', completed_at = NOW(),
         progress = progress || '{"error_detail":"superseded by reingest"}'::jsonb
       WHERE entity_id = $1 AND stage NOT IN ('done', 'error')`,
      [entity.id],
    );
    // Clear all derived artifacts — chunks, section_summaries, and entity relations.
    // Note: section_summary has ON DELETE CASCADE on entity_id, NOT on chunk,
    // so we must delete sections explicitly before or after deleting chunks.
    await db.query(`DELETE FROM section_summary WHERE entity_id = $1`, [entity.id]);
    await db.query(`DELETE FROM chunk WHERE entity_id = $1`, [entity.id]);
    await db.query(
      `DELETE FROM relation WHERE (source_id = $1 OR target_id = $1) AND source_kind = 'entity'`,
      [entity.id],
    );
    // Reset entity status and summary
    await db.query(
      `UPDATE entity SET status = 'pending', summary = NULL, summary_version = 0, updated_at = NOW() WHERE id = $1`,
      [entity.id],
    );

    // Queue new ingest job
    // force_reingest tells the pipeline to bypass the Phase 2 "already complete"
    // checkpoint and re-chunk/re-embed/re-summarise the existing rawstore content
    // from scratch. raw_store_key is intentionally NOT cleared — the rawstore is
    // the preservation layer for content that may no longer be reachable.
    const progressInit = force
      ? '{"retry_count":0,"force_reingest":true}'
      : '{"retry_count":0,"from_rawstore":true}';
    await db.query(
      `INSERT INTO job (id, entity_id, kind, stage, progress, expires_at)
       VALUES ($1, $2, 'ingest', 'queued', $3::jsonb, NOW() + ($4 || ' days')::interval)`,
      [jobId, entity.id, progressInit, ttl],
    );

    jobs.push({ job_id: jobId, entity_id: entity.id });
  }

  return { queued: jobs.length, jobs };
}

// ── Tool: vkb_job ─────────────────────────────────────────────────────────────
export async function handleJob(jobId: string) {
  const db = getPool();
  const { rows } = await db.query<{
    stage: string; progress: unknown; entity_id: string | null; kind: string;
  }>('SELECT stage, progress, entity_id, kind FROM job WHERE id = $1', [jobId]);
  if (!rows[0]) throw new Error(`Job not found: ${jobId}`);
  return rows[0];
}

// ── Tool: vkb_query ───────────────────────────────────────────────────────────
export async function handleQuery(payload: QueryPayload, adapters: Adapters) {
  const db = getPool();
  const { text, k = 10, type, threshold, include_sections = false } = payload;
  const minSim = threshold ?? config.RELATION_THRESHOLD;

  const [queryEmbedding] = await adapters.embed.embed([text]);
  const vec = serializeVector(queryEmbedding);

  // $1=vec, $2=k, $3=fts text; optional $4=type
  let entityFilter = '';
  const params: unknown[] = [vec, k, text];
  if (type) {
    params.push(type);
    entityFilter = `AND e.type = $${params.length}`;
  }

  // Hybrid vector + full-text search merged via Reciprocal Rank Fusion (RRF).
  // Both paths scan the same corpus independently; the CTE merges them so that
  // a chunk matching on both keyword and semantics floats to the top, while a
  // chunk only matching by name (zero vector similarity) still surfaces.
  // RRF score = 1/(60 + vec_rank) + 1/(60 + fts_rank); k=60 is the standard
  // smoothing constant that prevents high-rank outliers from dominating.
  //
  // FTS uses ts_rank_cd (cover density) with a minimum score of 0.05 to
  // suppress incidental mentions — e.g. a chunk about silk fabric that happens
  // to contain the word "Silk" once will not surface as a keyword match for
  // the character named Silk.
  const { rows } = await db.query<{
    chunk_id: string; chunk_summary: string | null; similarity: number;
    rrf_score: number; fts_match: boolean;
    entity_id: string; entity_type: string; entity_summary: string | null;
    raw_store_key: string | null;
    embedding: string | null;
  }>(
    `WITH vec_ranked AS (
       SELECT c.id AS chunk_id,
              1 - (c.embedding <=> $1::vector) AS similarity,
              ROW_NUMBER() OVER (ORDER BY c.embedding <=> $1::vector) AS vec_rank
       FROM chunk c
       JOIN entity e ON e.id = c.entity_id
       WHERE c.embedding IS NOT NULL AND e.status = 'ready' ${entityFilter}
       ORDER BY c.embedding <=> $1::vector
       LIMIT $2
     ),
     fts_ranked AS (
       SELECT chunk_id,
              ROW_NUMBER() OVER (ORDER BY fts_score DESC) AS fts_rank
       FROM (
         SELECT c.id AS chunk_id,
                ts_rank_cd(
                  to_tsvector('english', COALESCE(c.summary, '')),
                  websearch_to_tsquery('english', $3)
                ) AS fts_score
         FROM chunk c
         JOIN entity e ON e.id = c.entity_id
         WHERE to_tsvector('english', COALESCE(c.summary, ''))
                 @@ websearch_to_tsquery('english', $3)
           AND e.status = 'ready' ${entityFilter}
         ORDER BY fts_score DESC
         LIMIT $2
       ) sub
       WHERE fts_score > 0.05
     ),
     merged AS (
       SELECT
         COALESCE(v.chunk_id, f.chunk_id)                             AS chunk_id,
         COALESCE(v.similarity, 0)                                    AS similarity,
         COALESCE(1.0 / (60 + v.vec_rank), 0::float)
           + COALESCE(1.0 / (60 + f.fts_rank), 0::float)             AS rrf_score,
         (f.chunk_id IS NOT NULL)                                     AS fts_match
       FROM vec_ranked v
       FULL OUTER JOIN fts_ranked f ON f.chunk_id = v.chunk_id
     )
     SELECT
       m.chunk_id, m.similarity, m.rrf_score, m.fts_match,
       c.summary         AS chunk_summary,
       c.raw_store_key,
       c.embedding::text AS embedding,
       e.id              AS entity_id,
       e.type            AS entity_type,
       e.summary         AS entity_summary
     FROM merged m
     JOIN chunk c ON c.id = m.chunk_id
     JOIN entity e ON e.id = c.entity_id
     ORDER BY m.rrf_score DESC
     LIMIT $2`,
    params,
  );

  // FTS keyword matches bypass the similarity threshold — the model may be
  // looking up a proper noun it doesn't know semantically. Vector-only hits
  // still require the configured minimum similarity.
  const filtered = rows.filter(r => r.fts_match || r.similarity >= minSim);

  // Fetch section summaries if requested
  const sectionMap = new Map<string, string>();
  if (include_sections && filtered.length > 0) {
    const chunkIds = filtered.map(r => r.chunk_id);
    const { rows: secRows } = await db.query<{
      chunk_ids: string[]; summary: string;
    }>(
      `SELECT chunk_ids, summary FROM section_summary
       WHERE entity_id = ANY($1::uuid[])`,
      [filtered.map(r => r.entity_id)],
    );
    for (const sec of secRows) {
      for (const cid of sec.chunk_ids) {
        if (!sectionMap.has(cid)) sectionMap.set(cid, sec.summary);
      }
    }
  }

  // Fetch top-3 relations per chunk
  const chunkIds = filtered.map(r => r.chunk_id);
  const relMap = new Map<string, RelationRef[]>();
  if (chunkIds.length > 0) {
    const { rows: relRows } = await db.query<{
      source_id: string; target_id: string; target_kind: string;
      rel_type: string; origin: string; confidence: number; weight: number;
      target_summary: string | null;
    }>(
      `SELECT r.source_id, r.target_id, r.target_kind, r.rel_type, r.origin, r.confidence, r.weight,
              COALESCE(
                (SELECT summary FROM chunk WHERE id = r.target_id),
                (SELECT summary FROM entity WHERE id = r.target_id)
              ) AS target_summary
       FROM relation r
       WHERE r.source_id = ANY($1::uuid[])
       ORDER BY r.confidence DESC`,
      [chunkIds],
    );

    for (const rel of relRows) {
      const list = relMap.get(rel.source_id) ?? [];
      if (list.length < 3) {
        list.push({
          target_id: rel.target_id,
          target_kind: rel.target_kind as RelationKind,
          target_summary: rel.target_summary ?? '',
          rel_type: rel.rel_type,
          origin: rel.origin as RelationOrigin,
          confidence: rel.confidence,
          weight: rel.weight,
        });
        relMap.set(rel.source_id, list);
      }
    }
  }

  const results: QueryResultItem[] = filtered.map(r => ({
    chunk_id:       r.chunk_id,
    chunk_summary:  r.chunk_summary ?? '',
    entity_id:      r.entity_id,
    entity_type:    r.entity_type,
    entity_summary: r.entity_summary ?? '',
    similarity:     r.similarity,
    rrf_score:      r.rrf_score,
    keyword_match:  r.fts_match || undefined,
    section_summary: include_sections ? sectionMap.get(r.chunk_id) : undefined,
    raw_store_key:  r.raw_store_key ?? '',
    relations:      relMap.get(r.chunk_id) ?? [],
  }));

  // Non-blocking lazy relation materialisation
  setImmediate(() => {
    materializeRelations(db, filtered.map(r => ({
      id: r.chunk_id,
      embedding: r.embedding,
    }))).catch(e => log.warn('Lazy relation write failed:', (e as Error).message));
  });

  if (results.length === 0) {
    const appliedThreshold = minSim;
    const nextThreshold = appliedThreshold > 0.5 ? 0.5
                        : appliedThreshold > 0.3 ? 0.3
                        : appliedThreshold > 0.1 ? 0.1
                        : null;
    return {
      results,
      hint: nextThreshold !== null
        ? `No results at threshold=${appliedThreshold.toFixed(2)} (vector) or via keyword search. Retry vkb_query with threshold=${nextThreshold} — relevant content may exist at a lower similarity score.`
        : `No results via vector or keyword search. The knowledge base may not contain content relevant to this query, or embeddings may not be ready yet.`,
    };
  }

  // When results are returned but scores are weak, signal the model so it
  // doesn't loop trying minor query variations. A rank-1 hit in one list only
  // scores 1/61 ≈ 0.016; rank-1 in both scores ≈ 0.033. Below 0.02 means
  // all results are low-ranked tail matches in a single path — likely noise.
  const maxRrf = Math.max(...results.map(r => r.rrf_score));
  const maxSim = Math.max(...results.map(r => r.similarity));
  const ftsHits = results.filter(r => r.keyword_match).length;
  if (maxRrf < 0.02 && maxSim < minSim) {
    return {
      results,
      hint: `Low-confidence results (best rrf_score=${maxRrf.toFixed(4)}, best similarity=${maxSim.toFixed(2)}, keyword_hits=${ftsHits}). These are the closest matches available but may not be directly relevant. Avoid retrying with minor query variations — instead try vkb_get on a promising entity_id, broaden to a descriptive semantic query, or accept that the VKB may not contain specific content on this topic.`,
    };
  }

  return { results };
}

async function materializeRelations(
  db: Pool,
  chunks: Array<{ id: string; embedding: string | null }>,
): Promise<void> {
  const withEmbeds = chunks.filter(c => c.embedding != null);
  if (withEmbeds.length < 2) return;

  const parsed = withEmbeds.map(c => ({
    id: c.id,
    vec: JSON.parse(c.embedding!) as number[],
  }));

  const candidates: Array<{ src: string; tgt: string; sim: number }> = [];
  for (let i = 0; i < parsed.length; i++) {
    for (let j = i + 1; j < parsed.length; j++) {
      const sim = cosineSim(parsed[i].vec, parsed[j].vec);
      if (sim >= config.RELATION_THRESHOLD) {
        candidates.push({ src: parsed[i].id, tgt: parsed[j].id, sim });
      }
    }
  }

  // Sort by sim desc, apply top-K per chunk
  candidates.sort((a, b) => b.sim - a.sim);
  const perChunk = new Map<string, number>();
  for (const { src, tgt, sim } of candidates) {
    const srcCount = perChunk.get(src) ?? 0;
    const tgtCount = perChunk.get(tgt) ?? 0;
    if (srcCount >= config.RELATION_TOP_K || tgtCount >= config.RELATION_TOP_K) continue;

    await db.query(
      `INSERT INTO relation
         (id, source_id, target_id, source_kind, target_kind, rel_type, origin, weight, confidence, last_seen_at)
       VALUES ($1,$2,$3,'chunk','chunk','semantic','semantic',$4,$4,NOW())
       ON CONFLICT (source_id, target_id, rel_type) DO UPDATE
         SET last_seen_at = NOW(),
             weight = EXCLUDED.weight,
             confidence = LEAST(1.0, relation.confidence + $5)`,
      [uuid(), src, tgt, sim, config.RELATION_CONFIDENCE_STEP],
    );

    perChunk.set(src, srcCount + 1);
    perChunk.set(tgt, tgtCount + 1);
  }
}

// ── Tool: vkb_get ─────────────────────────────────────────────────────────────
export async function handleGet(id: string, kind?: string) {
  const db = getPool();

  // Auto-detect kind
  let resolvedKind = kind;
  if (!resolvedKind) {
    const { rows } = await db.query<{ tbl: string }>(
      `SELECT 'entity' AS tbl FROM entity WHERE id = $1
       UNION ALL SELECT 'chunk' AS tbl FROM chunk WHERE id = $1 LIMIT 1`,
      [id],
    );
    resolvedKind = rows[0]?.tbl ?? 'entity';
  }

  if (resolvedKind === 'entity') {
    const { rows: entRows } = await db.query(
      `SELECT e.*, array_agg(c.id ORDER BY c.seq) FILTER (WHERE c.id IS NOT NULL) AS chunk_ids
       FROM entity e LEFT JOIN chunk c ON c.entity_id = e.id
       WHERE e.id = $1 GROUP BY e.id`,
      [id],
    );
    if (!entRows[0]) throw new Error(`Entity not found: ${id}`);

    const { rows: sections } = await db.query(
      'SELECT * FROM section_summary WHERE entity_id = $1 ORDER BY seq', [id],
    );
    const { rows: relations } = await db.query(
      'SELECT * FROM relation WHERE source_id = $1 OR target_id = $1 ORDER BY confidence DESC',
      [id],
    );

    // Tag context: co-tagged entities linked via `tag:*` relations, with their
    // summaries and the list of shared tags — lets callers avoid extra round-trips.
    const { rows: tagCtxRows } = await db.query<{
      entity_id: string; type: string; summary: string | null;
      tags: unknown; shared_tags: string[];
    }>(
      `SELECT
         e.id                                                           AS entity_id,
         e.type,
         e.summary,
         COALESCE(e.meta->'tags', e.meta->'tag', '[]'::jsonb)          AS tags,
         array_agg(DISTINCT substring(r.rel_type FROM 5))              AS shared_tags
       FROM relation r
       JOIN entity e
         ON e.id = CASE WHEN r.source_id = $1 THEN r.target_id ELSE r.source_id END
       WHERE (r.source_id = $1 OR r.target_id = $1)
         AND r.rel_type LIKE 'tag:%'
         AND r.source_kind = 'entity' AND r.target_kind = 'entity'
       GROUP BY e.id, e.type, e.summary, e.meta
       ORDER BY array_length(array_agg(DISTINCT r.rel_type), 1) DESC, e.id`,
      [id],
    );

    return { ...entRows[0], sections, relations, tag_context: tagCtxRows };
  } else {
    const { rows } = await db.query('SELECT * FROM chunk WHERE id = $1', [id]);
    if (!rows[0]) throw new Error(`Chunk not found: ${id}`);
    const { rows: relations } = await db.query(
      'SELECT * FROM relation WHERE source_id = $1 OR target_id = $1 ORDER BY confidence DESC',
      [id],
    );
    return { ...rows[0], relations };
  }
}

// ── Tool: vkb_raw ─────────────────────────────────────────────────────────────
export async function handleRaw(id: string, kind: string | undefined, adapters: Adapters) {
  const db = getPool();
  let rawKey: string | null = null;

  const resolvedKind = kind ?? 'entity';
  if (resolvedKind === 'entity') {
    const { rows } = await db.query<{ raw_store_key: string | null; meta: unknown }>(
      'SELECT raw_store_key, meta FROM entity WHERE id = $1', [id],
    );
    if (!rows[0]) throw new Error(`Entity not found: ${id}`);
    rawKey = rows[0].raw_store_key;
    if (!rawKey) throw new Error('No raw store key for this entity — still processing?');
    const text = await adapters.rawstore.read(rawKey);
    return { text, meta: rows[0].meta };
  } else {
    const { rows } = await db.query<{ raw_store_key: string | null }>(
      'SELECT raw_store_key FROM chunk WHERE id = $1', [id],
    );
    if (!rows[0]) throw new Error(`Chunk not found: ${id}`);
    rawKey = rows[0].raw_store_key;
    if (!rawKey) throw new Error('No raw store key for this chunk');
    // raw_store_key for chunks is "path/chunks.ndjson#seqIndex"
    const [ndjsonKey, idxStr] = rawKey.split('#');
    const ndjson = await adapters.rawstore.read(ndjsonKey);
    const lines = ndjson.split('\n').filter(Boolean);
    const line = JSON.parse(lines[parseInt(idxStr ?? '0', 10)]) as { text: string };
    return { text: line.text, meta: {} };
  }
}

// ── Tool: vkb_relate ─────────────────────────────────────────────────────────
export async function handleRelate(
  sourceId: string,
  targetId: string,
  relType: string,
  weight?: number,
) {
  const db = getPool();

  // Determine kinds
  async function resolveKind(id: string): Promise<RelationKind> {
    const { rows } = await db.query<{ t: string }>(
      `SELECT 'entity' AS t FROM entity WHERE id = $1
       UNION ALL SELECT 'chunk' AS t FROM chunk WHERE id = $1 LIMIT 1`,
      [id],
    );
    return (rows[0]?.t ?? 'entity') as RelationKind;
  }

  const [sourceKind, targetKind] = await Promise.all([
    resolveKind(sourceId), resolveKind(targetId),
  ]);

  // Auto-compute weight if not supplied
  let w = weight;
  if (w == null) {
    const { rows } = await db.query<{ se: string | null; te: string | null }>(
      `SELECT
         (SELECT embedding::text FROM chunk WHERE id = $1) AS se,
         (SELECT embedding::text FROM chunk WHERE id = $2) AS te`,
      [sourceId, targetId],
    );
    if (rows[0]?.se && rows[0]?.te) {
      const a = JSON.parse(rows[0].se) as number[];
      const b = JSON.parse(rows[0].te) as number[];
      w = cosineSim(a, b);
    } else {
      w = 1.0;
    }
  }

  const relId = uuid();
  await db.query(
    `INSERT INTO relation
       (id,source_id,target_id,source_kind,target_kind,rel_type,origin,weight,confidence)
     VALUES ($1,$2,$3,$4,$5,$6,'asserted',$7,1.0)
     ON CONFLICT (source_id,target_id,rel_type) DO UPDATE
       SET weight = EXCLUDED.weight, confidence = 1.0, origin = 'asserted'`,
    [relId, sourceId, targetId, sourceKind, targetKind, relType, w],
  );
  return { relation_id: relId };
}

// ── Tool: vkb_neighbors ──────────────────────────────────────────────────────
export async function handleNeighbors(
  id: string,
  hops: number = 2,
  minConfidence: number = 0.0,
  relType?: string,
  maxNodes: number = 50,
) {
  const db = getPool();

  // Build recursive CTE params. cteParams.push(x) returns new length == $N index.
  const cteParams: unknown[] = [id, hops, minConfidence, maxNodes];
  const relTypeClause = relType
    ? `AND r.rel_type = $${cteParams.push(relType)}`
    : '';

  const { rows: nodeRows } = await db.query<{ node_id: string; min_hop: number }>(
    `WITH RECURSIVE subgraph AS (
       SELECT $1::uuid AS node_id,
              0        AS hop,
              ARRAY[$1::uuid] AS visited
       UNION ALL
       SELECT
         CASE WHEN r.source_id = sg.node_id THEN r.target_id
              ELSE r.source_id END AS node_id,
         sg.hop + 1,
         sg.visited || CASE WHEN r.source_id = sg.node_id THEN r.target_id
                            ELSE r.source_id END
       FROM subgraph sg
       JOIN relation r ON (r.source_id = sg.node_id OR r.target_id = sg.node_id)
       WHERE sg.hop < $2
         AND r.confidence >= $3
         ${relTypeClause}
         AND NOT (
           CASE WHEN r.source_id = sg.node_id THEN r.target_id
                ELSE r.source_id END = ANY(sg.visited)
         )
     )
     SELECT node_id, MIN(hop) AS min_hop
     FROM subgraph
     GROUP BY node_id
     LIMIT $4`,
    cteParams,
  );

  const nodeIds = nodeRows.map(r => r.node_id);
  if (nodeIds.length === 0) return { seed_id: id, hops, nodes: [], edges: [] };

  const hopByNode = new Map(nodeRows.map(r => [r.node_id, Number(r.min_hop)]));

  const [{ rows: entityRows }, { rows: chunkRows }] = await Promise.all([
    db.query<{ id: string; type: string; summary: string | null; status: string }>(
      `SELECT id, type, summary, status FROM entity WHERE id = ANY($1::uuid[])`,
      [nodeIds],
    ),
    db.query<{ id: string; entity_id: string; seq: number; summary: string | null }>(
      `SELECT id, entity_id, seq, summary FROM chunk WHERE id = ANY($1::uuid[])`,
      [nodeIds],
    ),
  ]);

  const nodes = [
    ...entityRows.map(e => ({ ...e, kind: 'entity' as const, hop: hopByNode.get(e.id) ?? 0 })),
    ...chunkRows.map(c => ({ ...c, kind: 'chunk'  as const, hop: hopByNode.get(c.id) ?? 0 })),
  ];

  const edgeParams: unknown[] = [nodeIds, minConfidence];
  const edgeRelTypeClause = relType
    ? `AND rel_type = $${edgeParams.push(relType)}`
    : '';

  const { rows: edges } = await db.query(
    `SELECT id, source_id, target_id, source_kind, target_kind,
            rel_type, origin, weight, confidence
     FROM relation
     WHERE source_id = ANY($1::uuid[])
       AND target_id = ANY($1::uuid[])
       AND confidence >= $2
       ${edgeRelTypeClause}
     ORDER BY confidence DESC`,
    edgeParams,
  );

  return { seed_id: id, hops, nodes, edges };
}

// ── Tool: vkb_delete ─────────────────────────────────────────────────────────
export async function handleDelete(id: string, adapters: Adapters) {
  const db = getPool();

  const { rows } = await db.query<{ raw_store_key: string | null }>(
    'SELECT raw_store_key FROM entity WHERE id = $1', [id],
  );
  if (!rows[0]) throw new Error(`Entity not found: ${id}`);

  // Delete RawStore files
  if (rows[0].raw_store_key) {
    const dir = rows[0].raw_store_key.replace(/\/entity\.md$/, '');
    await adapters.rawstore.delete(dir).catch(() => {/* best-effort */});
  }

  // Delete jobs referencing this entity (FK constraint on job.entity_id)
  await db.query('DELETE FROM job WHERE entity_id = $1', [id]);

  // CASCADE handles chunks, sections, via FK
  await db.query('DELETE FROM entity WHERE id = $1', [id]);
  // Clean up relations referencing deleted chunks/entity
  await db.query('DELETE FROM relation WHERE source_id = $1 OR target_id = $1', [id]);

  return { ok: true };
}

// ── Tool: vkb_finetune ────────────────────────────────────────────────────────
/**
 * Queue a finetune job that runs LLM relation extraction and meta tag
 * enrichment against entity summaries — no re-chunking or re-embedding.
 *
 * @param entityIds - Optional list of entity IDs to scope the run. When
 *   omitted the pipeline processes all ready entities with a summary.
 * @param scope - Optional entity type filter (works alongside entityIds).
 */
export async function handleFinetune(entityIds?: string[], scope?: string) {
  const db  = getPool();
  const ttl = config.JOB_TTL_DAYS;
  const jobId = uuid();
  await db.query(
    `INSERT INTO job (id, kind, stage, progress, expires_at)
     VALUES ($1,'finetune','queued', $2::jsonb, NOW() + ($3 || ' days')::interval)`,
    [jobId, JSON.stringify({ retry_count: 0, entity_ids: entityIds ?? null, scope: scope ?? null }), ttl],
  );
  return { job_id: jobId };
}

// ── Tool: vkb_retune ─────────────────────────────────────────────────────────
export async function handleRetune(scope?: string, force?: boolean) {
  const db = getPool();

  const jobId = uuid();
  const ttl   = config.JOB_TTL_DAYS;
  await db.query(
    `INSERT INTO job (id, kind, stage, progress, expires_at)
     VALUES ($1,'retune','queued', $2::jsonb, NOW() + ($3 || ' days')::interval)`,
    [jobId, JSON.stringify({ retry_count: 0, scope, force: force ?? false }), ttl],
  );
  return { job_id: jobId };
}

// ── Tool: vkb_status ─────────────────────────────────────────────────────────
export async function handleStatus() {
  const db = getPool();

  const [
    { rows: counts },
    { rows: relOrigins },
    { rows: queue },
    { rows: lastRetune },
    { rows: idxRows  },
  ] = await Promise.all([
    db.query<{ entities: string; chunks: string; sections: string; relations: string }>(
      `SELECT
         (SELECT COUNT(*) FROM entity)          AS entities,
         (SELECT COUNT(*) FROM chunk)           AS chunks,
         (SELECT COUNT(*) FROM section_summary) AS sections,
         (SELECT COUNT(*) FROM relation)        AS relations`,
    ),
    db.query<{ origin: string; cnt: string }>(
      `SELECT origin, COUNT(*) AS cnt FROM relation GROUP BY origin`,
    ),
    db.query<{ cnt: string }>(
      `SELECT COUNT(*) AS cnt FROM job WHERE stage NOT IN ('done','error')`,
    ),
    db.query<{ completed_at: Date | null }>(
      `SELECT completed_at FROM job WHERE kind = 'retune' AND stage = 'done' ORDER BY completed_at DESC LIMIT 1`,
    ),
    db.query<{ indexname: string }>(
      `SELECT indexname FROM pg_indexes WHERE tablename = 'chunk' AND indexname = 'idx_chunk_embedding_ivfflat'`,
    ),
  ]);

  return {
    entity_count:   parseInt(counts[0]?.entities  ?? '0', 10),
    chunk_count:    parseInt(counts[0]?.chunks     ?? '0', 10),
    section_count:  parseInt(counts[0]?.sections   ?? '0', 10),
    relation_count: parseInt(counts[0]?.relations  ?? '0', 10),
    relation_by_origin: Object.fromEntries(relOrigins.map(r => [r.origin, parseInt(r.cnt, 10)])),
    queue_depth: parseInt(queue[0]?.cnt ?? '0', 10),
    last_retune:    lastRetune[0]?.completed_at ?? null,
    ivfflat_index:  idxRows.length > 0,
    embed_model:    config.EMBED_MODEL,
    config: {
      relation_threshold:   config.RELATION_THRESHOLD,
      relation_top_k:       config.RELATION_TOP_K,
      relation_ttl_days:    config.RELATION_TTL_DAYS,
      section_strategy:     config.SECTION_STRATEGY,
    },
  };
}

// ── Tool: vkb_migrate ────────────────────────────────────────────────────────
export async function handleMigrate() {
  const results: Array<{ file: string; status: 'ok' | 'error'; error?: string }> = [];

  // Wrap runMigrations so we can capture per-file results.
  // We re-implement the loop here (rather than calling runMigrations directly)
  // to collect individual file outcomes.
  const { default: fs }   = await import('fs');
  const { default: path } = await import('path');
  const { fileURLToPath } = await import('url');
  const db = getPool();

  const migrationsDir = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    '..', 'db', 'migrations',
  );
  const files = fs.readdirSync(migrationsDir).filter((f: string) => f.endsWith('.sql')).sort();

  for (const file of files) {
    try {
      const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
      await db.query(sql);
      results.push({ file, status: 'ok' });
    } catch (e) {
      results.push({ file, status: 'error', error: String(e) });
      // Stop on first failure — later migrations may depend on earlier ones.
      break;
    }
  }

  const failed = results.find(r => r.status === 'error');
  return {
    ran: results.length,
    results,
    ok: !failed,
  };
}

function cosineSim(a: number[], b: number[]): number {
  let dot = 0, na = 0, nb = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

// ── Tool: vkb_ingest_okf ─────────────────────────────────────────────────────

export interface OkfIngestResult {
  bundle_path: string;
  queued: number;
  skipped: number;
  failed: number;
  relations_asserted: number;
  documents: Array<{
    bundle_doc_path: string;
    entity_id: string;
    job_id: string | null;
    skipped?: boolean;
    error?: string;
  }>;
}

/**
 * Ingest an OKF bundle directory into vkb.
 *
 * Steps:
 *   1. Parse the bundle: walk all .md files, extract frontmatter and cross-links.
 *   2. Bulk-ingest all concept documents (dedup by file path / content hash).
 *   3. Assert OKF cross-links as `origin: asserted` relations (confidence 1.0,
 *      never pruned). The relation type is derived from the nearest section
 *      heading in the source document (e.g. a link under "# Joins" -> "okf:joins").
 *
 * @param bundlePath     Absolute or CWD-relative path to the OKF bundle root.
 * @param sourceContext  Provenance context applied to all ingested documents
 *                       (defaults to "external").
 */
export async function handleIngestOkf(
  bundlePath: string,
  sourceContext?: SourceContext,
): Promise<OkfIngestResult> {
  const absBundle = resolvePath(bundlePath);

  // 1. Parse bundle
  const docs = await parseOkfBundle(absBundle);

  // 2. Build ingest payloads
  // Use the absolute file path as `ref` so the pipeline can:
  //   (a) detect prior versions on re-ingest (file-path dedup)
  //   (b) compute a content hash for unchanged-file short-circuit
  const payloads: IngestPayload[] = docs.map(doc => {
    const fm = doc.frontmatter;
    const tags = normaliseTags(fm.tags);
    const meta: Record<string, unknown> = {
      ...fm,
      tags,
      okf_path:   doc.bundlePath,
      okf_bundle: absBundle,
    };
    return {
      type:           fm.type ?? 'okf_document',
      ref:            doc.filePath,
      source_context: sourceContext ?? 'external',
      meta,
    } satisfies IngestPayload;
  });

  // 3. Bulk ingest
  const bulk = await handleIngestBulk(payloads);

  // Build bundlePath -> entity_id map
  const pathToEntityId = new Map<string, string>();
  for (let i = 0; i < bulk.results.length; i++) {
    const r = bulk.results[i];
    if (r.entity_id) pathToEntityId.set(docs[i].bundlePath, r.entity_id);
  }

  // 4. Assert OKF cross-link relations
  // Cross-links are explicit human-authored relationship declarations.
  // Map them to `origin: asserted` vkb relations (confidence 1.0, never pruned).
  let relationsAsserted = 0;
  for (let i = 0; i < docs.length; i++) {
    const sourceEntityId = pathToEntityId.get(docs[i].bundlePath);
    if (!sourceEntityId) continue;
    for (const link of docs[i].links) {
      const targetEntityId = pathToEntityId.get(link.targetBundlePath);
      if (!targetEntityId || targetEntityId === sourceEntityId) continue;
      const relType = okfRelType(link);
      try {
        await handleRelate(sourceEntityId, targetEntityId, relType, 1.0);
        relationsAsserted++;
      } catch {
        // Non-fatal: skip if target entity failed to ingest
      }
    }
  }

  // 5. Shape response
  const documents = bulk.results.map((r, i) => ({
    bundle_doc_path: docs[i]?.bundlePath ?? '',
    entity_id:       r.entity_id,
    job_id:          r.job_id,
    skipped:         r.skipped,
    error:           r.error,
  }));

  return {
    bundle_path:        absBundle,
    queued:             bulk.queued,
    skipped:            bulk.skipped,
    failed:             bulk.failed,
    relations_asserted: relationsAsserted,
    documents,
  };
}

// ── Lint ──────────────────────────────────────────────────────────────────────

/**
 * Queue a lint job and (optionally) spawn the lint worker.
 *
 * @param checks      Subset of checks to run. Defaults to all three.
 * @param entityIds   Limit faithfulness + contradiction checks to these entities.
 */
export async function handleLint(
  checks?: Array<'orphan' | 'faithfulness' | 'contradiction'>,
  entityIds?: string[],
): Promise<{ job_id: string }> {
  const db = getPool();
  const { rows } = await db.query<{ id: string }>(
    `INSERT INTO job (kind, stage, progress, expires_at)
     VALUES ('lint', 'queued',
       $1::jsonb,
       NOW() + INTERVAL '7 days')
     RETURNING id`,
    [
      JSON.stringify({
        retry_count: 0,
        lint_checks: checks ?? null,
        lint_entity_ids: entityIds ?? null,
      }),
    ],
  );
  const jobId = rows[0].id;

  // Attempt to wake the lint worker (non-fatal if coordinator is unavailable)
  try {
    const { spawnLintWorker } = await import('../coordinator.js');
    spawnLintWorker();
  } catch {
    // Worker pool not running in this process — the lint-worker daemon will
    // pick up the job on its next poll cycle.
  }

  return { job_id: jobId };
}

export interface LintFindingsFilter {
  kind?: 'orphan' | 'unfaithful_summary' | 'contradiction';
  severity?: 'high' | 'medium' | 'low';
  status?: 'open' | 'resolved' | 'dismissed';
  entity_id?: string;
  job_id?: string;
  limit?: number;
  offset?: number;
}

/**
 * Query stored lint findings. All filter parameters are optional.
 */
export async function handleLintFindings(filter: LintFindingsFilter = {}): Promise<{
  findings: LintFinding[];
  total: number;
}> {
  const db = getPool();
  const where: string[] = [];
  const params: unknown[] = [];

  if (filter.kind) {
    params.push(filter.kind);
    where.push(`f.kind = $${params.length}`);
  }
  if (filter.severity) {
    params.push(filter.severity);
    where.push(`f.severity = $${params.length}`);
  }
  if (filter.status) {
    params.push(filter.status);
    where.push(`f.status = $${params.length}`);
  } else {
    // Default to open findings only
    where.push(`f.status = 'open'`);
  }
  if (filter.entity_id) {
    params.push(filter.entity_id);
    where.push(`(f.entity_id = $${params.length} OR f.related_entity_id = $${params.length})`);
  }
  if (filter.job_id) {
    params.push(filter.job_id);
    where.push(`f.job_id = $${params.length}`);
  }

  const whereClause = where.length ? `WHERE ${where.join(' AND ')}` : '';

  const limit  = Math.min(filter.limit  ?? 50, 200);
  const offset = filter.offset ?? 0;

  const { rows: countRows } = await db.query<{ cnt: string }>(
    `SELECT COUNT(*) AS cnt FROM lint_finding f ${whereClause}`,
    params,
  );
  const total = parseInt(countRows[0]?.cnt ?? '0', 10);

  params.push(limit, offset);
  const { rows } = await db.query<LintFinding>(
    `SELECT f.id, f.kind, f.severity, f.entity_id, f.related_entity_id,
            f.description, f.detail, f.status, f.job_id, f.created_at, f.resolved_at
     FROM lint_finding f
     ${whereClause}
     ORDER BY
       CASE f.severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
       f.created_at DESC
     LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params,
  );

  return { findings: rows, total };
}
