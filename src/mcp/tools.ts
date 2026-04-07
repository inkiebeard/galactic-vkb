import { Pool } from 'pg';
import { v4 as uuid } from 'uuid';
import { config } from '../config.js';
import { getPool, serializeVector } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import type {
  IngestPayload, QueryPayload, QueryResultItem,
  RelationRef, RelationKind, RelationOrigin,
} from '../types.js';
import { createLogger } from '../logger.js';

const log = createLogger('query');

// ── Envelope helper ───────────────────────────────────────────────────────────
function ok<T>(data: T)  { return { ok: true  as const, data }; }
function err(e: unknown) { return { ok: false as const, error: String(e) }; }

// ── Tool: vkb_ingest ──────────────────────────────────────────────────────────
export async function handleIngest(payload: IngestPayload) {
  if (!payload || typeof payload !== 'object') throw new Error('Request body is missing or not JSON');
  const db = getPool();
  const { type, text, ref, meta = {} } = payload;
  if (!type) throw new Error('Field "type" is required (e.g. "url", "doc", "note")');
  if (!text && !ref) throw new Error('At least one of text or ref is required');

  const entityId = uuid();
  const jobId    = uuid();
  const ttl      = config.JOB_TTL_DAYS;

  await db.query(
    `INSERT INTO entity (id, type, ref, meta, status) VALUES ($1,$2,$3,$4,'pending')`,
    [entityId, type, ref ?? null, JSON.stringify(meta)],
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

  return { job_id: jobId, entity_id: entityId };
}

// ── Tool: vkb_reingest ───────────────────────────────────────────────────
export async function handleReingest(entityId?: string) {
  const db = getPool();
  const ttl = config.JOB_TTL_DAYS;

  const { rows: entities } = await db.query<{ id: string }>(    
    entityId
      ? `SELECT id FROM entity WHERE id = $1 AND raw_store_key IS NOT NULL`
      : `SELECT id FROM entity WHERE raw_store_key IS NOT NULL`,
    entityId ? [entityId] : [],
  );

  if (entities.length === 0) {
    throw new Error(entityId ? `Entity ${entityId} not found or has no rawstore data` : 'No entities with rawstore data found');
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
    // Clear existing processing artifacts (chunks cascade to sections via FK)
    await db.query(`DELETE FROM chunk WHERE entity_id = $1`, [entity.id]);
    // Clear entity-level relations
    await db.query(
      `DELETE FROM relation WHERE (source_id = $1 OR target_id = $1) AND source_kind = 'entity'`,
      [entity.id],
    );
    // Reset entity
    await db.query(
      `UPDATE entity SET status = 'pending', summary = NULL, summary_version = 0, updated_at = NOW() WHERE id = $1`,
      [entity.id],
    );
    // Queue new ingest job with from_rawstore flag
    await db.query(
      `INSERT INTO job (id, entity_id, kind, stage, progress, expires_at)
       VALUES ($1, $2, 'ingest', 'queued', '{"retry_count":0,"from_rawstore":true}', NOW() + ($3 || ' days')::interval)`,
      [jobId, entity.id, ttl],
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

  let entityFilter = '';
  const params: unknown[] = [vec, k];
  if (type) {
    params.push(type);
    entityFilter = `AND e.type = $${params.length}`;
  }

  const { rows } = await db.query<{
    chunk_id: string; chunk_summary: string | null; similarity: number;
    entity_id: string; entity_type: string; entity_summary: string | null;
    raw_store_key: string | null;
    embedding: string | null;
  }>(
    `SELECT c.id AS chunk_id, c.summary AS chunk_summary,
            1 - (c.embedding <=> $1::vector) AS similarity,
            e.id AS entity_id, e.type AS entity_type, e.summary AS entity_summary,
            c.raw_store_key,
            c.embedding::text AS embedding
     FROM chunk c
     JOIN entity e ON e.id = c.entity_id
     WHERE c.embedding IS NOT NULL AND e.status = 'ready' ${entityFilter}
     ORDER BY c.embedding <=> $1::vector
     LIMIT $2`,
    params,
  );

  const filtered = rows.filter(r => r.similarity >= minSim);

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
        ? `No results at threshold=${appliedThreshold.toFixed(2)}. Retry vkb_query with threshold=${nextThreshold} — relevant content may exist at a lower similarity score.`
        : `No results even at threshold=${appliedThreshold.toFixed(2)}. The knowledge base may not contain content relevant to this query, or embeddings may not be ready yet.`,
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
    return { ...entRows[0], sections, relations };
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

function cosineSim(a: number[], b: number[]): number {
  let dot = 0, na = 0, nb = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}
