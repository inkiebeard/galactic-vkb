import { Pool, PoolClient } from 'pg';
import { v4 as uuid } from 'uuid';
import { config } from '../config.js';
import { getPool, serializeVector } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import { prompts } from './prompts.js';
import { createLogger } from '../logger.js';

const log = createLogger('retune');

function emit(msg: object) {
  if (process.send) process.send(msg);
}

async function patchProgress(db: Pool, jobId: string, patch: object) {
  await db.query(
    `UPDATE job SET progress = progress || $1::jsonb WHERE id = $2`,
    [JSON.stringify(patch), jobId],
  );
}

export async function runRetunePipeline(
  jobId: string,
  adapters: Adapters,
  opts: { scope?: string; force?: boolean } = {},
): Promise<void> {
  const db = getPool();

  try {
    await db.query(`UPDATE job SET stage = 'queued' WHERE id = $1`, [jobId]);

    // ── 1. Re-embed chunks with stale embed_model ─────────────────────────
    emit({ type: 'stage_change', job_id: jobId, stage: 're-embedding' });
    await db.query(`UPDATE job SET stage = 'embedding' WHERE id = $1`, [jobId]);

    const staleParams: unknown[] = [];
    const staleWhere: string[] = [];

    if (!opts.force) {
      staleParams.push(config.EMBED_MODEL);
      staleWhere.push(`embed_model IS DISTINCT FROM $${staleParams.length}`);
    }
    if (opts.scope) {
      staleParams.push(opts.scope);
      staleWhere.push(`e.type = $${staleParams.length}`);
    }
    const staleWhereClause = staleWhere.length ? 'WHERE ' + staleWhere.join(' AND ') : '';

    const { rows: staleChunks } = await db.query<{ id: string; raw_store_key: string }>(
      `SELECT c.id, c.raw_store_key FROM chunk c
       JOIN entity e ON e.id = c.entity_id
       ${staleWhereClause}`,
      staleParams,
    );

    let reEmbedCount = 0;
    for (const chunk of staleChunks) {
      try {
        // Read chunk text from RawStore
        const ndjsonKey = chunk.raw_store_key?.split('#')[0];
        const seqIdx = parseInt(chunk.raw_store_key?.split('#')[1] ?? '0', 10);
        if (!ndjsonKey) continue;

        const ndjson = await adapters.rawstore.read(ndjsonKey);
        const lines = ndjson.split('\n').filter(Boolean);
        const lineData = JSON.parse(lines[seqIdx] ?? lines[0]) as { text: string };

        const [embedding] = await adapters.embed.embed([lineData.text]);
        await db.query(
          `UPDATE chunk SET embedding = $1::vector, embed_model = $2, embed_version = embed_version + 1, embedded_at = NOW()
           WHERE id = $3`,
          [serializeVector(embedding), config.EMBED_MODEL, chunk.id],
        );
        reEmbedCount++;
      } catch (e) {
        log.warn(`Failed to re-embed chunk ${chunk.id}:`, (e as Error).message);
      }
    }
    await patchProgress(db, jobId, { re_embedded: reEmbedCount });

    // ── 2. Re-weight existing semantic relations ───────────────────────────
    emit({ type: 'stage_change', job_id: jobId, stage: 're-weighting' });

    const { rows: semanticRels } = await db.query<{
      id: string; source_id: string; target_id: string; confidence: number;
    }>(
      `SELECT id, source_id, target_id, confidence FROM relation
       WHERE origin = 'semantic'`,
    );

    let reweightCount = 0;
    for (const rel of semanticRels) {
      const { rows: embedRows } = await db.query<{ embedding: string | null }>(
        `SELECT embedding::text FROM chunk WHERE id = $1 OR id = $2`,
        [rel.source_id, rel.target_id],
      );
      if (embedRows.length < 2 || !embedRows[0].embedding || !embedRows[1].embedding) continue;

      const a = JSON.parse(embedRows[0].embedding) as number[];
      const b = JSON.parse(embedRows[1].embedding) as number[];
      const newWeight = cosineSim(a, b);

      let newConfidence = rel.confidence;
      if (newWeight < config.RELATION_THRESHOLD) {
        newConfidence = Math.max(0, rel.confidence - config.RELATION_CONFIDENCE_STEP);
      }

      await db.query(
        `UPDATE relation SET weight = $1, confidence = $2 WHERE id = $3`,
        [newWeight, newConfidence, rel.id],
      );
      reweightCount++;
    }
    await patchProgress(db, jobId, { reweighted: reweightCount });

    // ── 3. Prune stale semantic relations ─────────────────────────────────
    emit({ type: 'stage_change', job_id: jobId, stage: 'pruning' });

    const { rows: pruned } = await db.query<{ cnt: string }>(
      `WITH deleted AS (
         DELETE FROM relation
         WHERE origin = 'semantic'
           AND last_seen_at < NOW() - ($1 || ' days')::interval
           AND confidence < $2
         RETURNING id
       ) SELECT COUNT(*) AS cnt FROM deleted`,
      [config.RELATION_TTL_DAYS, config.RELATION_PRUNE_THRESHOLD],
    );
    await patchProgress(db, jobId, { pruned: parseInt(pruned[0]?.cnt ?? '0', 10) });

    // ── 4. Re-summarise (opt-in) ──────────────────────────────────────────
    if (config.RETUNE_SUMMARISE && reEmbedCount > 0) {
      emit({ type: 'stage_change', job_id: jobId, stage: 'summarising' });

      const { rows: affectedEntities } = await db.query<{ id: string }>(
        `SELECT DISTINCT entity_id AS id FROM chunk WHERE embedded_at > (
           SELECT COALESCE(MAX(completed_at), '2000-01-01') FROM job
           WHERE kind = 'retune' AND stage = 'done'
         )`,
      );

      for (const { id: eid } of affectedEntities) {
        try {
          await regenerateSummaries(db, eid, adapters);
        } catch (e) {
          log.warn(`Summary regen failed for entity ${eid}:`, (e as Error).message);
        }
      }
    }

    // ── 5. Manage ivfflat index ───────────────────────────────────────────
    emit({ type: 'stage_change', job_id: jobId, stage: 'indexing' });
    await manageIvfflat(db);

    // ── 6. Job TTL cleanup ────────────────────────────────────────────────
    emit({ type: 'stage_change', job_id: jobId, stage: 'cleaning' });

    await db.query(
      `DELETE FROM job WHERE expires_at < NOW() AND stage IN ('done','error') AND id != $1`,
      [jobId],
    );

    // Finalise
    await db.query(
      `UPDATE job SET stage = 'done', completed_at = NOW() WHERE id = $1`,
      [jobId],
    );
    emit({ type: 'complete', job_id: jobId });
    log.info(`Job ${jobId} complete`);

  } catch (err) {
    const msg = (err as Error).message;
    log.error('Error:', msg);
    await db.query(
      `UPDATE job SET stage = 'error', completed_at = NOW(),
       progress = progress || jsonb_build_object('error_detail', $1::text)
       WHERE id = $2`,
      [msg, jobId],
    );
    emit({ type: 'error', job_id: jobId, payload: msg });
  }
}

async function regenerateSummaries(db: Pool, entityId: string, adapters: Adapters): Promise<void> {
  const { rows: chunks } = await db.query<{ id: string; summary: string | null }>(
    'SELECT id, summary FROM chunk WHERE entity_id = $1 ORDER BY seq',
    [entityId],
  );

  // L2: section summaries
  const { rows: sections } = await db.query<{ id: string; chunk_ids: string[] }>(
    'SELECT id, chunk_ids FROM section_summary WHERE entity_id = $1 ORDER BY seq',
    [entityId],
  );
  const chunkSumMap = new Map(chunks.map(c => [c.id, c.summary ?? '']));

  for (const sec of sections) {
    const text = sec.chunk_ids.map(cid => chunkSumMap.get(cid) ?? '').join('\n\n');
    const summary = await adapters.llm.complete(prompts.sectionSummary, text);
    await db.query('UPDATE section_summary SET summary = $1 WHERE id = $2', [summary.trim(), sec.id]);
  }

  // L3: entity summary
  const { rows: secRows } = await db.query<{ summary: string }>(
    'SELECT summary FROM section_summary WHERE entity_id = $1 ORDER BY seq',
    [entityId],
  );
  const allSecSummaries = secRows.map(r => r.summary).join('\n\n');
  const entitySummary = await adapters.llm.complete(prompts.entitySummary, allSecSummaries);
  await db.query(
    `UPDATE entity SET summary = $1, summary_version = summary_version + 1, updated_at = NOW() WHERE id = $2`,
    [entitySummary.trim(), entityId],
  );
}

async function manageIvfflat(db: Pool): Promise<void> {
  const { rows } = await db.query<{ cnt: string }>('SELECT COUNT(*) AS cnt FROM chunk WHERE embedding IS NOT NULL');
  const count = parseInt(rows[0]?.cnt ?? '0', 10);

  if (count < config.IVFFLAT_THRESHOLD) return;

  // Check if index already exists
  const { rows: idxRows } = await db.query<{ indexname: string }>(
    `SELECT indexname FROM pg_indexes WHERE tablename = 'chunk' AND indexname = 'idx_chunk_embedding_ivfflat'`,
  );

  if (idxRows.length === 0) {
    log.info(`Creating ivfflat index (${count} vectors, lists=${config.IVFFLAT_LISTS})…`);
    // CREATE INDEX CONCURRENTLY must run outside a transaction block
    await db.query(
      `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chunk_embedding_ivfflat
       ON chunk USING ivfflat (embedding vector_cosine_ops)
       WITH (lists = ${config.IVFFLAT_LISTS})`,
    );
    log.info('ivfflat index created');
  } else {
    // Keep planner stats fresh
    await db.query('VACUUM ANALYZE chunk');
  }
}

function cosineSim(a: number[], b: number[]): number {
  let dot = 0, na = 0, nb = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}
