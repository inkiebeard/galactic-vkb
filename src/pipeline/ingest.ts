import * as yaml from 'js-yaml';
import { Pool } from 'pg';
import { v4 as uuid } from 'uuid';
import { config } from '../config.js';
import { getPool, serializeVector } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import { entityFilePath, chunksFilePath } from '../adapters/rawstore/filesystem.js';
import { prompts } from './prompts.js';
import type { JobProgress, JobStage, Section } from '../types.js';
import { createLogger } from '../logger.js';

const log = createLogger('ingest');

// ── Helpers ───────────────────────────────────────────────────────────────────

function emit(msg: object) {
  if (process.send) process.send(msg);
}

async function setStage(db: Pool, jobId: string, stage: JobStage, extra?: Partial<JobProgress>) {
  emit({ type: 'stage_change', job_id: jobId, stage });
  if (extra) {
    await db.query(
      `UPDATE job SET stage = $1, progress = progress || $2::jsonb WHERE id = $3`,
      [stage, JSON.stringify(extra), jobId],
    );
  } else {
    await db.query('UPDATE job SET stage = $1 WHERE id = $2', [stage, jobId]);
  }
}

async function patchProgress(db: Pool, jobId: string, patch: Partial<JobProgress>) {
  await db.query(
    `UPDATE job SET progress = progress || $1::jsonb WHERE id = $2`,
    [JSON.stringify(patch), jobId],
  );
}

// ── Main pipeline ─────────────────────────────────────────────────────────────

export async function runIngestPipeline(
  jobId: string,
  adapters: Adapters,
): Promise<void> {
  const db = getPool();

  // Load job + entity
  const { rows: jobRows } = await db.query<{ entity_id: string; progress: JobProgress }>(
    'SELECT entity_id, progress FROM job WHERE id = $1',
    [jobId],
  );
  if (!jobRows[0]) throw new Error(`Job not found: ${jobId}`);
  const entityId = jobRows[0].entity_id!;

  const { rows: entityRows } = await db.query<{
    type: string; ref: string | null; meta: Record<string, unknown>; raw_store_key: string | null;
  }>('SELECT type, ref, meta, raw_store_key FROM entity WHERE id = $1', [entityId]);
  if (!entityRows[0]) throw new Error(`Entity not found: ${entityId}`);
  const entity = entityRows[0];

  try {
    await db.query(`UPDATE entity SET status = 'processing', updated_at = NOW() WHERE id = $1`, [entityId]);

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 1 — PERSIST
    // Acquire raw text → write entity.md → commit raw_store_key to DB.
    // Nothing else runs until this checkpoint is durable. Once raw_store_key
    // is set, any later failure is self-healable via reingest from raw.
    // ════════════════════════════════════════════════════════════════════════
    await setStage(db, jobId, 'fetching');

    let rawText: string;

    if (entity.raw_store_key) {
      // Previous attempt already committed entity.md — reuse it directly.
      // This covers retries after any Phase 2/3 failure (OOM, LLM error, etc.)
      log.info(`Job ${jobId}: raw already committed at ${entity.raw_store_key}, skipping fetch`);
      const entityMd = await adapters.rawstore.read(entity.raw_store_key);
      rawText = entityMd.replace(/^---\n[\s\S]*?\n---\n\n?/, '');
    } else {
      // Step 1 — Acquire raw text
      if (entity.ref) {
        rawText = await adapters.fetch.fetch(entity.ref);
      } else {
        // Inline text was stashed in staging by handleIngest
        const stageKey = `staging/${entityId}.txt`;
        rawText = await adapters.rawstore.read(stageKey);
      }

      // Step 2 — Write entity.md to permanent rawstore (single converged path)
      const frontmatter = yaml.dump({
        vkb_version:    '0.4',
        entity_id:      entityId,
        type:           entity.type,
        ref:            entity.ref ?? null,
        ingested_at:    new Date().toISOString(),
        normalisation:  { strategy: entity.ref?.startsWith('http') ? 'readability' : 'passthrough' },
        chunk_config:   { strategy: 'sliding_window', size: config.CHUNK_SIZE, overlap: config.CHUNK_OVERLAP },
        section_config: { strategy: config.SECTION_STRATEGY, threshold: config.SECTION_SPLIT_THRESHOLD, max_size: config.SECTION_MAX_SIZE },
        embed_model:    config.EMBED_MODEL,
        meta:           entity.meta ?? {},
      });
      const entityKey = entityFilePath(entityId);
      await adapters.rawstore.write(entityKey, `---\n${frontmatter}---\n\n${rawText}`);

      // Step 3 — Commit raw_store_key. After this line the entity is self-healable.
      await db.query(
        `UPDATE entity SET raw_store_key = $1, updated_at = NOW() WHERE id = $2`,
        [entityKey, entityId],
      );

      // Step 4 — Delete staging file only after the canonical copy is durable
      if (!entity.ref) {
        const stageKey = `staging/${entityId}.txt`;
        await adapters.rawstore.delete(stageKey).catch(() => { /* already gone is fine */ });
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 2 — DERIVE
    // All steps are deterministic and cheap. Clear any stale artifacts from a
    // previous failed attempt first so every run is fully idempotent.
    // ════════════════════════════════════════════════════════════════════════
    await setStage(db, jobId, 'chunking');

    // Step 5 — Clear stale derived artifacts before regenerating
    await db.query(`DELETE FROM section_summary WHERE entity_id = $1`, [entityId]);
    await db.query(
      `DELETE FROM relation WHERE (source_id = $1 OR target_id = $1) AND source_kind = 'entity'`,
      [entityId],
    );
    await db.query(`DELETE FROM chunk WHERE entity_id = $1`, [entityId]);
    await adapters.rawstore.delete(chunksFilePath(entityId)).catch(() => { /* absent is fine */ });

    // Step 6 — Chunk
    const rawChunks = adapters.chunk.chunk(rawText, { size: config.CHUNK_SIZE, overlap: config.CHUNK_OVERLAP });
    await patchProgress(db, jobId, { chunks_total: rawChunks.length });

    const chunkIds: string[] = rawChunks.map(() => uuid());
    const ndjsonLines = rawChunks.map((c, i) =>
      JSON.stringify({ chunk_id: chunkIds[i], entity_id: entityId, seq: c.seq, text: c.text }),
    );
    const chunksKey = chunksFilePath(entityId);
    await adapters.rawstore.write(chunksKey, ndjsonLines.join('\n'));

    for (let i = 0; i < rawChunks.length; i++) {
      await db.query(
        `INSERT INTO chunk (id, entity_id, seq, raw_store_key) VALUES ($1, $2, $3, $4)`,
        [chunkIds[i], entityId, rawChunks[i].seq, `${chunksKey}#${i}`],
      );
    }

    // Sequential chunk→chunk relations
    for (let i = 1; i < chunkIds.length; i++) {
      await db.query(
        `INSERT INTO relation (id, source_id, target_id, source_kind, target_kind, rel_type, origin, weight, confidence)
         VALUES ($1,$2,$3,'chunk','chunk','sequential','content_heuristic',1.0,1.0)
         ON CONFLICT (source_id, target_id, rel_type) DO NOTHING`,
        [uuid(), chunkIds[i - 1], chunkIds[i]],
      );
    }

    // Step 7 — Embed
    await setStage(db, jobId, 'embedding');

    const chunkTexts = rawChunks.map(c => c.text);
    const embeddings = await adapters.embed.embed(chunkTexts);

    for (let i = 0; i < chunkIds.length; i++) {
      await db.query(
        `UPDATE chunk SET embedding = $1::vector, embed_model = $2, embed_version = 1, embedded_at = NOW()
         WHERE id = $3`,
        [serializeVector(embeddings[i]), config.EMBED_MODEL, chunkIds[i]],
      );
      await patchProgress(db, jobId, { chunks_done: i + 1 });
    }

    // Signal if ivfflat index is now warranted
    const { rows: countRows } = await db.query<{ cnt: string }>('SELECT COUNT(*) AS cnt FROM chunk');
    if (parseInt(countRows[0].cnt, 10) > config.IVFFLAT_THRESHOLD) {
      emit({ type: 'progress', job_id: jobId, payload: { needs_ivfflat: true } });
    }

    // Step 8 — Section
    await setStage(db, jobId, 'sectioning');

    const orderedChunks = chunkIds.map((id, i) => ({
      id,
      seq:       rawChunks[i].seq,
      text:      rawChunks[i].text,
      embedding: embeddings[i],
    }));

    const sections: Section[] = adapters.section.section(orderedChunks, {
      threshold:  config.SECTION_SPLIT_THRESHOLD,
      windowSize: config.SECTION_WINDOW_SIZE,
      maxSize:    config.SECTION_MAX_SIZE,
    });

    const sectionIds: string[] = [];
    for (const sec of sections) {
      const sid = uuid();
      sectionIds.push(sid);
      await db.query(
        `INSERT INTO section_summary (id, entity_id, chunk_ids, seq, summary, strategy)
         VALUES ($1,$2,$3,$4,'',$5)`,
        [sid, entityId, sec.chunk_ids, sec.seq, config.SECTION_STRATEGY],
      );
    }
    await patchProgress(db, jobId, { sections_done: sections.length });

    // ════════════════════════════════════════════════════════════════════════
    // PHASE 3 — INTELLIGENCE
    // LLM-heavy work. Failures here leave raw + chunks intact so a retry can
    // resume from Phase 2 cleanup without re-fetching or re-chunking.
    // ════════════════════════════════════════════════════════════════════════
    await setStage(db, jobId, 'summarising');

    // Step 9 — Summarise: L1 chunk → L2 section → L3 entity
    for (let i = 0; i < rawChunks.length; i++) {
      const summary = await adapters.llm.complete(prompts.chunkSummary, rawChunks[i].text);
      await db.query('UPDATE chunk SET summary = $1 WHERE id = $2', [summary.trim(), chunkIds[i]]);
    }

    const { rows: chunkSumRows } = await db.query<{ id: string; summary: string }>(
      'SELECT id, summary FROM chunk WHERE entity_id = $1 ORDER BY seq',
      [entityId],
    );
    const chunkSumMap = new Map(chunkSumRows.map(r => [r.id, r.summary ?? '']));

    for (let i = 0; i < sections.length; i++) {
      const sectionText = sections[i].chunk_ids.map(cid => chunkSumMap.get(cid) ?? '').join('\n\n');
      const secSummary = await adapters.llm.complete(prompts.sectionSummary, sectionText);
      await db.query('UPDATE section_summary SET summary = $1 WHERE id = $2', [secSummary.trim(), sectionIds[i]]);
    }

    const { rows: secSumRows } = await db.query<{ summary: string }>(
      'SELECT summary FROM section_summary WHERE entity_id = $1 ORDER BY seq',
      [entityId],
    );
    const entitySummary = await adapters.llm.complete(
      prompts.entitySummary,
      secSumRows.map(r => r.summary).join('\n\n'),
    );
    await db.query(
      `UPDATE entity SET summary = $1, summary_version = summary_version + 1, updated_at = NOW() WHERE id = $2`,
      [entitySummary.trim(), entityId],
    );

    // Step 10 — Extract relations
    await setStage(db, jobId, 'extracting');

    const { rows: allEntityRefs } = await db.query<{ id: string; ref: string | null }>(
      `SELECT id, ref FROM entity WHERE id != $1 AND status = 'ready'`,
      [entityId],
    );
    const heuristicRelations = await adapters.heuristicExtractor.extract(
      entityId, rawText, allEntityRefs,
    );

    let relationsAdded = 0;
    for (const rel of heuristicRelations) {
      await db.query(
        `INSERT INTO relation (id,source_id,target_id,source_kind,target_kind,rel_type,origin,weight,confidence)
         VALUES ($1,$2,$3,'entity','entity',$4,'content_heuristic',1.0,1.0)
         ON CONFLICT (source_id,target_id,rel_type) DO NOTHING`,
        [uuid(), entityId, rel.target_entity_id, rel.rel_type],
      );
      relationsAdded++;
    }

    if (config.LLM_RELATION_EXTRACTION && allEntityRefs.length > 0) {
      const { rows: candidates } = await db.query<{ id: string; summary: string }>(
        `SELECT e.id, e.summary
         FROM entity e
         WHERE e.id != $1 AND e.status = 'ready' AND e.summary IS NOT NULL
         ORDER BY (
           SELECT MIN(c.embedding <=> (
             SELECT embedding FROM chunk WHERE entity_id = $1 ORDER BY seq LIMIT 1
           ))
           FROM chunk c WHERE c.entity_id = e.id
         ) ASC
         LIMIT $2`,
        [entityId, config.LLM_EXTRACT_CANDIDATES],
      );

      if (candidates.length > 0) {
        const llmRelations = await adapters.llmExtractor.extract(entitySummary.trim(), candidates);
        for (const rel of llmRelations) {
          await db.query(
            `INSERT INTO relation (id,source_id,target_id,source_kind,target_kind,rel_type,origin,weight,confidence)
             VALUES ($1,$2,$3,'entity','entity',$4,'content_llm',1.0,$5)
             ON CONFLICT (source_id,target_id,rel_type) DO UPDATE
               SET confidence = GREATEST(relation.confidence, EXCLUDED.confidence)`,
            [uuid(), entityId, rel.target_entity_id, rel.rel_type, rel.confidence],
          );
          relationsAdded++;
        }
      }
    }
    await patchProgress(db, jobId, { relations_added: relationsAdded });

    // Step 11 — Finalise
    await db.query(`UPDATE entity SET status = 'ready', updated_at = NOW() WHERE id = $1`, [entityId]);
    await db.query(`UPDATE job SET stage = 'done', completed_at = NOW() WHERE id = $1`, [jobId]);
    emit({ type: 'complete', job_id: jobId });
    log.info(`Job ${jobId} complete (entity ${entityId})`);

  } catch (err) {
    const msg = (err as Error).message;
    log.error(`Job ${jobId} error:`, msg);

    await db.query(
      `UPDATE job
       SET stage = CASE
             WHEN (progress->>'retry_count')::int >= $1 THEN 'error'
             ELSE 'queued'
           END,
           worker_pid = NULL,
           progress = progress
             || jsonb_build_object('retry_count', (progress->>'retry_count')::int + 1)
             || jsonb_build_object('error_detail', $2::text),
           completed_at = CASE
             WHEN (progress->>'retry_count')::int >= $1 THEN NOW()
             ELSE NULL
           END
       WHERE id = $3`,
      [config.INGEST_MAX_RETRIES - 1, msg, jobId],
    );
    emit({ type: 'error', job_id: jobId, payload: msg });
  }
}
