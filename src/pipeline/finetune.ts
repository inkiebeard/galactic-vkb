/**
 * Finetune pipeline — per-entity relationship extraction + meta tag enrichment.
 *
 * Unlike retune (which re-embeds stale chunks and re-weights semantic relations
 * across the whole KB), finetune operates on a specific set of entities:
 *
 *   Stage 1 — linking:  LLM relation extraction against semantically similar
 *                        entities, upserted as content_llm relations.
 *   Stage 2 — tagging:  LLM extracts keyword tags from the entity summary,
 *                        merges them into meta.tags, and asserts tag:* relations
 *                        to other entities that share the same tag.
 *
 * Progress fields stored in job.progress:
 *   entity_ids?: string[]   — if null/absent, processes all ready entities
 *   scope?: string          — optional entity type filter (combined with entity_ids)
 *   total: number
 *   linking_done: number
 *   tagging_done: number
 */
import { v4 as uuid } from 'uuid';
import { Pool } from 'pg';
import { config } from '../config.js';
import { getPool } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import { prompts } from './prompts.js';
import { createLogger } from '../logger.js';
import { pMap } from '../util/pmap.js';

const log = createLogger('finetune');

function emit(msg: object) {
  if (process.send) process.send(msg);
}

async function patchProgress(db: Pool, jobId: string, patch: object) {
  await db.query(
    `UPDATE job SET progress = progress || $1::jsonb WHERE id = $2`,
    [JSON.stringify(patch), jobId],
  );
}

export async function runFinetunePipeline(jobId: string, adapters: Adapters): Promise<void> {
  const db = getPool();

  try {
    const { rows: jobRows } = await db.query<{
      progress: { entity_ids?: string[] | null; scope?: string | null; retry_count: number };
    }>(
      'SELECT progress FROM job WHERE id = $1',
      [jobId],
    );
    if (!jobRows[0]) throw new Error(`Job not found: ${jobId}`);
    const { entity_ids, scope } = jobRows[0].progress;

    // ── Build entity list ────────────────────────────────────────────────────
    const where: string[] = ["e.status = 'ready'", 'e.summary IS NOT NULL'];
    const params: unknown[] = [];
    if (entity_ids?.length) {
      params.push(entity_ids);
      where.push(`e.id = ANY($${params.length}::uuid[])`);
    }
    if (scope) {
      params.push(scope);
      where.push(`e.type = $${params.length}`);
    }

    const { rows: entities } = await db.query<{
      id: string; summary: string; meta: Record<string, unknown>;
    }>(
      `SELECT e.id, e.summary, e.meta FROM entity e WHERE ${where.join(' AND ')} ORDER BY e.created_at`,
      params,
    );

    if (entities.length === 0) {
      await db.query(`UPDATE job SET stage = 'done', completed_at = NOW() WHERE id = $1`, [jobId]);
      emit({ type: 'complete', job_id: jobId });
      log.info(`Finetune job ${jobId}: no entities to process, done immediately`);
      return;
    }

    await patchProgress(db, jobId, { total: entities.length, linking_done: 0, tagging_done: 0 });

    // ════════════════════════════════════════════════════════════════════════
    // STAGE 1 — LINKING
    // For each entity, find its top-N semantically nearest neighbours (by
    // embedding cosine distance on first chunk) and run LLM relation
    // extraction against their summaries. Upserts as content_llm relations.
    // ════════════════════════════════════════════════════════════════════════
    emit({ type: 'stage_change', job_id: jobId, stage: 'extracting' });
    await db.query(`UPDATE job SET stage = 'extracting' WHERE id = $1`, [jobId]);

    let linkingDone = 0;
    await pMap(entities, config.SUMMARY_CONCURRENCY, async (entity) => {
      try {
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
          [entity.id, config.LLM_EXTRACT_CANDIDATES],
        );

        if (candidates.length > 0) {
          const llmRelations = await adapters.llmExtractor.extract(entity.summary, candidates);
          for (const rel of llmRelations) {
            await db.query(
              `INSERT INTO relation
                 (id,source_id,target_id,source_kind,target_kind,rel_type,origin,weight,confidence)
               VALUES ($1,$2,$3,'entity','entity',$4,'content_llm',1.0,$5)
               ON CONFLICT (source_id,target_id,rel_type) DO UPDATE
                 SET confidence = GREATEST(relation.confidence, EXCLUDED.confidence),
                     last_seen_at = NOW()`,
              [uuid(), entity.id, rel.target_entity_id, rel.rel_type, rel.confidence],
            );
          }
        }
      } catch (e) {
        log.warn(`Linking failed for entity ${entity.id}:`, (e as Error).message);
      }
      linkingDone++;
      if (linkingDone % 5 === 0 || linkingDone === entities.length) {
        await patchProgress(db, jobId, { linking_done: linkingDone });
      }
    });

    // ════════════════════════════════════════════════════════════════════════
    // STAGE 2 — TAGGING
    // LLM extracts keyword tags from each entity summary. New tags are merged
    // into meta.tags (deduped, lowercase). After merging, tag:* relations are
    // asserted between this entity and all others that share the same tag.
    // ════════════════════════════════════════════════════════════════════════
    emit({ type: 'stage_change', job_id: jobId, stage: 'summarising' });
    await db.query(`UPDATE job SET stage = 'summarising' WHERE id = $1`, [jobId]);

    let taggingDone = 0;
    await pMap(entities, config.SUMMARY_CONCURRENCY, async (entity) => {
      try {
        const rawResponse = await adapters.llm.complete(prompts.metaTagExtract, entity.summary);
        let newTags: string[] = [];
        try {
          const parsed = JSON.parse(rawResponse.trim()) as unknown;
          if (Array.isArray(parsed)) {
            newTags = (parsed as unknown[])
              .filter((t): t is string => typeof t === 'string' && t.trim().length > 0)
              .map(t => t.trim().toLowerCase())
              .slice(0, 8);
          }
        } catch {
          // LLM didn't return valid JSON — skip
        }

        if (newTags.length > 0) {
          // Read fresh meta (another pMap slot may have already updated this entity)
          const { rows: metaRows } = await db.query<{ meta: Record<string, unknown> }>(
            'SELECT meta FROM entity WHERE id = $1',
            [entity.id],
          );
          const currentMeta = metaRows[0]?.meta ?? {};
          const currentTags = Array.isArray(currentMeta.tags) ? (currentMeta.tags as string[]) : [];
          const mergedTags = [...new Set([...currentTags, ...newTags])];

          await db.query(
            `UPDATE entity
             SET meta = jsonb_set(meta, '{tags}', $1::jsonb), updated_at = NOW()
             WHERE id = $2`,
            [JSON.stringify(mergedTags), entity.id],
          );

          // Assert tag:* entity relations for any genuinely new tags
          const addedTags = newTags.filter(t => !currentTags.includes(t));
          for (const tag of addedTags) {
            const { rows: taggedEntities } = await db.query<{ id: string }>(
              `SELECT id FROM entity
               WHERE id != $1 AND status = 'ready'
                 AND (
                   (meta ? 'tags' AND meta->'tags' @> jsonb_build_array($2::text))
                   OR (meta ? 'tag'  AND meta->'tag'  @> jsonb_build_array($2::text))
                 )`,
              [entity.id, tag],
            );
            for (const tagged of taggedEntities) {
              await db.query(
                `INSERT INTO relation
                   (id,source_id,target_id,source_kind,target_kind,rel_type,origin,weight,confidence)
                 VALUES ($1,$2,$3,'entity','entity',$4,'content_heuristic',1.0,1.0)
                 ON CONFLICT (source_id,target_id,rel_type) DO NOTHING`,
                [uuid(), entity.id, tagged.id, `tag:${tag}`],
              );
            }
          }
        }
      } catch (e) {
        log.warn(`Tagging failed for entity ${entity.id}:`, (e as Error).message);
      }
      taggingDone++;
      if (taggingDone % 5 === 0 || taggingDone === entities.length) {
        await patchProgress(db, jobId, { tagging_done: taggingDone });
      }
    });

    await db.query(`UPDATE job SET stage = 'done', completed_at = NOW() WHERE id = $1`, [jobId]);
    emit({ type: 'complete', job_id: jobId });
    log.info(`Finetune job ${jobId} complete (${entities.length} entities)`);

  } catch (err) {
    const msg = (err as Error).message;
    log.error(`Finetune job ${jobId} error:`, msg);
    await db.query(
      `UPDATE job SET stage = 'error', completed_at = NOW(),
       progress = progress || jsonb_build_object('error_detail', $1::text)
       WHERE id = $2`,
      [msg, jobId],
    );
    emit({ type: 'error', job_id: jobId, payload: msg });
  }
}
