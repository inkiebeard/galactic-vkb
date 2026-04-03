import { Pool } from 'pg';
import { config } from '../config.js';
import { createLogger } from '../logger.js';

const log = createLogger('vkb');

const TRACKED_KEYS = [
  'embed_model',
  'embed_dim',
  'chunk_size',
  'chunk_overlap',
  'section_strategy',
] as const;

type TrackedKey = typeof TRACKED_KEYS[number];

function currentValues(): Record<TrackedKey, string> {
  return {
    embed_model: config.EMBED_MODEL,
    embed_dim: String(config.EMBED_DIM),
    chunk_size: String(config.CHUNK_SIZE),
    chunk_overlap: String(config.CHUNK_OVERLAP),
    section_strategy: config.SECTION_STRATEGY,
  };
}

export async function detectAndHandleDrift(db: Pool): Promise<void> {
  const current = currentValues();

  // Read stored values
  const { rows } = await db.query<{ key: string; value: string }>(
    'SELECT key, value FROM config_state WHERE key = ANY($1)',
    [TRACKED_KEYS as unknown as string[]],
  );
  const stored = new Map(rows.map(r => [r.key, r.value]));

  const drifted: TrackedKey[] = [];
  for (const key of TRACKED_KEYS) {
    const prev = stored.get(key);
    if (prev !== undefined && prev !== current[key]) {
      drifted.push(key);
    }
  }

  // Upsert current values (only update updated_at when value actually changed)
  for (const key of TRACKED_KEYS) {
    await db.query(
      `INSERT INTO config_state (key, value, updated_at) VALUES ($1, $2, NOW())
       ON CONFLICT (key) DO UPDATE
         SET value = EXCLUDED.value,
             updated_at = CASE
               WHEN config_state.value IS DISTINCT FROM EXCLUDED.value THEN NOW()
               ELSE config_state.updated_at
             END`,
      [key, current[key]],
    );
  }

  if (drifted.length === 0) return;

  const needsReEmbed = drifted.some(k => k === 'embed_model' || k === 'embed_dim');
  const needsReChunk = drifted.some(
    k => k === 'chunk_size' || k === 'chunk_overlap' || k === 'section_strategy',
  );

  const { rows: chunkCount } = await db.query<{ cnt: string }>('SELECT COUNT(*) AS cnt FROM chunk');
  const { rows: entityCount } = await db.query<{ cnt: string }>('SELECT COUNT(*) AS cnt FROM entity');

  log.warn('⚠  Config drift detected:', drifted.join(', '));
  log.warn(`   Affected: ${entityCount[0].cnt} entities, ${chunkCount[0].cnt} chunks`);
  if (needsReEmbed) {
    log.warn('   embed_model/embed_dim changed → chunks need re-embedding');
    log.warn('   Run: vkb_retune with force:true, or wait for scheduled retune');
  }
  if (needsReChunk) {
    log.warn('   chunk/section config changed → entities need full re-chunk + re-section + re-embed');
    log.warn('   Run: vkb_retune with force:true to rebuild affected entities');
  }
}
