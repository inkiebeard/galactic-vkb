import express, { RequestHandler } from 'express';
import { createServer as createHttpServer } from 'http';
import { createServer as createHttpsServer } from 'https';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import { IncomingMessage } from 'http';
import { UMAP } from 'umap-js';
import { config } from '../config.js';
import { getPool, isDbConnectionError } from '../db/client.js';
import type { Adapters } from '../adapters/registry.js';
import { handleIngest, handleQuery, handleRetune, handleStatus, handleDelete, handleReingest, handleFinetune } from '../mcp/tools.js';
import { createLogger } from '../logger.js';
import { loadTls } from '../tls.js';

const log = createLogger('obs');

// Mulberry32 — fast seeded PRNG so UMAP projections are deterministic.
function seededRandom(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s += 0x6d2b79f5;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) >>> 0;
    return ((t ^ (t >>> 14)) >>> 0) / 0x100000000;
  };
}

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Error response helper ─────────────────────────────────────────────────────
function sendErr(res: import('express').Response, e: unknown, fallbackStatus = 500): void {
  if (isDbConnectionError(e)) {
    res.status(503).json({ ok: false, error: 'Database unavailable — check that PostgreSQL (Docker) is running' });
  } else {
    res.status(fallbackStatus).json({ ok: false, error: String(e) });
  }
}

// ── Auth middleware ────────────────────────────────────────────────────────────
function makeAuth(): RequestHandler {
  return (req, res, next) => {
    if (!config.OBS_SECRET) return next();
    const auth = req.headers.authorization ?? '';
    if (!auth.startsWith('Bearer ') || auth.slice(7) !== config.OBS_SECRET) {
      res.status(401).json({ ok: false, error: 'Unauthorized' });
      return;
    }
    next();
  };
}

// ── WebSocket broadcast ───────────────────────────────────────────────────────
const clients = new Set<WebSocket>();

export function broadcastEvent(event: object): void {
  const msg = JSON.stringify(event);
  for (const ws of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg, err => { if (err) clients.delete(ws); });
    }
  }
}

export function startObsServer(adapters: Adapters): void {
  const app    = express();
  const tls    = loadTls();
  const scheme = tls ? 'https' : 'http';
  const http   = tls ? createHttpsServer(tls, app) : createHttpServer(app);
  const wss    = new WebSocketServer({ server: http, path: '/stream' });
  const auth = makeAuth();
  const db   = getPool();
  // Express 5 types params as string | string[]; route params are always single strings
  const param = (v: string | string[]): string => Array.isArray(v) ? v[0] : v;

  wss.on('connection', (ws, req: IncomingMessage) => {
    // Validate auth token on WebSocket upgrade when OBS_SECRET is set.
    // Browsers cannot send custom HTTP headers during WS upgrade, so the token
    // is accepted as a query-string parameter: /stream?token=<secret>
    if (config.OBS_SECRET) {
      const url    = new URL(req.url ?? '', `http://${req.headers.host ?? 'localhost'}`);
      const token  = url.searchParams.get('token') ?? '';
      const bearer = (req.headers.authorization ?? '').replace(/^Bearer\s+/i, '');
      if (token !== config.OBS_SECRET && bearer !== config.OBS_SECRET) {
        ws.close(1008, 'Unauthorized');
        return;
      }
    }
    clients.add(ws);
    ws.on('close', () => clients.delete(ws));
    ws.on('error', () => clients.delete(ws));
  });

  app.use(express.json({ limit: '50mb' }));

  // ── Debug request/response logging ──────────────────────────────────────────
  app.use((req, res, next) => {
    log.debug(`→ ${req.method} ${req.path}`, req.body);
    const origJson = res.json.bind(res);
    res.json = function debugJson(body: unknown) {
      log.debug(`← ${req.method} ${req.path} ${res.statusCode}`, body);
      return origJson(body);
    };
    next();
  });

  // Redirect legacy paths to SPA hash routes (must be before static middleware)
  app.get('/viz',      (_req, res) => res.redirect('/#viz'));
  app.get('/viz/',     (_req, res) => res.redirect('/#viz'));
  app.get('/ingest',   (_req, res) => res.redirect('/#ingest'));
  app.get('/ingest/',  (_req, res) => res.redirect('/#ingest'));

  // Serve SPA + all static assets from public/ root
  app.use(express.static(join(__dirname, '../../public')));

  // ── Health ──────────────────────────────────────────────────────────────
  app.get('/health', auth, async (_req, res) => {
    let postgresOk = false;
    let postgresErr: string | null = null;
    try {
      await db.query('SELECT 1');
      postgresOk = true;
    } catch (e) {
      postgresErr = isDbConnectionError(e)
        ? 'Connection refused — is PostgreSQL running?'
        : String(e);
    }
    let ollamaOk = false;
    try {
      const r = await fetch(`${config.OLLAMA_BASE_URL}/api/version`);
      ollamaOk = r.ok;
    } catch { /* unreachable */ }
    if (!postgresOk) {
      res.status(503).json({ ok: false, error: postgresErr, postgres: false, ollama: ollamaOk });
    } else {
      res.json({ ok: true, uptime: process.uptime(), postgres: true, ollama: ollamaOk });
    }
  });

  // ── Status ──────────────────────────────────────────────────────────────
  app.get('/status', auth, async (_req, res) => {
    try {
      const data = await handleStatus();
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e); }
  });

  // ── Entities ─────────────────────────────────────────────────────────────
  app.get('/entities', auth, async (req, res) => {
    try {
      const { type, status, source_context, q, limit = '50', offset = '0', from, to } = req.query as Record<string, string>;
      const params: unknown[] = [];
      const where: string[] = [];

      if (type)           { params.push(type);           where.push(`type = $${params.length}`); }
      if (status)         { params.push(status);         where.push(`status = $${params.length}`); }
      if (source_context) { params.push(source_context); where.push(`source_context = $${params.length}`); }
      if (from)           { params.push(from);           where.push(`created_at >= $${params.length}`); }
      if (to)             { params.push(to);             where.push(`created_at <= $${params.length}`); }
      if (q) {
        params.push(q);
        where.push(`to_tsvector('english', COALESCE(summary,'')) @@ plainto_tsquery('english', $${params.length})`);
      }

      const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
      const lim  = Math.min(200, parseInt(limit,  10) || 50);
      const off  = Math.max(0,    parseInt(offset, 10) || 0);

      const { rows: entities } = await db.query(
        `SELECT id, type, ref, source_context, summary, status, created_at, updated_at, meta,
                (SELECT COUNT(*) FROM chunk           WHERE entity_id = e.id)::int AS chunk_count,
                (SELECT COUNT(*) FROM section_summary WHERE entity_id = e.id)::int AS section_count
         FROM entity e ${whereClause} ORDER BY created_at DESC LIMIT $${params.length+1} OFFSET $${params.length+2}`,
        [...params, lim, off],
      );
      const { rows: cnt } = await db.query<{ cnt: string }>(
        `SELECT COUNT(*) AS cnt FROM entity e ${whereClause}`, params,
      );
      res.json({ ok: true, data: { entities, total: parseInt(cnt[0]?.cnt ?? '0', 10) } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Broken entities ───────────────────────────────────────────────────────
  // Must be registered BEFORE /entities/:id so Express doesn't treat 'broken'
  // as a UUID parameter. Returns all non-ready entities annotated with their
  // latest job state and a remediation hint: 'reingest' (raw committed),
  // 'no_raw' (source lost), or 'stuck' (active job but stalled).
  app.get('/entities/broken', auth, async (_req, res) => {
    try {
      const { rows } = await db.query<{
        id: string; type: string; ref: string | null; status: string;
        raw_store_key: string | null; meta: Record<string, unknown>;
        created_at: Date; updated_at: Date;
        job_id: string | null; job_stage: string | null;
        job_error: string | null; job_created_at: Date | null;
        chunk_count: string;
      }>(`
        SELECT
          e.id, e.type, e.ref, e.status, e.raw_store_key, e.meta,
          e.created_at, e.updated_at,
          j.id          AS job_id,
          j.stage       AS job_stage,
          j.progress->>'error_detail' AS job_error,
          j.created_at  AS job_created_at,
          (SELECT COUNT(*) FROM chunk WHERE entity_id = e.id)::text AS chunk_count
        FROM entity e
        LEFT JOIN LATERAL (
          SELECT id, stage, progress, created_at
          FROM job
          WHERE entity_id = e.id
          ORDER BY created_at DESC
          LIMIT 1
        ) j ON true
        WHERE e.status != 'ready'
        ORDER BY e.updated_at DESC
      `);

      const broken = rows.map(r => {
        const chunks = parseInt(r.chunk_count, 10);
        let remedy: 'reingest' | 'no_raw' | 'stuck';
        if (r.raw_store_key) {
          remedy = r.job_stage && !['done', 'error'].includes(r.job_stage) ? 'stuck' : 'reingest';
        } else {
          remedy = 'no_raw';
        }
        return {
          id: r.id, type: r.type, ref: r.ref, status: r.status,
          has_raw: !!r.raw_store_key, meta: r.meta,
          created_at: r.created_at, updated_at: r.updated_at,
          chunk_count: chunks,
          latest_job: r.job_id ? {
            id: r.job_id, stage: r.job_stage, error: r.job_error, created_at: r.job_created_at,
          } : null,
          remedy,
        };
      });

      res.json({ ok: true, data: { broken, total: broken.length } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Entities: UMAP projection ──────────────────────────────────────────────
  // Mean-pools each entity's chunk embeddings into a single vector, then
  // projects entity centroid vectors from N-dim → 3-dim via UMAP.
  app.get('/entities/projection', auth, async (req, res) => {
    try {
      // Fetch all chunk embeddings grouped by entity, plus entity metadata
      const { rows } = await db.query<{
        entity_id: string; type: string; status: string; ref: string | null;
        meta: Record<string, unknown>; summary: string | null;
        embedding: string;
      }>(
        `SELECT c.entity_id, e.type, e.status, e.ref, e.meta, e.summary,
                c.embedding::text AS embedding
         FROM chunk c
         JOIN entity e ON e.id = c.entity_id
         WHERE c.embedding IS NOT NULL
         ORDER BY c.entity_id`,
      );

      if (!rows.length) {
        res.json({ ok: true, data: { points: [] } });
        return;
      }

      // Group embeddings by entity and compute centroid (mean pool)
      const entityMap = new Map<string, {
        type: string; status: string; ref: string | null;
        meta: Record<string, unknown>; summary: string | null;
        sum: number[]; count: number;
      }>();

      for (const r of rows) {
        const vec = JSON.parse(r.embedding as unknown as string) as number[];
        const entry = entityMap.get(r.entity_id);
        if (entry) {
          for (let i = 0; i < vec.length; i++) entry.sum[i] += vec[i];
          entry.count++;
        } else {
          entityMap.set(r.entity_id, {
            type: r.type, status: r.status, ref: r.ref,
            meta: r.meta, summary: r.summary,
            sum: vec.slice(), count: 1,
          });
        }
      }

      const entityIds = [...entityMap.keys()];
      const vectors   = entityIds.map(id => {
        const e = entityMap.get(id)!;
        return e.sum.map(v => v / e.count);
      });

      const nNeighbors = Math.min(15, vectors.length - 1);
      const umap = new UMAP({ nComponents: 3, nNeighbors, minDist: 0.1, nEpochs: 200, random: seededRandom(0x564b4221) });
      const embedding3d = await umap.fitAsync(vectors);

      // Normalise to ±300 units
      const coords = embedding3d as number[][];
      let minV = Infinity, maxV = -Infinity;
      for (const pt of coords) for (const v of pt) { if (v < minV) minV = v; if (v > maxV) maxV = v; }
      const range = maxV - minV || 1;
      const scale = 600 / range;

      const points = entityIds.map((id, i) => {
        const e = entityMap.get(id)!;
        return {
          id,
          type:    e.type,
          status:  e.status,
          ref:     e.ref,
          meta:    e.meta,
          summary: e.summary,
          x: (coords[i][0] - minV - range / 2) * scale,
          y: (coords[i][1] - minV - range / 2) * scale,
          z: (coords[i][2] - minV - range / 2) * scale,
        };
      });

      res.json({ ok: true, data: { points } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Entity detail ─────────────────────────────────────────────────────────
  app.get('/entities/:id', auth, async (req, res) => {
    try {
      const id = param(req.params.id);
      const { rows: ent } = await db.query('SELECT * FROM entity WHERE id = $1', [id]);
      if (!ent[0]) return void res.status(404).json({ ok: false, error: 'Not found' });
      const { rows: chunks }   = await db.query('SELECT id,seq,summary,embed_model,embedded_at FROM chunk WHERE entity_id = $1 ORDER BY seq', [id]);
      const { rows: sections } = await db.query('SELECT * FROM section_summary WHERE entity_id = $1 ORDER BY seq', [id]);
      const { rows: relations } = await db.query(
        'SELECT * FROM relation WHERE source_id = $1 OR target_id = $1 ORDER BY confidence DESC', [id],
      );
      res.json({ ok: true, data: { ...ent[0], chunks, sections, relations } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Entity raw ────────────────────────────────────────────────────────────
  app.get('/entities/:id/raw', auth, async (req, res) => {
    try {
      const { rows } = await db.query<{ raw_store_key: string | null }>('SELECT raw_store_key FROM entity WHERE id = $1', [param(req.params.id)]);
      if (!rows[0]) return void res.status(404).json({ ok: false, error: 'Not found' });
      if (!rows[0].raw_store_key) return void res.status(404).json({ ok: false, error: 'No raw file' });
      const text = await adapters.rawstore.read(rows[0].raw_store_key);
      res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
      res.send(text);
    } catch (e) { sendErr(res, e); }
  });

  // ── Delete entity ─────────────────────────────────────────────────────────
  app.delete('/entities/:id', auth, async (req, res) => {
    try {
      const data = await handleDelete(param(req.params.id), adapters);
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e); }
  });

  // ── Bulk entity actions ───────────────────────────────────────────────────
  // POST /entities/bulk-action { ids: string[], action: 'delete' | 'reingest' | 'reingest_force' }
  app.post('/entities/bulk-action', auth, async (req, res) => {
    try {
      const { ids, action } = (req.body ?? {}) as { ids?: unknown; action?: unknown };
      if (!Array.isArray(ids) || ids.length === 0) {
        return void res.status(400).json({ ok: false, error: '"ids" must be a non-empty array' });
      }
      if (!['delete', 'reingest', 'reingest_force', 'finetune'].includes(action as string)) {
        return void res.status(400).json({ ok: false, error: '"action" must be delete, reingest, reingest_force, or finetune' });
      }
      const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      const invalid = (ids as unknown[]).filter(id => typeof id !== 'string' || !uuidRe.test(id));
      if (invalid.length > 0) {
        return void res.status(400).json({ ok: false, error: `Invalid UUIDs: ${invalid.join(', ')}` });
      }
      const entityIds = ids as string[];
      const results: Array<{ id: string; ok: boolean; job_id?: string; error?: string }> = [];

      if (action === 'delete') {
        for (const id of entityIds) {
          try {
            await handleDelete(id, adapters);
            results.push({ id, ok: true });
          } catch (e) {
            results.push({ id, ok: false, error: String(e) });
          }
        }
      } else if (action === 'finetune') {
        // One job for all selected entities — pipeline handles them together
        try {
          const data = await handleFinetune(entityIds);
          // Return the single job_id paired with each id so the UI can register it
          for (const id of entityIds) {
            results.push({ id, ok: true, job_id: data.job_id });
          }
        } catch (e) {
          for (const id of entityIds) {
            results.push({ id, ok: false, error: String(e) });
          }
        }
      } else {
        const force = action === 'reingest_force';
        for (const id of entityIds) {
          try {
            const data = await handleReingest(id, force);
            results.push({ id, ok: true, job_id: data.jobs?.[0]?.job_id });
          } catch (e) {
            results.push({ id, ok: false, error: String(e) });
          }
        }
      }

      const failed = results.filter(r => !r.ok).length;
      res.json({ ok: true, data: { results, succeeded: results.length - failed, failed } });
    } catch (e) { sendErr(res, e, 400); }
  });

  // ── Chunks: UMAP projection ──────────────────────────────────────────────
  // Returns { id, entity_id, seq, summary, x, y, z } for all embedded chunks.
  // Embeddings are projected from N-dim → 3-dim via UMAP so spatial proximity
  // reflects semantic similarity.
  app.get('/chunks/projection', auth, async (req, res) => {
    try {
      const { rows } = await db.query<{
        id: string; entity_id: string; seq: number; summary: string | null;
        entity_type: string; entity_ref: string | null; entity_meta: Record<string, unknown>;
        embedding: string;
      }>(
        `SELECT c.id, c.entity_id, c.seq, c.summary,
                e.type AS entity_type, e.ref AS entity_ref, e.meta AS entity_meta,
                c.embedding::text AS embedding
         FROM chunk c
         JOIN entity e ON e.id = c.entity_id
         WHERE c.embedding IS NOT NULL
         ORDER BY c.entity_id, c.seq
         LIMIT 5000`,
      );

      if (!rows.length) {
        res.json({ ok: true, data: { points: [] } });
        return;
      }

      // Parse pgvector text representation "[0.1,0.2,...]" → number[]
      const vectors = rows.map(r => {
        const s = r.embedding as unknown as string;
        return JSON.parse(s.replace(/^\[/, '[').replace(/\]$/, ']')) as number[];
      });

      const nNeighbors = Math.min(15, vectors.length - 1);
      const umap = new UMAP({ nComponents: 3, nNeighbors, minDist: 0.1, nEpochs: 200, random: seededRandom(0x564b4221) });
      const embedding3d = await umap.fitAsync(vectors);

      // Normalise to roughly ±300 units (same scale as entity view)
      const coords = embedding3d as number[][];
      let minV = Infinity, maxV = -Infinity;
      for (const pt of coords) for (const v of pt) { if (v < minV) minV = v; if (v > maxV) maxV = v; }
      const range = maxV - minV || 1;
      const scale = 600 / range;

      const points = rows.map((r, i) => ({
        id:          r.id,
        entity_id:   r.entity_id,
        seq:         r.seq,
        summary:     r.summary,
        entity_type: r.entity_type,
        entity_ref:  r.entity_ref,
        entity_meta: r.entity_meta,
        x: (coords[i][0] - minV - range / 2) * scale,
        y: (coords[i][1] - minV - range / 2) * scale,
        z: (coords[i][2] - minV - range / 2) * scale,
      }));

      res.json({ ok: true, data: { points } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Chunks (list) ─────────────────────────────────────────────────────────
  app.get('/chunks', auth, async (req, res) => {
    try {
      const { entity_id, limit = '500', offset = '0' } = req.query as Record<string, string>;
      const params: unknown[] = [];
      const where: string[] = [];
      if (entity_id) { params.push(entity_id); where.push(`c.entity_id = $${params.length}`); }
      const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
      const lim = Math.min(2000, parseInt(limit, 10) || 500);
      const off = Math.max(0, parseInt(offset, 10) || 0);
      const { rows } = await db.query(
        `SELECT c.id, c.entity_id, c.seq, c.summary,
                e.type AS entity_type, e.ref AS entity_ref, e.meta AS entity_meta
         FROM chunk c
         JOIN entity e ON e.id = c.entity_id
         ${whereClause}
         ORDER BY c.entity_id, c.seq
         LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
        [...params, lim, off],
      );
      const { rows: cnt } = await db.query<{ cnt: string }>(
        `SELECT COUNT(*) AS cnt FROM chunk c JOIN entity e ON e.id = c.entity_id ${whereClause}`,
        params,
      );
      res.json({ ok: true, data: { chunks: rows, total: parseInt(cnt[0]?.cnt ?? '0', 10) } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Chunks ────────────────────────────────────────────────────────────────
  app.get('/chunks/:id', auth, async (req, res) => {
    try {
      const { rows } = await db.query('SELECT id,entity_id,seq,summary,embed_model,embedded_at,raw_store_key FROM chunk WHERE id = $1', [param(req.params.id)]);
      if (!rows[0]) return void res.status(404).json({ ok: false, error: 'Not found' });
      const { rows: relations } = await db.query(
        'SELECT * FROM relation WHERE source_id = $1 OR target_id = $1 ORDER BY confidence DESC', [param(req.params.id)],
      );
      res.json({ ok: true, data: { ...rows[0], relations } });
    } catch (e) { sendErr(res, e); }
  });

  app.get('/chunks/:id/raw', auth, async (req, res) => {
    try {
      const { rows } = await db.query<{ raw_store_key: string | null }>('SELECT raw_store_key FROM chunk WHERE id = $1', [param(req.params.id)]);
      if (!rows[0]) return void res.status(404).json({ ok: false, error: 'Not found' });
      if (!rows[0].raw_store_key) return void res.status(404).json({ ok: false, error: 'No raw file' });
      const [ndjsonKey, idxStr] = rows[0].raw_store_key.split('#');
      const ndjson = await adapters.rawstore.read(ndjsonKey);
      const lines  = ndjson.split('\n').filter(Boolean);
      const line   = JSON.parse(lines[parseInt(idxStr ?? '0', 10)]) as { text: string };
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.send(line.text);
    } catch (e) { sendErr(res, e); }
  });

  // ── Relations ─────────────────────────────────────────────────────────────
  app.get('/relations', auth, async (req, res) => {
    try {
      const { origin, rel_type, min_confidence, min_weight, source_kind, limit = '50', offset = '0' } = req.query as Record<string, string>;
      const params: unknown[] = [];
      const where: string[] = [];

      if (origin)         { params.push(origin);              where.push(`origin = $${params.length}`); }
      if (rel_type)       { params.push(rel_type);            where.push(`rel_type = $${params.length}`); }
      if (min_confidence) { params.push(min_confidence);      where.push(`confidence >= $${params.length}`); }
      if (min_weight)     { params.push(min_weight);          where.push(`weight >= $${params.length}`); }
      if (source_kind)    { params.push(source_kind);         where.push(`source_kind = $${params.length}`); }

      const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
      const lim = Math.min(500, parseInt(limit, 10) || 50);
      const off = Math.max(0,   parseInt(offset, 10) || 0);

      const { rows: relations } = await db.query(
        `SELECT * FROM relation ${whereClause} ORDER BY confidence DESC LIMIT $${params.length+1} OFFSET $${params.length+2}`,
        [...params, lim, off],
      );
      res.json({ ok: true, data: { relations } });
    } catch (e) { sendErr(res, e); }
  });

  // ── Jobs ──────────────────────────────────────────────────────────────────
  app.get('/jobs', auth, async (req, res) => {
    try {
      const { kind, stage, limit = '50' } = req.query as Record<string, string>;
      const params: unknown[] = [];
      const where: string[] = [];
      if (kind)  { params.push(kind);  where.push(`j.kind = $${params.length}`); }
      if (stage) { params.push(stage); where.push(`j.stage = $${params.length}`); }
      const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
      const lim = Math.min(200, parseInt(limit, 10) || 50);
      const { rows } = await db.query(
        `SELECT j.*, e.ref, e.meta FROM job j
         LEFT JOIN entity e ON e.id = j.entity_id
         ${whereClause} ORDER BY j.created_at DESC LIMIT $${params.length+1}`,
        [...params, lim],
      );
      res.json({ ok: true, data: { jobs: rows } });
    } catch (e) { sendErr(res, e); }
  });

  // ── HTTP ingest/query/retune ──────────────────────────────────────────────
  app.post('/ingest', auth, async (req, res) => {
    try {
      if (!req.body || typeof req.body !== 'object') {
        res.status(400).json({ ok: false, error: 'Request body is missing — ensure Content-Type is application/json' });
        return;
      }
      const data = await handleIngest(req.body);
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e, 400); }
  });

  app.post('/query', auth, async (req, res) => {
    try {
      const data = await handleQuery(req.body, adapters);
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e, 400); }
  });

  app.post('/retune', auth, async (req, res) => {
    try {
      const { scope, force } = req.body as { scope?: string; force?: boolean };
      const data = await handleRetune(scope, force);
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e, 400); }
  });
  app.post('/reingest', auth, async (req, res) => {
    try {
      const { entity_id, force } = (req.body ?? {}) as { entity_id?: string; force?: boolean };
      const data = await handleReingest(entity_id, force === true);
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e, 400); }
  });

  app.post('/finetune', auth, async (req, res) => {
    try {
      const { entity_ids, scope } = (req.body ?? {}) as { entity_ids?: string[]; scope?: string };
      const data = await handleFinetune(entity_ids, scope);
      res.json({ ok: true, data });
    } catch (e) { sendErr(res, e, 400); }
  });

  http.listen(config.OBS_PORT, () => {
    log.info(`${scheme.toUpperCase()} + WebSocket listening on :${config.OBS_PORT}`);
    log.info(`App UI:     ${scheme}://localhost:${config.OBS_PORT}/`);
  });
}
