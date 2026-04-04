import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { randomUUID } from 'crypto';
import { createServer as createHttpsServer } from 'https';
import express from 'express';
import { loadTls } from '../tls.js';
import { z } from 'zod';
import { config } from '../config.js';
import type { Adapters } from '../adapters/registry.js';
import {
  handleIngest, handleJob, handleQuery, handleGet, handleRaw,
  handleRelate, handleNeighbors, handleDelete, handleRetune, handleStatus,
} from './tools.js';
import { createLogger, setMcpLogTarget } from '../logger.js';

const log = createLogger('mcp');

// ── Envelope helpers ──────────────────────────────────────────────────────────
type ToolResult = { content: [{ type: 'text'; text: string }] };

function reply<T>(data: T): ToolResult {
  return { content: [{ type: 'text', text: JSON.stringify({ ok: true, data }) }] };
}
function replyErr(e: unknown): ToolResult {
  return { content: [{ type: 'text', text: JSON.stringify({ ok: false, error: String(e) }) }] };
}

function wrap<T>(name: string, args: unknown, fn: () => Promise<T>): Promise<ToolResult> {
  log.debug(`→ ${name}`, args);
  return fn()
    .then(data => { log.debug(`← ${name} ok`); return reply(data); })
    .catch(e  => { log.debug(`← ${name} error`, String(e)); return replyErr(e); });
}

export function createMcpServer(adapters: Adapters): McpServer {
  const server = new McpServer(
    { name: 'vkb', version: '0.1.0' },
    { capabilities: { logging: {} } },
  );

  // ── vkb_ingest ────────────────────────────────────────────────────────────
  server.registerTool('vkb_ingest', {
    description: 'Submit raw text or a URL/file path for ingestion. Returns job_id and entity_id immediately. Pipeline runs in background.',
    inputSchema: {
      type: z.string().describe('Entity type label (doc, url, note, code, etc.)'),
      text: z.string().optional().describe('Inline text content'),
      ref:  z.string().optional().describe('URL or file path to fetch'),
      meta: z.record(z.string(), z.any()).optional().describe('Arbitrary key-value metadata'),
    },
  }, async (args) => wrap('vkb_ingest', args, () => handleIngest(args)));

  // ── vkb_job ───────────────────────────────────────────────────────────────
  server.registerTool('vkb_job', {
    description: 'Poll a job by ID. Returns current pipeline stage, progress counters, and error detail if failed.',
    inputSchema: { job_id: z.string().uuid().describe('Job ID returned by vkb_ingest or vkb_retune') },
  }, async ({ job_id }) => wrap('vkb_job', { job_id }, () => handleJob(job_id)));

  // ── vkb_query ─────────────────────────────────────────────────────────────
  server.registerTool('vkb_query', {
    description: 'Semantic search. Returns top-k chunks with summaries, entity context, similarity scores, and relations.',
    inputSchema: {
      text:             z.string().describe('Query text — embedded on the fly'),
      k:                z.number().int().positive().optional().describe('Max results (default 10)'),
      type:             z.string().optional().describe('Filter by entity type'),
      threshold:        z.number().min(0).max(1).optional().describe('Minimum cosine similarity'),
      include_sections: z.boolean().optional().describe('Include L2 section summaries in results'),
    },
  }, async (args) => wrap('vkb_query', args, () => handleQuery(args, adapters)));

  // ── vkb_get ───────────────────────────────────────────────────────────────
  server.registerTool('vkb_get', {
    description: 'Fetch entity or chunk by ID. Includes summaries, chunk list, sections, and all relations.',
    inputSchema: {
      id:   z.string().uuid().describe('Entity or chunk UUID'),
      kind: z.enum(['entity', 'chunk']).optional().describe('Force kind detection'),
    },
  }, async ({ id, kind }) => wrap('vkb_get', { id, kind }, () => handleGet(id, kind)));

  // ── vkb_raw ───────────────────────────────────────────────────────────────
  server.registerTool('vkb_raw', {
    description: 'Fetch raw text for an entity or chunk from the RawStore (L0 — filesystem read).',
    inputSchema: {
      id:   z.string().uuid().describe('Entity or chunk UUID'),
      kind: z.enum(['entity', 'chunk']).optional(),
    },
  }, async ({ id, kind }) => wrap('vkb_raw', { id, kind }, () => handleRaw(id, kind, adapters)));

  // ── vkb_relate ────────────────────────────────────────────────────────────
  server.registerTool('vkb_relate', {
    description: 'Assert an explicit relation between any two entity or chunk IDs. Never pruned.',
    inputSchema: {
      source_id: z.string().uuid(),
      target_id: z.string().uuid(),
      rel_type:  z.string().describe('Relation type label'),
      weight:    z.number().min(0).max(1).optional().describe('Edge weight; auto-computed from cosine sim if omitted'),
    },
  }, async (args) => wrap('vkb_relate', args, () => handleRelate(args.source_id, args.target_id, args.rel_type, args.weight)));

  // ── vkb_neighbors ──────────────────────────────────────────────────────────
  server.registerTool('vkb_neighbors', {
    description: 'Retrieve an N-hop relation subgraph starting from a given entity or chunk ID. Returns all reachable nodes (with kind, summary, and hop distance) and the edges between them.',
    inputSchema: {
      id:             z.string().uuid().describe('Seed entity or chunk UUID'),
      hops:           z.number().int().min(1).max(5).optional().describe('Number of hops to traverse (default 2, max 5)'),
      min_confidence: z.number().min(0).max(1).optional().describe('Minimum edge confidence to follow (default 0)'),
      rel_type:       z.string().optional().describe('Only traverse edges of this relation type'),
      max_nodes:      z.number().int().positive().optional().describe('Cap on total nodes returned (default 50)'),
    },
  }, async ({ id, hops, min_confidence, rel_type, max_nodes }) =>
    wrap('vkb_neighbors', { id, hops, min_confidence, rel_type, max_nodes }, () => handleNeighbors(id, hops ?? 2, min_confidence ?? 0.0, rel_type, max_nodes ?? 50))
  );

  // ── vkb_delete ────────────────────────────────────────────────────────────
  server.registerTool('vkb_delete', {
    description: 'Delete entity and cascade: chunks, sections, relations, RawStore files. Non-reversible.',
    inputSchema: { id: z.string().uuid().describe('Entity ID to delete') },
  }, async ({ id }) => wrap('vkb_delete', { id }, () => handleDelete(id, adapters)));

  // ── vkb_retune ────────────────────────────────────────────────────────────
  server.registerTool('vkb_retune', {
    description: 'Trigger a retune sweep immediately. Returns retune job_id for polling.',
    inputSchema: {
      scope: z.string().optional().describe('Optional entity type filter'),
      force: z.boolean().optional().describe('Re-process all records, ignoring incremental cursor'),
    },
  }, async ({ scope, force }) => wrap('vkb_retune', { scope, force }, () => handleRetune(scope, force)));

  // ── vkb_status ────────────────────────────────────────────────────────────
  server.registerTool('vkb_status', {
    description: 'Full system snapshot: counts, queue depth, worker state, config, index status.',
  }, async () => wrap('vkb_status', {}, () => handleStatus()));

  return server;
}

export async function startMcpStdio(adapters: Adapters): Promise<void> {
  const server = createMcpServer(adapters);
  const transport = new StdioServerTransport();
  await server.connect(transport);
  setMcpLogTarget(server);
  log.info('Listening on stdio');
}

export async function startMcpHttp(adapters: Adapters, port: number): Promise<void> {
  const app = express();
  app.use(express.json());

  // ── Optional Bearer-token auth ─────────────────────────────────────────
  // Set MCP_SECRET to require a token on all /mcp requests. When unset,
  // access is unrestricted (suitable for localhost-only deployments).
  app.use('/mcp', (req, res, next) => {
    if (!config.MCP_SECRET) return next();
    const auth = req.headers.authorization ?? '';
    if (!auth.startsWith('Bearer ') || auth.slice(7) !== config.MCP_SECRET) {
      res.status(401).json({ error: 'Unauthorized' });
      return;
    }
    next();
  });

  const sessions = new Map<string, StreamableHTTPServerTransport>();

  app.all('/mcp', async (req, res) => {
    try {
      const sessionId = req.headers['mcp-session-id'] as string | undefined;
      if (sessionId && sessions.has(sessionId)) {
        await sessions.get(sessionId)!.handleRequest(req, res, req.body);
        return;
      }
      if (req.method !== 'POST') {
        res.status(400).json({ error: 'No active session — send POST to initialize.' });
        return;
      }
      const transport: StreamableHTTPServerTransport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
        onsessioninitialized: (sid) => { sessions.set(sid, transport); },
        onsessionclosed: (sid) => { sessions.delete(sid); },
      });
      const server = createMcpServer(adapters);
      await server.connect(transport);
      setMcpLogTarget(server);
      await transport.handleRequest(req, res, req.body);
    } catch (e) {
      if (!res.headersSent) res.status(500).json({ error: String(e) });
    }
  });

  const tls    = loadTls();
  const scheme = tls ? 'https' : 'http';
  const server = tls ? createHttpsServer(tls, app) : app;

  await new Promise<void>((resolve) => {
    server.listen(port, () => {
      log.info(`MCP ${scheme.toUpperCase()} listening on :${port}/mcp`);
      resolve();
    });
  });
}
