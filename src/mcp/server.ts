import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { randomUUID } from 'crypto';
import { createServer as createHttpsServer } from 'https';
import express from 'express';
import { loadTls } from '../tls.js';
import { z } from 'zod';
import type { Adapters } from '../adapters/registry.js';
import {
  handleIngest, handleJob, handleQuery, handleGet, handleRaw,
  handleRelate, handleDelete, handleRetune, handleStatus,
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

function wrap<T>(fn: () => Promise<T>): Promise<ToolResult> {
  return fn().then(reply).catch(replyErr);
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
  }, async (args) => wrap(() => handleIngest(args)));

  // ── vkb_job ───────────────────────────────────────────────────────────────
  server.registerTool('vkb_job', {
    description: 'Poll a job by ID. Returns current pipeline stage, progress counters, and error detail if failed.',
    inputSchema: { job_id: z.string().uuid().describe('Job ID returned by vkb_ingest or vkb_retune') },
  }, async ({ job_id }) => wrap(() => handleJob(job_id)));

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
  }, async (args) => wrap(() => handleQuery(args, adapters)));

  // ── vkb_get ───────────────────────────────────────────────────────────────
  server.registerTool('vkb_get', {
    description: 'Fetch entity or chunk by ID. Includes summaries, chunk list, sections, and all relations.',
    inputSchema: {
      id:   z.string().uuid().describe('Entity or chunk UUID'),
      kind: z.enum(['entity', 'chunk']).optional().describe('Force kind detection'),
    },
  }, async ({ id, kind }) => wrap(() => handleGet(id, kind)));

  // ── vkb_raw ───────────────────────────────────────────────────────────────
  server.registerTool('vkb_raw', {
    description: 'Fetch raw text for an entity or chunk from the RawStore (L0 — filesystem read).',
    inputSchema: {
      id:   z.string().uuid().describe('Entity or chunk UUID'),
      kind: z.enum(['entity', 'chunk']).optional(),
    },
  }, async ({ id, kind }) => wrap(() => handleRaw(id, kind, adapters)));

  // ── vkb_relate ────────────────────────────────────────────────────────────
  server.registerTool('vkb_relate', {
    description: 'Assert an explicit relation between any two entity or chunk IDs. Never pruned.',
    inputSchema: {
      source_id: z.string().uuid(),
      target_id: z.string().uuid(),
      rel_type:  z.string().describe('Relation type label'),
      weight:    z.number().min(0).max(1).optional().describe('Edge weight; auto-computed from cosine sim if omitted'),
    },
  }, async (args) => wrap(() => handleRelate(args.source_id, args.target_id, args.rel_type, args.weight)));

  // ── vkb_delete ────────────────────────────────────────────────────────────
  server.registerTool('vkb_delete', {
    description: 'Delete entity and cascade: chunks, sections, relations, RawStore files. Non-reversible.',
    inputSchema: { id: z.string().uuid().describe('Entity ID to delete') },
  }, async ({ id }) => wrap(() => handleDelete(id, adapters)));

  // ── vkb_retune ────────────────────────────────────────────────────────────
  server.registerTool('vkb_retune', {
    description: 'Trigger a retune sweep immediately. Returns retune job_id for polling.',
    inputSchema: {
      scope: z.string().optional().describe('Optional entity type filter'),
      force: z.boolean().optional().describe('Re-process all records, ignoring incremental cursor'),
    },
  }, async ({ scope, force }) => wrap(() => handleRetune(scope, force)));

  // ── vkb_status ────────────────────────────────────────────────────────────
  server.registerTool('vkb_status', {
    description: 'Full system snapshot: counts, queue depth, worker state, config, index status.',
  }, async () => wrap(() => handleStatus()));

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
