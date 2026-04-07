#!/usr/bin/env node
/**
 * Standalone MCP query debugger — zero dependencies, no codebase imports.
 * Works in two modes:
 *
 *   HTTP mode  (MCP_PORT > 0): connects to the running vkb HTTP MCP service.
 *   stdio mode (MCP_PORT = 0): spawns a fresh child process and talks MCP over
 *                              its stdin/stdout. Retries tool calls automatically
 *                              until the process finishes initialising or times out.
 *
 * Usage:
 *   node scripts/mcp-query.mjs <query text> [--port=3333] [--timeout=60] [--k=10]
 *                                            [--type=TYPE] [--threshold=0.75]
 *                                            [--sections] [--secret=TOKEN]
 *                                            [--cmd="node dist/index.js"]
 *
 * Port/secret/cmd defaults are read from .env in the current directory if present.
 * --cmd only applies to stdio mode; defaults to "node dist/index.js".
 *
 * Examples:
 *   node scripts/mcp-query.mjs "machine learning"
 *   node scripts/mcp-query.mjs "neural nets" --port=3333 --k=5 --threshold=0.6
 *   node scripts/mcp-query.mjs "auth flow"   --type=doc --sections
 *   node scripts/mcp-query.mjs "auth flow"   --port=0   --cmd="npx tsx src/index.ts"
 */

import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';

// ── .env loader (no dotenv dependency) ───────────────────────────────────────
function loadEnv(dir) {
  const envPath = path.join(dir, '.env');
  if (!fs.existsSync(envPath)) return {};
  const env = {};
  for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim().replace(/^["']|["']$/g, '');
    env[key] = val;
  }
  return env;
}

// ── Arg parser ────────────────────────────────────────────────────────────────
function parseArgs(argv) {
  const positional = [];
  const flags = {};
  for (const arg of argv) {
    if (arg.startsWith('--')) {
      const eq = arg.indexOf('=');
      if (eq === -1) flags[arg.slice(2)] = true;
      else           flags[arg.slice(2, eq)] = arg.slice(eq + 1);
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

// ── HTTP helper (handles JSON + SSE responses) ────────────────────────────────
function postJson(url, body, headers, timeoutMs) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const isHttps = parsed.protocol === 'https:';
    const lib = isHttps ? https : http;
    const payload = JSON.stringify(body);

    const reqHeaders = {
      'Content-Type':  'application/json',
      'Accept':        'application/json, text/event-stream',
      'Content-Length': Buffer.byteLength(payload),
      ...headers,
    };

    const req = lib.request({
      hostname: parsed.hostname,
      port:     parsed.port || (isHttps ? 443 : 80),
      path:     parsed.pathname,
      method:   'POST',
      headers:  reqHeaders,
      // Allow self-signed certs for local TLS dev setups
      rejectUnauthorized: false,
    }, (res) => {
      const status = res.statusCode;
      const sessionId = res.headers['mcp-session-id'];
      const ct = (res.headers['content-type'] ?? '').toLowerCase();

      let rawData = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { rawData += chunk; });
      res.on('end', () => {
        if (status < 200 || status >= 300) {
          return reject(new Error(`HTTP ${status}: ${rawData.slice(0, 300)}`));
        }

        // SSE stream — collect all "data:" lines
        if (ct.includes('text/event-stream')) {
          const messages = [];
          for (const line of rawData.split('\n')) {
            if (line.startsWith('data:')) {
              const json = line.slice(5).trim();
              if (json && json !== '[DONE]') {
                try { messages.push(JSON.parse(json)); } catch { /* skip */ }
              }
            }
          }
          return resolve({ sessionId, messages, raw: rawData });
        }

        // Plain JSON
        try {
          resolve({ sessionId, messages: [JSON.parse(rawData)], raw: rawData });
        } catch {
          reject(new Error(`Non-JSON response: ${rawData.slice(0, 300)}`));
        }
      });
    });

    const timer = setTimeout(() => {
      req.destroy(new Error(`Request timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    req.on('error', e => { clearTimeout(timer); reject(e); });
    req.on('close', () => clearTimeout(timer));
    req.write(payload);
    req.end();
  });
}

// ── MCP HTTP session ──────────────────────────────────────────────────────────
async function mcpHttpInit(baseUrl, authHeader, timeoutMs) {
  const { sessionId } = await postJson(
    `${baseUrl}/mcp`,
    {
      jsonrpc: '2.0',
      method:  'initialize',
      params:  {
        protocolVersion: '2024-11-05',
        capabilities:    {},
        clientInfo:      { name: 'mcp-query-debug', version: '1.0.0' },
      },
      id: 1,
    },
    authHeader ? { Authorization: `Bearer ${authHeader}` } : {},
    timeoutMs,
  );
  if (!sessionId) throw new Error('Server did not return mcp-session-id — is MCP_PORT > 0?');

  // Send initialized notification (fire-and-forget style)
  await postJson(
    `${baseUrl}/mcp`,
    { jsonrpc: '2.0', method: 'notifications/initialized', params: {} },
    {
      'mcp-session-id': sessionId,
      ...(authHeader ? { Authorization: `Bearer ${authHeader}` } : {}),
    },
    timeoutMs,
  ).catch(() => { /* notifications may get 202, ignore */ });

  return sessionId;
}

async function mcpHttpCall(baseUrl, sessionId, authHeader, toolName, toolArgs, timeoutMs) {
  const headers = {
    'mcp-session-id': sessionId,
    ...(authHeader ? { Authorization: `Bearer ${authHeader}` } : {}),
  };
  const { messages } = await postJson(
    `${baseUrl}/mcp`,
    {
      jsonrpc: '2.0',
      method:  'tools/call',
      params:  { name: toolName, arguments: toolArgs },
      id:      2,
    },
    headers,
    timeoutMs,
  );

  // Find the tools/call result message
  const resultMsg = messages.find(m => m.id === 2 && m.result !== undefined)
                 ?? messages.find(m => m.result !== undefined)
                 ?? messages[0];

  if (!resultMsg) throw new Error(`No result in response:\n${JSON.stringify(messages, null, 2)}`);
  if (resultMsg.error) throw new Error(`RPC error: ${JSON.stringify(resultMsg.error)}`);

  return resultMsg.result;
}

// Re-uses an existing session id to make an additional call with a new request id
async function mcpHttpCallId(baseUrl, sessionId, authHeader, toolName, toolArgs, id, timeoutMs) {
  const headers = {
    'mcp-session-id': sessionId,
    ...(authHeader ? { Authorization: `Bearer ${authHeader}` } : {}),
  };
  const { messages } = await postJson(
    `${baseUrl}/mcp`,
    { jsonrpc: '2.0', method: 'tools/call', params: { name: toolName, arguments: toolArgs }, id },
    headers,
    timeoutMs,
  );
  const resultMsg = messages.find(m => m.id === id && m.result !== undefined)
                 ?? messages.find(m => m.result !== undefined)
                 ?? messages[0];
  if (!resultMsg) throw new Error(`No result in response`);
  if (resultMsg.error) throw new Error(`RPC error: ${JSON.stringify(resultMsg.error)}`);
  return resultMsg.result;
}

// ── MCP stdio session ─────────────────────────────────────────────────────────
// Spawns a fresh child process and speaks JSON-RPC over its stdin/stdout.
// The child outputs all debug/log lines to stderr, so stdout is clean JSON-RPC.
// Retries tool calls with exponential backoff while the child is still starting up.

function spawnStdio(cmd, envVars) {
  const [bin, ...args] = cmd.split(/\s+/);
  const child = spawn(bin, args, {
    env: { ...process.env, ...envVars },
    stdio: ['pipe', 'pipe', 'inherit'], // stdin=pipe, stdout=pipe, stderr→parent's stderr
  });

  // Line-buffered reader for stdout
  const rl = createInterface({ input: child.stdout, crlfDelay: Infinity });
  const pending = new Map(); // id → { resolve, reject }
  const notifications = [];

  rl.on('line', (line) => {
    if (!line.trim()) return;
    let msg;
    try { msg = JSON.parse(line); } catch { return; } // skip non-JSON
    if (msg.id !== undefined && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(`RPC error: ${JSON.stringify(msg.error)}`));
      else           resolve(msg.result);
    } else {
      notifications.push(msg); // store unsolicited messages
    }
  });

  function send(msg) {
    child.stdin.write(JSON.stringify(msg) + '\n');
  }

  function request(msg, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        pending.delete(msg.id);
        reject(new Error(`stdio request timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      pending.set(msg.id, {
        resolve: v => { clearTimeout(timer); resolve(v); },
        reject:  e => { clearTimeout(timer); reject(e); },
      });
      send(msg);
    });
  }

  function destroy() {
    child.stdin.end();
    child.kill();
  }

  return { request, send, destroy, child };
}

async function mcpStdioQuery(cmd, envVars, toolArgs, timeoutMs) {
  process.stderr.write(`[mcp-query] spawning: ${cmd}\n`);
  const session = spawnStdio(cmd, envVars);

  const deadline = Date.now() + timeoutMs;

  function remaining() {
    const r = deadline - Date.now();
    if (r <= 0) throw new Error(`Timed out after ${timeoutMs}ms`);
    return r;
  }

  try {
    // Initialize
    process.stderr.write('[mcp-query] initializing session…\n');
    await session.request({
      jsonrpc: '2.0', id: 1, method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities:    {},
        clientInfo:      { name: 'mcp-query-debug', version: '1.0.0' },
      },
    }, remaining());
    session.send({ jsonrpc: '2.0', method: 'notifications/initialized', params: {} });
    process.stderr.write('[mcp-query] session ready, calling vkb_query…\n\n');

    // Retry loop — child returns "Server initializing" until DB/workers are up
    const retryDelay = 2000;
    let attempt = 0;
    while (true) {
      attempt++;
      const result = await session.request({
        jsonrpc: '2.0', id: 1 + attempt, method: 'tools/call',
        params: { name: 'vkb_query', arguments: toolArgs },
      }, remaining());

      // Unwrap MCP envelope
      let parsed = result;
      try {
        const text = result?.content?.[0]?.text;
        if (text) parsed = JSON.parse(text);
      } catch { /* leave as-is */ }

      // Check for "initializing" gate response
      const isInitializing = parsed?.ok === false &&
        typeof parsed?.error === 'string' &&
        parsed.error.toLowerCase().includes('initializ');

      if (!isInitializing) return { result, parsed };

      const waitMs = Math.min(retryDelay * attempt, 10000);
      process.stderr.write(`[mcp-query] server still initialising (attempt ${attempt}), retrying in ${waitMs / 1000}s…\n`);
      await new Promise(r => setTimeout(r, Math.min(waitMs, remaining())));
    }
  } finally {
    session.destroy();
  }
}

// ── main ──────────────────────────────────────────────────────────────────────
async function main() {
  const env = loadEnv(process.cwd());
  const { positional, flags } = parseArgs(process.argv.slice(2));

  const queryText = positional.join(' ').trim();
  if (!queryText) {
    console.error(`
Usage: node scripts/mcp-query.mjs <query text> [options]

Options:
  --port=PORT          MCP port; 0 = stdio mode, >0 = HTTP mode
                       (default: .env MCP_PORT or 3333)
  --timeout=SECONDS    Total timeout in seconds (default: 60)
  --k=N                Max results (default: 10)
  --type=TYPE          Filter by entity type
  --threshold=FLOAT    Min cosine similarity (default: server default)
  --sections           Include section summaries
  --secret=TOKEN       Bearer token for HTTP mode (default: .env MCP_SECRET)
  --cmd=COMMAND        Command to spawn in stdio mode
                       (default: .env MCP_DEBUG_CMD or "node dist/index.js")

HTTP mode  (port > 0): connects to the already-running vkb HTTP service.
stdio mode (port = 0): spawns a fresh child process, talks MCP over its stdio,
                       then kills it when done. Child's stderr is forwarded so
                       startup logs are visible.
`);
    process.exit(1);
  }

  const port      = parseInt(flags['port']    ?? env['MCP_PORT']    ?? '3333', 10);
  const timeoutMs = parseFloat(flags['timeout'] ?? '60') * 1000;
  const secret    = flags['secret'] ?? env['MCP_SECRET'] ?? null;
  const k         = flags['k']         ? parseInt(flags['k'],       10)    : undefined;
  const type      = flags['type']                                           ?? undefined;
  const threshold = flags['threshold'] ? parseFloat(flags['threshold'])    : undefined;
  const sections  = flags['sections'] === true || flags['sections'] === 'true';
  const cmd       = flags['cmd']       ?? env['MCP_DEBUG_CMD']             ?? 'node dist/index.js';

  const toolArgs = {
    text: queryText,
    ...(k          !== undefined ? { k }          : {}),
    ...(type       !== undefined ? { type }        : {}),
    ...(threshold  !== undefined ? { threshold }   : {}),
    ...(sections                 ? { include_sections: true } : {}),
  };

  const t0 = Date.now();
  let parsed;

  if (port === 0) {
    // ── stdio mode ───────────────────────────────────────────────────────────
    console.error(`\n[mcp-query] mode   : stdio (spawning child)`);
    console.error(`[mcp-query] cmd    : ${cmd}`);
    console.error(`[mcp-query] query  : ${queryText}`);
    console.error(`[mcp-query] args   : ${JSON.stringify(toolArgs)}`);
    console.error(`[mcp-query] timeout: ${timeoutMs / 1000}s\n`);

    // Forward .env values that the child needs (it loads its own .env too,
    // but MCP_PORT must be 0 to keep it in stdio mode)
    const childEnv = { MCP_PORT: '0' };

    try {
      const { parsed: p } = await mcpStdioQuery(cmd, childEnv, toolArgs, timeoutMs);
      parsed = p;
    } catch (e) {
      console.error(`[mcp-query] FAILED: ${e.message}`);
      process.exit(1);
    }

  } else {
    // ── HTTP mode ────────────────────────────────────────────────────────────
    const tlsCert = env['TLS_CERT'];
    const scheme  = tlsCert ? 'https' : 'http';
    const baseUrl = `${scheme}://localhost:${port}`;

    console.error(`\n[mcp-query] mode   : HTTP`);
    console.error(`[mcp-query] target : ${baseUrl}/mcp`);
    console.error(`[mcp-query] query  : ${queryText}`);
    console.error(`[mcp-query] args   : ${JSON.stringify(toolArgs)}`);
    console.error(`[mcp-query] timeout: ${timeoutMs / 1000}s\n`);

    let sessionId;
    try {
      process.stderr.write('[mcp-query] initializing session…\n');
      sessionId = await mcpHttpInit(baseUrl, secret, timeoutMs);
      process.stderr.write(`[mcp-query] session: ${sessionId}\n`);
    } catch (e) {
      const isRefused = e.code === 'ECONNREFUSED' ||
        (e.errors ?? []).some(err => err.code === 'ECONNREFUSED');
      if (isRefused) {
        console.error(
          `[mcp-query] INIT FAILED: nothing is listening on ${baseUrl}/mcp\n` +
          `\n` +
          `  The service is likely running in stdio mode (MCP_PORT=0),\n` +
          `  which is the default when launched by Claude Desktop.\n` +
          `\n` +
          `  To debug via this script you have two options:\n` +
          `\n` +
          `  1. Spawn a fresh isolated debug instance (stdio mode):\n` +
          `       node scripts/mcp-query.mjs "${queryText}" --port=0\n` +
          `\n` +
          `  2. Start the service manually in HTTP mode, then retry:\n` +
          `       MCP_PORT=${port} npm start\n`,
        );
      } else {
        console.error(`[mcp-query] INIT FAILED: ${e.message}`);
      }
      process.exit(1);
    }

    let result;
    try {
      process.stderr.write('[mcp-query] calling vkb_query…\n\n');
      result = await mcpHttpCall(baseUrl, sessionId, secret, 'vkb_query', toolArgs, timeoutMs);
    } catch (e) {
      console.error(`[mcp-query] CALL FAILED: ${e.message}`);
      process.exit(1);
    }

    parsed = result;
    try {
      const text = result?.content?.[0]?.text;
      if (text) parsed = JSON.parse(text);
    } catch { /* leave as-is */ }

    // ── Zero-result diagnostics ───────────────────────────────────────────
    const items = parsed?.data?.results ?? parsed?.results ?? [];
    if (items.length === 0) {
      process.stderr.write('\n[mcp-query] 0 results — running diagnostics…\n');

      // 1. Re-run with threshold=0 to see if data exists at all
      try {
        const uncappedResult = await mcpHttpCallId(
          baseUrl, sessionId, secret, 'vkb_query',
          { ...toolArgs, threshold: 0, k: toolArgs.k ?? 10 }, 3, timeoutMs,
        );
        let uncapped = uncappedResult;
        try { const t = uncappedResult?.content?.[0]?.text; if (t) uncapped = JSON.parse(t); } catch {}
        const uncappedItems = uncapped?.data?.results ?? uncapped?.results ?? [];

        if (uncappedItems.length > 0) {
          const scores = uncappedItems.map(r => r.similarity?.toFixed(3)).join(', ');
          const appliedThreshold = toolArgs.threshold ?? 0.75;
          process.stderr.write(
            `\n  DIAGNOSIS: threshold too high.\n` +
            `  Found ${uncappedItems.length} result(s) with threshold=0 (scores: ${scores}).\n` +
            `  All scores are below the applied threshold (${appliedThreshold}).\n` +
            `  Try: --threshold=0.3\n\n`,
          );
          process.stderr.write('  Top matches at threshold=0:\n');
          for (const r of uncappedItems.slice(0, 5)) {
            process.stderr.write(`    [${r.similarity?.toFixed(3)}] ${r.entity_type} › ${(r.chunk_summary ?? r.entity_summary ?? '').slice(0, 100)}\n`);
          }
        } else {
          process.stderr.write(`  No results even at threshold=0 — checking system status…\n`);
        }
      } catch { /* ignore diagnostic errors */ }

      // 2. Fetch vkb_status for a system snapshot
      try {
        const statusResult = await mcpHttpCallId(
          baseUrl, sessionId, secret, 'vkb_status', {}, 4, timeoutMs,
        );
        let status = statusResult;
        try { const t = statusResult?.content?.[0]?.text; if (t) status = JSON.parse(t); } catch {}
        const d = status?.data ?? status;
        process.stderr.write('\n  System status snapshot:\n');
        if (d?.counts) {
          const c = d.counts;
          process.stderr.write(`    entities : ${c.entities ?? '?'} total`);
          if (c.entities_by_status) {
            const byStatus = Object.entries(c.entities_by_status).map(([k,v]) => `${k}=${v}`).join(', ');
            process.stderr.write(` (${byStatus})`);
          }
          process.stderr.write('\n');
          process.stderr.write(`    chunks   : ${c.chunks ?? '?'}\n`);
          process.stderr.write(`    embedded : ${c.chunks_with_embeddings ?? '?'}\n`);
        } else {
          process.stderr.write(`    ${JSON.stringify(d).slice(0, 300)}\n`);
        }
        if (d?.counts?.chunks > 0 && (d?.counts?.chunks_with_embeddings ?? 0) === 0) {
          process.stderr.write(`\n  DIAGNOSIS: chunks exist but none have embeddings.\n`);
          process.stderr.write(`  The embed step likely failed or is still running.\n`);
          process.stderr.write(`  Check worker logs or re-ingest the entity.\n`);
        }
        if ((d?.counts?.entities_by_status?.pending ?? 0) > 0 || (d?.counts?.entities_by_status?.processing ?? 0) > 0) {
          process.stderr.write(`\n  NOTE: some entities are still pending/processing — retry once complete.\n`);
        }
      } catch { /* ignore diagnostic errors */ }

      process.stderr.write('\n');
    }
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(2);

  console.log(JSON.stringify(parsed, null, 2));
  console.error(`\n[mcp-query] done in ${elapsed}s`);

  // Summary line for quick scanning
  const items = parsed?.data?.results ?? parsed?.results;
  if (Array.isArray(items)) {
    console.error(`[mcp-query] ${items.length} result(s) returned`);
    if (items.length > 0) {
      console.error('\n── Top results ──');
      for (const r of items.slice(0, 5)) {
        console.error(`  [${r.similarity?.toFixed(3)}] ${r.entity_type} › ${(r.chunk_summary ?? r.entity_summary ?? '').slice(0, 100)}`);
      }
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
