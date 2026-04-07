#!/usr/bin/env tsx
/**
 * MCP Debug CLI
 * Calls tool handlers directly (bypassing the MCP transport layer) so you can
 * inspect real responses with full stack traces.
 *
 * Usage:
 *   tsx scripts/mcp-debug.ts <tool> [options]
 *
 * Tools
 *   status
 *   query    <text>  [--k=10] [--type=<t>] [--threshold=0.75] [--sections]
 *   get      <id>    [--kind=entity|chunk]
 *   raw      <id>    [--kind=entity|chunk]
 *   job      <job_id>
 *   ingest   --type=<t>  (--text=<s> | --ref=<url>)  [--meta=<json>]
 *   relate   <src_id> <tgt_id> <rel_type>  [--weight=0.9]
 *   neighbors <id>  [--hops=2] [--min_confidence=0] [--rel_type=<t>] [--max_nodes=50]
 *   delete   <id>
 *   retune   [--scope=<t>] [--force]
 */

import '../src/env.js';
import { waitForDb, getPool, closePool } from '../src/db/client.js';
import { getAdapters } from '../src/adapters/registry.js';
import {
  handleIngest, handleJob, handleQuery, handleGet, handleRaw,
  handleRelate, handleNeighbors, handleDelete, handleRetune, handleStatus,
} from '../src/mcp/tools.js';

// ── Minimal arg parser ────────────────────────────────────────────────────────
function parseArgs(argv: string[]) {
  const positional: string[] = [];
  const flags: Record<string, string | boolean> = {};
  for (const arg of argv) {
    if (arg.startsWith('--')) {
      const eq = arg.indexOf('=');
      if (eq === -1) {
        flags[arg.slice(2)] = true;
      } else {
        flags[arg.slice(2, eq)] = arg.slice(eq + 1);
      }
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function str(flags: Record<string, string | boolean>, key: string): string | undefined {
  const v = flags[key];
  return typeof v === 'string' ? v : undefined;
}
function num(flags: Record<string, string | boolean>, key: string): number | undefined {
  const v = str(flags, key);
  return v !== undefined ? parseFloat(v) : undefined;
}
function int(flags: Record<string, string | boolean>, key: string): number | undefined {
  const v = str(flags, key);
  return v !== undefined ? parseInt(v, 10) : undefined;
}
function bool(flags: Record<string, string | boolean>, key: string): boolean | undefined {
  const v = flags[key];
  return v === undefined ? undefined : v === true || v === 'true';
}

// ── Pretty print ──────────────────────────────────────────────────────────────
function print(data: unknown) {
  console.log(JSON.stringify(data, null, 2));
}

function printErr(label: string, e: unknown) {
  const msg = e instanceof Error ? e.stack ?? e.message : String(e);
  console.error(`\n[${label} ERROR]\n${msg}\n`);
}

// ── usage ─────────────────────────────────────────────────────────────────────
function usage() {
  console.error(`
MCP Debug CLI — calls tool handlers directly without the MCP transport layer.

Usage: tsx scripts/mcp-debug.ts <tool> [options]

Tools:
  status
  query    <text>     [--k=10] [--type=TYPE] [--threshold=0.75] [--sections]
  get      <id>       [--kind=entity|chunk]
  raw      <id>       [--kind=entity|chunk]
  job      <job_id>
  ingest   --type=TYPE  (--text=TEXT | --ref=URL)  [--meta=JSON_OBJ]
  relate   <src_id> <tgt_id> <rel_type>  [--weight=FLOAT]
  neighbors <id>     [--hops=2] [--min_confidence=0] [--rel_type=TYPE] [--max_nodes=50]
  delete   <id>
  retune   [--scope=TYPE] [--force]

Examples:
  tsx scripts/mcp-debug.ts status
  tsx scripts/mcp-debug.ts query "machine learning" --k=5 --threshold=0.7
  tsx scripts/mcp-debug.ts get 8a76667e-06c8-40ef-8937-8a15ab3d0e2d
  tsx scripts/mcp-debug.ts neighbors 8a76667e-06c8-40ef-8937-8a15ab3d0e2d --hops=2
  tsx scripts/mcp-debug.ts ingest --type=note --text="Hello world"
  tsx scripts/mcp-debug.ts job <job_id>
`);
  process.exit(1);
}

// ── main ──────────────────────────────────────────────────────────────────────
async function main() {
  const rawArgs = process.argv.slice(2);
  if (rawArgs.length === 0) usage();

  const [tool, ...rest] = rawArgs;
  const { positional, flags } = parseArgs(rest);

  // Tools that need adapters
  const needsAdapters = new Set(['query', 'raw', 'delete']);
  const adapters = needsAdapters.has(tool) ? getAdapters() : null;

  console.error(`\n[vkb-debug] connecting to DB…`);
  await waitForDb();
  console.error(`[vkb-debug] running tool: ${tool}\n`);

  try {
    switch (tool) {

      case 'status': {
        print(await handleStatus());
        break;
      }

      case 'query': {
        const text = positional[0];
        if (!text) { console.error('query requires <text>'); process.exit(1); }
        print(await handleQuery({
          text,
          k:                int(flags, 'k'),
          type:             str(flags, 'type'),
          threshold:        num(flags, 'threshold'),
          include_sections: bool(flags, 'sections') ?? bool(flags, 'include_sections'),
        }, adapters!));
        break;
      }

      case 'get': {
        const id = positional[0];
        if (!id) { console.error('get requires <id>'); process.exit(1); }
        print(await handleGet(id, str(flags, 'kind')));
        break;
      }

      case 'raw': {
        const id = positional[0];
        if (!id) { console.error('raw requires <id>'); process.exit(1); }
        print(await handleRaw(id, str(flags, 'kind'), adapters!));
        break;
      }

      case 'job': {
        const id = positional[0];
        if (!id) { console.error('job requires <job_id>'); process.exit(1); }
        print(await handleJob(id));
        break;
      }

      case 'ingest': {
        const type = str(flags, 'type');
        if (!type) { console.error('ingest requires --type'); process.exit(1); }
        const text = str(flags, 'text');
        const ref  = str(flags, 'ref');
        if (!text && !ref) { console.error('ingest requires --text or --ref'); process.exit(1); }
        const metaRaw = str(flags, 'meta');
        const meta = metaRaw ? JSON.parse(metaRaw) as Record<string, unknown> : undefined;
        print(await handleIngest({ type, text, ref, meta }));
        break;
      }

      case 'relate': {
        const [srcId, tgtId, relType] = positional;
        if (!srcId || !tgtId || !relType) {
          console.error('relate requires <src_id> <tgt_id> <rel_type>'); process.exit(1);
        }
        print(await handleRelate(srcId, tgtId, relType, num(flags, 'weight')));
        break;
      }

      case 'neighbors': {
        const id = positional[0];
        if (!id) { console.error('neighbors requires <id>'); process.exit(1); }
        print(await handleNeighbors(
          id,
          int(flags, 'hops')           ?? 2,
          num(flags, 'min_confidence') ?? 0.0,
          str(flags, 'rel_type'),
          int(flags, 'max_nodes')      ?? 50,
        ));
        break;
      }

      case 'delete': {
        const id = positional[0];
        if (!id) { console.error('delete requires <id>'); process.exit(1); }
        // Confirm destructive action
        if (!flags['yes'] && process.stdin.isTTY) {
          process.stderr.write(`Delete entity ${id}? This is non-reversible. Pass --yes to skip. [y/N] `);
          const answer = await new Promise<string>(res => {
            process.stdin.setEncoding('utf8');
            process.stdin.once('data', d => res(String(d).trim().toLowerCase()));
          });
          if (answer !== 'y' && answer !== 'yes') { console.error('Aborted.'); process.exit(0); }
        }
        print(await handleDelete(id, adapters!));
        break;
      }

      case 'retune': {
        print(await handleRetune(str(flags, 'scope'), bool(flags, 'force')));
        break;
      }

      default:
        console.error(`Unknown tool: ${tool}`);
        usage();
    }
  } catch (e) {
    printErr(tool, e);
    process.exitCode = 1;
  } finally {
    await closePool();
  }
}

main().catch(e => { console.error(e); process.exit(1); });
