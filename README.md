# vkb — Vector Knowledge Base

A self-contained, local-first semantic memory system built on Postgres + pgvector and Ollama. Exposes a full [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server so any MCP-capable host (Claude Desktop, Claude CLI, etc.) can ingest documents, query semantically, and traverse a knowledge graph.

---

## Architecture overview

```mermaid
graph TD
    subgraph Clients
        A[MCP Host\nClaude Desktop / CLI]
        B[Browser UI\nlocalhost:4242]
    end

    subgraph vkb process
        MCP[MCP Server\nstdio or HTTP/SSE :3333]
        HTTP[HTTP Server\n:4242]
        COORD[Coordinator\nWorker Pool]
        PIPE[Ingest / Retune\nPipeline]

        subgraph Adapters
            FETCH[fetch\nreadability]
            CHUNK[chunk\nsliding-window]
            EMBED[embed\nollama]
            SECT[section\nsimilarity-valley]
            REL[relation\nheuristic + LLM]
            LLM[llm\nollama]
            RAW[rawstore\nfilesystem]
        end
    end

    subgraph External
        PG[(Postgres\npgvector)]
        OLL[Ollama]
        FS[Filesystem\nrawstore/]
    end

    A -->|MCP tools| MCP
    B -->|REST / SSE| HTTP
    MCP --> COORD
    HTTP --> COORD
    COORD --> PIPE
    PIPE --> FETCH & CHUNK & EMBED & SECT & REL & LLM & RAW
    EMBED -->|vectors| OLL
    LLM --> OLL
    RAW --> FS
    PIPE -->|upsert| PG
```

---

## Ingestion pipeline

```mermaid
flowchart TD
    START([Job queued]) --> P1

    subgraph P1["Phase 1 — Persist (fetching)"]
        F1{raw_store_key\nalready set?}
        F1 -- yes --> REUSE[Load entity.md\nfrom rawstore]
        F1 -- no --> FETCH2[Fetch raw text\nURL → readability\nor staging file]
        FETCH2 --> WRITE[Write entity.md\nto rawstore]
        WRITE --> COMMIT[Commit raw_store_key\nto DB ✓ checkpoint]
    end

    REUSE & COMMIT --> P2

    subgraph P2["Phase 2 — Derive"]
        S2{section_summaries\nalready exist?}
        S2 -- yes --> RELOAD[Reload chunks +\nsections from DB]
        S2 -- no --> CHUNK2[Chunk\nsliding-window]
        CHUNK2 --> EMBED2[Embed\nnomic-embed-text]
        EMBED2 --> SECT2[Section\nsimilarity-valley]
        SECT2 --> SECT_COMMIT[Write section_summary\nto DB ✓ checkpoint]
    end

    RELOAD & SECT_COMMIT --> P3

    subgraph P3["Phase 3 — Intelligence (summarising)"]
        SUM_C[Summarise each chunk\nLLM]
        SUM_S[Summarise each section\nLLM]
        REL2[Extract relations\nheuristic + LLM]
    end

    P3 --> DONE([Entity ready\nstatus = ready])
```

---

## Requirements

| Tool | Version |
|---|---|
| Node.js | ≥ 22 |
| Docker + Docker Compose | any recent |
| [Ollama](https://ollama.ai) | any recent |

Pull the models vkb uses by default:

```bash
ollama pull nomic-embed-text
ollama pull gemma4:e4b
```

---

## Quick start

```bash
# 1. Clone and install
git clone <repo-url> galactic-vkb
cd galactic-vkb
npm install

# 2. Configure
cp .env.example .env
# Edit .env if needed — defaults work out of the box with Docker Compose

# 3. Start Postgres (pgvector-enabled)
npm run db:up

# 4. Run migrations
npm run migrate

# 5. Start vkb
npm run dev          # development (tsx watch)
# or
npm run build && npm start   # production
```

The UI is available at **http://localhost:4242/** once running. Navigate between the graph view (`#viz`) and ingest form (`#ingest`) using the header tabs.

---

## Adding vkb as an MCP server

vkb uses **stdio transport** when `MCP_PORT` is `0`. The MCP server is also accessible over HTTP/SSE when `MCP_PORT` > 0 (default **3333**).

### Claude Desktop (`claude_desktop_config.json`)

Open (or create) `~/.claude/claude_desktop_config.json` and add an entry under `mcpServers`:

```json
{
  "mcpServers": {
    "vkb": {
      "command": "node",
      "args": ["/absolute/path/to/galactic-vkb/dist/index.js"],
      "env": {
        "DATABASE_URL": "postgres://vkb:vkb@localhost:5433/vkb",
        "OLLAMA_BASE_URL": "http://localhost:11434",
        "EMBED_MODEL": "nomic-embed-text",
        "LLM_MODEL": "gemma4:e4b",
        "MCP_PORT": "0"
      }
    }
  }
}
```

> **Tip:** Run `npm run build` first so `dist/index.js` exists.  
> `MCP_PORT=0` forces stdio mode — no HTTP server is started.

For **development** (no build step), use `tsx` instead:

```json
{
  "mcpServers": {
    "vkb": {
      "command": "npx",
      "args": ["tsx", "/absolute/path/to/galactic-vkb/src/index.ts"],
      "env": {
        "DATABASE_URL": "postgres://vkb:vkb@localhost:5433/vkb",
        "OLLAMA_BASE_URL": "http://localhost:11434",
        "MCP_PORT": "0"
      }
    }
  }
}
```

### Claude CLI (inline)

```bash
MCP_PORT=0 DATABASE_URL=postgres://vkb:vkb@localhost:5433/vkb \
  claude --mcp-server "vkb:node /absolute/path/to/galactic-vkb/dist/index.js"
```

---

## Available MCP tools

| Tool | Description |
|---|---|
| `vkb_ingest` | Submit text, a URL, or a file path for ingestion |
| `vkb_job` | Poll a background job by ID |
| `vkb_query` | Semantic search across all ingested content |
| `vkb_get` | Fetch an entity or chunk by ID |
| `vkb_raw` | Read the raw stored text for an entity or chunk |
| `vkb_relate` | Assert an explicit relation between two entities |
| `vkb_neighbors` | Retrieve an N-hop relation subgraph from a seed node |
| `vkb_delete` | Delete an entity and all its data |
| `vkb_reingest` | Re-run the ingest pipeline for one or all entities that have stored raw content |
| `vkb_retune` | Trigger a re-embedding / relation refresh sweep |
| `vkb_status` | Full system snapshot (counts, queue depth, config) |

### `vkb_relate` — the feedback loop tool

> "The `vkb_relate` tool is underrated. As Claude works with your data and draws connections, you can have it assert new relations back into the graph. Over time Claude becomes a **contributor** to the knowledge base, not just a consumer. That's a genuinely interesting feedback loop."

Every relation asserted via `vkb_relate` is marked `origin: asserted` — it is never pruned by retune sweeps and carries `confidence: 1.0`. This makes Claude's synthesis durable: connections it draws during a session persist and become first-class edges that future queries and `vkb_neighbors` traversals can follow.

Typical pattern:
```
# 1. Query for relevant chunks
vkb_query { text: "transformer attention mechanism" }

# 2. Identify a cross-document insight, then assert it
vkb_relate { source_id: "<chunk-A>", target_id: "<chunk-B>",
             rel_type: "shares_mechanism_with" }

# 3. Traverse what's grown
vkb_neighbors { id: "<chunk-A>", hops: 2 }
```

### `vkb_neighbors` — N-hop subgraph retrieval

Walks the relation graph outward from a seed node up to `hops` steps (default 2, max 5). Returns:

- **`nodes`** — every reachable entity or chunk, annotated with `kind`, `hop` distance from the seed, and its summary.
- **`edges`** — all relations *between discovered nodes* (not just the traversal path), enabling local graph rendering or further reasoning.

| Parameter | Default | Description |
|---|---|---|
| `id` | required | Seed entity or chunk UUID |
| `hops` | `2` | Traversal depth (1–5) |
| `min_confidence` | `0.0` | Skip edges below this confidence |
| `rel_type` | — | Only follow edges of this type |
| `max_nodes` | `50` | Cap on total nodes returned |

---

## HTTP API

The observability server runs on `OBS_PORT` (default **4242**) and exposes both the SPA and a REST API.

All responses follow the envelope `{ ok: true, data: … }` / `{ ok: false, error: "…" }`.  
When Postgres is unreachable, routes that need the DB return **503** with `"Database unavailable"`.

### Authentication

Set `OBS_SECRET=your-secret` in `.env`. When set, all API requests must include:

```
Authorization: Bearer your-secret
```

### Health

```
GET /health
```

Returns the live status of each dependency:

```json
{ "ok": true, "uptime": 42.3, "postgres": true, "ollama": true }
```

Returns `503` with `"postgres": false` when Postgres is unreachable.

### Ingest

```bash
# URL
curl -X POST http://localhost:4242/ingest \
  -H "Content-Type: application/json" \
  -d '{ "type": "url", "ref": "https://example.com/article" }'

# Local file
curl -X POST http://localhost:4242/ingest \
  -H "Content-Type: application/json" \
  -d '{ "type": "doc", "ref": "/absolute/path/to/file.md" }'

# Inline text
curl -X POST http://localhost:4242/ingest \
  -H "Content-Type: application/json" \
  -d '{ "type": "note", "text": "Some text…", "meta": { "tags": ["example"] } }'
```

Response:

```json
{ "ok": true, "data": { "job_id": "3f2a1b4c-…", "entity_id": "9d8e7f6a-…" } }
```

Ingestion is **asynchronous** — poll `/jobs` for completion (see below).

### Re-ingest

```bash
# Re-run pipeline for a single entity (must have stored raw content)
curl -X POST http://localhost:4242/reingest \
  -H "Content-Type: application/json" \
  -d '{ "entity_id": "9d8e7f6a-…" }'

# Re-queue all entities that have stored raw content
curl -X POST http://localhost:4242/reingest \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Query

```bash
curl -X POST http://localhost:4242/query \
  -H "Content-Type: application/json" \
  -d '{ "text": "How does quantum entanglement work?", "k": 5 }'
```

### Retune

Starts a background retune sweep (re-embeds stale chunks, prunes weak relations):

```bash
curl -X POST http://localhost:4242/retune \
  -H "Content-Type: application/json" \
  -d '{ "scope": "all", "force": false }'
```

### Jobs

```
GET /jobs?kind=ingest&stage=queued&limit=50
```

| Query param | Values | Description |
|---|---|---|
| `kind` | `ingest`, `retune` | Filter by job type |
| `stage` | `queued`, `fetching`, `chunking`, `embedding`, `relating`, `done`, `error` | Filter by stage |
| `limit` | 1–200 (default 50) | Max results |

### Entities

```
GET  /entities?type=url&status=ready&q=quantum&from=2025-01-01&limit=50&offset=0
GET  /entities/broken
GET  /entities/:id
GET  /entities/:id/raw
DELETE /entities/:id
```

`/entities/broken` returns non-ready entities annotated with their latest job and a remediation hint:
- `reingest` — raw content is available, pipeline can be re-run
- `no_raw` — source content was not persisted; manual intervention needed
- `stuck` — an active job exists but has not progressed

### Chunks

```
GET /chunks/:id
GET /chunks/:id/raw
```

### Relations

```
GET /relations?origin=heuristic&rel_type=related_to&min_confidence=0.7&limit=50
```

| Query param | Description |
|---|---|
| `origin` | `heuristic`, `semantic`, `llm`, `asserted` |
| `rel_type` | Relation label string |
| `min_confidence` | Float 0–1 |
| `min_weight` | Float |
| `source_kind` | `entity` or `chunk` |

### Status

```
GET /status
```

Returns entity/chunk/relation counts, queue depths, and active config.

---

## WebSocket event stream

Connect to `ws://localhost:4242/stream` (or `wss://` with TLS) to receive live pipeline events.

| Event type | Description |
|---|---|
| `stage_change` | Job moved to a new pipeline stage |
| `progress` | Chunk/embed progress within a stage |
| `complete` | Job finished successfully |
| `error` | Job failed — payload contains error detail |
| `heartbeat` | Sent every ~30 s; confirms the coordinator is alive |
| `db_unavailable` | Postgres connection lost — UI shows an alert banner |
| `db_available` | Postgres connection restored |
| `worker_crash` | A worker process exited unexpectedly and is being respawned |

---

## npm scripts

| Script | Description |
|---|---|
| `npm run dev` | Start with tsx watch (hot reload) |
| `npm run build` | Compile TypeScript → `dist/` |
| `npm start` | Run compiled build |
| `npm run migrate` | Apply database migrations |
| `npm run db:up` | Start Dockerised Postgres |
| `npm run db:down` | Stop containers |
| `npm run db:reset` | Wipe volume and restart Postgres |
| `npm run pack:mcpb` | Build and package as a `.mcpb` bundle |

---

## Configuration

All settings are read from environment variables (or a `.env` file). Defaults are shown.

### Infrastructure

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://localhost/vkb` | Postgres connection string |
| `RAWSTORE_ADAPTER` | `filesystem` | Raw content storage: `filesystem` or `s3` |
| `RAWSTORE_PATH` | `./rawstore` | Root path for filesystem rawstore |
| `RAWSTORE_S3_BUCKET` | — | S3 bucket name (when `RAWSTORE_ADAPTER=s3`) |
| `RAWSTORE_S3_ENDPOINT` | — | S3-compatible endpoint URL (optional override) |

### Ollama / models

| Variable | Default | Description |
|---|---|---|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API base URL |
| `EMBED_MODEL` | `nomic-embed-text` | Embedding model |
| `EMBED_DIM` | `768` | Embedding dimension (must match model output) |
| `LLM_MODEL` | `gemma4:e4b` | LLM model for relation extraction and summarisation |
| `LLM_RELATION_EXTRACTION` | `true` | Use LLM to extract relations (set `false` to use heuristics only) |
| `LLM_EXTRACT_CANDIDATES` | `20` | Candidate chunks the LLM considers per extraction pass |

### Chunking & sectioning

| Variable | Default | Description |
|---|---|---|
| `CHUNK_SIZE` | `512` | Target chunk size in tokens |
| `CHUNK_OVERLAP` | `64` | Overlap between adjacent chunks |
| `SECTION_STRATEGY` | `similarity_valley` | Sectioning strategy: `similarity_valley` or `positional` |
| `SECTION_SPLIT_THRESHOLD` | `0.65` | Cosine similarity drop that triggers a section boundary |
| `SECTION_WINDOW_SIZE` | `5` | Sliding window size for valley detection |
| `SECTION_MAX_SIZE` | `8` | Max chunks per section |

### Relations

| Variable | Default | Description |
|---|---|---|
| `RELATION_THRESHOLD` | `0.75` | Minimum cosine similarity to create a relation |
| `RELATION_TOP_K` | `10` | Nearest neighbours considered per chunk |
| `RELATION_CONFIDENCE_STEP` | `0.05` | Increment applied on relation confirmation |
| `RELATION_TTL_DAYS` | `30` | Days before unconfirmed relations are pruned |
| `RELATION_PRUNE_THRESHOLD` | `0.6` | Confidence below which relations are pruned on retune |

### Workers & jobs

| Variable | Default | Description |
|---|---|---|
| `WORKER_CONCURRENCY` | `2` | Number of ingest worker processes |
| `INGEST_MAX_RETRIES` | `3` | Max times a failed job is re-queued |
| `RETUNE_INTERVAL_HOURS` | `6` | How often the retune worker runs automatically |
| `RETUNE_SUMMARISE` | `false` | Regenerate entity summaries during retune |
| `JOB_TTL_DAYS` | `7` | Days before completed/failed jobs are expired |

### Servers & security

| Variable | Default | Description |
|---|---|---|
| `MCP_PORT` | `3333` | MCP server port (`0` = stdio mode only) |
| `OBS_PORT` | `4242` | Observability HTTP/WebSocket server port |
| `OBS_SECRET` | — | Bearer token required on all API requests (leave unset to disable auth) |
| `TLS_CERT` | — | Path to TLS certificate file (enables HTTPS/WSS on all servers) |
| `TLS_KEY` | — | Path to TLS private key file |
| `LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |

### Custom prompts

Provide paths to YAML or plain-text files to override the built-in LLM prompts:

| Variable | Description |
|---|---|
| `SUMMARY_PROMPT_FILE` | Entity summary prompt |
| `CHUNK_SUMMARY_PROMPT_FILE` | Per-chunk summary prompt |
| `SECTION_SUMMARY_PROMPT_FILE` | Section summary prompt |
| `RELATION_EXTRACT_PROMPT_FILE` | Relation extraction prompt |

---

## Observability UI

The SPA at **http://localhost:4242/** has two views, selectable from the header:

**Graph (`#viz`)**
- Entity nodes coloured by status (ready / pending / error)
- Edges coloured by origin (heuristic / semantic / LLM / asserted)
- Click a node to inspect entity details, chunks, and relations
- Confidence histogram for relations

**Ingest (`#ingest`)**
- Submit URLs, local files, or inline text
- Live job progress via WebSocket
- Per-job stage tracker

A banner appears at the top of the UI if Postgres becomes unreachable while the server is running, and dismisses automatically on reconnection.

---

## Database resilience

On startup vkb probes the Postgres connection up to 10 times (3-second intervals) before giving up. This means it tolerates the Docker container taking a few seconds to become ready after `docker compose up`.

While running:
- HTTP routes that need the DB return **503** when the connection is lost
- The coordinator broadcasts a `db_unavailable` WebSocket event on the first failed heartbeat
- Ingest workers pause for 30 seconds between retries instead of the normal 2-second poll interval
- A `db_available` event is broadcast and the UI banner clears automatically when connectivity is restored

---

## TLS

Set `TLS_CERT` and `TLS_KEY` to paths of a certificate and private key to enable HTTPS and WSS on all servers. A self-signed cert for local development can be generated with:

```powershell
scripts/gen-cert.ps1
```
