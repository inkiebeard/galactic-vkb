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
        MCP[MCP Server\nstdio or HTTP Streamable :3333]
        HTTP[HTTP Server\n:4242]
        COORD[Coordinator\nWorker Pool]

        subgraph Workers
            IW[ingest-worker × N]
            FW[finetune-worker]
            RW[retune-worker]
        end

        subgraph Pipelines
            IPIPE[Ingest Pipeline]
            FPIPE[Finetune Pipeline]
            RPIPE[Retune Pipeline]
        end

        subgraph Adapters
            FETCH[fetch\nreadability / pdf / epub]
            CHUNK[chunk\nsliding-window]
            EMBED[embed\nollama]
            SECT[section\nsimilarity-valley]
            REL[relation\nheuristic + LLM]
            LLM[llm\nollama]
            RAW[rawstore\nfilesystem or s3]
        end
    end

    subgraph External
        PG[(Postgres\npgvector)]
        OLL[Ollama]
        FS[Filesystem / S3\nrawstore/]
    end

    A -->|MCP tools| MCP
    B -->|REST / WebSocket| HTTP
    MCP --> COORD
    HTTP --> COORD
    COORD --> IW & FW & RW
    IW --> IPIPE
    FW --> FPIPE
    RW --> RPIPE
    IPIPE & FPIPE & RPIPE --> FETCH & CHUNK & EMBED & SECT & REL & LLM & RAW
    EMBED -->|vectors| OLL
    LLM --> OLL
    RAW --> FS
    IPIPE & FPIPE & RPIPE -->|upsert| PG
```

---

## Ingestion pipeline

```mermaid
flowchart TD
    START([Job queued]) --> DEDUP

    DEDUP{Content hash\nalready known?}
    DEDUP -- yes, unchanged --> SKIP([Skip — existing entity returned])
    DEDUP -- no / changed --> FETCH

    subgraph FETCH["fetching — Persist"]
        FETCH2[Fetch raw text\nURL → readability / PDF / EPUB\nor staging file]
        FETCH2 --> HASH[Compute SHA-256\ncontent hash]
        HASH --> WRITE[Write entity.md + chunks.ndjson\nto RawStore]
        WRITE --> F_COMMIT[Commit raw_store_key\nto DB ✓]
    end

    F_COMMIT --> CHUNK_S

    subgraph CHUNK_S["chunking"]
        CHUNK2[Sliding-window chunk\nwrite chunk rows to DB]
    end

    CHUNK_S --> EMBED_S

    subgraph EMBED_S["embedding"]
        EMBED2[Embed each chunk\nvia Ollama]
    end

    EMBED_S --> SECT_S

    subgraph SECT_S["sectioning"]
        SECT2[Group chunks into sections\nsimilarity-valley or positional]
    end

    SECT_S --> SUM_S

    subgraph SUM_S["summarising"]
        SUM_C[Summarise each chunk — LLM]
        SUM_SEC[Summarise each section — LLM]
        SUM_E[Summarise entity — LLM]
    end

    SUM_S --> LINK_S

    subgraph LINK_S["linking"]
        REL_H[Heuristic relation extraction]
        REL_L[LLM relation extraction\noptional]
    end

    LINK_S --> TAG_S

    subgraph TAG_S["tagging"]
        TAG[Extract tags from meta\nAssert tag:* relations\nbetween co-tagged entities]
    end

    TAG_S --> DONE([Entity ready — status = ready])
```

## Finetune pipeline

The finetune pipeline enriches already-ingested entities without re-chunking or re-embedding. It runs as a separate `finetune-worker` process and is triggered via `vkb_finetune` (MCP) or `POST /finetune` (HTTP).

```mermaid
flowchart TD
    START([Finetune job queued]) --> EX

    subgraph EX["extracting"]
        TOP[Find top-N nearest neighbours\nvia embedding similarity]
        TOP --> LLM_REL[LLM relation extractor\nupsert content_llm relations]
    end

    EX --> TAG

    subgraph TAG["tagging"]
        LLM_TAG[LLM keyword tagger\nmerge tags into meta.tags]
        LLM_TAG --> TAG_REL[Assert tag:* relations\nbetween co-tagged entities]
    end

    TAG --> DONE([Finetune complete])
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

vkb uses **stdio transport** when `MCP_PORT` is `0`. The MCP server is also accessible over **HTTP Streamable** (MCP 2025-03-26 spec) when `MCP_PORT` > 0 (default **3333**), with session management for multi-client use.

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
| `vkb_ingest` | Submit text, a URL, or a file path for ingestion. Optional `source_context` (`external`\|`conversation`\|`self_authored`) and `meta` object. Inline text is deduplicated by SHA-256 before queuing. |
| `vkb_ingest_bulk` | Submit up to 200 items in a single call. Each item has the same shape as `vkb_ingest`. Deduplication is applied per-item; unchanged items are returned with `skipped: true`. |
| `vkb_job` | Poll a background job by ID. Returns `stage`, progress counters, `entity_id`, `kind`, and `error_detail` on failure. |
| `vkb_query` | Semantic search across all ingested content. Supports `k`, `type`, `threshold` (float 0–1), and `include_sections`. Returns an actionable hint when results are empty. |
| `vkb_get` | Fetch an entity or chunk by ID (`kind`: `entity`\|`chunk`). Entity responses include chunk IDs, sections, relations, and `tag_context` (co-tagged entities). |
| `vkb_raw` | Read the raw stored text for an entity or chunk from the RawStore. |
| `vkb_relate` | Assert an explicit relation between any two entity or chunk IDs. `weight` is auto-computed from cosine similarity if omitted. Asserted relations are never pruned. |
| `vkb_neighbors` | Retrieve an N-hop relation subgraph from a seed node. |
| `vkb_delete` | Delete an entity and all its data (chunks, sections, relations, RawStore files). Non-reversible. |
| `vkb_finetune` | Queue a finetune job: LLM relation extraction + LLM keyword tagging. No re-chunking or re-embedding. Accepts optional `entity_ids` array or `scope` (entity type filter). |
| `vkb_retune` | Trigger a re-embedding / relation refresh sweep immediately. `force: true` reprocesses all chunks regardless of embed model. |
| `vkb_status` | Full system snapshot (entity/chunk/relation counts, queue depth, worker state, config). |
| `vkb_migrate` | Run all pending SQL migrations. Idempotent. |

### `vkb_relate` — the feedback loop tool

> "The `vkb_relate` tool is underrated. As Claude works with your data and draws connections, you can have it assert new relations back into the graph. Over time Claude becomes a **contributor** to the knowledge base, not just a consumer. That's a genuinely interesting feedback loop."

Every relation asserted via `vkb_relate` is marked `origin: asserted` — it is never pruned by retune sweeps and carries `confidence: 1.0`. The optional `weight` parameter (float 0–1) lets you express relative strength; if omitted it is computed automatically from the cosine similarity between the two nodes. This makes Claude's synthesis durable: connections it draws during a session persist and become first-class edges that future queries and `vkb_neighbors` traversals can follow.

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

You can also use `vkb_finetune` to have the LLM automatically extract relations and keyword tags across a set of entities — a useful complement to explicit `vkb_relate` calls when working with a large corpus.

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

# Local file (supports .md, .txt, .pdf, .epub, .yaml, .json)
curl -X POST http://localhost:4242/ingest \
  -H "Content-Type: application/json" \
  -d '{ "type": "doc", "ref": "/absolute/path/to/file.md" }'

# Inline text
curl -X POST http://localhost:4242/ingest \
  -H "Content-Type: application/json" \
  -d '{ "type": "note", "text": "Some text…", "source_context": "conversation", "meta": { "tags": ["example"] } }'
```

Response:

```json
{ "ok": true, "data": { "job_id": "3f2a1b4c-…", "entity_id": "9d8e7f6a-…" } }
```

Ingestion is **asynchronous** — poll `/jobs` for completion. Inline text is deduplicated by SHA-256 content hash; if identical content already exists as a `ready` entity, the job is skipped and the existing entity is returned.

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

### Finetune

Runs LLM relation extraction and keyword tagging on already-ingested entities without re-chunking or re-embedding:

```bash
# Finetune specific entities
curl -X POST http://localhost:4242/finetune \
  -H "Content-Type: application/json" \
  -d '{ "entity_ids": ["9d8e7f6a-…"] }'

# Finetune all entities of a given type
curl -X POST http://localhost:4242/finetune \
  -H "Content-Type: application/json" \
  -d '{ "scope": "url" }'
```

### Query

```bash
curl -X POST http://localhost:4242/query \
  -H "Content-Type: application/json" \
  -d '{ "text": "How does quantum entanglement work?", "k": 5, "threshold": 0.7 }'
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
| `kind` | `ingest`, `retune`, `finetune` | Filter by job type |
| `stage` | `queued`, `fetching`, `chunking`, `embedding`, `sectioning`, `summarising`, `linking`, `tagging`, `extracting`, `done`, `error` | Filter by stage |
| `limit` | 1–200 (default 50) | Max results |

Results include the entity `ref` and `meta` for context.

### Entities

```
GET  /entities?type=url&status=ready&source_context=external&q=quantum&id=<uuid>&from=2025-01-01&limit=50&offset=0
GET  /entities/broken
GET  /entities/projection?offset=0&limit=500
GET  /entities/:id
GET  /entities/:id/raw
DELETE /entities/:id
POST /entities/bulk-action
```

`/entities/broken` returns non-ready entities annotated with their latest job and a remediation hint:
- `reingest` — raw content is available, pipeline can be re-run
- `no_raw` — source content was not persisted; manual intervention needed
- `stuck` — an active job exists but has not progressed

`/entities/projection` returns a paginated UMAP 3D projection (mean-pooled chunk embeddings per entity), used by the graph view. The projection is cached and recomputed in the background whenever an ingest job completes.

`POST /entities/bulk-action` applies an action to a list of entity IDs:

```json
{ "ids": ["<uuid>", "…"], "action": "delete" | "reingest" | "reingest_force" | "finetune" }
```

Returns `{ results, succeeded, failed }`.

### Chunks

```
GET /chunks?entity_id=<uuid>&limit=500&offset=0
GET /chunks/projection?offset=0&limit=500
GET /chunks/:id
GET /chunks/:id/raw
```

`/chunks/projection` returns a paginated UMAP 3D projection of individual chunk embeddings (same cache/versioning as entity projection).

### Relations

```
GET /relations?origin=heuristic&rel_type=related_to&min_confidence=0.7&limit=50
```

| Query param | Description |
|---|---|
| `origin` | `content_heuristic`, `content_llm`, `semantic`, `asserted` |
| `rel_type` | Relation label string |
| `min_confidence` | Float 0–1 |
| `min_weight` | Float |
| `source_kind` | `entity` or `chunk` |
| `limit` | 1–50000 (default 50) |

### Status

```
GET /status
```

Returns entity/chunk/relation counts, queue depths, worker state, index status, and active config.

---

## WebSocket event stream

Connect to `ws://localhost:4242/stream` (or `wss://` with TLS) to receive live pipeline events. Browsers that cannot send custom headers can authenticate via query param: `?token=<OBS_SECRET>`.

| Event type | Payload fields | Description |
|---|---|---|
| `stage_change` | `job_id`, `stage` | Job moved to a new pipeline stage |
| `complete` | `job_id` | Job finished successfully |
| `error` | `job_id`, `payload` | Job failed — payload contains error detail |
| `heartbeat` | `job_id` (empty), `pid` | Sent every ~10 s per worker; confirms the coordinator is alive |
| `worker_crash` | `name`, `code`, `signal`, `ts` | A worker process exited unexpectedly and is being respawned |
| `projection_version` | `resolution` (`chunk`\|`entity`), `version`, `total`, `ts` | UMAP projection recomputed after a job completes |
| `retune_scheduled` | `ts` | Coordinator queued a periodic retune sweep |
| `db_unavailable` | `ts` | Postgres connection lost — UI shows an alert banner |
| `db_available` | `ts` | Postgres connection restored |

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

### Summarisation

| Variable | Default | Description |
|---|---|---|
| `SUMMARY_CONCURRENCY` | `4` | Parallel LLM calls during the summarising stage |
| `SUMMARY_MAX_INPUT_CHARS` | `12000` | Max characters fed to the entity-level summary prompt (~3 k tokens) |

### Vector index

| Variable | Default | Description |
|---|---|---|
| `IVFFLAT_THRESHOLD` | `1000` | Chunk count above which an ivfflat index is created/maintained |
| `IVFFLAT_LISTS` | `100` | Number of ivfflat lists (tune alongside chunk count) |

### Workers & jobs

| Variable | Default | Description |
|---|---|---|
| `WORKER_CONCURRENCY` | `2` | Number of ingest worker processes |
| `INGEST_MAX_RETRIES` | `3` | Max times a failed job is re-queued |
| `RETUNE_INTERVAL_HOURS` | `6` | How often the retune worker runs automatically (`0` = disabled) |
| `RETUNE_SUMMARISE` | `false` | Regenerate entity summaries during retune |
| `JOB_TTL_DAYS` | `7` | Days before completed/failed jobs are expired |

### Servers & security

| Variable | Default | Description |
|---|---|---|
| `MCP_PORT` | `3333` | MCP server port (`0` = stdio mode only, no auth layer) |
| `OBS_PORT` | `4242` | Observability HTTP/WebSocket server port |
| `OBS_SECRET` | — | Bearer token for the REST/browser API on `OBS_PORT` (leave unset to disable) |
| `MCP_SECRET` | — | Bearer token for the HTTP MCP endpoint on `MCP_PORT` (leave unset to disable) |
| `TLS_CERT` | — | Path to TLS certificate file (enables HTTPS/WSS on all servers) |
| `TLS_KEY` | — | Path to TLS private key file |
| `LOG_LEVEL` | `info` | Log verbosity: `debug`, `info`, `warn`, `error`. Pass `--debug` at startup as a shorthand for `LOG_LEVEL=debug` — also enables full request/response logging on all HTTP endpoints and MCP tool call tracing |

> **Note — stdio mode has no auth layer.** When `MCP_PORT=0` the process communicates over its own stdin/stdout pipe; `MCP_SECRET` and `OBS_SECRET` have no effect on it. Only set `MCP_SECRET` when running in HTTP mode and exposing the port outside localhost — most MCP clients (including Claude Desktop) do not send an `Authorization` header, so setting `MCP_SECRET` in a Claude Desktop config will silently block all tool calls with `401 Unauthorized`.

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
