-- vkb schema v0.4
-- Requires: CREATE EXTENSION vector (pgvector)

CREATE EXTENSION IF NOT EXISTS vector;

-- ── entity ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS entity (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  type            TEXT        NOT NULL,
  ref             TEXT,
  raw_store_key   TEXT,
  summary         TEXT,
  summary_version INT         NOT NULL DEFAULT 0,
  meta            JSONB       NOT NULL DEFAULT '{}',
  status          TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending','processing','ready','error')),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entity_status     ON entity (status);
CREATE INDEX IF NOT EXISTS idx_entity_type       ON entity (type);
CREATE INDEX IF NOT EXISTS idx_entity_created_at ON entity (created_at DESC);
-- Full-text search on entity summary
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE tablename = 'entity' AND indexname = 'idx_entity_summary_fts'
  ) THEN
    CREATE INDEX idx_entity_summary_fts
      ON entity USING gin(to_tsvector('english', COALESCE(summary, '')));
  END IF;
END $$;

-- ── chunk ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chunk (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id     UUID        NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
  seq           INT         NOT NULL,
  raw_store_key TEXT,
  summary       TEXT,
  embedding     vector,             -- dimension set by EMBED_DIM; no column-level constraint for flexibility
  embed_model   TEXT,
  embed_version INT         NOT NULL DEFAULT 0,
  embedded_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_chunk_entity_id    ON chunk (entity_id);
CREATE INDEX IF NOT EXISTS idx_chunk_embed_model  ON chunk (embed_model);
CREATE INDEX IF NOT EXISTS idx_chunk_embedded_at  ON chunk (embedded_at);
-- NOTE: ivfflat index on chunk.embedding is created dynamically by the retune worker
-- once chunk count exceeds IVFFLAT_THRESHOLD.

-- ── section_summary ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS section_summary (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id   UUID        NOT NULL REFERENCES entity(id) ON DELETE CASCADE,
  chunk_ids   UUID[]      NOT NULL,
  seq         INT         NOT NULL,
  summary     TEXT        NOT NULL DEFAULT '',
  strategy    TEXT        NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_section_entity_id ON section_summary (entity_id);

-- ── relation ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS relation (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id   UUID        NOT NULL,
  target_id   UUID        NOT NULL,
  source_kind TEXT        NOT NULL CHECK (source_kind IN ('chunk','entity')),
  target_kind TEXT        NOT NULL CHECK (target_kind IN ('chunk','entity')),
  rel_type    TEXT        NOT NULL,
  origin      TEXT        NOT NULL
              CHECK (origin IN ('content_heuristic','content_llm','semantic','asserted')),
  weight      FLOAT4      NOT NULL DEFAULT 0,
  confidence  FLOAT4      NOT NULL DEFAULT 0,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source_id, target_id, rel_type)
);

CREATE INDEX IF NOT EXISTS idx_relation_source_id  ON relation (source_id);
CREATE INDEX IF NOT EXISTS idx_relation_target_id  ON relation (target_id);
CREATE INDEX IF NOT EXISTS idx_relation_confidence ON relation (confidence);
CREATE INDEX IF NOT EXISTS idx_relation_origin     ON relation (origin);
CREATE INDEX IF NOT EXISTS idx_relation_last_seen  ON relation (last_seen_at);

-- ── job ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_id    UUID        REFERENCES entity(id),
  kind         TEXT        NOT NULL CHECK (kind IN ('ingest','retune')),
  stage        TEXT        NOT NULL DEFAULT 'queued',
  progress     JSONB       NOT NULL DEFAULT '{"retry_count":0}',
  worker_pid   INT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  expires_at   TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days'
);

CREATE INDEX IF NOT EXISTS idx_job_stage      ON job (stage);
CREATE INDEX IF NOT EXISTS idx_job_kind       ON job (kind);
CREATE INDEX IF NOT EXISTS idx_job_expires_at ON job (expires_at);
CREATE INDEX IF NOT EXISTS idx_job_entity_id  ON job (entity_id);

-- ── config_state ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS config_state (
  key        TEXT        PRIMARY KEY,
  value      TEXT        NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
