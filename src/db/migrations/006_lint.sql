-- Migration 006: lint_finding table + expand job.kind constraint to include 'lint'.

-- ── 1. Expand job.kind CHECK ─────────────────────────────────────────────────
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname    = 'job_kind_check'
      AND conrelid   = 'job'::regclass
      AND pg_get_constraintdef(oid) LIKE '%lint%'
  ) THEN
    ALTER TABLE job DROP CONSTRAINT IF EXISTS job_kind_check;
    ALTER TABLE job ADD CONSTRAINT job_kind_check
      CHECK (kind IN ('ingest', 'retune', 'finetune', 'lint'));
  END IF;
END;
$$;

-- ── 2. lint_finding table ────────────────────────────────────────────────────
-- Stores findings produced by the lint pipeline. Each row is one issue
-- discovered against one (or two) entities. Findings persist across runs so
-- users can track which issues have been resolved or dismissed.
CREATE TABLE IF NOT EXISTS lint_finding (
  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  -- What kind of issue was found.
  -- 'orphan'             — entity has no inbound or outbound relations
  -- 'unfaithful_summary' — LLM summary contains claims not supported by source
  -- 'contradiction'      — two entities make contradictory factual claims
  kind              TEXT        NOT NULL
                    CHECK (kind IN ('orphan', 'unfaithful_summary', 'contradiction')),
  -- Estimated impact. 'high' means likely wrong / misleading; 'low' is cosmetic.
  severity          TEXT        NOT NULL
                    CHECK (severity IN ('high', 'medium', 'low')),
  -- Primary entity involved in the finding.
  entity_id         UUID        REFERENCES entity(id) ON DELETE CASCADE,
  -- Secondary entity (for contradiction findings only).
  related_entity_id UUID        REFERENCES entity(id) ON DELETE CASCADE,
  -- Human-readable description of the finding.
  description       TEXT        NOT NULL,
  -- Structured detail: unsupported claims, contradiction description, etc.
  detail            JSONB       NOT NULL DEFAULT '{}'::jsonb,
  -- Workflow state: 'open' = needs review; 'resolved' = fixed; 'dismissed' = won't fix.
  status            TEXT        NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'resolved', 'dismissed')),
  -- The lint job that produced this finding.
  job_id            UUID        REFERENCES job(id) ON DELETE SET NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS lint_finding_entity_id      ON lint_finding (entity_id);
CREATE INDEX IF NOT EXISTS lint_finding_rel_entity_id  ON lint_finding (related_entity_id);
CREATE INDEX IF NOT EXISTS lint_finding_kind_status    ON lint_finding (kind, status);
CREATE INDEX IF NOT EXISTS lint_finding_job_id         ON lint_finding (job_id);
