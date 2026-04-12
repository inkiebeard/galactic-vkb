-- Migration 004: expand the job.kind CHECK constraint to include 'finetune'.
-- The original constraint (001_initial) only allowed ('ingest','retune').
-- This migration is idempotent: it only replaces the constraint when 'finetune'
-- is not already present in the definition.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname      = 'job_kind_check'
      AND conrelid     = 'job'::regclass
      AND pg_get_constraintdef(oid) LIKE '%finetune%'
  ) THEN
    ALTER TABLE job DROP CONSTRAINT IF EXISTS job_kind_check;
    ALTER TABLE job ADD CONSTRAINT job_kind_check
      CHECK (kind IN ('ingest', 'retune', 'finetune'));
  END IF;
END;
$$;
