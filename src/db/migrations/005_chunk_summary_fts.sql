-- Add full-text search index on chunk.summary to support hybrid vector+keyword queries.
-- The expression index mirrors the pattern used for entity.summary in migration 001.
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE tablename = 'chunk' AND indexname = 'idx_chunk_summary_fts'
  ) THEN
    CREATE INDEX idx_chunk_summary_fts
      ON chunk USING gin(to_tsvector('english', COALESCE(summary, '')));
  END IF;
END $$;
