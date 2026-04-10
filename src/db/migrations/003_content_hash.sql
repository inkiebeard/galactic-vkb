-- vkb migration v0.4.2 — content hashing and entity versioning
-- Adds deterministic SHA-256 fingerprinting to detect unchanged content on
-- re-ingest and to link successive versions of the same document together.

-- content_hash: lower-hex SHA-256 of the raw text body fetched during Phase 1.
-- NULL until Phase 1 completes.  Indexed for fast exact-match dedup checks.
ALTER TABLE entity
  ADD COLUMN IF NOT EXISTS content_hash TEXT;

-- previous_version_id: FK to the entity this one supersedes, when a document
-- is re-submitted under the same ref but the content has changed.
-- NULL for first-ever ingests and for text-only (no-ref) entities.
ALTER TABLE entity
  ADD COLUMN IF NOT EXISTS previous_version_id UUID
    REFERENCES entity(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_entity_content_hash ON entity (content_hash);
CREATE INDEX IF NOT EXISTS idx_entity_ref           ON entity (ref);
CREATE INDEX IF NOT EXISTS idx_entity_prev_version  ON entity (previous_version_id);
