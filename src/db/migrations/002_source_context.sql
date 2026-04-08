-- vkb migration v0.4.1 — add source_context to entity
-- Tracks the provenance context of each ingested entity.

ALTER TABLE entity
  ADD COLUMN IF NOT EXISTS source_context TEXT NOT NULL DEFAULT 'external'
    CHECK (source_context IN ('external', 'conversation', 'self_authored'));

CREATE INDEX IF NOT EXISTS idx_entity_source_context ON entity (source_context);
