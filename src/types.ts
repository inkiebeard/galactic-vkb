// ── Domain row types ──────────────────────────────────────────────────────────

export type EntityStatus = 'pending' | 'processing' | 'ready' | 'error';

/**
 * The provenance context of an ingested entity:
 * - `external`      — third-party source (URLs, epubs, articles, docs) — **default**
 * - `conversation`  — transcript of a meeting, chat history with an AI agent, etc.
 * - `self_authored` — content created by the user (blog post, note, journal entry, vlog, etc.)
 */
export type SourceContext = 'external' | 'conversation' | 'self_authored';
export type JobStage =
  | 'queued' | 'fetching' | 'chunking' | 'embedding'
  | 'sectioning' | 'summarising' | 'extracting' | 'done' | 'error';
export type JobKind = 'ingest' | 'retune';
export type RelationOrigin = 'content_heuristic' | 'content_llm' | 'semantic' | 'asserted';
export type RelationKind = 'chunk' | 'entity';

export interface Entity {
  id: string;
  type: string;
  ref: string | null;
  source_context: SourceContext;
  raw_store_key: string | null;
  content_hash: string | null;
  previous_version_id: string | null;
  summary: string | null;
  summary_version: number;
  meta: Record<string, unknown>;
  status: EntityStatus;
  created_at: Date;
  updated_at: Date;
}

export interface Chunk {
  id: string;
  entity_id: string;
  seq: number;
  raw_store_key: string | null;
  summary: string | null;
  embedding: number[] | null;
  embed_model: string | null;
  embed_version: number;
  embedded_at: Date | null;
}

export interface SectionSummary {
  id: string;
  entity_id: string;
  chunk_ids: string[];
  seq: number;
  summary: string;
  strategy: string;
  created_at: Date;
}

export interface Relation {
  id: string;
  source_id: string;
  target_id: string;
  source_kind: RelationKind;
  target_kind: RelationKind;
  rel_type: string;
  origin: RelationOrigin;
  weight: number;
  confidence: number;
  last_seen_at: Date;
  created_at: Date;
}

export interface JobProgress {
  chunks_total?: number;
  chunks_done?: number;
  sections_done?: number;
  relations_added?: number;
  retry_count: number;
  error_detail?: string;
  from_rawstore?: boolean;
  /** When true, bypass all self-heal checkpoints: re-fetch raw, re-write entity.md,
   *  re-chunk and re-embed from scratch. Does not delete the entity or rawstore key. */
  force_reingest?: boolean;
  /** Total number of LLM summarisation calls for this job (chunks + sections + 1 entity). */
  summary_steps_total?: number;
  /** How many summarisation calls have completed so far. */
  summary_steps_done?: number;
  /** Set when the ingest was skipped because content is identical to a prior version. */
  skipped?: boolean;
  /** Entity ID of the existing entity whose content matched. */
  duplicate_of?: string;
}

export interface Job {
  id: string;
  entity_id: string | null;
  kind: JobKind;
  stage: JobStage;
  progress: JobProgress;
  worker_pid: number | null;
  created_at: Date;
  completed_at: Date | null;
  expires_at: Date;
}

export interface ConfigState {
  key: string;
  value: string;
  updated_at: Date;
}

// ── Pipeline payload types ────────────────────────────────────────────────────

export interface IngestPayload {
  type: string;
  text?: string;
  ref?: string;
  source_context?: SourceContext;
  meta?: Record<string, unknown>;
}

export interface QueryPayload {
  text: string;
  k?: number;
  type?: string;
  threshold?: number;
  include_sections?: boolean;
}

export interface RelationRef {
  target_id: string;
  target_kind: RelationKind;
  target_summary: string;
  rel_type: string;
  origin: RelationOrigin;
  confidence: number;
  weight: number;
}

export interface QueryResultItem {
  chunk_id: string;
  chunk_summary: string;
  entity_id: string;
  entity_type: string;
  entity_summary: string;
  similarity: number;
  section_summary?: string;
  raw_store_key: string;
  relations: RelationRef[];
}

// ── Adapter low-level types ───────────────────────────────────────────────────

export interface ChunkConfig {
  size: number;
  overlap: number;
}

export interface SectionConfig {
  threshold: number;
  windowSize: number;
  maxSize: number;
}

export interface RawChunk {
  text: string;
  seq: number;
}

export interface OrderedChunk {
  id: string;
  seq: number;
  text: string;
  embedding: number[];
}

export interface Section {
  chunk_ids: string[];
  seq: number;
}

export interface RawRelation {
  target_entity_id: string;
  rel_type: string;
  confidence: number;
}

// ── Worker IPC ────────────────────────────────────────────────────────────────

export interface WorkerMessage {
  type: 'stage_change' | 'progress' | 'complete' | 'error' | 'heartbeat';
  job_id: string;
  stage?: JobStage;
  payload?: unknown;
}
