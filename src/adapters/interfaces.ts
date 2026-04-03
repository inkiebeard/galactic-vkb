import type { RawChunk, OrderedChunk, Section, ChunkConfig, SectionConfig, RawRelation } from '../types.js';

export interface EmbedAdapter {
  /** Embed a batch of texts. Returns one float array per input text. */
  embed(texts: string[]): Promise<number[][]>;
}

export interface LLMAdapter {
  /** Chat completion. Returns the assistant message content. */
  complete(system: string, user: string): Promise<string>;
}

export interface ChunkAdapter {
  /** Split text into raw chunks with seq numbers. */
  chunk(text: string, config: ChunkConfig): RawChunk[];
}

export interface SectionAdapter {
  /** Group ordered chunks (with embeddings) into sections. */
  section(chunks: OrderedChunk[], config: SectionConfig): Section[];
}

export interface FetchAdapter {
  /** Fetch and normalise text from a URL or file path. */
  fetch(ref: string): Promise<string>;
}

export interface RawStoreAdapter {
  write(key: string, content: string): Promise<void>;
  read(key: string): Promise<string>;
  delete(key: string): Promise<void>;
  exists(key: string): Promise<boolean>;
}

export interface HeuristicRelationExtractor {
  /** Find relationships by scanning entity text for known refs / identifiers. */
  extract(
    entityId: string,
    entityText: string,
    candidates: Array<{ id: string; ref: string | null }>,
  ): Promise<RawRelation[]>;
}

export interface LLMRelationExtractor {
  /** Use an LLM to find relationships between entity summaries. */
  extract(
    entitySummary: string,
    candidates: Array<{ id: string; summary: string }>,
  ): Promise<RawRelation[]>;
}
