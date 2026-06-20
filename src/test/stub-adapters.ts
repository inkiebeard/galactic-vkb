/**
 * Stub adapters for integration tests.
 *
 * These are lightweight in-process implementations that:
 *   - Produce deterministic, repeatable outputs (embed, LLM)
 *   - Require no external services (no Ollama, no S3)
 *   - Use a real FilesystemRawStore pointed at a temp directory
 *
 * Usage:
 *   const rawstoreDir = await mkdtemp(join(tmpdir(), 'vkb-test-'));
 *   const adapters = makeStubAdapters(rawstoreDir);
 *   setAdapters(adapters);
 */

import { createHash } from 'crypto';
import { SlidingWindowChunker } from '../adapters/chunk/sliding-window.js';
import { PositionalSectionAdapter } from '../adapters/section/positional.js';
import { FilesystemRawStore } from '../adapters/rawstore/filesystem.js';
import type {
  EmbedAdapter, LLMAdapter, FetchAdapter, RawStoreAdapter,
  HeuristicRelationExtractor, LLMRelationExtractor,
  ChunkAdapter, SectionAdapter,
} from '../adapters/interfaces.js';
import type { Adapters } from '../adapters/registry.js';
import type { RawRelation } from '../types.js';

// ── Embedding dimension used by vkb (matches EMBED_DIM default) ──────────────
const EMBED_DIM = 768;

/**
 * Produces a deterministic unit vector for each input text by seeding from a
 * SHA-256 hash. Different texts produce genuinely different vectors, so cosine
 * similarity queries return meaningful ordering in tests.
 */
class StubEmbedAdapter implements EmbedAdapter {
  async embed(texts: string[]): Promise<number[][]> {
    return texts.map(text => {
      const hash = createHash('sha256').update(text).digest();
      const v: number[] = new Array(EMBED_DIM);
      for (let i = 0; i < EMBED_DIM; i++) {
        // Spread the 32-byte hash across 768 dims by cycling with a salt
        const byte = hash[(i * 7 + Math.floor(i / 32)) % 32];
        v[i] = (byte - 128) / 128;
      }
      const mag = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
      return v.map(x => x / mag);
    });
  }
}

/**
 * Returns deterministic completions shaped to match what the pipeline expects
 * for each prompt type. Detection is based on recognisable phrases in the
 * system prompt.
 */
class StubLLMAdapter implements LLMAdapter {
  async complete(system: string, user: string): Promise<string> {
    const s = system.toLowerCase();

    // Lint: summary faithfulness check
    if (s.includes('faithfulness') || s.includes('fact-checking') && s.includes('summary')) {
      return JSON.stringify({ faithfulness: 'high', issues: [] });
    }

    // Lint: contradiction check
    if (s.includes('contradiction') || s.includes('contradicts')) {
      return JSON.stringify({ contradicts: false, severity: 'low', description: '' });
    }

    // Meta-tag extraction → JSON array of strings
    if (s.includes('keyword tag') || s.includes('keyword tags')) {
      return JSON.stringify(['stub-tag', 'test']);
    }

    // Relation extraction → empty JSON array (no LLM-derived relations)
    if (s.includes('relationship') || s.includes('relationships')) {
      return '[]';
    }

    // Entity / chunk / section summary → plain text
    const snippet = user.replace(/\s+/g, ' ').slice(0, 80);
    return `Stub summary of: ${snippet}`;
  }
}

/** Returns no LLM-derived relations — keeps the graph clean for tests. */
class StubLLMRelationExtractor implements LLMRelationExtractor {
  async extract(_summary: string, _candidates: Array<{ id: string; summary: string }>): Promise<RawRelation[]> {
    return [];
  }
}

/** Returns no heuristic relations. */
class StubHeuristicExtractor implements HeuristicRelationExtractor {
  async extract(
    _entityId: string,
    _entityText: string,
    _candidates: Array<{ id: string; ref: string | null }>,
  ): Promise<RawRelation[]> {
    return [];
  }
}

/** Minimal fetch adapter — not called by any test (tests pass text directly). */
class StubFetchAdapter implements FetchAdapter {
  async fetch(ref: string): Promise<string> {
    return `Fetched stub content for: ${ref}`;
  }
}

/**
 * Build a complete Adapters bundle for integration tests.
 *
 * @param rawstoreDir  Absolute path to a writable directory used as the
 *                     rawstore. Call `rm(rawstoreDir, { recursive: true })`
 *                     in afterAll to clean up.
 */
export function makeStubAdapters(rawstoreDir: string): Adapters {
  return {
    embed:               new StubEmbedAdapter(),
    llm:                 new StubLLMAdapter(),
    chunk:               new SlidingWindowChunker(),
    section:             new PositionalSectionAdapter(),
    fetch:               new StubFetchAdapter(),
    rawstore:            new FilesystemRawStore(rawstoreDir),
    heuristicExtractor:  new StubHeuristicExtractor(),
    llmExtractor:        new StubLLMRelationExtractor(),
  };
}
