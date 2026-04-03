import { config } from '../config.js';
import type {
  EmbedAdapter, LLMAdapter, ChunkAdapter, SectionAdapter,
  FetchAdapter, RawStoreAdapter, HeuristicRelationExtractor, LLMRelationExtractor,
} from './interfaces.js';
import { OllamaEmbedAdapter } from './embed/ollama.js';
import { OllamaLLMAdapter } from './llm/ollama.js';
import { SlidingWindowChunker } from './chunk/sliding-window.js';
import { SimilarityValleySectionAdapter } from './section/similarity-valley.js';
import { PositionalSectionAdapter } from './section/positional.js';
import { ReadabilityFetcher } from './fetch/readability.js';
import { FilesystemRawStore } from './rawstore/filesystem.js';
import { HeuristicExtractor } from './relation/heuristic.js';
import { OllamaRelationExtractor } from './relation/llm.js';

export interface Adapters {
  embed: EmbedAdapter;
  llm: LLMAdapter;
  chunk: ChunkAdapter;
  section: SectionAdapter;
  fetch: FetchAdapter;
  rawstore: RawStoreAdapter;
  heuristicExtractor: HeuristicRelationExtractor;
  llmExtractor: LLMRelationExtractor;
}

let _adapters: Adapters | null = null;

export function getAdapters(): Adapters {
  if (_adapters) return _adapters;

  const embed = new OllamaEmbedAdapter(config.OLLAMA_BASE_URL, config.EMBED_MODEL);
  const llm   = new OllamaLLMAdapter(config.OLLAMA_BASE_URL, config.LLM_MODEL);

  const chunk: ChunkAdapter = new SlidingWindowChunker();

  const section: SectionAdapter =
    config.SECTION_STRATEGY === 'positional'
      ? new PositionalSectionAdapter()
      : new SimilarityValleySectionAdapter();

  const fetchAdapter: FetchAdapter = new ReadabilityFetcher();

  let rawstore: RawStoreAdapter;
  if (config.RAWSTORE_ADAPTER === 's3') {
    throw new Error(
      'S3RawStore is not yet implemented. Set RAWSTORE_ADAPTER=filesystem or implement S3RawStore.',
    );
  } else {
    rawstore = new FilesystemRawStore(config.RAWSTORE_PATH);
  }

  const heuristicExtractor = new HeuristicExtractor();
  const llmExtractor       = new OllamaRelationExtractor(llm);

  _adapters = { embed, llm, chunk, section, fetch: fetchAdapter, rawstore, heuristicExtractor, llmExtractor };
  return _adapters;
}

/** Reset adapter singletons (used in tests or after config change). */
export function resetAdapters(): void {
  _adapters = null;
}
