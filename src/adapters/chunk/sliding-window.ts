import type { ChunkAdapter } from '../interfaces.js';
import type { RawChunk, ChunkConfig } from '../../types.js';

/**
 * SlidingWindowChunker splits text into overlapping windows.
 * Token count is approximated as word count (1 word ≈ 1.3 tokens).
 * A minimum chunk size filter skips degenerate fragments near boundaries.
 */
export class SlidingWindowChunker implements ChunkAdapter {
  private static readonly MIN_CHUNK_WORDS = 20;

  chunk(text: string, cfg: ChunkConfig): RawChunk[] {
    const words = text.split(/\s+/).filter(Boolean);
    if (words.length === 0) return [];

    const chunks: RawChunk[] = [];
    const step = Math.max(1, cfg.size - cfg.overlap);
    let seq = 0;

    for (let start = 0; start < words.length; start += step) {
      const slice = words.slice(start, start + cfg.size);
      if (slice.length < SlidingWindowChunker.MIN_CHUNK_WORDS && chunks.length > 0) {
        // Append short tail to the previous chunk to avoid degenerate summaries
        const last = chunks[chunks.length - 1];
        last.text = last.text + ' ' + slice.join(' ');
        break;
      }
      chunks.push({ text: slice.join(' '), seq: seq++ });
      if (start + cfg.size >= words.length) break;
    }

    return chunks;
  }
}
