import type { SectionAdapter } from '../interfaces.js';
import type { OrderedChunk, Section, SectionConfig } from '../../types.js';

function cosineSim(a: number[], b: number[]): number {
  let dot = 0, na = 0, nb = 0;
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    dot += a[i] * b[i];
    na  += a[i] * a[i];
    nb  += b[i] * b[i];
  }
  if (na === 0 || nb === 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

/**
 * Cuts section boundaries where cosine similarity between adjacent chunks
 * drops below the threshold (similarity valley).
 */
export class SimilarityValleySectionAdapter implements SectionAdapter {
  section(chunks: OrderedChunk[], cfg: SectionConfig): Section[] {
    if (chunks.length === 0) return [];

    const sorted = [...chunks].sort((a, b) => a.seq - b.seq);
    const sections: Section[] = [];
    let current: string[] = [sorted[0].id];

    for (let i = 1; i < sorted.length; i++) {
      const sim = cosineSim(sorted[i - 1].embedding, sorted[i].embedding);
      const cutBySimilarity = sim < cfg.threshold;
      const cutByCap = current.length >= cfg.maxSize;

      if (cutBySimilarity || cutByCap) {
        sections.push({ chunk_ids: current, seq: sections.length });
        current = [sorted[i].id];
      } else {
        current.push(sorted[i].id);
      }
    }

    if (current.length > 0) {
      sections.push({ chunk_ids: current, seq: sections.length });
    }

    return sections;
  }
}
