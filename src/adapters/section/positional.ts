import type { SectionAdapter } from '../interfaces.js';
import type { OrderedChunk, Section, SectionConfig } from '../../types.js';

/**
 * Groups chunks into fixed positional windows of SECTION_WINDOW_SIZE,
 * capped by SECTION_MAX_SIZE. Deterministic and LLM-free.
 */
export class PositionalSectionAdapter implements SectionAdapter {
  section(chunks: OrderedChunk[], cfg: SectionConfig): Section[] {
    if (chunks.length === 0) return [];

    const sorted = [...chunks].sort((a, b) => a.seq - b.seq);
    const windowSize = Math.min(cfg.windowSize, cfg.maxSize);
    const sections: Section[] = [];

    for (let i = 0; i < sorted.length; i += windowSize) {
      const slice = sorted.slice(i, i + windowSize);
      sections.push({
        chunk_ids: slice.map(c => c.id),
        seq: sections.length,
      });
    }

    return sections;
  }
}
