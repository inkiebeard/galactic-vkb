import type { HeuristicRelationExtractor } from '../interfaces.js';
import type { RawRelation } from '../../types.js';

// Url/path patterns we consider strong-signal references
const URL_PATTERN = /https?:\/\/[^\s"'<>)]+/gi;
const PATH_PATTERN = /(?:^|\s)(\/[\w./-]+|\.{1,2}\/[\w./-]+)/gm;

export class HeuristicExtractor implements HeuristicRelationExtractor {
  async extract(
    _entityId: string,
    entityText: string,
    candidates: Array<{ id: string; ref: string | null }>,
  ): Promise<RawRelation[]> {
    const relations: RawRelation[] = [];

    // Collect all URLs and paths mentioned in the text
    const mentionedUrls = new Set((entityText.match(URL_PATTERN) ?? []).map(u => u.toLowerCase()));
    const mentionedPaths = new Set((entityText.match(PATH_PATTERN) ?? []).map(p => p.trim().toLowerCase()));

    for (const candidate of candidates) {
      if (!candidate.ref) continue;

      const ref = candidate.ref.toLowerCase();

      const matched =
        mentionedUrls.has(ref) ||
        mentionedPaths.has(ref) ||
        entityText.toLowerCase().includes(ref);

      if (matched) {
        relations.push({
          target_entity_id: candidate.id,
          rel_type: 'references',
          confidence: 1.0,
        });
      }
    }

    return relations;
  }
}
