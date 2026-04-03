import type { LLMRelationExtractor } from '../interfaces.js';
import type { LLMAdapter } from '../interfaces.js';
import type { RawRelation } from '../../types.js';
import { prompts } from '../../pipeline/prompts.js';
import { createLogger } from '../../logger.js';

const log = createLogger('relation-llm');

interface ExtractedRelation {
  target_index: number;
  rel_type: string;
  confidence: number;
}

export class OllamaRelationExtractor implements LLMRelationExtractor {
  constructor(private readonly llm: LLMAdapter) {}

  async extract(
    entitySummary: string,
    candidates: Array<{ id: string; summary: string }>,
  ): Promise<RawRelation[]> {
    if (candidates.length === 0) return [];

    const candidateBlock = candidates
      .map((c, i) => `[${i}] ${c.summary}`)
      .join('\n\n');

    const userPrompt =
      `NEW DOCUMENT SUMMARY:\n${entitySummary}\n\nCANDIDATE DOCUMENT SUMMARIES:\n${candidateBlock}`;

    let raw: string;
    try {
      raw = await this.llm.complete(prompts.relationExtract, userPrompt);
    } catch (e) {
      log.warn('LLM call failed:', (e as Error).message);
      return [];
    }

    // Extract JSON array from LLM response (model may wrap it in markdown)
    const jsonMatch = raw.match(/\[[\s\S]*\]/);
    if (!jsonMatch) return [];

    let parsed: ExtractedRelation[];
    try {
      parsed = JSON.parse(jsonMatch[0]) as ExtractedRelation[];
    } catch {
      return [];
    }

    const relations: RawRelation[] = [];
    for (const item of parsed) {
      const target = candidates[item.target_index];
      if (!target) continue;
      const confidence = Math.min(1, Math.max(0, Number(item.confidence) || 0));
      if (confidence < 0.3) continue; // ignore low-quality extractions

      relations.push({
        target_entity_id: target.id,
        rel_type: item.rel_type ?? 'relates_to',
        confidence,
      });
    }

    return relations;
  }
}
