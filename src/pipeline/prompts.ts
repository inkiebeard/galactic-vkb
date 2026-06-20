import * as fs from 'fs';
import { config } from '../config.js';
import { createLogger } from '../logger.js';

const log = createLogger('prompts');

// ── Built-in default prompts ──────────────────────────────────────────────────

const BUILTIN_CHUNK_SUMMARY = `Summarize the following text in 1-2 sentences. Be concise and capture the essential meaning. Do not add information not present in the text. Return only the summary text with no pretext, labels, or introductory phrases.`;

const BUILTIN_SECTION_SUMMARY = `Summarize the following section in 2-3 sentences. Capture the key topics, themes, and any important conclusions. Return only the summary text with no pretext, labels, or introductory phrases.`;

const BUILTIN_ENTITY_SUMMARY = `Write a comprehensive summary of the following document in 3-6 sentences. Include the main topics, key findings, important concepts, and overall purpose. Be thorough but concise. Return only the summary text with no pretext, labels, or introductory phrases.`;

const BUILTIN_RELATION_EXTRACT = `You are an expert at identifying relationships between documents.

Given the NEW DOCUMENT SUMMARY and a numbered list of CANDIDATE DOCUMENT SUMMARIES, identify which candidates the new document:
1. Directly references or cites
2. Conceptually depends on
3. Semantically relates to (same topic, complementary information)

Return ONLY a JSON array. Each element:
{ "target_index": <0-based number>, "rel_type": "references|depends_on|relates_to", "confidence": <0.0 to 1.0> }

Return [] if no meaningful relationships exist. Do not guess or over-relate.`;

const BUILTIN_META_TAG_EXTRACT = `Extract 3-8 concise keyword tags that best represent the main topics, domain, entities, and themes of the following document summary. Return ONLY a JSON array of lowercase short phrases (e.g. ["machine learning", "neural networks", "python"]). Return [] if no meaningful tags can be extracted.`;


const BUILTIN_SUMMARY_FAITHFULNESS = `You are a fact-checking assistant. Your task is to identify claims in a generated summary that are NOT supported by or contradict the source content.

Assess faithfulness at three levels:
- "high":   all claims in the summary are supported by the source content
- "medium": mostly accurate, but contains minor embellishments or phrasing not explicitly in the source
- "low":    contains specific factual claims absent from or directly contradicted by the source

Return ONLY valid JSON in this exact shape:
{
  "faithfulness": "high" | "medium" | "low",
  "issues": [
    { "claim": "<exact claim from summary>", "issue": "unsupported" | "contradicts", "detail": "<brief explanation>" }
  ]
}

Return { "faithfulness": "high", "issues": [] } when no issues are found. Do not flag paraphrasing or minor rewording as issues.`;

const BUILTIN_CONTRADICTION_CHECK = `You are a fact-checking assistant comparing two knowledge base entries for factual contradictions.

A contradiction means the two entries make mutually exclusive claims about the same specific fact, concept, metric, date, or event — not merely different perspectives or emphasis.

Return ONLY valid JSON in this exact shape:
{
  "contradicts": true | false,
  "severity": "high" | "medium" | "low",
  "description": "<concise description of the contradiction, or empty string if none>"
}

Severity guide:
- "high":   direct, unambiguous factual contradiction (e.g. opposite boolean claims, incompatible numeric values)
- "medium": tension or inconsistency that likely reflects an error and needs human review
- "low":    minor discrepancy that might be due to timing, scope, or framing differences

Set contradicts to false — and description to "" — when entries are merely complementary or discuss the same topic from different angles.`;

function loadOrDefault(filePath: string | undefined, builtin: string): string {
  if (!filePath) return builtin;
  try {
    return fs.readFileSync(filePath, 'utf8').trim();
  } catch {
    log.warn(`Could not load prompt file '${filePath}', using built-in.`);
    return builtin;
  }
}

export const prompts = {
  get chunkSummary(): string {
    return loadOrDefault(config.CHUNK_SUMMARY_PROMPT_FILE, BUILTIN_CHUNK_SUMMARY);
  },
  get sectionSummary(): string {
    return loadOrDefault(config.SECTION_SUMMARY_PROMPT_FILE, BUILTIN_SECTION_SUMMARY);
  },
  get entitySummary(): string {
    return loadOrDefault(config.SUMMARY_PROMPT_FILE, BUILTIN_ENTITY_SUMMARY);
  },
  get relationExtract(): string {
    return loadOrDefault(config.RELATION_EXTRACT_PROMPT_FILE, BUILTIN_RELATION_EXTRACT);
  },
  get metaTagExtract(): string {
    return BUILTIN_META_TAG_EXTRACT;
  },
  get summaryFaithfulness(): string {
    return BUILTIN_SUMMARY_FAITHFULNESS;
  },
  get contradictionCheck(): string {
    return BUILTIN_CONTRADICTION_CHECK;
  },
};
