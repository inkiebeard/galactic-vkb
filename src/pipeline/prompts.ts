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
};
