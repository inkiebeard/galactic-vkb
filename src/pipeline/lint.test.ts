/**
 * Tests for lint pipeline helpers.
 *
 * Strategy:
 *   - parseFaithfulnessResponse / parseContradictionResponse are pure JSON
 *     parsers — test all the LLM output shapes they must handle (clean JSON,
 *     markdown code fences, missing fields, garbage).
 *   - Prompt existence — smoke-test that all prompts are non-empty strings
 *     containing key structural words, so a future accidental deletion doesn't
 *     go undetected.
 *
 * Intentionally excluded from this file:
 *   - runLintPipeline (requires Postgres + Ollama — integration/E2E territory)
 *   - checkOrphans / checkFaithfulness / checkContradictions (same)
 */

import { describe, it, expect } from 'vitest';
import {
  parseFaithfulnessResponse,
  parseContradictionResponse,
} from './lint.js';
import { prompts } from './prompts.js';

// ── parseFaithfulnessResponse ─────────────────────────────────────────────────

describe('parseFaithfulnessResponse', () => {
  it('parses a clean high-faithfulness response', () => {
    const raw = JSON.stringify({ faithfulness: 'high', issues: [] });
    const result = parseFaithfulnessResponse(raw);
    expect(result).not.toBeNull();
    expect(result!.faithfulness).toBe('high');
    expect(result!.issues).toHaveLength(0);
  });

  it('parses a low-faithfulness response with issues', () => {
    const raw = JSON.stringify({
      faithfulness: 'low',
      issues: [
        { claim: 'Revenue doubled YoY', issue: 'unsupported', detail: 'Source never mentions YoY comparison.' },
        { claim: 'Table has 10M rows', issue: 'contradicts', detail: 'Source says 2M rows.' },
      ],
    });
    const result = parseFaithfulnessResponse(raw);
    expect(result!.faithfulness).toBe('low');
    expect(result!.issues).toHaveLength(2);
    expect(result!.issues[0].issue).toBe('unsupported');
    expect(result!.issues[1].issue).toBe('contradicts');
  });

  it('strips markdown code fences before parsing', () => {
    const raw = '```json\n{"faithfulness":"medium","issues":[]}\n```';
    const result = parseFaithfulnessResponse(raw);
    expect(result).not.toBeNull();
    expect(result!.faithfulness).toBe('medium');
  });

  it('strips plain code fences (no language tag)', () => {
    const raw = '```\n{"faithfulness":"high","issues":[]}\n```';
    const result = parseFaithfulnessResponse(raw);
    expect(result).not.toBeNull();
  });

  it('returns null for completely invalid JSON', () => {
    expect(parseFaithfulnessResponse('not json at all')).toBeNull();
  });

  it('returns null when faithfulness field is missing', () => {
    const raw = JSON.stringify({ issues: [] });
    expect(parseFaithfulnessResponse(raw)).toBeNull();
  });

  it('returns null when issues is not an array', () => {
    const raw = JSON.stringify({ faithfulness: 'high', issues: 'none' });
    // Parser validates Array.isArray(issues) — non-array LLM output → null
    expect(parseFaithfulnessResponse(raw)).toBeNull();
  });

  it('returns null for an empty string', () => {
    expect(parseFaithfulnessResponse('')).toBeNull();
  });

  it('returns null for a JSON array (not an object)', () => {
    expect(parseFaithfulnessResponse('[]')).toBeNull();
  });

  it('handles extra whitespace around the JSON', () => {
    const raw = '  \n  {"faithfulness":"high","issues":[]}  \n  ';
    const result = parseFaithfulnessResponse(raw);
    expect(result).not.toBeNull();
    expect(result!.faithfulness).toBe('high');
  });
});

// ── parseContradictionResponse ────────────────────────────────────────────────

describe('parseContradictionResponse', () => {
  it('parses a no-contradiction response', () => {
    const raw = JSON.stringify({ contradicts: false, severity: 'low', description: '' });
    const result = parseContradictionResponse(raw);
    expect(result).not.toBeNull();
    expect(result!.contradicts).toBe(false);
  });

  it('parses a confirmed contradiction', () => {
    const raw = JSON.stringify({
      contradicts: true,
      severity: 'high',
      description: 'Entry A says table has 2M rows; Entry B says 10M rows.',
    });
    const result = parseContradictionResponse(raw);
    expect(result!.contradicts).toBe(true);
    expect(result!.severity).toBe('high');
    expect(result!.description).toContain('Entry A');
  });

  it('strips json code fences', () => {
    const raw = '```json\n{"contradicts":false,"severity":"low","description":""}\n```';
    const result = parseContradictionResponse(raw);
    expect(result).not.toBeNull();
    expect(result!.contradicts).toBe(false);
  });

  it('returns null for non-JSON response', () => {
    expect(parseContradictionResponse('These two entries do not contradict each other.')).toBeNull();
  });

  it('returns null when contradicts field is missing', () => {
    const raw = JSON.stringify({ severity: 'low', description: '' });
    expect(parseContradictionResponse(raw)).toBeNull();
  });

  it('returns null when contradicts is not a boolean', () => {
    const raw = JSON.stringify({ contradicts: 'yes', severity: 'low', description: '' });
    // contradicts is a string, not boolean → parser returns null
    expect(parseContradictionResponse(raw)).toBeNull();
  });

  it('returns null for a JSON array', () => {
    expect(parseContradictionResponse('[]')).toBeNull();
  });

  it('handles all three severity levels', () => {
    for (const severity of ['high', 'medium', 'low'] as const) {
      const raw = JSON.stringify({ contradicts: true, severity, description: 'test' });
      const result = parseContradictionResponse(raw);
      expect(result!.severity).toBe(severity);
    }
  });
});

// ── prompts smoke tests ───────────────────────────────────────────────────────

describe('prompts', () => {
  it('summaryFaithfulness is a non-empty string', () => {
    expect(typeof prompts.summaryFaithfulness).toBe('string');
    expect(prompts.summaryFaithfulness.length).toBeGreaterThan(50);
  });

  it('summaryFaithfulness mentions faithfulness levels', () => {
    const p = prompts.summaryFaithfulness;
    expect(p).toContain('"high"');
    expect(p).toContain('"medium"');
    expect(p).toContain('"low"');
  });

  it('summaryFaithfulness asks for JSON output', () => {
    expect(prompts.summaryFaithfulness.toLowerCase()).toContain('json');
  });

  it('contradictionCheck is a non-empty string', () => {
    expect(typeof prompts.contradictionCheck).toBe('string');
    expect(prompts.contradictionCheck.length).toBeGreaterThan(50);
  });

  it('contradictionCheck mentions contradicts field', () => {
    expect(prompts.contradictionCheck).toContain('"contradicts"');
  });

  it('contradictionCheck asks for JSON output', () => {
    expect(prompts.contradictionCheck.toLowerCase()).toContain('json');
  });

  it('metaTagExtract is a non-empty string', () => {
    expect(typeof prompts.metaTagExtract).toBe('string');
    expect(prompts.metaTagExtract.length).toBeGreaterThan(20);
  });

  it('relationExtract is a non-empty string', () => {
    expect(typeof prompts.relationExtract).toBe('string');
    expect(prompts.relationExtract.length).toBeGreaterThan(20);
  });

  it('entitySummary is a non-empty string', () => {
    expect(typeof prompts.entitySummary).toBe('string');
    expect(prompts.entitySummary.length).toBeGreaterThan(20);
  });
});
