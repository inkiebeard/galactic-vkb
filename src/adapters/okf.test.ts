/**
 * Tests for the OKF (Open Knowledge Format) bundle adapter.
 *
 * Strategy:
 *   - Pure helpers (normaliseTags, okfRelType, parseFrontmatter, nearestHeading,
 *     extractLinks) → unit tests with no I/O.
 *   - parseOkfBundle → integration tests against real temp directories created
 *     with Node's fs/promises API. No mocking needed: the function is pure I/O
 *     with no DB or LLM dependencies.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtemp, writeFile, mkdir, rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

import {
  normaliseTags,
  okfRelType,
  parseFrontmatter,
  nearestHeading,
  extractLinks,
  parseOkfBundle,
} from './okf.js';
import type { OkfLink } from './okf.js';

// ── normaliseTags ─────────────────────────────────────────────────────────────

describe('normaliseTags', () => {
  it('returns [] for undefined', () => {
    expect(normaliseTags(undefined)).toEqual([]);
  });

  it('returns [] for empty string', () => {
    expect(normaliseTags('')).toEqual([]);
  });

  it('splits a comma-separated string', () => {
    expect(normaliseTags('a, b, c')).toEqual(['a', 'b', 'c']);
  });

  it('trims whitespace from string items', () => {
    expect(normaliseTags('  foo ,  bar  ')).toEqual(['foo', 'bar']);
  });

  it('filters empty segments from string', () => {
    expect(normaliseTags('a,,b')).toEqual(['a', 'b']);
  });

  it('returns an array unchanged (converted to strings)', () => {
    expect(normaliseTags(['x', 'y', 'z'])).toEqual(['x', 'y', 'z']);
  });

  it('trims whitespace from array items', () => {
    expect(normaliseTags(['  foo ', 'bar '])).toEqual(['foo', 'bar']);
  });

  it('filters blank array items', () => {
    expect(normaliseTags(['a', '', 'b'])).toEqual(['a', 'b']);
  });
});

// ── okfRelType ────────────────────────────────────────────────────────────────

describe('okfRelType', () => {
  const link = (section?: string): OkfLink => ({
    targetBundlePath: 'other.md',
    linkText: 'other',
    section,
  });

  it('returns okf:references when no section', () => {
    expect(okfRelType(link())).toBe('okf:references');
  });

  it('lowercases a simple heading', () => {
    expect(okfRelType(link('Joins'))).toBe('okf:joins');
  });

  it('replaces spaces with underscores', () => {
    expect(okfRelType(link('Related Metrics'))).toBe('okf:related_metrics');
  });

  it('collapses multiple non-alnum runs into single underscore', () => {
    expect(okfRelType(link('See Also (links)'))).toBe('okf:see_also_links');
  });

  it('trims leading and trailing underscores', () => {
    expect(okfRelType(link('---'))).toBe('okf:references');
  });

  it('handles mixed case and punctuation', () => {
    expect(okfRelType(link('Used By / Consumers'))).toBe('okf:used_by_consumers');
  });
});

// ── nearestHeading ────────────────────────────────────────────────────────────

describe('nearestHeading', () => {
  const md = `# Alpha

Some text.

## Beta

More text here.

### Gamma

Last text.`;

  it('returns undefined when no heading exists before pos', () => {
    expect(nearestHeading(md, 0)).toBeUndefined();
  });

  it('returns the heading immediately above the position', () => {
    const pos = md.indexOf('Some text.');
    expect(nearestHeading(md, pos)).toBe('Alpha');
  });

  it('returns the closest heading for nested headings', () => {
    const pos = md.indexOf('Last text.');
    expect(nearestHeading(md, pos)).toBe('Gamma');
  });

  it('returns the last heading before the given position', () => {
    const pos = md.indexOf('More text here.');
    expect(nearestHeading(md, pos)).toBe('Beta');
  });
});

// ── parseFrontmatter ──────────────────────────────────────────────────────────

describe('parseFrontmatter', () => {
  it('returns empty frontmatter and full body when no YAML block', () => {
    const { frontmatter, body } = parseFrontmatter('# Title\n\nBody text.');
    expect(frontmatter).toEqual({});
    expect(body).toContain('# Title');
  });

  it('parses basic frontmatter fields', () => {
    const input = `---
type: BigQuery Table
title: Orders
description: Fact table for orders
tags: sales, revenue
---
# Body
`;
    const { frontmatter, body } = parseFrontmatter(input);
    expect(frontmatter.type).toBe('BigQuery Table');
    expect(frontmatter.title).toBe('Orders');
    expect(frontmatter.description).toBe('Fact table for orders');
    expect(frontmatter.tags).toBe('sales, revenue');
    expect(body).toContain('# Body');
    expect(body).not.toContain('type:');
  });

  it('parses array tags', () => {
    const input = `---
tags:
  - machine learning
  - python
---
body
`;
    const { frontmatter } = parseFrontmatter(input);
    expect(Array.isArray(frontmatter.tags)).toBe(true);
    expect(frontmatter.tags).toContain('machine learning');
  });

  it('handles malformed YAML gracefully — returns empty frontmatter', () => {
    const input = `---
key: [unclosed bracket
---
body
`;
    const { frontmatter, body } = parseFrontmatter(input);
    expect(frontmatter).toEqual({});
    expect(body).toContain('body');
  });

  it('preserves extension fields', () => {
    const input = `---
type: Metric
custom_field: foobar
---
`;
    const { frontmatter } = parseFrontmatter(input);
    expect(frontmatter.custom_field).toBe('foobar');
  });
});

// ── extractLinks ──────────────────────────────────────────────────────────────

describe('extractLinks', () => {
  const BUNDLE = '/bundle';
  const FILE   = '/bundle/docs/source.md';

  it('returns empty array when body has no links', () => {
    expect(extractLinks('No links here.', FILE, BUNDLE)).toEqual([]);
  });

  it('extracts a relative .md link', () => {
    const body = '[Orders](../tables/orders.md)';
    const links = extractLinks(body, FILE, BUNDLE);
    expect(links).toHaveLength(1);
    expect(links[0].targetBundlePath).toBe('tables/orders.md');
    expect(links[0].linkText).toBe('Orders');
  });

  it('skips external https:// links', () => {
    const body = '[Google](https://google.com)';
    expect(extractLinks(body, FILE, BUNDLE)).toHaveLength(0);
  });

  it('skips same-file anchor links', () => {
    const body = '[Section](#heading)';
    expect(extractLinks(body, FILE, BUNDLE)).toHaveLength(0);
  });

  it('skips mailto: links', () => {
    const body = '[Email](mailto:test@example.com)';
    expect(extractLinks(body, FILE, BUNDLE)).toHaveLength(0);
  });

  it('skips non-.md relative links', () => {
    const body = '[Image](../images/chart.png)';
    expect(extractLinks(body, FILE, BUNDLE)).toHaveLength(0);
  });

  it('deduplicates multiple links to the same target', () => {
    const body = '[A](../tables/orders.md) and [B](../tables/orders.md)';
    const links = extractLinks(body, FILE, BUNDLE);
    expect(links).toHaveLength(1);
  });

  it('strips anchors from .md links before resolving', () => {
    const body = '[Orders](../tables/orders.md#section)';
    const links = extractLinks(body, FILE, BUNDLE);
    expect(links).toHaveLength(1);
    expect(links[0].targetBundlePath).toBe('tables/orders.md');
  });

  it('skips links that escape the bundle root', () => {
    const body = '[Escape](../../outside.md)';
    expect(extractLinks(body, FILE, BUNDLE)).toHaveLength(0);
  });

  it('annotates links with the nearest section heading', () => {
    const body = `## Joins\n\nSee [Orders](../tables/orders.md) for details.`;
    const links = extractLinks(body, FILE, BUNDLE);
    expect(links[0].section).toBe('Joins');
  });

  it('sets section to undefined when no heading precedes the link', () => {
    const body = `See [Orders](../tables/orders.md) for details.\n\n## Joins`;
    const links = extractLinks(body, FILE, BUNDLE);
    expect(links[0].section).toBeUndefined();
  });
});

// ── parseOkfBundle (integration) ─────────────────────────────────────────────

describe('parseOkfBundle', () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'vkb-okf-test-'));
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  it('throws when the directory does not exist', async () => {
    await expect(parseOkfBundle('/nonexistent/path/bundle')).rejects.toThrow();
  });

  it('throws when the path is not a directory', async () => {
    const file = join(tmpDir, 'not-a-dir.txt');
    await writeFile(file, 'hello');
    await expect(parseOkfBundle(file)).rejects.toThrow();
  });

  it('throws when the bundle has no .md files', async () => {
    await writeFile(join(tmpDir, 'readme.txt'), 'not markdown');
    await expect(parseOkfBundle(tmpDir)).rejects.toThrow(/No \.md files/);
  });

  it('parses a single flat .md file', async () => {
    await writeFile(join(tmpDir, 'orders.md'), `---
type: BigQuery Table
title: Orders
tags: sales, revenue
---
# Orders

Fact table for orders.
`);
    const docs = await parseOkfBundle(tmpDir);
    expect(docs).toHaveLength(1);
    expect(docs[0].bundlePath).toBe('orders.md');
    expect(docs[0].frontmatter.type).toBe('BigQuery Table');
    expect(docs[0].frontmatter.title).toBe('Orders');
    expect(docs[0].body).toContain('Fact table for orders');
    expect(docs[0].links).toHaveLength(0);
  });

  it('resolves cross-links between documents', async () => {
    await writeFile(join(tmpDir, 'revenue.md'), `---
type: Metric
---
## Source Data

See [Orders](orders.md).
`);
    await writeFile(join(tmpDir, 'orders.md'), `---
type: BigQuery Table
---
The source table.
`);

    const docs = await parseOkfBundle(tmpDir);
    expect(docs).toHaveLength(2);

    const revenue = docs.find(d => d.bundlePath === 'revenue.md')!;
    expect(revenue.links).toHaveLength(1);
    expect(revenue.links[0].targetBundlePath).toBe('orders.md');
    expect(revenue.links[0].section).toBe('Source Data');
  });

  it('walks subdirectories and uses bundle-relative paths', async () => {
    const sub = join(tmpDir, 'tables');
    await mkdir(sub);
    await writeFile(join(sub, 'orders.md'), `---\ntype: Table\n---\nOrders.`);
    await writeFile(join(tmpDir, 'index.md'), `---\ntype: Index\n---\nTop level.`);

    const docs = await parseOkfBundle(tmpDir);
    const paths = docs.map(d => d.bundlePath).sort();
    expect(paths).toContain('index.md');
    expect(paths).toContain('tables/orders.md');
  });

  it('ignores hidden files and directories', async () => {
    await writeFile(join(tmpDir, '.hidden.md'), `---\ntype: Hidden\n---\nHidden.`);
    await writeFile(join(tmpDir, 'visible.md'), `---\ntype: Visible\n---\nVisible.`);
    const docs = await parseOkfBundle(tmpDir);
    expect(docs.map(d => d.bundlePath)).not.toContain('.hidden.md');
    expect(docs.map(d => d.bundlePath)).toContain('visible.md');
  });

  it('handles documents with no frontmatter', async () => {
    await writeFile(join(tmpDir, 'bare.md'), `# Just a heading\n\nNo frontmatter here.`);
    const docs = await parseOkfBundle(tmpDir);
    expect(docs[0].frontmatter).toEqual({});
    expect(docs[0].body).toContain('Just a heading');
  });
});
