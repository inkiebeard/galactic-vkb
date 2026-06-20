/**
 * OKF (Open Knowledge Format) bundle adapter.
 *
 * Parses a directory of markdown files conforming to the OKF v0.1 spec:
 *   https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing
 *
 * Each .md file in the bundle is one "concept document" with optional YAML
 * frontmatter carrying structured fields (type, title, description, resource,
 * tags, timestamp) and a markdown body that may cross-link to other concepts
 * in the same bundle using normal markdown links.
 *
 * This adapter:
 *   1. Walks the bundle directory recursively, finding all .md files.
 *   2. Parses YAML frontmatter from each file.
 *   3. Extracts internal cross-links and annotates them with the nearest
 *      section heading, so callers can mint typed relations (e.g. "okf:joins").
 *   4. Returns a flat list of OkfDocument records ready for bulk ingest.
 */

import { readdir, readFile, stat } from 'fs/promises';
import { join, relative, resolve, dirname, normalize } from 'path';
import * as yaml from 'js-yaml';

// ── Public types ──────────────────────────────────────────────────────────────

/** YAML frontmatter fields defined by OKF v0.1. */
export interface OkfFrontmatter {
  /** Concept type — the only required OKF field (e.g. "BigQuery Table", "Metric"). */
  type?: string;
  title?: string;
  description?: string;
  /** URL of the underlying resource (e.g. a BigQuery console link). */
  resource?: string;
  /** Free-form string tags. May arrive as an array or comma-separated string. */
  tags?: string | string[];
  /** ISO-8601 timestamp of the last known update to the concept. */
  timestamp?: string;
  /** Any producer-defined extension fields are preserved as-is. */
  [key: string]: unknown;
}

/** A resolved internal cross-link found in a concept document. */
export interface OkfLink {
  /** Bundle-relative path of the link target (e.g. "tables/customers.md"). */
  targetBundlePath: string;
  /** Display text of the link. */
  linkText: string;
  /**
   * The nearest markdown heading at or above the link, if any.
   * Used to mint a typed relation label: e.g. a link under "# Joins" → "okf:joins".
   */
  section?: string;
}

/** A parsed OKF concept document, ready for conversion to an IngestPayload. */
export interface OkfDocument {
  /** Absolute filesystem path. */
  filePath: string;
  /** Path relative to the bundle root (e.g. "sales/tables/orders.md"). */
  bundlePath: string;
  /** Parsed YAML frontmatter fields. */
  frontmatter: OkfFrontmatter;
  /** Markdown body with frontmatter block stripped. */
  body: string;
  /** All resolved internal cross-links found in the body. */
  links: OkfLink[];
}

// ── Bundle parsing ────────────────────────────────────────────────────────────

/** Walk a directory recursively and return absolute paths of all .md files. */
async function walkMarkdown(dir: string): Promise<string[]> {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (err) {
    throw new Error(`Cannot read OKF bundle directory: ${dir} — ${(err as Error).message}`);
  }
  const results: string[] = [];
  for (const entry of entries) {
    if (entry.name.startsWith('.')) continue; // skip hidden files/dirs
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...(await walkMarkdown(full)));
    } else if (entry.isFile() && entry.name.endsWith('.md')) {
      results.push(full);
    }
  }
  return results.sort(); // stable order for reproducible ingestion
}

const FRONTMATTER_RE = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/;

export function parseFrontmatter(content: string): { frontmatter: OkfFrontmatter; body: string } {
  const match = FRONTMATTER_RE.exec(content);
  if (!match) return { frontmatter: {}, body: content };
  let frontmatter: OkfFrontmatter = {};
  try {
    const parsed = yaml.load(match[1]);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      frontmatter = parsed as OkfFrontmatter;
    }
  } catch {
    // Malformed frontmatter — treat as body-only document
  }
  return { frontmatter, body: content.slice(match[0].length) };
}

/**
 * Find the nearest markdown heading at or before `pos` in `text`.
 * Returns the heading text without the `#` prefix(es).
 */
export function nearestHeading(text: string, pos: number): string | undefined {
  const re = /^#{1,6}\s+(.+)$/gm;
  let last: string | undefined;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    if (m.index >= pos) break;
    last = m[1].trim();
  }
  return last;
}

/**
 * Extract all internal cross-links (links to other .md files in the bundle)
 * from a concept body. Skips external URLs and same-file anchors.
 */
export function extractLinks(body: string, filePath: string, bundleRoot: string): OkfLink[] {
  const links: OkfLink[] = [];
  const seen = new Set<string>(); // deduplicate same source→target pairs
  const re = /\[([^\]]*)\]\(([^)\s]+)\)/g;
  let m: RegExpExecArray | null;

  while ((m = re.exec(body)) !== null) {
    const [, linkText, rawHref] = m;

    // Skip external, anchor-only, and mailto links
    if (
      /^https?:\/\//i.test(rawHref) ||
      rawHref.startsWith('#') ||
      rawHref.startsWith('mailto:')
    ) continue;

    // Only follow .md links — these are the concept cross-links
    const hrefNoAnchor = rawHref.split('#')[0];
    if (!hrefNoAnchor.endsWith('.md')) continue;

    // Resolve relative to the source file's directory
    const abs = resolve(dirname(filePath), hrefNoAnchor);

    // Express as a bundle-relative path (normalise to forward slashes)
    const bundleRelative = relative(bundleRoot, abs).replace(/\\/g, '/');

    // Skip links that escape the bundle root
    if (bundleRelative.startsWith('..')) continue;

    const dedupeKey = `${bundleRelative}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);

    const section = nearestHeading(body, m.index);
    links.push({ targetBundlePath: bundleRelative, linkText: linkText.trim(), section });
  }

  return links;
}

/**
 * Parse an OKF bundle directory and return all concept documents.
 *
 * @param bundleDir  Absolute or relative path to the OKF bundle root.
 * @returns          All discovered .md files parsed into OkfDocument records.
 * @throws           If the directory cannot be read or contains no .md files.
 */
export async function parseOkfBundle(bundleDir: string): Promise<OkfDocument[]> {
  const absRoot = resolve(bundleDir);

  // Verify it's actually a directory
  try {
    const info = await stat(absRoot);
    if (!info.isDirectory()) {
      throw new Error(`OKF bundle path is not a directory: ${absRoot}`);
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      throw new Error(`OKF bundle directory not found: ${absRoot}`);
    }
    throw err;
  }

  const filePaths = await walkMarkdown(absRoot);
  if (filePaths.length === 0) {
    throw new Error(`No .md files found in OKF bundle: ${absRoot}`);
  }

  const docs: OkfDocument[] = [];
  for (const filePath of filePaths) {
    const content = await readFile(filePath, 'utf-8');
    const { frontmatter, body } = parseFrontmatter(content);
    const bundlePath = relative(absRoot, filePath).replace(/\\/g, '/');
    const links = extractLinks(body, filePath, absRoot);
    docs.push({ filePath, bundlePath, frontmatter, body, links });
  }

  return docs;
}

// ── Relation label helpers ────────────────────────────────────────────────────

/**
 * Derive a vkb relation type label from an OKF cross-link.
 *
 * The nearest section heading of the link is used when available so that
 * a link appearing under "# Joins" becomes `okf:joins`, one under
 * "# Related Metrics" becomes `okf:related_metrics`, etc.
 * Falls back to the generic `okf:references` label.
 */
export function okfRelType(link: OkfLink): string {
  if (!link.section) return 'okf:references';
  const sanitised = link.section
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_') // non-alphanumeric runs → underscores
    .replace(/^_+|_+$/g, '');    // trim leading/trailing underscores
  return sanitised ? `okf:${sanitised}` : 'okf:references';
}

/**
 * Normalise OKF `tags` field to a string array.
 * Handles array form, comma-separated string form, and missing field.
 */
export function normaliseTags(tags: string | string[] | undefined): string[] {
  if (!tags) return [];
  if (Array.isArray(tags)) return tags.map(t => String(t).trim()).filter(Boolean);
  return String(tags).split(',').map(t => t.trim()).filter(Boolean);
}
