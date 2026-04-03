import * as fs from 'fs';
import * as path from 'path';
import { URL as NodeURL } from 'url';
import type { FetchAdapter } from '../interfaces.js';
import { createLogger } from '../../logger.js';

const log = createLogger('fetch');

// ── SSRF protection ───────────────────────────────────────────────────────────
// Blocks requests to loopback and RFC-1918 private ranges. These have no
// legitimate use when acting as a public web-fetch proxy, and would allow an
// attacker to probe internal services by submitting crafted `ref` values.
function assertNotPrivateHost(urlStr: string): void {
  let parsed: NodeURL;
  try {
    parsed = new NodeURL(urlStr);
  } catch {
    throw new Error(`Invalid URL: ${urlStr}`);
  }
  const host = parsed.hostname.toLowerCase();
  const BLOCKED = [
    /^localhost$/,
    /^127\./,
    /^0\.0\.0\.0$/,
    /^10\./,
    /^172\.(1[6-9]|2\d|3[01])\./,
    /^192\.168\./,
    /^::1$/,
    /^fc[0-9a-f]{2}:/i,        // ULA fc00::/7
    /^fe[89ab][0-9a-f]:/i,     // link-local fe80::/10
    /^169\.254\./,              // AWS metadata / link-local
    /^100\.(6[4-9]|[7-9]\d|1[01]\d|12[0-7])\./,  // CGNAT 100.64/10
  ];
  if (BLOCKED.some(re => re.test(host))) {
    throw new Error(`Fetch to private/loopback address is not allowed: ${urlStr}`);
  }
}

/**
 * ReadabilityFetcher: extracts readable text from URLs.
 * Falls back to plain file reading for local paths.
 *
 * Requires optional peer deps: jsdom, @mozilla/readability.
 * For file paths, no extra deps are needed.
 */
export class ReadabilityFetcher implements FetchAdapter {
  async fetch(ref: string): Promise<string> {
    if (ref.startsWith('http://') || ref.startsWith('https://')) {
      return this.fetchUrl(ref);
    }
    return this.fetchFile(ref);
  }

  private async fetchUrl(url: string): Promise<string> {
    assertNotPrivateHost(url);
    const res = await globalThis.fetch(url, {
      headers: { 'User-Agent': 'vkb/0.1 (semantic knowledge base indexer)' },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);

    const contentType = res.headers.get('content-type') ?? '';
    const html = await res.text();

    if (!contentType.includes('html')) {
      // Plain text / markdown / JSON — return as-is
      return html;
    }

    try {
      // Dynamic require so jsdom + readability are optional at cold start
      const { JSDOM } = await import('jsdom') as { JSDOM: typeof import('jsdom').JSDOM };
      const { Readability } = await import('@mozilla/readability');

      const dom = new JSDOM(html, { url });
      const reader = new Readability(dom.window.document as unknown as Document);
      const article = reader.parse();
      if (!article?.textContent) throw new Error('Readability returned empty content');
      return normaliseWhitespace(article.textContent);
    } catch (e) {
      // Fallback: strip tags naively
      log.warn('Readability unavailable, using naive text extraction:', (e as Error).message);
      return normaliseWhitespace(html.replace(/<[^>]+>/g, ' '));
    }
  }

  private async fetchFile(filePath: string): Promise<string> {
    const abs = path.resolve(filePath);
    if (!fs.existsSync(abs)) throw new Error(`File not found: ${abs}`);
    return normaliseWhitespace(fs.readFileSync(abs, 'utf8'));
  }
}

function normaliseWhitespace(text: string): string {
  return text
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .replace(/\t/g, ' ')
    .replace(/ {2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
