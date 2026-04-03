import * as fs from 'fs';
import * as path from 'path';
import type { FetchAdapter } from '../interfaces.js';
import { createLogger } from '../../logger.js';

const log = createLogger('fetch');

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
