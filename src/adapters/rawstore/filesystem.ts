import * as fs from 'fs';
import * as path from 'path';
import type { RawStoreAdapter } from '../interfaces.js';

/**
 * FilesystemRawStore: stores entity and chunk files on local disk.
 * Layout: {base}/{id[0..1]}/{id[2..3]}/{id}/entity.md
 *                                         {id}/chunks.ndjson
 */
export class FilesystemRawStore implements RawStoreAdapter {
  constructor(private readonly basePath: string) {}

  private resolve(key: string): string {
    const base = path.resolve(this.basePath);
    const abs  = path.resolve(base, key);
    // Prevent path traversal: resolved path must stay inside basePath
    if (abs !== base && !abs.startsWith(base + path.sep)) {
      throw new Error(`Illegal rawstore key — path escapes base directory: ${key}`);
    }
    return abs;
  }

  async write(key: string, content: string): Promise<void> {
    const abs = this.resolve(key);
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, content, 'utf8');
  }

  async read(key: string): Promise<string> {
    const abs = this.resolve(key);
    if (!fs.existsSync(abs)) throw new Error(`RawStore key not found: ${key}`);
    return fs.readFileSync(abs, 'utf8');
  }

  async delete(key: string): Promise<void> {
    const abs = this.resolve(key);
    if (fs.existsSync(abs)) fs.rmSync(abs, { recursive: true, force: true });
  }

  async exists(key: string): Promise<boolean> {
    return fs.existsSync(this.resolve(key));
  }
}

/**
 * Build the shard-prefix directory key for an entity.
 * Returns: "{id[0..1]}/{id[2..3]}/{id}"
 */
export function entityDir(entityId: string): string {
  const clean = entityId.replace(/-/g, '');
  return `${clean.slice(0, 2)}/${clean.slice(2, 4)}/${entityId}`;
}

export function entityFilePath(entityId: string): string {
  return `${entityDir(entityId)}/entity.md`;
}

export function chunksFilePath(entityId: string): string {
  return `${entityDir(entityId)}/chunks.ndjson`;
}
