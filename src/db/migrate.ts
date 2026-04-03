import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { getPool } from './client.js';
import { createLogger } from '../logger.js';

const log = createLogger('migrate');

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

export async function runMigrations(): Promise<void> {
  const db = getPool();
  const migrationsDir = path.resolve(__dirname, 'migrations');
  const files = fs.readdirSync(migrationsDir).filter(f => f.endsWith('.sql')).sort();

  for (const file of files) {
    const sql = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
    log.info(`Running ${file}…`);
    await db.query(sql);
    log.info(`Done: ${file}`);
  }
}

// Allow running directly: tsx src/db/migrate.ts
if (process.argv[1] === __filename) {
  runMigrations()
    .then(() => { log.info('All migrations complete.'); process.exit(0); })
    .catch(e => { log.error('Failed:', e); process.exit(1); });
}
