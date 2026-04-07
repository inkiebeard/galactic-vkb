// Load .env relative to this file so it works regardless of CWD.
// import.meta.dirname requires Node >= 21.2 (project requires >= 22).
import { config } from 'dotenv';
import { join } from 'path';

// Redirect stdout→stderr for this call — dotenv v17 can emit to stdout,
// which would corrupt the MCP stdio JSON stream.
const _write = process.stdout.write.bind(process.stdout);
(process.stdout as any).write = (...args: Parameters<typeof process.stdout.write>) =>
  process.stderr.write(args[0] as any);
config({ path: join(import.meta.dirname, '..', '.env') });
process.stdout.write = _write;
