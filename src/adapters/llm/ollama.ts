import { getPool } from '../../db/client.js';
import type { LLMAdapter } from '../interfaces.js';

// Cross-process mutex: only one Ollama LLM call may be in-flight at a time.
// This prevents concurrent model-load attempts from exceeding available RAM
// (e.g. "model requires more system memory than is available").
// Any integer constant works; this one is unlikely to clash with other locks.
const OLLAMA_LLM_LOCK_KEY = 42424242;

interface OllamaChatResponse {
  message: { role: string; content: string };
}

export class OllamaLLMAdapter implements LLMAdapter {
  constructor(
    private readonly baseUrl: string,
    private readonly model: string,
  ) {}

  async complete(system: string, user: string): Promise<string> {
    const client = await getPool().connect();
    try {
      // Acquire a transaction-level advisory lock. Blocks until no other
      // process holds it, then proceeds exclusively. Released automatically
      // when the transaction ends (COMMIT or ROLLBACK).
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1)', [OLLAMA_LLM_LOCK_KEY]);

      const res = await fetch(`${this.baseUrl}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: this.model,
          stream: false,
          messages: [
            { role: 'system', content: system },
            { role: 'user',   content: user   },
          ],
        }),
      });

      // Commit immediately after the HTTP call completes to release the lock
      // and let the next queued caller proceed, even if the response is an error.
      await client.query('COMMIT');

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Ollama chat failed (${res.status}): ${body}`);
      }

      const data = await res.json() as OllamaChatResponse;
      return data.message?.content ?? '';
    } catch (e) {
      // If we never reached COMMIT (e.g. fetch threw), roll back to ensure
      // the advisory lock is released and the connection is left in a clean state.
      await client.query('ROLLBACK').catch(() => { /* already committed or no active txn */ });
      throw e;
    } finally {
      client.release();
    }
  }
}
