import type { EmbedAdapter } from '../interfaces.js';

export class OllamaEmbedAdapter implements EmbedAdapter {
  constructor(
    private readonly baseUrl: string,
    private readonly model: string,
  ) {}

  async embed(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) return [];

    // Use /api/embed (Ollama batch endpoint, available since 0.1.31)
    const res = await fetch(`${this.baseUrl}/api/embed`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: this.model, input: texts }),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Ollama embed failed (${res.status}): ${body}`);
    }

    const data = await res.json() as { embeddings: number[][] };
    if (!Array.isArray(data.embeddings)) {
      throw new Error('Unexpected Ollama embed response shape');
    }
    return data.embeddings;
  }
}
