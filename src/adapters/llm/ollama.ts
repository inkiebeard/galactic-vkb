import type { LLMAdapter } from '../interfaces.js';

interface OllamaChatResponse {
  message: { role: string; content: string };
}

export class OllamaLLMAdapter implements LLMAdapter {
  constructor(
    private readonly baseUrl: string,
    private readonly model: string,
  ) {}

  async complete(system: string, user: string): Promise<string> {
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

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Ollama chat failed (${res.status}): ${body}`);
    }

    const data = await res.json() as OllamaChatResponse;
    return data.message?.content ?? '';
  }
}
