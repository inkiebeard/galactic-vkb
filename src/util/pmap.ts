/**
 * Concurrency-limited async map utilities.
 *
 * Uses a shared-cursor worker pool so items are processed as fast as they
 * finish, not in fixed batches. Output order always matches input order.
 */

/**
 * Map `items` through `fn` with at most `concurrency` promises in-flight.
 * Rejects immediately if any `fn` call throws (fail-fast, same as Promise.all).
 */
export async function pMap<T, R>(
  items: readonly T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (cursor < items.length) {
      const i = cursor++;
      results[i] = await fn(items[i], i);
    }
  }
  const slots = Math.min(Math.max(concurrency, 1), items.length);
  await Promise.all(Array.from({ length: slots }, worker));
  return results;
}

/**
 * Same as `pMap` but never throws — each slot captures fulfilled/rejected
 * independently (same contract as Promise.allSettled).
 */
export async function pMapSettled<T, R>(
  items: readonly T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<PromiseSettledResult<R>[]> {
  const results: PromiseSettledResult<R>[] = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (cursor < items.length) {
      const i = cursor++;
      try {
        results[i] = { status: 'fulfilled', value: await fn(items[i], i) };
      } catch (e) {
        results[i] = { status: 'rejected', reason: e };
      }
    }
  }
  const slots = Math.min(Math.max(concurrency, 1), items.length);
  await Promise.all(Array.from({ length: slots }, worker));
  return results;
}
