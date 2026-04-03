import { readFileSync } from 'fs';
import { config } from './config.js';

export interface TlsOptions { cert: Buffer; key: Buffer; }

/**
 * Returns TLS cert + key buffers if TLS_CERT and TLS_KEY are configured,
 * otherwise null (plain HTTP mode).
 */
export function loadTls(): TlsOptions | null {
  if (!config.TLS_CERT || !config.TLS_KEY) return null;
  return {
    cert: readFileSync(config.TLS_CERT),
    key:  readFileSync(config.TLS_KEY),
  };
}
