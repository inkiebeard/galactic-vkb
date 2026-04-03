export const config = {
  // ── Infrastructure ──────────────────────────────────────────────────────
  DATABASE_URL:         process.env.DATABASE_URL         ?? 'postgres://localhost/vkb',
  RAWSTORE_ADAPTER:     process.env.RAWSTORE_ADAPTER     ?? 'filesystem',
  RAWSTORE_PATH:        process.env.RAWSTORE_PATH         ?? './rawstore',
  RAWSTORE_S3_BUCKET:   process.env.RAWSTORE_S3_BUCKET   ?? '',
  RAWSTORE_S3_ENDPOINT: process.env.RAWSTORE_S3_ENDPOINT ?? '',

  // ── Ollama ───────────────────────────────────────────────────────────────
  OLLAMA_BASE_URL: process.env.OLLAMA_BASE_URL ?? 'http://localhost:11434',
  EMBED_MODEL:     process.env.EMBED_MODEL     ?? 'nomic-embed-text',
  EMBED_DIM:       parseInt(process.env.EMBED_DIM ?? '768', 10),
  LLM_MODEL:       process.env.LLM_MODEL       ?? 'llama3.2',

  // ── Chunking ─────────────────────────────────────────────────────────────
  CHUNK_SIZE:    parseInt(process.env.CHUNK_SIZE    ?? '512', 10),
  CHUNK_OVERLAP: parseInt(process.env.CHUNK_OVERLAP ?? '64',  10),

  // ── Sectioning ───────────────────────────────────────────────────────────
  SECTION_STRATEGY:         process.env.SECTION_STRATEGY           ?? 'similarity_valley',
  SECTION_SPLIT_THRESHOLD:  parseFloat(process.env.SECTION_SPLIT_THRESHOLD ?? '0.65'),
  SECTION_WINDOW_SIZE:      parseInt(process.env.SECTION_WINDOW_SIZE   ?? '5',  10),
  SECTION_MAX_SIZE:         parseInt(process.env.SECTION_MAX_SIZE      ?? '8',  10),

  // ── Relations ────────────────────────────────────────────────────────────
  RELATION_THRESHOLD:       parseFloat(process.env.RELATION_THRESHOLD        ?? '0.75'),
  RELATION_TOP_K:           parseInt(process.env.RELATION_TOP_K              ?? '10',   10),
  RELATION_CONFIDENCE_STEP: parseFloat(process.env.RELATION_CONFIDENCE_STEP  ?? '0.05'),
  RELATION_TTL_DAYS:        parseInt(process.env.RELATION_TTL_DAYS           ?? '30',   10),
  RELATION_PRUNE_THRESHOLD: parseFloat(process.env.RELATION_PRUNE_THRESHOLD  ?? '0.6'),
  LLM_RELATION_EXTRACTION:  process.env.LLM_RELATION_EXTRACTION !== 'false',
  LLM_EXTRACT_CANDIDATES:   parseInt(process.env.LLM_EXTRACT_CANDIDATES      ?? '20',   10),

  // ── Vector index ─────────────────────────────────────────────────────────
  IVFFLAT_THRESHOLD: parseInt(process.env.IVFFLAT_THRESHOLD ?? '1000', 10),
  IVFFLAT_LISTS:     parseInt(process.env.IVFFLAT_LISTS     ?? '100',  10),

  // ── Workers ──────────────────────────────────────────────────────────────
  WORKER_CONCURRENCY:    parseInt(process.env.WORKER_CONCURRENCY    ?? '2', 10),
  INGEST_MAX_RETRIES:    parseInt(process.env.INGEST_MAX_RETRIES    ?? '3', 10),
  RETUNE_INTERVAL_HOURS: parseInt(process.env.RETUNE_INTERVAL_HOURS ?? '6', 10),
  RETUNE_SUMMARISE:      process.env.RETUNE_SUMMARISE === 'true',
  JOB_TTL_DAYS:          parseInt(process.env.JOB_TTL_DAYS          ?? '7', 10),

  // ── Prompts ──────────────────────────────────────────────────────────────
  SUMMARY_PROMPT_FILE:         process.env.SUMMARY_PROMPT_FILE          as string | undefined,
  CHUNK_SUMMARY_PROMPT_FILE:   process.env.CHUNK_SUMMARY_PROMPT_FILE    as string | undefined,
  SECTION_SUMMARY_PROMPT_FILE: process.env.SECTION_SUMMARY_PROMPT_FILE  as string | undefined,
  RELATION_EXTRACT_PROMPT_FILE:process.env.RELATION_EXTRACT_PROMPT_FILE as string | undefined,

  // ── Logging ─────────────────────────────────────────────────────────────
  LOG_LEVEL: (process.env.LOG_LEVEL ?? 'info') as 'debug' | 'info' | 'warn' | 'error',

  // ── Servers ──────────────────────────────────────────────────────────────
  MCP_PORT:   parseInt(process.env.MCP_PORT   ?? '3333', 10),
  OBS_PORT:   parseInt(process.env.OBS_PORT   ?? '4242', 10),
  OBS_SECRET: process.env.OBS_SECRET as string | undefined,

  // ── TLS (optional — set both to enable HTTPS on all servers) ─────────────
  TLS_CERT: process.env.TLS_CERT as string | undefined,
  TLS_KEY:  process.env.TLS_KEY  as string | undefined,
} as const;
