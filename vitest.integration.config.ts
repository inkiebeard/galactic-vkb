/**
 * Vitest config for integration tests.
 *
 * Integration tests require a running PostgreSQL instance with pgvector.
 * Set TEST_DATABASE_URL to point at a test database, e.g.:
 *
 *   TEST_DATABASE_URL=postgres://vkb:vkb@localhost:5433/vkb_test npm run test:integration
 *
 * One-time setup of the test database (from your docker-compose postgres):
 *   docker exec -it <postgres-container> createdb -U vkb vkb_test
 *   # or: psql postgres://vkb:vkb@localhost:5433/vkb -c "CREATE DATABASE vkb_test;"
 *
 * Migrations are run automatically by openTestDb() in each beforeAll.
 * Files run serially (fileParallelism: false) to avoid migration races on
 * idempotent-but-not-atomic DDL like CREATE EXTENSION.
 */
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/**/*.integration.test.ts'],
    globals: false,
    // Longer timeouts — pipeline + DB round-trips per test
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // Serial file execution prevents CREATE EXTENSION race between workers
    fileParallelism: false,
    // Run tests within a file sequentially (beforeEach truncation is serial)
    sequence: {
      concurrent: false,
    },
  },
});
