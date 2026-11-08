import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: false,
    // Both suites migrate the same Postgres schema; parallel files race on
    // catalog types (pg_type unique violations).
    fileParallelism: false,
  },
});
