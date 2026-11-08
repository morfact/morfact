// Both stores must satisfy the engine's conformance contract, including the
// delayed-continue case a loop uses to schedule its next round.

import { Pool } from 'pg';
import { afterAll, describe, it } from 'vitest';

import { storeConformanceCases } from '../../core/src/testing/index.js';
import { MemoryWorkflowStateStore } from './memory-store.js';
import { PostgresWorkflowStateStore } from './postgres-store.js';

describe.each([
  {
    name: 'memory',
    create: async () => new MemoryWorkflowStateStore() as MemoryWorkflowStateStore | PostgresWorkflowStateStore,
  },
  {
    name: 'postgres',
    create: async () => {
      const pool = new Pool({
        host: '127.0.0.1',
        port: 5432,
        user: 'postgres',
        password: 'postgres',
        database: 'morfact_test',
      });
      await pool.query(
        'DROP TABLE IF EXISTS wf_commands, wf_interactions, wf_outbox, wf_task_deps, wf_tasks, wf_runs CASCADE',
      );
      const store = new PostgresWorkflowStateStore(pool);
      await store.migrate();
      return store;
    },
  },
])('store conformance ($name)', ({ create }) => {
  const closables: PostgresWorkflowStateStore[] = [];
  afterAll(async () => {
    for (const store of closables) await store.close();
  });

  const relevant = storeConformanceCases.filter((item) =>
    /continue|delay|idempot|claim a ready/.test(item.name),
  );

  it.each(relevant.map((item) => [item.name, item] as const))(
    '%s',
    async (_name, testCase) => {
      const store = await create();
      if (store instanceof PostgresWorkflowStateStore) closables.push(store);
      await testCase.run(async () => store);
    },
  );
});
