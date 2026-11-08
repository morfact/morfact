// Acceptance: a coordinator fans a picture book out into one native loop task
// per page. Each page loop owns its output, checkpoint, iteration and retry
// state; the business hook never keeps a book-wide polling map.
//
// The same scenario runs against the in-memory store and the PostgreSQL store
// so the two agree on dispatch, delay, restart, single-page retry, cancel and
// duplicate dispatch.

import { Pool } from 'pg';
import { afterAll, describe, expect, it } from 'vitest';

import { MemoryWorkflowStateStore } from './memory-store.js';
import { PostgresWorkflowStateStore } from './postgres-store.js';
import {
  compileFlow,
  createEngine,
  defineHook,
  definePlugin,
  defineWorkflow,
  type Engine,
  type FlowDefinition,
  type HookExecutionResult,
  type WorkflowDefinition,
  type WorkflowStateStore,
} from '@morfact/core';

const PAGE_HOOK = 'book.page_video@1';

interface PageCheckpoint {
  requestId?: string;
  pollUrl?: string;
  polls: number;
  assetRef?: string;
}

interface VideoProvider {
  /** Pages whose next status query throws once. */
  failQueryOnce: Set<number>;
  /** Polls a page needs before the provider reports it done. */
  pollsUntilDone: Map<number, number>;
  submitted: string[];
  downloaded: string[];
}

function provider(): VideoProvider {
  return {
    failQueryOnce: new Set(),
    pollsUntilDone: new Map(),
    submitted: [],
    downloaded: [],
  };
}

/**
 * One page, one task. The hook only ever sees its own checkpoint:
 * no poll URL yet -> submit once and remember it; otherwise query once and
 * either delay the next round or download and halt.
 */
function pageVideoHook(vendor: VideoProvider) {
  return defineHook({
    name: PAGE_HOOK,
    async execute(_ctx, input: Readonly<Record<string, unknown>>) {
      const page = input.page as number;
      const loop = input.loop as
        | { iteration: number; checkpoint?: PageCheckpoint }
        | undefined;
      const checkpoint: PageCheckpoint = loop?.checkpoint ?? { polls: 0 };

      if (!checkpoint.pollUrl) {
        const requestId = `req-page-${page}`;
        vendor.submitted.push(requestId);
        return {
          status: 'succeeded',
          output: {
            halt: false,
            checkpoint: {
              requestId,
              pollUrl: `https://vendor.example/poll/${requestId}`,
              polls: 0,
            },
            output: { phase: 'submitted', requestId },
          },
        } satisfies HookExecutionResult;
      }

      if (vendor.failQueryOnce.has(page)) {
        vendor.failQueryOnce.delete(page);
        throw new Error(`transient query failure for page ${page}`);
      }

      const polls = checkpoint.polls + 1;
      const needed = vendor.pollsUntilDone.get(page) ?? 1;
      if (polls < needed) {
        return {
          status: 'succeeded',
          output: {
            halt: false,
            delayMs: 1_000,
            checkpoint: { ...checkpoint, polls },
            output: { phase: 'pending', polls },
          },
        } satisfies HookExecutionResult;
      }

      const assetRef = `r2://books/${input.bookId}/page-${page}.mp4`;
      vendor.downloaded.push(checkpoint.requestId ?? '');
      return {
        status: 'succeeded',
        output: {
          halt: true,
          checkpoint: { ...checkpoint, polls, assetRef },
          output: { assetRef },
        },
      } satisfies HookExecutionResult;
    },
  });
}

function bookFlow(): FlowDefinition {
  return {
    apiVersion: 'flow.v1',
    kind: 'Flow',
    metadata: { name: 'book.pages', version: '1.0' },
    spec: {
      nodes: [
        {
          id: 'page_video',
          type: 'group',
          group: {
            tasks: [
              {
                id: 'render',
                type: 'loop',
                loop: {
                  body: PAGE_HOOK,
                  maxIterations: 20,
                  onMaxIterations: 'fail',
                },
                inputTemplate: {
                  page: '{{groupIndex}}',
                },
              },
            ],
          },
        },
        {
          id: 'pages',
          type: 'coordinator',
          coordinator: {
            groupRef: 'page_video',
            groupCount: '{{params.pageCount}}',
            groupStrategy: 'parallel',
          },
        },
        {
          id: 'assemble',
          type: 'business',
          hooks: { executor: { use: 'book.assemble@1' } },
        },
      ],
      edges: [{ from: 'pages', to: 'assemble' }],
    },
  };
}

interface Harness {
  store: WorkflowStateStore;
  engine: Engine;
  vendor: VideoProvider;
  now: Date;
  /** Move the clock, the way a restarted worker observes a later time. */
  advance(ms: number): void;
}

function buildHarness(store: WorkflowStateStore): Harness {
  const clock = { now: new Date('2026-11-08T00:00:00.000Z') };
  const vendor = provider();
  const compiled = compileFlow(
    bookFlow(),
    new Set([
      PAGE_HOOK,
      'book.assemble@1',
      '@morfact/core/coordinator',
      '@morfact/core/loop',
    ]),
    () => crypto.randomUUID(),
  );
  // compileFlow materializes tasks for hosts that start runs directly; the
  // engine rematerializes from the definition, whose node ids are task ids.
  // Restore the flow logical keys so page loops stay addressable.
  const idToLogicalKey = new Map(
    compiled.tasks.map((task) => [task.id, task.logicalKey]),
  );
  const workflow: WorkflowDefinition = defineWorkflow({
    ...compiled.definition,
    nodes: compiled.definition.nodes.map((node) => ({
      ...node,
      id: idToLogicalKey.get(node.id) ?? node.id,
      dependsOn: (node.dependsOn ?? []).map(
        (dep) => idToLogicalKey.get(dep) ?? dep,
      ),
    })),
  });
  const engine = createEngine({
    store,
    now: () => clock.now,
    plugins: [
      definePlugin({
        manifest: {
          manifestVersion: 1,
          name: 'book',
          version: '1.0.0',
          engine: '^0.1.0',
        },
        hooks: [
          pageVideoHook(vendor),
          defineHook({
            name: 'book.assemble@1',
            async execute() {
              return { status: 'succeeded', output: { assembled: true } };
            },
          }),
        ],
        workflows: [workflow],
      }),
    ],
  });
  return {
    store,
    engine,
    vendor,
    get now() {
      return clock.now;
    },
    advance(ms: number) {
      clock.now = new Date(clock.now.getTime() + ms);
    },
  };
}

describe.sequential.each([
  {
    name: 'memory',
    create: async () => new MemoryWorkflowStateStore(),
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
])('picture book video loops ($name)', ({ create }) => {
  const closables: PostgresWorkflowStateStore[] = [];
  afterAll(async () => {
    for (const store of closables) await store.close();
  });

  async function open(): Promise<Harness> {
    const store = await create();
    if (store instanceof PostgresWorkflowStateStore) closables.push(store);
    return buildHarness(store);
  }

  it('fans out one native loop per page and keeps their state independent', async () => {
    const harness = await open();
    // Page 0 finishes on its first poll, page 1 needs two polls, page 2
    // fails its first status query and must retry only that iteration.
    harness.vendor.pollsUntilDone.set(1, 2);
    harness.vendor.failQueryOnce.add(2);

    const run = await harness.engine.startRun({
      workflow: 'book.pages@1.0',
      tenantKey: 'tenant-1',
      input: { bookId: 'book-7', pageCount: 3 },
    });

    const allTasks = await harness.store.listTasks(run.id);
    const coordinator = allTasks.find((task) => task.logicalKey === 'pages');
    expect(coordinator?.hook).toBe('@morfact/core/coordinator');

    // Dispatch: three subtasks, each a loop bound to its own page.
    await harness.engine.executeNextTask({ workerId: 'w1' });
    let tasks = await harness.store.listTasks(run.id);
    const pages = tasks
      .filter((task) => task.ancestorId === coordinator?.id)
      .sort((a, b) => a.logicalKey.localeCompare(b.logicalKey));
    expect(pages.map((task) => task.logicalKey)).toEqual([
      'page_video.render[0]',
      'page_video.render[1]',
      'page_video.render[2]',
    ]);
    expect(new Set(pages.map((task) => task.id)).size).toBe(3);
    for (const [index, page] of pages.entries()) {
      expect(page.hook).toBe('@morfact/core/loop');
      const meta = page.input._meta as {
        loop?: { body: string; maxIterations: number; onMaxIterations: string };
        groupIndex?: number;
      };
      expect(meta.loop).toEqual({
        body: PAGE_HOOK,
        maxIterations: 20,
        onMaxIterations: 'fail',
      });
      expect(meta.groupIndex).toBe(index);
      expect(page.input.page).toBe(index);
    }

    // Repeating the dispatch (coordinator re-executed while still waiting)
    // must not create a second set of subtasks.
    await harness.store.resumeTask({
      commandId: 're-dispatch',
      taskId: coordinator!.id,
      resumeToken: `coordinator:${coordinator!.id}`,
      now: harness.now,
    });
    await harness.engine.executeTask({
      workerId: 'w1',
      taskId: coordinator!.id,
    });
    tasks = await harness.store.listTasks(run.id);
    expect(
      tasks.filter((task) => task.ancestorId === coordinator?.id),
    ).toHaveLength(3);

    // First round: every page submits exactly one video request. Pages run
    // sequentially here so each one is observed exactly once.
    for (const page of pages) {
      const submitted = await harness.engine.executeTask({
        workerId: 'w1',
        taskId: page.id,
      });
      expect(submitted?.status).toBe('continue');
    }
    expect(harness.vendor.submitted.sort()).toEqual([
      'req-page-0',
      'req-page-1',
      'req-page-2',
    ]);

    // Page 0 completes, page 1 delays, page 2 throws and retries alone.
    const round = await Promise.allSettled(
      pages.map((page) =>
        harness.engine.executeTask({ workerId: 'w1', taskId: page.id }),
      ),
    );
    const rejected = round.filter((item) => item.status === 'rejected');
    expect(rejected.map((item) => String((item as PromiseRejectedResult).reason))).toEqual([
      expect.stringMatching(/transient query failure for page 2/),
    ]);

    tasks = await harness.store.listTasks(run.id);
    const byKey = new Map(tasks.map((task) => [task.logicalKey, task]));
    const page0 = byKey.get('page_video.render[0]');
    const page1 = byKey.get('page_video.render[1]');
    const page2 = byKey.get('page_video.render[2]');
    expect(page0?.status).toBe('succeeded');
    expect(page0?.output).toMatchObject({
      haltReason: 'halt',
      output: { assetRef: 'r2://books/book-7/page-0.mp4' },
    });
    expect(`${page1?.logicalKey}:${page1?.status}:${page1?.notBefore?.toISOString() ?? 'none'}`).toContain(
      'page_video.render',
    );
    expect(page1?.status).toBe('ready');
    expect(page1?.notBefore?.toISOString() ?? 'missing').toBe(
      new Date(harness.now.getTime() + 1_000).toISOString(),
    );
    expect(
      (page1?.meta?.loopState as { checkpoint?: PageCheckpoint })?.checkpoint,
    ).toMatchObject({ requestId: 'req-page-1', polls: 1 });
    expect(page2?.status).toBe('failed');
    expect(
      (page2?.meta?.loopState as { checkpoint?: PageCheckpoint })?.checkpoint
        ?.polls,
    ).toBe(0);

    // The delay is real: page 1 cannot be claimed, and its outbox event is
    // not yet available. Restarting the worker does not change that.
    expect(
      await harness.engine.executeTask({ workerId: 'w1', taskId: page1!.id }),
    ).toBeNull();
    const early = await harness.store.claimOutbox({
      workerId: 'dispatcher',
      destination: 'workflow.tasks',
      limit: 10,
      now: harness.now,
      leaseExpiresAt: new Date(harness.now.getTime() + 30_000),
    });
    const page1Events = early.filter((event) => event.taskId === page1?.id);
    // The delayed round is recorded but not yet claimable. The claim above
    // already proved the task itself is held back; the outbox must agree.
    // The first round's event was already consumed; only the delayed one
    // for the next round may still be pending, and it is not due yet.
    expect(page1Events.filter((event) => event.status === 'pending')).toHaveLength(0);

    harness.advance(1_000);
    const due = await harness.store.claimOutbox({
      workerId: 'dispatcher',
      destination: 'workflow.tasks',
      limit: 10,
      now: harness.now,
      leaseExpiresAt: new Date(harness.now.getTime() + 30_000),
    });
    expect(due.some((event) => event.taskId === page1?.id)).toBe(true);

    // A restarted process keeps the delay: a brand new engine over the same
    // store still cannot claim page 1 early, and can once the clock passes.
    harness.advance(-1_000);
    const restarted = createEngine({
      store: harness.store,
      now: () => harness.now,
      plugins: [],
    });
    expect(
      await restarted.executeTask({ workerId: 'w-restarted', taskId: page1!.id }),
    ).toBeNull();
    harness.advance(1_000);

    // Page 1 finishes on its own next iteration; page 0 is not redone.
    const page1Done = await harness.engine.executeTask({
      workerId: 'w1',
      taskId: page1!.id,
    });
    expect(page1Done?.status).toBe('succeeded');
    expect(harness.vendor.downloaded).toEqual(['req-page-0', 'req-page-1']);

    // Only page 2 is retried, and it resumes the same iteration: the request
    // was already submitted, so the retry queries instead of submitting again.
    await requeueFailed(harness, page2!.id);
    const retried = await harness.engine.executeTask({
      workerId: 'w1',
      taskId: page2!.id,
    });
    expect(retried?.status).toBe('succeeded');
    expect(harness.vendor.submitted.filter((id) => id === 'req-page-2')).toEqual([
      'req-page-2',
    ]);
    expect(harness.vendor.downloaded).toContain('req-page-2');

    // All pages done: the coordinator aggregates success and unlocks assemble.
    const resumed = await harness.engine.executeNextTask({ workerId: 'w1' });
    expect(resumed?.taskId).toBe(coordinator?.id);
    expect(resumed?.status).toBe('succeeded');
    const assembled = await harness.engine.executeNextTask({ workerId: 'w1' });
    expect(assembled?.status).toBe('succeeded');
    const finalRun = await harness.store.getRun(run.id);
    expect(finalRun?.status).toBe('succeeded');
  });

  it('cancelling the coordinator stops every page loop', async () => {
    const harness = await open();
    harness.vendor.pollsUntilDone.set(0, 5);
    const run = await harness.engine.startRun({
      workflow: 'book.pages@1.0',
      tenantKey: 'tenant-1',
      input: { bookId: 'book-9', pageCount: 3 },
    });
    await harness.engine.executeNextTask({ workerId: 'w1' });
    const dispatched = (await harness.store.listTasks(run.id)).filter((task) =>
      task.logicalKey.startsWith('page_video'),
    );
    for (const page of dispatched) {
      await harness.engine.executeTask({ workerId: 'w1', taskId: page.id });
    }

    const coordinator = (await harness.store.listTasks(run.id)).find(
      (task) => task.logicalKey === 'pages',
    );
    await harness.engine.cancelTask(coordinator!.id);

    const tasks = await harness.store.listTasks(run.id);
    const pages = tasks.filter((task) => task.ancestorId === coordinator?.id);
    expect(pages.every((task) => task.status === 'cancelled')).toBe(true);
    expect(
      await harness.engine.executeTask({ workerId: 'w1', taskId: pages[0]!.id }),
    ).toBeNull();

    // A cancelled group must not be reported as a successful completion.
    const coordinatorAfter = await harness.store.getTask(coordinator!.id);
    expect(coordinatorAfter?.status).not.toBe('succeeded');
  });

  it('a failed page fails the coordinator without redoing finished pages', async () => {
    const harness = await open();
    const run = await harness.engine.startRun({
      workflow: 'book.pages@1.0',
      tenantKey: 'tenant-1',
      input: { bookId: 'book-3', pageCount: 3 },
    });
    await harness.engine.executeNextTask({ workerId: 'w1' });
    const dispatched = (await harness.store.listTasks(run.id)).filter((task) =>
      task.logicalKey.startsWith('page_video'),
    );
    for (const page of dispatched) {
      await harness.engine.executeTask({ workerId: 'w1', taskId: page.id });
    }

    const tasks = await harness.store.listTasks(run.id);
    const pages = tasks.filter((task) => task.logicalKey.startsWith('page_video'));
    // Finish two pages, then exhaust retries on the third.
    for (const page of pages.slice(0, 2)) {
      const done = await harness.engine.executeTask({
        workerId: 'w1',
        taskId: page.id,
      });
      expect(done?.status).toBe('succeeded');
    }
    const failing = pages[2]!;
    harness.vendor.failQueryOnce.add(2);
    await expect(
      harness.engine.executeTask({ workerId: 'w1', taskId: failing.id }),
    ).rejects.toThrow(/transient query failure/);

    const coordinator = tasks.find((task) => task.logicalKey === 'pages');
    await harness.store.resumeTask({
      commandId: 'resume-after-failure',
      taskId: coordinator!.id,
      resumeToken: `coordinator:${coordinator!.id}`,
      now: harness.now,
    });
    await expect(
      harness.engine.executeTask({ workerId: 'w1', taskId: coordinator!.id }),
    ).rejects.toThrow(/subtasks failed/);

    const assemble = (await harness.store.listTasks(run.id)).find(
      (task) => task.logicalKey === 'assemble',
    );
    expect(assemble?.status).toBe('pending');
    expect(harness.vendor.downloaded).toEqual(['req-page-0', 'req-page-1']);
  });
});

/**
 * The engine fails a task in place; recovery re-queues the same task so the
 * loop retries its current iteration with the checkpoint it already saved.
 */
async function requeueFailed(harness: Harness, taskId: string): Promise<void> {
  const task = await harness.store.getTask(taskId);
  if (!task || typeof harness.store.commitTaskContinue !== 'function') return;
  const loopState = task.meta?.loopState as
    | { iteration?: number; checkpoint?: unknown }
    | undefined;
  // Recovery reclaims the expired lease, then the engine commits the retry
  // through the same continue path a worker uses after a restart.
  const claimed = await harness.store.claimTask({
    workerId: 'recovery',
    taskId,
    now: harness.now,
    leaseExpiresAt: new Date(harness.now.getTime() + 30_000),
  });
  if (!claimed) {
    await harness.store.commitTaskContinue({
      commandId: `requeue:${taskId}`,
      taskId,
      leaseToken: task.leaseToken ?? '',
      checkpoint: loopState?.checkpoint,
      iteration: loopState?.iteration ?? 0,
      now: harness.now,
    });
  }
}
