import { describe, expect, it } from 'vitest';

import { MemoryWorkflowStateStore } from './memory-store.js';
import {
  createEngine,
  compileFlow,
  defineHook,
  definePlugin,
  defineWorkflow,
  type FlowDefinition,
} from '@morfact/core';

const noop = defineHook({
  name: 'test.noop@1',
  async execute(_ctx, input: Readonly<Record<string, unknown>>) {
    return { status: 'succeeded' as const, output: input };
  },
});

describe('MemoryWorkflowStateStore + builtin hooks', () => {
  it('spawns a child run and resumes the parent when the child succeeds', async () => {
    const store = new MemoryWorkflowStateStore();
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'spawn-demo',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [noop],
          workflows: [
            defineWorkflow({
              schemaVersion: 1,
              name: 'child.demo',
              version: '1.0',
              nodes: [{ id: 'work', hook: noop.name }],
            }),
            defineWorkflow({
              schemaVersion: 1,
              name: 'parent.demo',
              version: '1.0',
              nodes: [
                {
                  id: 'spawn',
                  hook: '@morfact/core/spawn',
                  input: {
                    _meta: {
                      spawn: {
                        childTemplateName: 'child.demo',
                        childTemplateVersion: '1.0',
                      },
                    },
                  },
                },
              ],
            }),
          ],
        }),
      ],
    });

    const parent = await engine.startRun({
      workflow: 'parent.demo@1.0',
      tenantKey: 't1',
    });

    const spawnResult = await engine.executeNextTask({ workerId: 'w1' });
    expect(spawnResult?.status).toBe('waiting');

    const children = await engine.listRuns({ parentTaskId: spawnResult?.taskId });
    expect(children).toHaveLength(1);

    const childResult = await engine.executeNextTask({ workerId: 'w1' });
    expect(childResult?.status).toBe('succeeded');
    expect(childResult?.runId).toBe(children[0]?.id);

    const parentAgain = await engine.executeNextTask({ workerId: 'w1' });
    expect(parentAgain?.status).toBe('succeeded');
    expect(parentAgain?.runId).toBe(parent.id);
  });

  it('blocks a gate until required interaction fields are present', async () => {
    const store = new MemoryWorkflowStateStore();
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'gate-demo',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [],
          workflows: [
            defineWorkflow({
              schemaVersion: 1,
              name: 'gate.demo',
              version: '1.0',
              nodes: [
                {
                  id: 'review',
                  hook: '@morfact/core/gate',
                  input: {
                    _meta: {
                      interaction: {
                        kind: 'approval',
                        schema: { required: ['approved'] },
                      },
                    },
                  },
                },
              ],
            }),
          ],
        }),
      ],
    });

    const run = await engine.startRun({
      workflow: 'gate.demo@1.0',
      tenantKey: 't1',
    });
    const waiting = await engine.executeNextTask({ workerId: 'w1' });
    expect(waiting?.status).toBe('waiting');

    const interactions = await store.listInteractions(waiting!.taskId);
    expect(interactions).toHaveLength(1);
    expect(interactions[0]?.kind).toBe('approval');

    await engine.resumeTask(waiting!.taskId, 'gate:' + waiting!.taskId, {
      approved: true,
    });
    const done = await engine.executeNextTask({ workerId: 'w1' });
    expect(done?.status).toBe('succeeded');
    expect(done?.runId).toBe(run.id);
  });

  it('coordinator appends subtasks and resumes when they complete', async () => {
    const store = new MemoryWorkflowStateStore();
    const flow: FlowDefinition = {
      apiVersion: 'flow.v1',
      kind: 'Flow',
      metadata: { name: 'coord.demo', version: '1.0' },
      spec: {
        nodes: [
          {
            id: 'fanout',
            type: 'coordinator',
            coordinator: {
              taskGroupTemplate: 'units',
              groupCount: 2,
              groupStrategy: 'parallel',
            },
            hooks: { executor: { use: '@morfact/core/coordinator' } },
          },
        ],
        taskGroupTemplates: {
          units: {
            tasks: [
              {
                id: 'unit',
                type: 'business',
                hooks: { executor: { use: noop.name } },
              },
            ],
          },
        },
      },
    };
    const compiled = compileFlow(
      flow,
      new Set(['@morfact/core/coordinator', noop.name]),
      () => crypto.randomUUID(),
    );
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'coord-demo',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [noop],
          workflows: [compiled.definition],
        }),
      ],
    });

    await engine.startRun({
      workflow: 'coord.demo@1.0',
      tenantKey: 't1',
    });

    const waiting = await engine.executeNextTask({ workerId: 'w1' });
    expect(waiting?.status).toBe('waiting');

    const subA = await engine.executeNextTask({ workerId: 'w1' });
    const subB = await engine.executeNextTask({ workerId: 'w1' });
    expect(subA?.status).toBe('succeeded');
    expect(subB?.status).toBe('succeeded');

    const coordinatorDone = await engine.executeNextTask({ workerId: 'w1' });
    expect(coordinatorDone?.status).toBe('succeeded');
    expect(coordinatorDone?.taskId).toBe(waiting?.taskId);
  });

  it('loops the same task until the body halts, without unlocking downstream', async () => {
    const store = new MemoryWorkflowStateStore();
    const paginate = defineHook({
      name: 'book.paginate_page@1',
      async execute(_ctx, input: Readonly<Record<string, unknown>>) {
        const loop = input.loop as
          | { checkpoint?: { cursor: number } }
          | undefined;
        const cursor = loop?.checkpoint?.cursor ?? 0;
        if (cursor >= 1) {
          return {
            status: 'succeeded' as const,
            output: {
              halt: true,
              checkpoint: { cursor },
              output: { cursor },
            },
          };
        }
        return {
          status: 'succeeded' as const,
          output: {
            halt: false,
            checkpoint: { cursor: cursor + 1 },
            output: { cursor },
          },
        };
      },
    });
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'loop-demo',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [paginate],
          workflows: [
            defineWorkflow({
              schemaVersion: 1,
              name: 'loop.demo',
              version: '1.0',
              nodes: [
                {
                  id: 'paginate',
                  hook: '@morfact/core/loop',
                  input: {
                    _meta: {
                      loop: {
                        body: paginate.name,
                        maxIterations: 8,
                      },
                    },
                  },
                },
                {
                  id: 'done',
                  hook: '@morfact/core/completed',
                  dependsOn: ['paginate'],
                },
              ],
            }),
          ],
        }),
      ],
    });

    const run = await engine.startRun({
      workflow: 'loop.demo@1.0',
      tenantKey: 't1',
    });
    const first = await engine.executeNextTask({ workerId: 'w1' });
    expect(first?.status).toBe('continue');
    const afterFirst = await store.listTasks(run.id);
    const loopTask = afterFirst.find((t) => t.logicalKey === 'paginate');
    const doneTask = afterFirst.find((t) => t.logicalKey === 'done');
    expect(loopTask?.status).toBe('ready');
    expect(doneTask?.status).toBe('pending');
    expect(
      (loopTask?.meta?.loopState as { checkpoint?: { cursor: number } })
        ?.checkpoint?.cursor,
    ).toBe(1);

    const second = await engine.executeNextTask({ workerId: 'w1' });
    expect(second?.status).toBe('succeeded');
    expect(second?.taskId).toBe(first?.taskId);

    const finished = await engine.getTask(second!.taskId);
    expect(finished?.output).toMatchObject({
      haltReason: 'halt',
      truncated: false,
      output: { cursor: 1 },
    });

    const tail = await engine.executeNextTask({ workerId: 'w1' });
    expect(tail?.status).toBe('succeeded');
    expect(tail?.taskId).not.toBe(first?.taskId);
  });

  it('fails when maxIterations is exhausted (default fuse)', async () => {
    const store = new MemoryWorkflowStateStore();
    const neverHalt = defineHook({
      name: 'book.never_halt@1',
      async execute(_ctx, input: Readonly<Record<string, unknown>>) {
        const loop = input.loop as { checkpoint?: { n: number } } | undefined;
        const n = loop?.checkpoint?.n ?? 0;
        return {
          status: 'succeeded' as const,
          output: { halt: false, checkpoint: { n: n + 1 } },
        };
      },
    });
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'loop-cap-fail',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [neverHalt],
          workflows: [
            defineWorkflow({
              schemaVersion: 1,
              name: 'loop.cap.fail',
              version: '1.0',
              nodes: [
                {
                  id: 'paginate',
                  hook: '@morfact/core/loop',
                  input: {
                    _meta: {
                      loop: {
                        body: neverHalt.name,
                        maxIterations: 2,
                      },
                    },
                  },
                },
              ],
            }),
          ],
        }),
      ],
    });

    await engine.startRun({
      workflow: 'loop.cap.fail@1.0',
      tenantKey: 't1',
    });
    const first = await engine.executeNextTask({ workerId: 'w1' });
    expect(first?.status).toBe('continue');
    await expect(engine.executeNextTask({ workerId: 'w1' })).rejects.toThrow(
      /maxIterations/,
    );
    const task = await engine.getTask(first!.taskId);
    expect(task?.status).toBe('failed');
    expect(
      (task?.meta?.loopState as { checkpoint?: { n: number } })?.checkpoint?.n,
    ).toBe(1);
  });

  it('succeeds with truncated=true when onMaxIterations is succeed', async () => {
    const store = new MemoryWorkflowStateStore();
    const neverHalt = defineHook({
      name: 'book.never_halt_ok@1',
      async execute(_ctx, input: Readonly<Record<string, unknown>>) {
        const loop = input.loop as { checkpoint?: { n: number } } | undefined;
        const n = loop?.checkpoint?.n ?? 0;
        return {
          status: 'succeeded' as const,
          output: { halt: false, checkpoint: { n: n + 1 }, output: { n } },
        };
      },
    });
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'loop-cap-ok',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [neverHalt],
          workflows: [
            defineWorkflow({
              schemaVersion: 1,
              name: 'loop.cap.ok',
              version: '1.0',
              nodes: [
                {
                  id: 'paginate',
                  hook: '@morfact/core/loop',
                  input: {
                    _meta: {
                      loop: {
                        body: neverHalt.name,
                        maxIterations: 2,
                        onMaxIterations: 'succeed',
                      },
                    },
                  },
                },
              ],
            }),
          ],
        }),
      ],
    });

    await engine.startRun({
      workflow: 'loop.cap.ok@1.0',
      tenantKey: 't1',
    });
    expect((await engine.executeNextTask({ workerId: 'w1' }))?.status).toBe(
      'continue',
    );
    const done = await engine.executeNextTask({ workerId: 'w1' });
    expect(done?.status).toBe('succeeded');
    const task = await engine.getTask(done!.taskId);
    expect(task?.output).toMatchObject({
      haltReason: 'max_iterations',
      truncated: true,
    });
  });

  it('keeps the last checkpoint when the body throws', async () => {
    const store = new MemoryWorkflowStateStore();
    const flaky = defineHook({
      name: 'book.flaky@1',
      async execute(_ctx, input: Readonly<Record<string, unknown>>) {
        const loop = input.loop as { checkpoint?: { n: number } } | undefined;
        const n = loop?.checkpoint?.n ?? 0;
        if (n === 1) {
          throw new Error('page failed');
        }
        return {
          status: 'succeeded' as const,
          output: { halt: false, checkpoint: { n: n + 1 } },
        };
      },
    });
    const engine = createEngine({
      store,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'loop-throw',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [flaky],
          workflows: [
            defineWorkflow({
              schemaVersion: 1,
              name: 'loop.throw',
              version: '1.0',
              nodes: [
                {
                  id: 'paginate',
                  hook: '@morfact/core/loop',
                  input: {
                    _meta: {
                      loop: {
                        body: flaky.name,
                        maxIterations: 8,
                      },
                    },
                  },
                },
              ],
            }),
          ],
        }),
      ],
    });

    await engine.startRun({
      workflow: 'loop.throw@1.0',
      tenantKey: 't1',
    });
    const first = await engine.executeNextTask({ workerId: 'w1' });
    expect(first?.status).toBe('continue');
    await expect(engine.executeNextTask({ workerId: 'w1' })).rejects.toThrow(
      'page failed',
    );
    const task = await engine.getTask(first!.taskId);
    expect(task?.status).toBe('failed');
    expect(
      (task?.meta?.loopState as { checkpoint?: { n: number } })?.checkpoint?.n,
    ).toBe(1);
  });
});
