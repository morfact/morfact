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
});
