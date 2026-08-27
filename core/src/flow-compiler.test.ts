import { describe, expect, it } from 'vitest';

import {
  compileFlow,
  validateFlowDefinition,
  WorkflowValidationError,
  type FlowDefinition,
} from './index.js';

function loopFlow(overrides?: Partial<FlowDefinition['spec']['nodes'][number]>): FlowDefinition {
  return {
    apiVersion: 'flow.v1',
    kind: 'Flow',
    metadata: { name: 'loop.demo', version: '1.0' },
    spec: {
      nodes: [
        {
          id: 'paginate',
          type: 'loop',
          loop: {
            body: 'book.paginate_page@1',
            maxIterations: 8,
            onMaxIterations: 'fail',
          },
          ...overrides,
        },
      ],
    },
  };
}

const hooks = new Set(['@morfact/core/loop', 'book.paginate_page@1']);

describe('compileFlow type: loop', () => {
  it('maps type loop to @morfact/core/loop and embeds config in _meta', () => {
    const compiled = compileFlow(loopFlow(), hooks, () => 'task-1');
    const node = compiled.definition.nodes[0];
    expect(node?.hook).toBe('@morfact/core/loop');
    const meta = (node?.input as { _meta?: { loop?: unknown } })._meta;
    expect(meta?.loop).toEqual({
      body: 'book.paginate_page@1',
      maxIterations: 8,
      onMaxIterations: 'fail',
    });
  });

  it('rejects a loop node without body', () => {
    const flow = loopFlow({ loop: { body: '', maxIterations: 3 } });
    expect(() => validateFlowDefinition(flow, hooks)).toThrow(
      WorkflowValidationError,
    );
  });

  it('rejects a loop node with maxIterations < 1', () => {
    const flow = loopFlow({
      loop: { body: 'book.paginate_page@1', maxIterations: 0 },
    });
    expect(() => validateFlowDefinition(flow, hooks)).toThrow(
      WorkflowValidationError,
    );
  });

  it('rejects a missing body hook', () => {
    expect(() =>
      validateFlowDefinition(loopFlow(), new Set(['@morfact/core/loop'])),
    ).toThrow(/missing loop body hook/);
  });
});

describe('compileFlow type: group', () => {
  const businessHook = 'demo.work@1';
  const finalizeHook = 'demo.finalize@1';
  const groupHooks = new Set([
    businessHook,
    finalizeHook,
    '@morfact/core/coordinator',
  ]);

  it('inlines a wired group into the main DAG with internal edges', () => {
    const flow: FlowDefinition = {
      apiVersion: 'flow.v1',
      kind: 'Flow',
      metadata: { name: 'group.inline', version: '1.0' },
      spec: {
        nodes: [
          {
            id: 'film_spec',
            type: 'group',
            group: {
              tasks: [
                {
                  id: 'gen_a',
                  type: 'business',
                  hooks: { executor: { use: businessHook } },
                },
                {
                  id: 'gen_b',
                  type: 'business',
                  hooks: { executor: { use: businessHook } },
                },
                {
                  id: 'finalize',
                  type: 'business',
                  hooks: { executor: { use: finalizeHook } },
                },
              ],
              edges: [
                { from: 'gen_a', to: 'finalize' },
                { from: 'gen_b', to: 'finalize' },
              ],
              exits: ['finalize'],
            },
          },
          {
            id: 'downstream',
            type: 'business',
            hooks: { executor: { use: businessHook } },
          },
        ],
        edges: [{ from: 'film_spec', to: 'downstream' }],
      },
    };

    const compiled = compileFlow(flow, groupHooks, () => crypto.randomUUID());
    expect(compiled.tasks).toHaveLength(4);

    const finalize = compiled.tasks.find((t) => t.logicalKey === 'film_spec.finalize');
    const downstream = compiled.tasks.find((t) => t.logicalKey === 'downstream');
    expect(finalize?.dependsOn).toHaveLength(2);
    expect(downstream?.dependsOn).toHaveLength(1);
    expect(
      compiled.tasks.find((t) => t.logicalKey === 'film_spec.gen_a')?.dependsOn,
    ).toHaveLength(0);
  });

  it('does not inline an unwired group definition', () => {
    const flow: FlowDefinition = {
      apiVersion: 'flow.v1',
      kind: 'Flow',
      metadata: { name: 'group.unwired', version: '1.0' },
      spec: {
        nodes: [
          {
            id: 'orphan_group',
            type: 'group',
            group: {
              tasks: [
                {
                  id: 'only',
                  type: 'business',
                  hooks: { executor: { use: businessHook } },
                },
              ],
            },
          },
          {
            id: 'fanout',
            type: 'coordinator',
            coordinator: {
              groupRef: 'orphan_group',
              groupCount: 2,
            },
            hooks: { executor: { use: '@morfact/core/coordinator' } },
          },
        ],
      },
    };

    const compiled = compileFlow(flow, groupHooks, () => crypto.randomUUID());
    expect(compiled.tasks).toHaveLength(1);
    expect(compiled.tasks[0]?.hook).toBe('@morfact/core/coordinator');
    const meta = (compiled.tasks[0]?.input as { _meta?: { coordinator?: { groupSpec?: { id: string } } } })._meta;
    expect(meta?.coordinator?.groupSpec?.id).toBe('orphan_group');
  });

  it('embeds groupRef on coordinator and keeps legacy taskGroupTemplates working', () => {
    const flow: FlowDefinition = {
      apiVersion: 'flow.v1',
      kind: 'Flow',
      metadata: { name: 'group.refs', version: '1.0' },
      spec: {
        nodes: [
          {
            id: 'act_plan',
            type: 'group',
            group: {
              tasks: [
                {
                  id: 'plan',
                  type: 'business',
                  hooks: { executor: { use: businessHook } },
                },
              ],
            },
          },
          {
            id: 'legacy_fanout',
            type: 'coordinator',
            coordinator: {
              taskGroupTemplate: 'legacy_units',
              groupCount: 1,
            },
            hooks: { executor: { use: '@morfact/core/coordinator' } },
          },
          {
            id: 'new_fanout',
            type: 'coordinator',
            coordinator: {
              groupRef: 'act_plan',
              groupCount: 2,
            },
            hooks: { executor: { use: '@morfact/core/coordinator' } },
          },
        ],
        taskGroupTemplates: {
          legacy_units: {
            tasks: [
              {
                id: 'unit',
                type: 'business',
                hooks: { executor: { use: businessHook } },
              },
            ],
          },
        },
      },
    };

    const compiled = compileFlow(flow, groupHooks, () => crypto.randomUUID());
    const legacy = compiled.tasks.find((t) => t.logicalKey === 'legacy_fanout');
    const modern = compiled.tasks.find((t) => t.logicalKey === 'new_fanout');
    expect(
      (legacy?.input as { _meta?: { coordinator?: { groupSpec?: { id: string } } } })._meta
        ?.coordinator?.groupSpec?.id,
    ).toBe('legacy_units');
    expect(
      (modern?.input as { _meta?: { coordinator?: { groupRef?: string } } })._meta
        ?.coordinator?.groupRef,
    ).toBe('act_plan');
  });

  it('rejects forbidden inner node types inside a group', () => {
    const flow: FlowDefinition = {
      apiVersion: 'flow.v1',
      kind: 'Flow',
      metadata: { name: 'group.bad', version: '1.0' },
      spec: {
        nodes: [
          {
            id: 'bad_group',
            type: 'group',
            group: {
              tasks: [
                {
                  id: 'spawn_child',
                  type: 'spawn' as 'business',
                  hooks: { executor: { use: businessHook } },
                },
              ],
            },
          },
          {
            id: 'downstream',
            type: 'business',
            hooks: { executor: { use: businessHook } },
          },
        ],
        edges: [{ from: 'bad_group', to: 'downstream' }],
      },
    };

    expect(() => validateFlowDefinition(flow, groupHooks)).toThrow(/forbidden type 'spawn'/);
  });
});
