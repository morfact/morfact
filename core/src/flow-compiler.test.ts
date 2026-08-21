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
