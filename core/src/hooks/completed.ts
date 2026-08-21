// hooks/completed.ts — built-in completion marker hook.
// Runs as the final task; signals that the workflow has reached its end.

import type {
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
} from '../contracts.js';

export interface CompletedOutput {
  completed: true;
  completedAt: string;
}

export const completedHook: HookDefinition<
  Record<string, unknown>,
  CompletedOutput,
  unknown
> = {
  name: '@morfact/core/completed',
  async execute(
    ctx: HookExecutionContext<unknown>,
    _input: Record<string, unknown>,
  ): Promise<HookExecutionResult<CompletedOutput>> {
    return {
      status: 'succeeded',
      output: {
        completed: true,
        completedAt: new Date().toISOString(),
      },
    };
  },
};
