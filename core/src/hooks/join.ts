// hooks/join.ts — built-in join passthrough hook.
// When a join node reaches 0 deps, this hook runs and passes through.

import type {
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
} from '../contracts.js';

export interface JoinOutput {
  joined: true;
  mode: string;
  joinedAt: string;
}

export const joinHook: HookDefinition<
  Record<string, unknown>,
  JoinOutput,
  unknown
> = {
  name: '@morfact/core/join',
  async execute(
    ctx: HookExecutionContext<unknown>,
    input: Record<string, unknown>,
  ): Promise<HookExecutionResult<JoinOutput>> {
    const meta = input._meta as Record<string, unknown> | undefined;
    const joinConfig = meta?.join as Record<string, unknown> | undefined;
    const mode = String(joinConfig?.mode ?? 'all');

    return {
      status: 'succeeded',
      output: {
        joined: true,
        mode,
        joinedAt: new Date().toISOString(),
      },
    };
  },
};
