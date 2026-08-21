// hooks/gate.ts — built-in human-in-the-loop gate.
//
// If the compiled node carried `interaction` and required fields are still
// missing, the hook returns waiting. The engine creates a TaskInteraction
// record on that path. After submitInteraction + resumeTask, required fields
// are expected on task input and the gate passes through.

import type {
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
} from '../contracts.js';
import type { InteractionConfig } from '../flow-types.js';

function missingRequiredFields(
  input: Record<string, unknown>,
  interaction: InteractionConfig | undefined,
): string[] {
  const required = interaction?.schema?.required ?? [];
  return required.filter((field) => {
    const value = input[field];
    return value === undefined || value === null || value === '';
  });
}

export const gateHook: HookDefinition<
  Record<string, unknown>,
  Record<string, unknown>,
  unknown
> = {
  name: '@morfact/core/gate',
  async execute(
    ctx: HookExecutionContext<unknown>,
    input: Record<string, unknown>,
  ): Promise<HookExecutionResult<Record<string, unknown>>> {
    const meta = input._meta as Record<string, unknown> | undefined;
    const interaction = meta?.interaction as InteractionConfig | undefined;
    const missing = missingRequiredFields(input, interaction);

    if (interaction && missing.length > 0) {
      return {
        status: 'waiting',
        reason: 'interaction',
        resumeToken: `gate:${ctx.task.id}`,
      };
    }

    return {
      status: 'succeeded',
      output: input,
    };
  },
};
