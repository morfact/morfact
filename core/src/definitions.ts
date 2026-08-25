import type {
  EngineLifecycle,
  HookDefinition,
  WorkflowDefinition,
  WorkflowPlugin,
} from './contracts.js';

export function defineHook<
  TInput,
  TOutput,
  TServices = unknown,
>(
  definition: HookDefinition<TInput, TOutput, TServices>,
): HookDefinition<TInput, TOutput, TServices> {
  return Object.freeze(definition);
}

export function defineWorkflow(
  definition: WorkflowDefinition,
): WorkflowDefinition {
  return Object.freeze(definition);
}

export function definePlugin(plugin: WorkflowPlugin): WorkflowPlugin {
  return Object.freeze(plugin);
}

/** Freeze an engine-level lifecycle (not a business hook). */
export function defineLifecycle(
  definition: EngineLifecycle,
): EngineLifecycle {
  return Object.freeze(definition);
}
