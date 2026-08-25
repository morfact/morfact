import type {
  AfterExecuteResult,
  BeforeExecuteResult,
  EngineLifecycle,
  HookExecutionResult,
  TaskCommitKind,
  TaskLifecycleEvent,
} from './contracts.js';

export function sortLifecycles(
  items: readonly EngineLifecycle[],
): readonly EngineLifecycle[] {
  return items
    .map((item, index) => ({ item, index }))
    .sort((a, b) => {
      const orderA = a.item.order ?? 0;
      const orderB = b.item.order ?? 0;
      if (orderA !== orderB) {
        return orderA - orderB;
      }
      return a.index - b.index;
    })
    .map(({ item }) => item);
}

export function commitKindFromResult(
  result: HookExecutionResult,
): Exclude<TaskCommitKind, 'failure'> {
  if (result.status === 'waiting') return 'waiting';
  if (result.status === 'continue') return 'continue';
  return 'success';
}

function isBeforeExecuteResult(
  value: unknown,
): value is BeforeExecuteResult {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  return 'input' in value || 'shortCircuit' in value;
}

function isAfterExecuteResult(value: unknown): value is AfterExecuteResult {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  return 'result' in value;
}

async function notifyLifecycleErrors(
  lifecycles: readonly EngineLifecycle[],
  error: unknown,
  event: TaskLifecycleEvent,
): Promise<void> {
  for (const lifecycle of lifecycles) {
    if (!lifecycle.onLifecycleError) continue;
    try {
      await lifecycle.onLifecycleError(error, event);
    } catch {
      // Soft-phase error handlers must not throw into the engine.
    }
  }
}

/** Soft phases: errors are swallowed and reported via onLifecycleError. */
export async function dispatchSoftLifecycle(
  lifecycles: readonly EngineLifecycle[],
  method: 'onTaskClaimed' | 'afterCommit' | 'onTaskFailed',
  event: TaskLifecycleEvent,
): Promise<void> {
  for (const lifecycle of lifecycles) {
    const handler = lifecycle[method];
    if (!handler) continue;
    try {
      await handler.call(lifecycle, event);
    } catch (error) {
      await notifyLifecycleErrors(lifecycles, error, event);
    }
  }
}

/**
 * Hard phase: throws propagate to the engine (task failure before commit).
 * Pipeline rewrite: later handlers see prior rewrites; shortCircuit stops the chain.
 */
export async function runBeforeExecute(
  lifecycles: readonly EngineLifecycle[],
  base: Omit<TaskLifecycleEvent, 'phase' | 'input' | 'result' | 'error' | 'commitKind'> & {
    readonly input: unknown;
  },
): Promise<{ input: unknown; shortCircuit?: HookExecutionResult }> {
  let input = base.input;
  for (const lifecycle of lifecycles) {
    if (!lifecycle.beforeExecute) continue;
    const event: TaskLifecycleEvent = {
      ...base,
      phase: 'beforeExecute',
      input,
    };
    const out = await lifecycle.beforeExecute(event);
    if (!isBeforeExecuteResult(out)) continue;
    if ('shortCircuit' in out) {
      return { input, shortCircuit: out.shortCircuit };
    }
    input = out.input;
  }
  return { input };
}

/** Hard phase: pipeline rewrite of HookExecutionResult. */
export async function runAfterExecute(
  lifecycles: readonly EngineLifecycle[],
  base: Omit<TaskLifecycleEvent, 'phase' | 'result' | 'error' | 'commitKind'> & {
    readonly result: HookExecutionResult;
  },
): Promise<HookExecutionResult> {
  let result = base.result;
  for (const lifecycle of lifecycles) {
    if (!lifecycle.afterExecute) continue;
    const event: TaskLifecycleEvent = {
      ...base,
      phase: 'afterExecute',
      result,
    };
    const out = await lifecycle.afterExecute(event);
    if (!isAfterExecuteResult(out)) continue;
    result = out.result;
  }
  return result;
}

/** Hard phase: errors propagate. */
export async function dispatchBeforeCommit(
  lifecycles: readonly EngineLifecycle[],
  event: TaskLifecycleEvent,
): Promise<void> {
  for (const lifecycle of lifecycles) {
    if (!lifecycle.beforeCommit) continue;
    await lifecycle.beforeCommit(event);
  }
}
