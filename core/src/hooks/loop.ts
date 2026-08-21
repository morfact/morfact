// hooks/loop.ts — iterate one body hook on a single task.
//
// The flow graph stays a DAG. Each round is a real commit of checkpoint /
// progress; the engine re-queues the same task via `commitTaskContinue`.
// Throw retries the same iteration (existing maxAttempts). The next
// iteration starts only when the body returns `{ halt: false, checkpoint }`.

import type {
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
} from '../contracts.js';
import type { LoopConfig } from '../flow-types.js';

export interface LoopRuntimeInput {
  readonly iteration: number;
  readonly checkpoint?: unknown;
}

export interface LoopBodyOutput {
  readonly halt: boolean;
  readonly checkpoint?: unknown;
  readonly output?: unknown;
}

export interface LoopOutput {
  readonly haltReason: 'halt' | 'max_iterations';
  readonly truncated: boolean;
  readonly iteration: number;
  readonly checkpoint?: unknown;
  readonly output?: unknown;
}

function readLoopConfig(input: Record<string, unknown>): LoopConfig {
  const meta = input._meta as Record<string, unknown> | undefined;
  const config = meta?.loop as LoopConfig | undefined;
  if (!config || typeof config.body !== 'string' || config.body.length === 0) {
    throw new Error('Loop node requires _meta.loop.body');
  }
  if (
    typeof config.maxIterations !== 'number' ||
    !Number.isFinite(config.maxIterations) ||
    config.maxIterations < 1
  ) {
    throw new Error('Loop node requires _meta.loop.maxIterations >= 1');
  }
  return config;
}

function readRuntime(input: Record<string, unknown>): LoopRuntimeInput {
  const loop = input.loop as LoopRuntimeInput | undefined;
  if (!loop || typeof loop !== 'object') {
    return { iteration: 0 };
  }
  const iteration =
    typeof loop.iteration === 'number' && Number.isFinite(loop.iteration)
      ? loop.iteration
      : 0;
  return loop.checkpoint !== undefined
    ? { iteration, checkpoint: loop.checkpoint }
    : { iteration };
}

function parseBodyOutput(output: unknown): LoopBodyOutput {
  if (!output || typeof output !== 'object' || Array.isArray(output)) {
    throw new Error(
      'Loop body must return an object { halt, checkpoint?, output? }',
    );
  }
  const record = output as Record<string, unknown>;
  if (typeof record.halt !== 'boolean') {
    throw new Error('Loop body output.halt must be a boolean');
  }
  if (!record.halt && !('checkpoint' in record)) {
    throw new Error('Loop body must return checkpoint when halt is false');
  }
  return {
    halt: record.halt,
    ...('checkpoint' in record ? { checkpoint: record.checkpoint } : {}),
    ...('output' in record ? { output: record.output } : {}),
  };
}

function bodyInput(
  input: Record<string, unknown>,
  runtime: LoopRuntimeInput,
): Record<string, unknown> {
  const rest = { ...input };
  delete rest._meta;
  delete rest.loop;
  return {
    ...rest,
    loop:
      runtime.checkpoint !== undefined
        ? { iteration: runtime.iteration, checkpoint: runtime.checkpoint }
        : { iteration: runtime.iteration },
  };
}

function successOutput(
  haltReason: LoopOutput['haltReason'],
  truncated: boolean,
  iteration: number,
  parsed: LoopBodyOutput,
  fallbackCheckpoint: unknown,
): LoopOutput {
  const checkpoint =
    parsed.checkpoint !== undefined ? parsed.checkpoint : fallbackCheckpoint;
  return {
    haltReason,
    truncated,
    iteration,
    ...(checkpoint !== undefined ? { checkpoint } : {}),
    ...(parsed.output !== undefined ? { output: parsed.output } : {}),
  };
}

export function createLoopHook(
  hooks: ReadonlyMap<string, HookDefinition>,
): HookDefinition<Record<string, unknown>, LoopOutput, unknown> {
  return {
    name: '@morfact/core/loop',
    async execute(
      ctx: HookExecutionContext<unknown>,
      input: Record<string, unknown>,
    ): Promise<HookExecutionResult<LoopOutput>> {
      const config = readLoopConfig(input);
      if (config.body === '@morfact/core/loop') {
        throw new Error('Loop body must not be @morfact/core/loop');
      }
      const body = hooks.get(config.body);
      if (!body) {
        throw new Error(`Loop body hook is not registered: ${config.body}`);
      }

      const runtime = readRuntime(input);
      const result = await body.execute(ctx, bodyInput(input, runtime));

      if (result.status === 'waiting') {
        return result;
      }
      if (result.status === 'continue') {
        throw new Error(
          'Loop body must return { halt, checkpoint } on success, not continue',
        );
      }

      const parsed = parseBodyOutput(result.output);
      if (parsed.halt) {
        return {
          status: 'succeeded',
          output: successOutput(
            'halt',
            false,
            runtime.iteration,
            parsed,
            runtime.checkpoint,
          ),
        };
      }

      const nextIteration = runtime.iteration + 1;
      const onMax = config.onMaxIterations ?? 'fail';
      if (nextIteration >= config.maxIterations) {
        if (onMax === 'succeed') {
          return {
            status: 'succeeded',
            output: successOutput(
              'max_iterations',
              true,
              runtime.iteration,
              parsed,
              runtime.checkpoint,
            ),
          };
        }
        throw new Error(
          `Loop exceeded maxIterations (${String(config.maxIterations)})`,
        );
      }

      return {
        status: 'continue',
        checkpoint: parsed.checkpoint,
        iteration: nextIteration,
        output: {
          truncated: false,
          iteration: runtime.iteration,
          checkpoint: parsed.checkpoint,
          ...(parsed.output !== undefined ? { output: parsed.output } : {}),
        },
      };
    },
  };
}
