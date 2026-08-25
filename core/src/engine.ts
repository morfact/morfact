import type {
  Engine,
  EngineOptions,
  ExecuteNextTaskCommand,
  ExecuteTaskCommand,
  ExecuteTaskResult,
  HookDefinition,
  HookExecutionResult,
  ListRunsFilter,
  StartRunCommand,
  TaskLifecycleEvent,
  TaskRun,
  WorkflowDefinition,
} from './contracts.js';
import {
  materializeWorkflow,
  validateWorkflowDefinition,
} from './compiler.js';
import { resolveWorkflowTaskInput } from './input-bindings.js';
import {
  HookRegistrationError,
  StoreProtocolError,
  WorkflowValidationError,
} from './errors.js';
import { joinHook } from './hooks/join.js';
import { gateHook } from './hooks/gate.js';
import { completedHook } from './hooks/completed.js';
import { coordinatorHook } from './hooks/coordinator.js';
import { createSpawnHook } from './hooks/spawn.js';
import { createLoopHook } from './hooks/loop.js';
import {
  createBuiltinRuntimeServices,
  mergeEngineServices,
} from './builtin-services.js';
import type { InteractionConfig } from './flow-types.js';
import {
  commitKindFromResult,
  dispatchBeforeCommit,
  dispatchSoftLifecycle,
  runAfterExecute,
  runBeforeExecute,
  sortLifecycles,
} from './lifecycle.js';

function workflowKey(name: string, version: string): string {
  return `${name}@${version}`;
}

function defaultIdGenerator(): string {
  return crypto.randomUUID();
}

function withLoopRuntime(
  input: Record<string, unknown>,
  task: Pick<TaskRun, 'meta'>,
): Record<string, unknown> {
  const loopState = task.meta?.['loopState'];
  if (!loopState || typeof loopState !== 'object' || Array.isArray(loopState)) {
    return input;
  }
  const state = loopState as Record<string, unknown>;
  const iteration =
    typeof state.iteration === 'number' && Number.isFinite(state.iteration)
      ? state.iteration
      : 0;
  return {
    ...input,
    loop:
      state.checkpoint !== undefined
        ? { iteration, checkpoint: state.checkpoint }
        : { iteration },
  };
}

function installUnique<T>(
  target: Map<string, T>,
  key: string,
  value: T,
  duplicateError: (key: string) => Error,
): void {
  if (target.has(key)) {
    throw duplicateError(key);
  }
  target.set(key, value);
}

export function createEngine(options: EngineOptions): Engine {
  if (options.store.protocolVersion !== 1) {
    throw new StoreProtocolError(
      `Unsupported store protocol: ${String(options.store.protocolVersion)}`,
    );
  }

  const hooks = new Map<string, HookDefinition>();
  const workflows = new Map<string, WorkflowDefinition>();
  const now = options.now ?? (() => new Date());
  const createId = options.idGenerator ?? defaultIdGenerator;
  const lifecycles = sortLifecycles(options.lifecycle ?? []);
  let runtimeServices: unknown = options.services;

  const builtinHooks: readonly HookDefinition[] = [
    joinHook,
    gateHook,
    completedHook,
    coordinatorHook,
    createSpawnHook(options.childTemplateRegistry),
    createLoopHook(hooks),
  ];

  for (const hook of builtinHooks) {
    installUnique(hooks, hook.name, hook, (key) =>
      new HookRegistrationError(`Duplicate built-in hook: ${key}`),
    );
  }

  async function executeClaimedTask(
    command: ExecuteNextTaskCommand,
  ): Promise<ExecuteTaskResult | null> {
    const claimedAt = now();
    const leaseDurationMs = command.leaseDurationMs ?? 30_000;
    const claimed = await options.store.claimTask({
      workerId: command.workerId,
      ...(command.taskId === undefined ? {} : { taskId: command.taskId }),
      now: claimedAt,
      leaseExpiresAt: new Date(claimedAt.getTime() + leaseDurationMs),
    });
    if (!claimed) {
      return null;
    }

    const hook = hooks.get(claimed.task.hook);
    if (!hook) {
      throw new HookRegistrationError(
        `Claimed task references missing hook: ${claimed.task.hook}`,
      );
    }

    const lifecycleBase = {
      workerId: command.workerId,
      run: {
        id: claimed.run.id,
        tenantKey: claimed.run.tenantKey,
        input: claimed.run.input,
        context: claimed.run.context,
        ...(claimed.run.resourceRef === undefined
          ? {}
          : { resourceRef: claimed.run.resourceRef }),
      },
      task: {
        id: claimed.task.id,
        logicalKey: claimed.task.logicalKey,
        hook: claimed.task.hook,
        attempt: claimed.task.attempt,
      },
    } as const;

    await dispatchSoftLifecycle(lifecycles, 'onTaskClaimed', {
      ...lifecycleBase,
      phase: 'claimed',
    });

    const heartbeatIntervalMs =
      command.heartbeatIntervalMs ?? Math.max(1, Math.floor(leaseDurationMs / 3));
    if (heartbeatIntervalMs >= leaseDurationMs) {
      throw new StoreProtocolError(
        'Task heartbeat interval must be shorter than the lease duration',
      );
    }
    let heartbeatFailure: unknown;
    let heartbeatPromise: Promise<void> | undefined;
    const heartbeatTimer = setInterval(() => {
      if (heartbeatPromise !== undefined || heartbeatFailure !== undefined) {
        return;
      }
      const heartbeatAt = now();
      heartbeatPromise = options.store
        .heartbeatTask({
          taskId: claimed.task.id,
          leaseToken: claimed.task.leaseToken,
          now: heartbeatAt,
          leaseExpiresAt: new Date(heartbeatAt.getTime() + leaseDurationMs),
        })
        .then(() => undefined)
        .catch((error: unknown) => {
          heartbeatFailure = error;
        })
        .finally(() => {
          heartbeatPromise = undefined;
        });
    }, heartbeatIntervalMs);
    heartbeatTimer.unref?.();

    let result: HookExecutionResult | undefined;
    let hookInput: unknown;
    /** True once a store outcome write succeeded (success/waiting/continue/failure). */
    let finalized = false;
    try {
      try {
        const resolved = resolveWorkflowTaskInput(
          claimed.task.input,
          claimed.run,
        );
        const resolvedInput =
          resolved &&
          typeof resolved === 'object' &&
          !Array.isArray(resolved)
            ? withLoopRuntime(
                resolved as Record<string, unknown>,
                claimed.task,
              )
            : resolved;

        const before = await runBeforeExecute(lifecycles, {
          ...lifecycleBase,
          input: resolvedInput,
        });
        hookInput = before.input;

        if (before.shortCircuit !== undefined) {
          result = before.shortCircuit;
        } else {
          result = await hook.execute(
            {
              run: {
                id: claimed.run.id,
                tenantKey: claimed.run.tenantKey,
                input: claimed.run.input,
                context: claimed.run.context,
                ...(claimed.run.resourceRef === undefined
                  ? {}
                  : { resourceRef: claimed.run.resourceRef }),
              },
              task: {
                id: claimed.task.id,
                logicalKey: claimed.task.logicalKey,
                attempt: claimed.task.attempt,
              },
              services: runtimeServices,
              signal: command.signal ?? new AbortController().signal,
              now,
            },
            hookInput,
          );
        }

        result = await runAfterExecute(lifecycles, {
          ...lifecycleBase,
          input: hookInput,
          result,
        });
      } finally {
        clearInterval(heartbeatTimer);
        await heartbeatPromise;
      }

      if (heartbeatFailure !== undefined) {
        throw new StoreProtocolError(
          `Task heartbeat failed before commit: ${
            heartbeatFailure instanceof Error
              ? heartbeatFailure.message
              : String(heartbeatFailure)
          }`,
          { cause: heartbeatFailure },
        );
      }

      if (result === undefined) {
        throw new StoreProtocolError('Task execution produced no result');
      }

      const commitKind = commitKindFromResult(result);
      await dispatchBeforeCommit(lifecycles, {
        ...lifecycleBase,
        phase: 'beforeCommit',
        input: hookInput,
        result,
        commitKind,
      });

      if (result.status === 'waiting') {
        if (result.reason === 'interaction') {
          const existing = await options.store.listInteractions(claimed.task.id);
          if (existing.length === 0) {
            const resolvedInput = resolveWorkflowTaskInput(
              claimed.task.input,
              claimed.run,
            ) as Record<string, unknown>;
            const meta = resolvedInput._meta as
              | Record<string, unknown>
              | undefined;
            const interaction = meta?.interaction as
              | InteractionConfig
              | undefined;
            await options.store.createInteraction({
              interactionId: createId(),
              tenantKey: claimed.run.tenantKey,
              runId: claimed.run.id,
              taskId: claimed.task.id,
              kind: interaction?.kind ?? 'input',
              ...(interaction?.schema !== undefined
                ? {
                    schema: interaction.schema as Readonly<
                      Record<string, unknown>
                    >,
                  }
                : {}),
              ...(interaction?.uiHints !== undefined
                ? { uiHints: interaction.uiHints }
                : {}),
              now: now(),
            });
          }
        }
        await options.store.suspendTask({
          commandId: createId(),
          taskId: claimed.task.id,
          leaseToken: claimed.task.leaseToken,
          reason: result.reason,
          resumeToken: result.resumeToken,
          now: now(),
        });
        finalized = true;
        await dispatchSoftLifecycle(lifecycles, 'afterCommit', {
          ...lifecycleBase,
          phase: 'afterCommit',
          input: hookInput,
          result,
          commitKind,
        });
        return {
          taskId: claimed.task.id,
          runId: claimed.run.id,
          status: 'waiting',
        };
      }

      if (result.status === 'continue') {
        if (typeof options.store.commitTaskContinue !== 'function') {
          throw new StoreProtocolError(
            'Store does not implement commitTaskContinue (required by @morfact/core/loop)',
          );
        }
        if (claimed.task.leaseToken === undefined) {
          throw new StoreProtocolError(
            'Claimed task is missing a lease token; cannot commit continue',
          );
        }
        await options.store.commitTaskContinue({
          commandId: createId(),
          taskId: claimed.task.id,
          leaseToken: claimed.task.leaseToken,
          checkpoint: result.checkpoint,
          iteration: result.iteration,
          ...(result.output === undefined ? {} : { output: result.output }),
          now: now(),
        });
        finalized = true;
        await dispatchSoftLifecycle(lifecycles, 'afterCommit', {
          ...lifecycleBase,
          phase: 'afterCommit',
          input: hookInput,
          result,
          commitKind,
        });
        return {
          taskId: claimed.task.id,
          runId: claimed.run.id,
          status: 'continue',
        };
      }

      await options.store.commitTaskSuccess({
        commandId: createId(),
        taskId: claimed.task.id,
        leaseToken: claimed.task.leaseToken,
        output: result.output,
        ...(result.contextPatch === undefined
          ? {}
          : { contextPatch: result.contextPatch }),
        now: now(),
      });
      finalized = true;
      await maybeResumeWaiters(claimed.task.id);
      await dispatchSoftLifecycle(lifecycles, 'afterCommit', {
        ...lifecycleBase,
        phase: 'afterCommit',
        input: hookInput,
        result,
        commitKind,
      });
      return {
        taskId: claimed.task.id,
        runId: claimed.run.id,
        status: 'succeeded',
      };
    } catch (error) {
      if (!finalized) {
        try {
          await options.store.commitTaskFailure({
            commandId: createId(),
            taskId: claimed.task.id,
            leaseToken: claimed.task.leaseToken,
            error: {
              message: error instanceof Error ? error.message : String(error),
            },
            now: now(),
          });
          finalized = true;
          await maybeResumeWaiters(claimed.task.id);
        } catch {
          // Prefer surfacing the original execution/lifecycle error.
        }
        const failedEvent: TaskLifecycleEvent = {
          ...lifecycleBase,
          phase: 'failed',
          ...(hookInput === undefined ? {} : { input: hookInput }),
          ...(result === undefined ? {} : { result }),
          error,
          commitKind: 'failure',
        };
        await dispatchSoftLifecycle(lifecycles, 'onTaskFailed', failedEvent);
      }
      throw error;
    }
  }

  async function maybeResumeWaiters(taskId: string): Promise<void> {
    const task = await options.store.getTask(taskId);
    if (!task) return;

    if (task.ancestorId) {
      const coordinator = await options.store.getTask(task.ancestorId);
      if (
        coordinator?.status === 'waiting' &&
        coordinator.resumeToken !== undefined
      ) {
        const siblings = (await options.store.listTasks(task.runId)).filter(
          (item) => item.ancestorId === task.ancestorId,
        );
        const unfinished = siblings.filter(
          (item) =>
            item.status !== 'succeeded' &&
            item.status !== 'skipped' &&
            item.status !== 'cancelled',
        );
        if (siblings.length > 0 && unfinished.length === 0) {
          await options.store.resumeTask({
            commandId: createId(),
            taskId: coordinator.id,
            resumeToken: coordinator.resumeToken,
            now: now(),
          });
        }
      }
    }

    const run = await options.store.getRun(task.runId);
    if (
      !run?.parentTaskId ||
      (run.status !== 'succeeded' &&
        run.status !== 'failed' &&
        run.status !== 'cancelled')
    ) {
      return;
    }
    const parent = await options.store.getTask(run.parentTaskId);
    if (
      parent?.status === 'waiting' &&
      parent.waitingReason === 'child_run' &&
      parent.resumeToken !== undefined
    ) {
      await options.store.resumeTask({
        commandId: createId(),
        taskId: parent.id,
        resumeToken: parent.resumeToken,
        now: now(),
      });
    }
  }

  function resolveWorkflowSpec(nameAndVersion: string): {
    definition: WorkflowDefinition;
    tasks: ReturnType<typeof materializeWorkflow>;
  } {
    // Try direct WorkflowDefinition lookup first
    const directDef = workflows.get(nameAndVersion);
    if (directDef) {
      validateWorkflowDefinition(directDef, new Set(hooks.keys()));
      return {
        definition: directDef,
        tasks: materializeWorkflow(directDef, createId),
      };
    }

    // Try FlowDefinition lookup (need to compile)
    // Flow definitions are also stored in workflows map after compilation
    throw new WorkflowValidationError(
      `Unknown workflow: ${nameAndVersion}`,
    );
  }

  for (const plugin of options.plugins ?? []) {
    for (const hook of plugin.hooks) {
      installUnique(
        hooks,
        hook.name,
        hook,
        (key) => new HookRegistrationError(`Duplicate hook registration: ${key}`),
      );
    }

    for (const workflow of plugin.workflows ?? []) {
      const key = workflowKey(workflow.name, workflow.version);
      installUnique(
        workflows,
        key,
        workflow,
        (duplicateKey) =>
          new WorkflowValidationError(
            `Duplicate workflow registration: ${duplicateKey}`,
          ),
      );
    }
  }

  runtimeServices = mergeEngineServices(
    createBuiltinRuntimeServices({
      store: options.store,
      workflows,
      createId,
      now,
    }),
    options.services,
  );

  return {
    hooks,
    workflows,
    async validate() {
      const capabilities = await options.store.getCapabilities();
      if (!capabilities.atomicTaskCommit) {
        throw new StoreProtocolError(
          'Store lacks required atomic task commit capability',
        );
      }

      const availableHooks = new Set(hooks.keys());
      for (const workflow of workflows.values()) {
        validateWorkflowDefinition(workflow, availableHooks);
      }
    },
    async startRun(command: StartRunCommand) {
      const { definition, tasks } = resolveWorkflowSpec(command.workflow);

      return options.store.createRun({
        runId: createId(),
        tenantKey: command.tenantKey,
        ...(command.resourceRef === undefined
          ? {}
          : { resourceRef: command.resourceRef }),
        ...(command.correlationKey === undefined
          ? {}
          : { correlationKey: command.correlationKey }),
        definition,
        input: command.input ?? {},
        context: command.context ?? {},
        tasks,
        now: now(),
      });
    },
    executeTask(command: ExecuteTaskCommand) {
      return executeClaimedTask(command);
    },
    executeNextTask(command: ExecuteNextTaskCommand) {
      return executeClaimedTask(command);
    },
    async resumeTask(
      taskId: string,
      resumeToken: string,
      input?: Readonly<Record<string, unknown>>,
    ) {
      return options.store.resumeTask({
        commandId: createId(),
        taskId,
        resumeToken,
        ...(input === undefined ? {} : { input }),
        now: now(),
      });
    },
    cancelTask(taskId: string) {
      return options.store.cancelTask({
        commandId: createId(),
        taskId,
        now: now(),
      });
    },
    cancelRun(runId: string) {
      return options.store.cancelRun({
        commandId: createId(),
        runId,
        now: now(),
      });
    },
    completePollingTask(taskId: string, output: unknown) {
      return options.store.completePollingTask({
        commandId: createId(),
        taskId,
        output,
        now: now(),
      });
    },
    failPollingTask(
      taskId: string,
      error: { readonly message: string },
    ) {
      return options.store.failPollingTask({
        commandId: createId(),
        taskId,
        error,
        now: now(),
      });
    },
    async spawnChildRun(command) {
      const { definition, tasks } = resolveWorkflowSpec(command.workflow);

      const parentTask = await options.store.getTask(command.parentTaskId);
      if (!parentTask) {
        throw new WorkflowValidationError(
          `Parent task not found: ${command.parentTaskId}`,
        );
      }
      if (parentTask.status !== 'waiting' || parentTask.waitingReason !== 'child_run') {
        throw new StoreProtocolError(
          `Parent task is not waiting for child runs: ${command.parentTaskId} (status: ${parentTask.status})`,
        );
      }

      return options.store.createChildRun({
        runId: createId(),
        tenantKey: command.tenantKey,
        parentTaskId: command.parentTaskId,
        ...(command.resourceRef === undefined
          ? {}
          : { resourceRef: command.resourceRef }),
        definition,
        input: command.input ?? {},
        context: command.context ?? {},
        tasks,
        now: now(),
      });
    },
    listRuns(filter?: ListRunsFilter) {
      return options.store.listRuns(filter);
    },
    getTask(taskId: string) {
      return options.store.getTask(taskId);
    },
    getRun(runId: string) {
      return options.store.getRun(runId);
    },
    listTasks(runId: string) {
      return options.store.listTasks(runId);
    },
  };
}
