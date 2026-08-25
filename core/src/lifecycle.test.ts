import { describe, expect, it, vi } from 'vitest';

import {
  createEngine,
  defineHook,
  defineLifecycle,
  definePlugin,
  StoreProtocolError,
} from './index.js';
import type {
  EngineLifecycle,
  TaskLifecyclePhase,
  WorkflowStateStore,
} from './index.js';

function createBaseStore(): WorkflowStateStore {
  const unsupported = () => Promise.reject(new Error('Not implemented'));
  return {
    protocolVersion: 1,
    async getCapabilities() {
      return {
        atomicTaskCommit: true,
        transactionalOutbox: true,
        taskLeases: true,
      };
    },
    createRun: unsupported,
    claimTask: unsupported,
    heartbeatTask: unsupported,
    commitTaskSuccess: unsupported,
    commitTaskFailure: unsupported,
    suspendTask: unsupported,
    resumeTask: unsupported,
    cancelTask: unsupported,
    cancelRun: unsupported,
    completePollingTask: unsupported,
    failPollingTask: unsupported,
    createChildRun: unsupported,
    listRuns: unsupported,
    claimOutbox: unsupported,
    markOutboxDispatched: unsupported,
    markOutboxFailed: unsupported,
    getTask: unsupported,
    getRun: unsupported,
    listTasks: unsupported,
    createInteraction: unsupported,
    submitInteraction: unsupported,
    getInteraction: unsupported,
    listInteractions: unsupported,
  };
}

function createClaimableStore(options?: {
  readonly hookName?: string;
  readonly taskInput?: Readonly<Record<string, unknown>>;
  readonly failHeartbeat?: boolean;
}): {
  store: WorkflowStateStore;
  commits: {
    success: unknown[];
    failure: unknown[];
  };
} {
  const hookName = options?.hookName ?? 'test.lifecycle@1';
  const taskInput = options?.taskInput ?? { value: 1 };
  let claimed = false;
  const commits = {
    success: [] as unknown[],
    failure: [] as unknown[],
  };

  const store: WorkflowStateStore = {
    ...createBaseStore(),
    async claimTask(command) {
      if (claimed || command.taskId !== 'task-1') return null;
      claimed = true;
      return {
        run: {
          id: 'run-1',
          tenantKey: 'tenant-a',
          definitionName: 'test.lifecycle',
          definitionVersion: '1.0.0',
          definition: {
            schemaVersion: 1,
            name: 'test.lifecycle',
            version: '1.0.0',
            nodes: [],
          },
          input: {},
          context: {},
          status: 'running',
          createdAt: command.now,
          updatedAt: command.now,
        },
        task: {
          id: 'task-1',
          runId: 'run-1',
          tenantKey: 'tenant-a',
          logicalKey: 'node-a',
          hook: hookName,
          input: taskInput,
          status: 'leased',
          dependenciesRemaining: 0,
          attempt: 1,
          maxAttempts: 3,
          leaseOwner: command.workerId,
          leaseToken: 'lease-1',
          leaseExpiresAt: command.leaseExpiresAt,
          createdAt: command.now,
          updatedAt: command.now,
        },
      };
    },
    async heartbeatTask(command) {
      if (options?.failHeartbeat) {
        throw new Error('heartbeat down');
      }
      return {
        id: command.taskId,
        runId: 'run-1',
        tenantKey: 'tenant-a',
        logicalKey: 'node-a',
        hook: hookName,
        input: taskInput,
        status: 'leased',
        dependenciesRemaining: 0,
        attempt: 1,
        maxAttempts: 3,
        leaseOwner: 'worker',
        leaseToken: command.leaseToken,
        leaseExpiresAt: command.leaseExpiresAt,
        createdAt: command.now,
        updatedAt: command.now,
      };
    },
    async commitTaskSuccess(command) {
      commits.success.push(command);
      return {
        taskId: command.taskId,
        taskStatus: 'succeeded',
        runStatus: 'succeeded',
        unlockedTaskIds: [],
        replayed: false,
      };
    },
    async commitTaskFailure(command) {
      commits.failure.push(command);
      return {
        taskId: command.taskId,
        taskStatus: 'failed',
        runStatus: 'failed',
        unlockedTaskIds: [],
        replayed: false,
      };
    },
    async getTask() {
      return null;
    },
  };

  return { store, commits };
}

const workHook = defineHook({
  name: 'test.lifecycle@1',
  async execute(_ctx, input) {
    return {
      status: 'succeeded',
      output: input,
    } as const;
  },
});

function pluginWith(hook = workHook) {
  return definePlugin({
    manifest: {
      manifestVersion: 1,
      name: 'test.lifecycle-plugin',
      version: '1.0.0',
      engine: '^0.1.0',
    },
    hooks: [hook],
  });
}

describe('engine lifecycle', () => {
  it('runs phases in order and pipelines input/result rewrites', async () => {
    const phases: TaskLifecyclePhase[] = [];
    let seenHookInput: unknown;
    const { store, commits } = createClaimableStore({
      taskInput: { value: 1 },
    });

    const lifecycle: EngineLifecycle = defineLifecycle({
      name: 'rewrite',
      order: 10,
      onTaskClaimed(event) {
        phases.push(event.phase);
      },
      beforeExecute(event) {
        phases.push(event.phase);
        const input = event.input as { value: number };
        return { input: { value: input.value + 10 } };
      },
      afterExecute(event) {
        phases.push(event.phase);
        const output = (event.result as { status: 'succeeded'; output: { value: number } })
          .output;
        return {
          result: {
            status: 'succeeded',
            output: { value: output.value * 2 },
          },
        };
      },
      beforeCommit(event) {
        phases.push(event.phase);
        expect(event.commitKind).toBe('success');
      },
      afterCommit(event) {
        phases.push(event.phase);
      },
    });

    const earlier = defineLifecycle({
      name: 'earlier-rewrite',
      order: 1,
      beforeExecute(event) {
        const input = event.input as { value: number };
        return { input: { value: input.value + 1 } };
      },
    });

    const hook = defineHook({
      name: 'test.lifecycle@1',
      async execute(_ctx, input) {
        seenHookInput = input;
        return { status: 'succeeded', output: input } as const;
      },
    });

    const engine = createEngine({
      store,
      plugins: [pluginWith(hook)],
      lifecycle: [lifecycle, earlier],
      idGenerator: () => 'cmd-1',
    });

    const result = await engine.executeTask({
      workerId: 'w1',
      taskId: 'task-1',
    });

    expect(result?.status).toBe('succeeded');
    // earlier (+1) then rewrite (+10) => 12
    expect(seenHookInput).toEqual({ value: 12 });
    expect(commits.success[0]).toMatchObject({
      output: { value: 24 },
    });
    expect(phases).toEqual([
      'claimed',
      'beforeExecute',
      'afterExecute',
      'beforeCommit',
      'afterCommit',
    ]);
  });

  it('supports billing-style hold / settle / release', async () => {
    const ledger = {
      held: false,
      settled: false,
      released: false,
    };

    const billing = defineLifecycle({
      name: 'billing',
      beforeExecute() {
        ledger.held = true;
      },
      beforeCommit(event) {
        if (event.commitKind !== 'success') return;
        if (!ledger.held) throw new Error('missing hold');
        ledger.settled = true;
        ledger.held = false;
      },
      onTaskFailed() {
        if (ledger.held) {
          ledger.released = true;
          ledger.held = false;
        }
      },
    });

    const okStore = createClaimableStore();
    const okEngine = createEngine({
      store: okStore.store,
      plugins: [pluginWith()],
      lifecycle: [billing],
      idGenerator: () => 'cmd-ok',
    });
    await okEngine.executeTask({ workerId: 'w1', taskId: 'task-1' });
    expect(ledger).toEqual({ held: false, settled: true, released: false });
    expect(okStore.commits.success).toHaveLength(1);
    expect(okStore.commits.failure).toHaveLength(0);

    ledger.held = false;
    ledger.settled = false;
    ledger.released = false;

    const failBilling = defineLifecycle({
      name: 'billing-fail-settle',
      beforeExecute() {
        ledger.held = true;
      },
      beforeCommit() {
        throw new Error('settle refused');
      },
      onTaskFailed() {
        if (ledger.held) {
          ledger.released = true;
          ledger.held = false;
        }
      },
    });

    const failStore = createClaimableStore();
    const failEngine = createEngine({
      store: failStore.store,
      plugins: [pluginWith()],
      lifecycle: [failBilling],
      idGenerator: () => 'cmd-fail',
    });

    await expect(
      failEngine.executeTask({ workerId: 'w1', taskId: 'task-1' }),
    ).rejects.toThrow('settle refused');
    expect(ledger).toEqual({ held: false, settled: false, released: true });
    expect(failStore.commits.success).toHaveLength(0);
    expect(failStore.commits.failure).toHaveLength(1);
  });

  it('shortCircuit skips the business hook but still commits', async () => {
    const executed = vi.fn();
    const hook = defineHook({
      name: 'test.lifecycle@1',
      async execute() {
        executed();
        return { status: 'succeeded', output: null } as const;
      },
    });

    const { store, commits } = createClaimableStore();
    const engine = createEngine({
      store,
      plugins: [pluginWith(hook)],
      lifecycle: [
        defineLifecycle({
          name: 'deny',
          beforeExecute() {
            return {
              shortCircuit: {
                status: 'succeeded',
                output: { denied: true },
              },
            };
          },
        }),
      ],
      idGenerator: () => 'cmd-sc',
    });

    const result = await engine.executeTask({
      workerId: 'w1',
      taskId: 'task-1',
    });
    expect(result?.status).toBe('succeeded');
    expect(executed).not.toHaveBeenCalled();
    expect(commits.success[0]).toMatchObject({
      output: { denied: true },
    });
  });

  it('swallows afterCommit errors via onLifecycleError', async () => {
    const softErrors: unknown[] = [];
    const { store, commits } = createClaimableStore();
    const engine = createEngine({
      store,
      plugins: [pluginWith()],
      lifecycle: [
        defineLifecycle({
          name: 'noisy',
          afterCommit() {
            throw new Error('metrics down');
          },
          onLifecycleError(error) {
            softErrors.push(error);
          },
        }),
      ],
      idGenerator: () => 'cmd-soft',
    });

    await expect(
      engine.executeTask({ workerId: 'w1', taskId: 'task-1' }),
    ).resolves.toMatchObject({ status: 'succeeded' });
    expect(commits.success).toHaveLength(1);
    expect(softErrors).toHaveLength(1);
    expect(String(softErrors[0])).toContain('metrics down');
  });

  it('records failure when heartbeat fails before commit', async () => {
    const released = vi.fn();
    const { store, commits } = createClaimableStore({ failHeartbeat: true });
    const slowHook = defineHook({
      name: 'test.lifecycle@1',
      async execute() {
        await new Promise((resolve) => setTimeout(resolve, 30));
        return { status: 'succeeded', output: null } as const;
      },
    });

    const engine = createEngine({
      store,
      plugins: [pluginWith(slowHook)],
      lifecycle: [
        defineLifecycle({
          name: 'billing',
          onTaskFailed() {
            released();
          },
        }),
      ],
      idGenerator: () => 'cmd-hb',
    });

    await expect(
      engine.executeTask({
        workerId: 'w1',
        taskId: 'task-1',
        leaseDurationMs: 90,
        heartbeatIntervalMs: 10,
      }),
    ).rejects.toBeInstanceOf(StoreProtocolError);
    expect(commits.failure).toHaveLength(1);
    expect(commits.success).toHaveLength(0);
    expect(released).toHaveBeenCalledTimes(1);
  });
});
