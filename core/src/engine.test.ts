import { describe, expect, it } from 'vitest';

import {
  createEngine,
  defineHook,
  definePlugin,
  defineWorkflow,
  HookRegistrationError,
  WorkflowValidationError,
} from './index.js';
import type { WorkflowStateStore } from './index.js';

function createStore(): WorkflowStateStore {
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

const noopHook = defineHook({
  name: 'test.noop@1',
  async execute() {
    return { status: 'succeeded', output: null } as const;
  },
});

function createRunnableStore(): WorkflowStateStore {
  let runInput: Readonly<Record<string, unknown>> = {};
  let runContext: Readonly<Record<string, unknown>> = {};
  let claimed = false;
  return {
    ...createStore(),
    async createRun(command) {
      runInput = command.input;
      runContext = command.context;
      return {
        id: command.runId,
        tenantKey: command.tenantKey,
        definitionName: command.definition.name,
        definitionVersion: command.definition.version,
        definition: command.definition,
        input: runInput,
        context: runContext,
        status: 'running',
        createdAt: command.now,
        updatedAt: command.now,
      };
    },
    async claimTask(command) {
      if (claimed || command.taskId !== 'task-bound') return null;
      claimed = true;
      return {
        run: {
          id: 'run-bound',
          tenantKey: 'tenant',
          definitionName: 'test.binding',
          definitionVersion: '1.0.0',
          definition: {
            schemaVersion: 1,
            name: 'test.binding',
            version: '1.0.0',
            nodes: [],
          },
          input: runInput,
          context: runContext,
          status: 'running',
          createdAt: command.now,
          updatedAt: command.now,
        },
        task: {
          id: 'task-bound',
          runId: 'run-bound',
          tenantKey: 'tenant',
          logicalKey: 'bound',
          hook: 'test.binding@1',
          input: {
            request: '${run.input.request}',
            prior: '${run.context.prior.value}',
          },
          status: 'leased',
          dependenciesRemaining: 0,
          attempt: 1,
          maxAttempts: 1,
          leaseOwner: command.workerId,
          leaseToken: 'lease',
          leaseExpiresAt: command.leaseExpiresAt,
          createdAt: command.now,
          updatedAt: command.now,
        },
      };
    },
    async commitTaskSuccess(command) {
      return {
        taskId: command.taskId,
        taskStatus: 'succeeded',
        runStatus: 'succeeded',
        unlockedTaskIds: [],
        replayed: false,
      };
    },
    async heartbeatTask(command) {
      return {
        id: command.taskId,
        runId: 'run-bound',
        tenantKey: 'tenant',
        logicalKey: 'bound',
        hook: 'test.binding@1',
        input: {},
        status: 'leased',
        dependenciesRemaining: 0,
        attempt: 1,
        maxAttempts: 1,
        leaseOwner: 'worker',
        leaseToken: command.leaseToken,
        leaseExpiresAt: command.leaseExpiresAt,
        createdAt: command.now,
        updatedAt: command.now,
      };
    },
    async getTask() {
      return null;
    },
  };
}

describe('createEngine', () => {
  it('assembles and validates an explicit plugin', async () => {
    const workflow = defineWorkflow({
      schemaVersion: 1,
      name: 'test.flow',
      version: '1.0.0',
      nodes: [{ id: 'noop', hook: noopHook.name }],
    });
    const plugin = definePlugin({
      manifest: {
        manifestVersion: 1,
        name: 'test.plugin',
        version: '1.0.0',
        engine: '^0.1.0',
      },
      hooks: [noopHook],
      workflows: [workflow],
    });

    const engine = createEngine({ store: createStore(), plugins: [plugin] });

    await expect(engine.validate()).resolves.toBeUndefined();
    expect(engine.hooks.get(noopHook.name)).toBe(noopHook);
    expect(engine.workflows.get('test.flow@1.0.0')).toBe(workflow);
  });

  it('rejects duplicate hook registrations immediately', () => {
    const plugin = definePlugin({
      manifest: {
        manifestVersion: 1,
        name: 'test.duplicates',
        version: '1.0.0',
        engine: '^0.1.0',
      },
      hooks: [noopHook, noopHook],
    });

    expect(() =>
      createEngine({ store: createStore(), plugins: [plugin] }),
    ).toThrow(HookRegistrationError);
  });

  it('rejects workflows that reference missing hooks', async () => {
    const workflow = defineWorkflow({
      schemaVersion: 1,
      name: 'test.invalid',
      version: '1.0.0',
      nodes: [{ id: 'missing', hook: 'test.missing@1' }],
    });
    const plugin = definePlugin({
      manifest: {
        manifestVersion: 1,
        name: 'test.invalid-plugin',
        version: '1.0.0',
        engine: '^0.1.0',
      },
      hooks: [],
      workflows: [workflow],
    });
    const engine = createEngine({ store: createStore(), plugins: [plugin] });

    await expect(engine.validate()).rejects.toThrow(WorkflowValidationError);
  });

  it('rejects cyclic workflow dependencies', async () => {
    const workflow = defineWorkflow({
      schemaVersion: 1,
      name: 'test.cycle',
      version: '1.0.0',
      nodes: [
        { id: 'left', hook: noopHook.name, dependsOn: ['right'] },
        { id: 'right', hook: noopHook.name, dependsOn: ['left'] },
      ],
    });
    const plugin = definePlugin({
      manifest: {
        manifestVersion: 1,
        name: 'test.cycle-plugin',
        version: '1.0.0',
        engine: '^0.1.0',
      },
      hooks: [noopHook],
      workflows: [workflow],
    });
    const engine = createEngine({ store: createStore(), plugins: [plugin] });

    await expect(engine.validate()).rejects.toThrow('dependency cycle');
  });

  it('proxies resume, cancel, and polling completion to the store', async () => {
    let resumedTaskId: string | undefined;
    let resumedToken: string | undefined;
    let resumedInput: Readonly<Record<string, unknown>> | undefined;
    let cancelledTaskId: string | undefined;
    let cancelledRunId: string | undefined;
    let pollingCompletedTaskId: string | undefined;
    let pollingCompletedOutput: unknown;
    let pollingFailedTaskId: string | undefined;
    let pollingFailedError: string | undefined;

    const testStore = {
      ...createStore(),
      resumeTask: async (command: any) => {
        resumedTaskId = command.taskId;
        resumedToken = command.resumeToken;
        resumedInput = command.input;
        return { id: command.taskId, status: 'ready' };
      },
      cancelTask: async (command: any) => {
        cancelledTaskId = command.taskId;
        return { id: command.taskId, status: 'cancelled' };
      },
      cancelRun: async (command: any) => {
        cancelledRunId = command.runId;
        return { id: command.runId, status: 'cancelled' };
      },
      completePollingTask: async (command: any) => {
        pollingCompletedTaskId = command.taskId;
        pollingCompletedOutput = command.output;
        return { taskId: command.taskId, taskStatus: 'succeeded', runStatus: 'succeeded', unlockedTaskIds: [], replayed: false };
      },
      failPollingTask: async (command: any) => {
        pollingFailedTaskId = command.taskId;
        pollingFailedError = command.error?.message;
        return { taskId: command.taskId, taskStatus: 'failed', runStatus: 'failed', unlockedTaskIds: [], replayed: false };
      },
    };

    const engine = createEngine({
      store: testStore,
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'test.ops',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [noopHook],
        }),
      ],
      idGenerator: () => 'id',
    });

    await engine.resumeTask('task-1', 'token-x', { answer: 42 });
    expect(resumedTaskId).toBe('task-1');
    expect(resumedToken).toBe('token-x');
    expect(resumedInput).toEqual({ answer: 42 });

    await engine.resumeTask('task-2', 'token-y');
    expect(resumedTaskId).toBe('task-2');
    expect(resumedInput).toBeUndefined();

    await engine.cancelTask('task-c');
    expect(cancelledTaskId).toBe('task-c');

    await engine.cancelRun('run-c');
    expect(cancelledRunId).toBe('run-c');

    await engine.completePollingTask('task-p', { done: true });
    expect(pollingCompletedTaskId).toBe('task-p');
    expect(pollingCompletedOutput).toEqual({ done: true });

    await engine.failPollingTask('task-f', { message: 'boom' });
    expect(pollingFailedTaskId).toBe('task-f');
    expect(pollingFailedError).toBe('boom');
  });

  it('spawnChildRun validates parent state and delegates to the store', async () => {
    let createChildRunCalled = false;
    const testStore = {
      ...createStore(),
      getTask: async () => ({ id: 'parent-task', status: 'waiting', waitingReason: 'child_run' } as any),
      createChildRun: async (command: any) => {
        createChildRunCalled = true;
        return { id: command.runId, tenantKey: command.tenantKey, status: 'running' } as any;
      },
    };

    const engine = createEngine({
      store: testStore,
      idGenerator: () => 'child-run-id',
      plugins: [
        definePlugin({
          manifest: { manifestVersion: 1, name: 'test', version: '1.0.0', engine: '^0.1.0' },
          hooks: [noopHook],
          workflows: [defineWorkflow({
            schemaVersion: 1,
            name: 'test.child',
            version: '1.0.0',
            nodes: [{ id: 'n', hook: noopHook.name }],
          })],
        }),
      ],
    });

    await engine.spawnChildRun({
      parentTaskId: 'parent-task',
      workflow: 'test.child@1.0.0',
      tenantKey: 'tenant',
      input: { key: 'value' },
    });
    expect(createChildRunCalled).toBe(true);

    await expect(
      engine.spawnChildRun({
        parentTaskId: 'parent-task',
        workflow: 'test.missing@1.0.0',
        tenantKey: 'tenant',
      }),
    ).rejects.toThrow('Unknown workflow');
  });

  it('listRuns delegates to the store with filters', async () => {
    let receivedFilter: unknown;
    const testStore = {
      ...createStore(),
      listRuns: async (filter: any) => {
        receivedFilter = filter;
        return [];
      },
    };

    const engine = createEngine({ store: testStore, plugins: [] });
    await engine.listRuns({ tenantKey: 't1', status: 'running' });
    expect(receivedFilter).toEqual({ tenantKey: 't1', status: 'running' });

    await engine.listRuns();
    expect(receivedFilter).toBeUndefined();
  });

  it('executes a requested task with resolved run bindings', async () => {
    let received: unknown;
    const bindingHook = defineHook({
      name: 'test.binding@1',
      async execute(context, input) {
        received = { context, input };
        return { status: 'succeeded', output: input } as const;
      },
    });
    const workflow = defineWorkflow({
      schemaVersion: 1,
      name: 'test.binding',
      version: '1.0.0',
      nodes: [{ id: 'bound', hook: bindingHook.name }],
    });
    const engine = createEngine({
      store: createRunnableStore(),
      idGenerator: () => 'id',
      plugins: [
        definePlugin({
          manifest: {
            manifestVersion: 1,
            name: 'test.binding-plugin',
            version: '1.0.0',
            engine: '^0.1.0',
          },
          hooks: [bindingHook],
          workflows: [workflow],
        }),
      ],
    });

    await engine.startRun({
      workflow: 'test.binding@1.0.0',
      tenantKey: 'tenant',
      input: { request: { id: 'request-1' } },
      context: { prior: { value: 42 } },
    });
    await expect(
      engine.executeTask({ workerId: 'worker', taskId: 'task-bound' }),
    ).resolves.toMatchObject({ status: 'succeeded' });
    await expect(engine.getTask('task-bound')).resolves.toBeNull();
    expect(received).toMatchObject({
      context: {
        run: {
          input: { request: { id: 'request-1' } },
          context: { prior: { value: 42 } },
        },
      },
      input: {
        request: { id: 'request-1' },
        prior: 42,
      },
    });
  });
});


