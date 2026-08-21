import type {
  CreateRunCommand,
  WorkflowStateStore,
} from '../contracts.js';

export interface StoreConformanceCase {
  readonly name: string;
  run(createStore: () => Promise<WorkflowStateStore>): Promise<void>;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(`Store conformance failed: ${message}`);
  }
}

function runCommand(
  suffix: string,
  tasks: CreateRunCommand['tasks'],
): CreateRunCommand {
  return {
    runId: `conformance-run-${suffix}`,
    tenantKey: 'conformance-tenant',
    definition: {
      schemaVersion: 1,
      name: `conformance.${suffix}`,
      version: '1.0.0',
      nodes: [],
    },
    input: {},
    context: {},
    tasks,
    now: new Date('2026-01-01T00:00:00.000Z'),
  };
}

export const storeConformanceCases: readonly StoreConformanceCase[] = [
  {
    name: 'claims a ready task only once under concurrency',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('claim', [
          {
            id: 'conformance-task-claim',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const claims = await Promise.all([
        store.claimTask({
          workerId: 'worker-a',
          now: new Date('2026-01-01T00:00:01.000Z'),
          leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
        }),
        store.claimTask({
          workerId: 'worker-b',
          now: new Date('2026-01-01T00:00:01.000Z'),
          leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
        }),
      ]);
      assert(
        claims.filter((claim) => claim !== null).length === 1,
        'a ready task must have exactly one lease owner',
      );
    },
  },
  {
    name: 'fences a stale lease owner',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('fencing', [
          {
            id: 'conformance-task-fencing',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 2,
          },
        ]),
      );
      const stale = await store.claimTask({
        workerId: 'worker-a',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:00:02.000Z'),
      });
      const fresh = await store.claimTask({
        workerId: 'worker-b',
        now: new Date('2026-01-01T00:00:03.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:03.000Z'),
      });
      assert(stale && fresh, 'expired task must be reclaimable');

      let rejected = false;
      try {
        await store.commitTaskSuccess({
          commandId: 'conformance-stale-command',
          taskId: stale.task.id,
          leaseToken: stale.task.leaseToken,
          output: {},
          now: new Date('2026-01-01T00:00:04.000Z'),
        });
      } catch {
        rejected = true;
      }
      assert(rejected, 'stale lease owner must not commit');
    },
  },
  {
    name: 'reads tasks and extends only the current lease',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('heartbeat', [
          {
            id: 'conformance-task-heartbeat',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const created = await store.getTask('conformance-task-heartbeat');
      assert(created?.status === 'ready', 'getTask must return the created task');

      const claim = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-task-heartbeat',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:00:10.000Z'),
      });
      assert(claim, 'directed task must be claimable');
      const heartbeated = await store.heartbeatTask({
        taskId: claim.task.id,
        leaseToken: claim.task.leaseToken,
        now: new Date('2026-01-01T00:00:05.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:00:20.000Z'),
      });
      assert(
        heartbeated.leaseExpiresAt?.getTime() ===
          new Date('2026-01-01T00:00:20.000Z').getTime(),
        'heartbeat must extend the lease',
      );

      let staleRejected = false;
      try {
        await store.heartbeatTask({
          taskId: claim.task.id,
          leaseToken: 'stale-token',
          now: new Date('2026-01-01T00:00:06.000Z'),
          leaseExpiresAt: new Date('2026-01-01T00:00:30.000Z'),
        });
      } catch {
        staleRejected = true;
      }
      assert(staleRejected, 'heartbeat must fence a stale lease token');
    },
  },
  {
    name: 'replays an identical command idempotently',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('idempotency', [
          {
            id: 'conformance-task-idempotency',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const claim = await store.claimTask({
        workerId: 'worker',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claim, 'ready task must be claimable');
      const command = {
        commandId: 'conformance-same-command',
        taskId: claim.task.id,
        leaseToken: claim.task.leaseToken,
        output: { done: true },
        now: new Date('2026-01-01T00:00:02.000Z'),
      };
      const first = await store.commitTaskSuccess(command);
      const replay = await store.commitTaskSuccess(command);
      assert(!first.replayed, 'first command application must not be a replay');
      assert(replay.replayed, 'second identical command must be a replay');
    },
  },
  {
    name: 'replays concurrent identical commands idempotently',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('concurrent-idempotency', [
          {
            id: 'conformance-task-concurrent-idempotency',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const claim = await store.claimTask({
        workerId: 'worker',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claim, 'ready task must be claimable');
      const command = {
        commandId: 'conformance-concurrent-command',
        taskId: claim.task.id,
        leaseToken: claim.task.leaseToken,
        output: { done: true },
        now: new Date('2026-01-01T00:00:02.000Z'),
      };

      const results = await Promise.all([
        store.commitTaskSuccess(command),
        store.commitTaskSuccess(command),
      ]);
      assert(
        results.filter((result) => result.replayed).length === 1,
        'exactly one concurrent command application must be a replay',
      );
    },
  },
  {
    name: 'unlocks a successor only after every dependency succeeds',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('join', [
          {
            id: 'conformance-task-left',
            logicalKey: 'left',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
          {
            id: 'conformance-task-right',
            logicalKey: 'right',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
          {
            id: 'conformance-task-join',
            logicalKey: 'join',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: ['left', 'right'],
            maxAttempts: 1,
          },
        ]),
      );

      const first = await store.claimTask({
        workerId: 'worker-a',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(first, 'first root task must be claimable');
      await store.commitTaskSuccess({
        commandId: 'conformance-complete-first',
        taskId: first.task.id,
        leaseToken: first.task.leaseToken,
        output: {},
        now: new Date('2026-01-01T00:00:02.000Z'),
      });
      const halfway = await store.listTasks(first.run.id);
      assert(
        halfway.find((task) => task.logicalKey === 'join')?.status === 'pending',
        'join must remain pending after only one dependency succeeds',
      );

      const second = await store.claimTask({
        workerId: 'worker-b',
        now: new Date('2026-01-01T00:00:03.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:03.000Z'),
      });
      assert(second, 'second root task must be claimable');
      await store.commitTaskSuccess({
        commandId: 'conformance-complete-second',
        taskId: second.task.id,
        leaseToken: second.task.leaseToken,
        output: {},
        now: new Date('2026-01-01T00:00:04.000Z'),
      });
      const ready = await store.listTasks(first.run.id);
      assert(
        ready.find((task) => task.logicalKey === 'join')?.status === 'ready',
        'join must become ready after all dependencies succeed',
      );
    },
  },
  {
    name: 'writes and dispatches ready task outbox events',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('outbox', [
          {
            id: 'conformance-task-outbox',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );

      const claimed = await store.claimOutbox({
        workerId: 'dispatcher',
        destination: 'workflow.tasks',
        limit: 10,
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claimed.length === 1, 'ready task must create one outbox event');
      const event = claimed[0];
      assert(event, 'claimed outbox event must exist');
      assert(
        event.payload.taskId === 'conformance-task-outbox',
        'outbox payload must identify the ready task',
      );
      assert(event.leaseToken, 'claimed outbox event must have a lease token');

      await store.markOutboxDispatched({
        eventId: event.id,
        leaseToken: event.leaseToken,
        now: new Date('2026-01-01T00:00:02.000Z'),
      });
      const replay = await store.claimOutbox({
        workerId: 'dispatcher',
        destination: 'workflow.tasks',
        limit: 10,
        now: new Date('2026-01-01T00:00:03.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:03.000Z'),
      });
      assert(replay.length === 0, 'dispatched event must not be claimed again');
    },
  },
  {
    name: 'requeues failed outbox dispatches and fences stale dispatchers',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('outbox-retry', [
          {
            id: 'conformance-task-outbox-retry',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const firstClaim = await store.claimOutbox({
        workerId: 'dispatcher-a',
        destination: 'workflow.tasks',
        limit: 1,
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      const first = firstClaim[0];
      assert(first?.leaseToken, 'first dispatcher must receive a lease token');
      await store.markOutboxFailed({
        eventId: first.id,
        leaseToken: first.leaseToken,
        error: { message: 'queue unavailable' },
        retryAt: new Date('2026-01-01T00:00:05.000Z'),
        maxAttempts: 3,
        now: new Date('2026-01-01T00:00:02.000Z'),
      });
      const secondClaim = await store.claimOutbox({
        workerId: 'dispatcher-b',
        destination: 'workflow.tasks',
        limit: 1,
        now: new Date('2026-01-01T00:00:06.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:06.000Z'),
      });
      const second = secondClaim[0];
      assert(second?.leaseToken, 'failed event must become claimable after retryAt');
      assert(second.attempts === 1, 'failed dispatch must increment attempts');

      let staleRejected = false;
      try {
        await store.markOutboxDispatched({
          eventId: first.id,
          leaseToken: first.leaseToken,
          now: new Date('2026-01-01T00:00:07.000Z'),
        });
      } catch {
        staleRejected = true;
      }
      assert(staleRejected, 'stale dispatcher must not acknowledge the event');
    },
  },
  {
    name: 'resumes a waiting task and fires a ready outbox event',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('resume', [
          {
            id: 'conformance-task-resume',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const claim = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-task-resume',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claim, 'task must be claimable');
      await store.suspendTask({
        commandId: 'conformance-suspend-resume',
        taskId: claim.task.id,
        leaseToken: claim.task.leaseToken,
        reason: 'interaction',
        resumeToken: 'resume-abc',
        now: new Date('2026-01-01T00:00:02.000Z'),
      });

      let badTokenRejected = false;
      try {
        await store.resumeTask({
          commandId: 'conformance-bad-token',
          taskId: claim.task.id,
          resumeToken: 'wrong-token',
          now: new Date('2026-01-01T00:00:03.000Z'),
        });
      } catch {
        badTokenRejected = true;
      }
      assert(badTokenRejected, 'resume must reject a wrong resume token');

      const resumed = await store.resumeTask({
        commandId: 'conformance-resume-task',
        taskId: claim.task.id,
        resumeToken: 'resume-abc',
        input: { submitted: true },
        now: new Date('2026-01-01T00:00:03.000Z'),
      });
      assert(resumed.status === 'ready', 'resumed task must be ready');
      assert(
        (resumed.input as Record<string, unknown>).submitted === true,
        'resumed task must have updated input',
      );

      const outbox = await store.claimOutbox({
        workerId: 'dispatcher',
        destination: 'workflow.tasks',
        limit: 10,
        now: new Date('2026-01-01T00:00:04.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:04.000Z'),
      });
      assert(
        outbox.some((event) => event.taskId === claim.task.id),
        'resume must create a ready outbox event',
      );
    },
  },
  {
    name: 'cancels a task and sets run to cancelled when all tasks end',
    async run(createStore) {
      const store = await createStore();
      await store.createRun(
        runCommand('cancel-task', [
          {
            id: 'conformance-task-cancel',
            logicalKey: 'first',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ]),
      );
      const cancelled = await store.cancelTask({
        commandId: 'conformance-cancel-task',
        taskId: 'conformance-task-cancel',
        now: new Date('2026-01-01T00:00:01.000Z'),
      });
      assert(cancelled.status === 'cancelled', 'cancelled task must be cancelled');
      const run = await store.getRun(cancelled.runId);
      assert(run?.status === 'cancelled', 'run must be cancelled when all tasks end');
    },
  },
  {
    name: 'cancels an entire run and all its non-terminal tasks',
    async run(createStore) {
      const store = await createStore();
      const cmd = runCommand('cancel-run', [
        {
          id: 'conformance-task-a',
          logicalKey: 'a',
          hook: 'conformance.noop@1',
          input: {},
          dependsOn: [],
          maxAttempts: 1,
        },
        {
          id: 'conformance-task-b',
          logicalKey: 'b',
          hook: 'conformance.noop@1',
          input: {},
          dependsOn: ['a'],
          maxAttempts: 1,
        },
      ]);
      await store.createRun(cmd);

      const run = await store.cancelRun({
        commandId: 'conformance-cancel-run',
        runId: cmd.runId,
        now: new Date('2026-01-01T00:00:01.000Z'),
      });
      assert(run.status === 'cancelled', 'run must be cancelled');

      const tasks = await store.listTasks(cmd.runId);
      assert(
        tasks.every((task) => task.status === 'cancelled'),
        'all non-terminal tasks must be cancelled',
      );
    },
  },
  {
    name: 'completes and fails external polling tasks',
    async run(createStore) {
      const store = await createStore();
      const cmd = runCommand('poll', [
        {
          id: 'conformance-task-poll-ok',
          logicalKey: 'external-ok',
          hook: 'conformance.noop@1',
          input: {},
          dependsOn: [],
          maxAttempts: 1,
        },
        {
          id: 'conformance-task-poll-fail',
          logicalKey: 'external-fail',
          hook: 'conformance.noop@1',
          input: {},
          dependsOn: [],
          maxAttempts: 1,
        },
      ]);
      await store.createRun(cmd);

      const claimOk = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-task-poll-ok',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claimOk, 'external task must be claimable');
      await store.suspendTask({
        commandId: 'conformance-suspend-poll-ok',
        taskId: claimOk.task.id,
        leaseToken: claimOk.task.leaseToken,
        reason: 'external',
        resumeToken: 'poll-ok',
        now: new Date('2026-01-01T00:00:02.000Z'),
      });
      const result = await store.completePollingTask({
        commandId: 'conformance-complete-poll',
        taskId: claimOk.task.id,
        output: { done: true },
        now: new Date('2026-01-01T00:00:03.000Z'),
      });
      assert(result.taskStatus === 'succeeded', 'polling completion must succeed');

      const claimFail = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-task-poll-fail',
        now: new Date('2026-01-01T00:00:04.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:04.000Z'),
      });
      assert(claimFail, 'second external task must be claimable');
      await store.suspendTask({
        commandId: 'conformance-suspend-poll-fail',
        taskId: claimFail.task.id,
        leaseToken: claimFail.task.leaseToken,
        reason: 'external',
        resumeToken: 'poll-fail',
        now: new Date('2026-01-01T00:00:05.000Z'),
      });
      const failResult = await store.failPollingTask({
        commandId: 'conformance-fail-poll',
        taskId: claimFail.task.id,
        error: { message: 'api error' },
        now: new Date('2026-01-01T00:00:06.000Z'),
      });
      assert(failResult.taskStatus === 'failed', 'polling failure must set task failed');
      assert(failResult.runStatus === 'failed', 'run must be failed on terminal failure');
    },
  },
  {
    name: 'creates child runs and auto-resumes parent on all children completion',
    async run(createStore) {
      const store = await createStore();
      const parentCmd = runCommand('child-parent', [
        {
          id: 'conformance-parent-task',
          logicalKey: 'parent',
          hook: 'conformance.noop@1',
          input: {},
          dependsOn: [],
          maxAttempts: 1,
        },
      ]);
      await store.createRun(parentCmd);

      const claim = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-parent-task',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claim, 'parent task must be claimable');
      await store.suspendTask({
        commandId: 'conformance-suspend-parent',
        taskId: claim.task.id,
        leaseToken: claim.task.leaseToken,
        reason: 'child_run',
        resumeToken: 'child-resume',
        now: new Date('2026-01-01T00:00:02.000Z'),
      });

      const child1 = await store.createChildRun({
        runId: 'conformance-child-1',
        tenantKey: 'conformance-tenant',
        parentTaskId: 'conformance-parent-task',
        definition: parentCmd.definition,
        input: {},
        context: {},
        tasks: [
          {
            id: 'conformance-child1-task',
            logicalKey: 'child1',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ],
        now: new Date('2026-01-01T00:00:03.000Z'),
      });
      assert(child1.status === 'running', 'child run must be running');

      const child2 = await store.createChildRun({
        runId: 'conformance-child-2',
        tenantKey: 'conformance-tenant',
        parentTaskId: 'conformance-parent-task',
        definition: parentCmd.definition,
        input: {},
        context: {},
        tasks: [
          {
            id: 'conformance-child2-task',
            logicalKey: 'child2',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 1,
          },
        ],
        now: new Date('2026-01-01T00:00:04.000Z'),
      });

      const parentAfterSpawn = await store.getTask('conformance-parent-task');
      assert(
        parentAfterSpawn?.status === 'waiting',
        'parent must remain waiting after child spawn',
      );

      const c1Claim = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-child1-task',
        now: new Date('2026-01-01T00:00:05.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:05.000Z'),
      });
      assert(c1Claim, 'child task must be claimable');
      await store.commitTaskSuccess({
        commandId: 'conformance-complete-child1',
        taskId: c1Claim.task.id,
        leaseToken: c1Claim.task.leaseToken,
        output: {},
        now: new Date('2026-01-01T00:00:06.000Z'),
      });

      const parentAfterOne = await store.getTask('conformance-parent-task');
      assert(
        parentAfterOne?.status === 'waiting',
        'parent must remain waiting after first child completes',
      );

      const c2Claim = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-child2-task',
        now: new Date('2026-01-01T00:00:07.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:07.000Z'),
      });
      assert(c2Claim, 'second child task must be claimable');
      await store.commitTaskSuccess({
        commandId: 'conformance-complete-child2',
        taskId: c2Claim.task.id,
        leaseToken: c2Claim.task.leaseToken,
        output: {},
        now: new Date('2026-01-01T00:00:08.000Z'),
      });

      const parentResumed = await store.getTask('conformance-parent-task');
      assert(
        parentResumed?.status === 'ready',
        'parent must be auto-resumed after all children complete',
      );
    },
  },
  {
    name: 're-queues the same task on continue without unlocking downstream',
    async run(createStore) {
      const store = await createStore();
      if (typeof store.commitTaskContinue !== 'function') {
        return;
      }
      await store.createRun(
        runCommand('continue', [
          {
            id: 'conformance-task-loop',
            logicalKey: 'loop',
            hook: 'conformance.loop@1',
            input: {},
            dependsOn: [],
            maxAttempts: 3,
          },
          {
            id: 'conformance-task-after',
            logicalKey: 'after',
            hook: 'conformance.noop@1',
            input: {},
            dependsOn: ['loop'],
            maxAttempts: 1,
          },
        ]),
      );
      const claim = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-task-loop',
        now: new Date('2026-01-01T00:00:01.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:01.000Z'),
      });
      assert(claim, 'loop task must be claimable');
      assert(claim.task.leaseToken, 'claimed task must have a lease token');
      await store.commitTaskContinue({
        commandId: 'conformance-continue',
        taskId: claim.task.id,
        leaseToken: claim.task.leaseToken,
        checkpoint: { cursor: 1 },
        iteration: 1,
        output: { cursor: 0 },
        now: new Date('2026-01-01T00:00:02.000Z'),
      });
      const looped = await store.getTask('conformance-task-loop');
      assert(looped?.status === 'ready', 'continued task must be ready again');
      assert(
        (looped.meta?.loopState as { iteration?: number })?.iteration === 1,
        'continue must persist loopState.iteration',
      );
      const after = await store.getTask('conformance-task-after');
      assert(
        after?.status === 'pending',
        'continue must not unlock downstream tasks',
      );
      const again = await store.claimTask({
        workerId: 'worker',
        taskId: 'conformance-task-loop',
        now: new Date('2026-01-01T00:00:03.000Z'),
        leaseExpiresAt: new Date('2026-01-01T00:01:03.000Z'),
      });
      assert(again, 'continued task must be claimable again');
    },
  },
  {
    name: 'lists runs with filters and pagination',
    async run(createStore) {
      const store = await createStore();
      const cmd1 = runCommand('list-a', [
        {
          id: 'conformance-list-a',
          logicalKey: 'a',
          hook: 'conformance.noop@1',
          input: {},
          dependsOn: [],
          maxAttempts: 1,
        },
      ]);
      const cmd2 = { ...runCommand('list-b', []), correlationKey: 'corr-b', resourceRef: 'ref-b', tenantKey: 'conformance-tenant' };
      await store.createRun(cmd1);
      await store.createRun(cmd2 as any);

      const all = await store.listRuns();
      assert(all.length >= 2, 'all runs must be visible');

      const byCorr = await store.listRuns({ correlationKey: 'corr-b' });
      assert(byCorr.length === 1, 'filter by correlationKey must return one run');
      assert(byCorr[0]!.correlationKey === 'corr-b', 'correlation key must match');

      const limited = await store.listRuns({ limit: 1 });
      assert(limited.length === 1, 'limit must be respected');
    },
  },
];
