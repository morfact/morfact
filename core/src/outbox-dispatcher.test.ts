import { describe, expect, it, vi } from 'vitest';

import type {
  OutboxEvent,
  WorkflowStateStore,
} from './contracts.js';
import { createOutboxDispatcher } from './outbox-dispatcher.js';

function event(): OutboxEvent {
  return {
    id: 'event-1',
    tenantKey: 'tenant',
    runId: 'run-1',
    taskId: 'task-1',
    eventType: 'task.ready',
    payloadSchemaVersion: 1,
    payload: { taskId: 'task-1', runId: 'run-1', tenantKey: 'tenant' },
    destination: 'workflow.tasks',
    dedupKey: 'task.ready:task-1',
    status: 'leased',
    availableAt: new Date('2026-01-01T00:00:00.000Z'),
    attempts: 0,
    leaseOwner: 'dispatcher',
    leaseToken: 'lease-1',
    leaseExpiresAt: new Date('2026-01-01T00:01:00.000Z'),
    createdAt: new Date('2026-01-01T00:00:00.000Z'),
  };
}

function store(overrides: Partial<WorkflowStateStore>): WorkflowStateStore {
  const unsupported = async (): Promise<never> => {
    throw new Error('unsupported');
  };
  return {
    protocolVersion: 1,
    getCapabilities: unsupported,
    createRun: unsupported,
    claimTask: unsupported,
    heartbeatTask: unsupported,
    commitTaskSuccess: unsupported,
    commitTaskFailure: unsupported,
    suspendTask: unsupported,
    claimOutbox: unsupported,
    markOutboxDispatched: unsupported,
    markOutboxFailed: unsupported,
    getTask: unsupported,
    getRun: unsupported,
    listTasks: unsupported,
    ...overrides,
  };
}

describe('createOutboxDispatcher', () => {
  it('publishes and acknowledges claimed events', async () => {
    const claimed = event();
    const publish = vi.fn(async () => undefined);
    const markOutboxDispatched = vi.fn(async () => undefined);
    const dispatcher = createOutboxDispatcher({
      store: store({
        claimOutbox: async () => [claimed],
        markOutboxDispatched,
      }),
      publisher: { publish },
      workerId: 'dispatcher',
    });

    await expect(dispatcher.dispatch({ limit: 1 })).resolves.toEqual({
      claimed: 1,
      dispatched: 1,
      failed: 0,
    });
    expect(publish).toHaveBeenCalledWith(claimed);
    expect(markOutboxDispatched).toHaveBeenCalledWith(
      expect.objectContaining({
        eventId: claimed.id,
        leaseToken: claimed.leaseToken,
      }),
    );
  });

  it('records publisher failures for retry', async () => {
    const claimed = event();
    const markOutboxFailed = vi.fn(async () => undefined);
    const dispatcher = createOutboxDispatcher({
      store: store({
        claimOutbox: async () => [claimed],
        markOutboxFailed,
      }),
      publisher: {
        publish: async () => {
          throw new Error('queue unavailable');
        },
      },
      workerId: 'dispatcher',
      now: () => new Date('2026-01-01T00:00:01.000Z'),
    });

    await expect(dispatcher.dispatch()).resolves.toEqual({
      claimed: 1,
      dispatched: 0,
      failed: 1,
    });
    expect(markOutboxFailed).toHaveBeenCalledWith(
      expect.objectContaining({
        eventId: claimed.id,
        leaseToken: claimed.leaseToken,
        error: { message: 'queue unavailable' },
      }),
    );
  });
});
