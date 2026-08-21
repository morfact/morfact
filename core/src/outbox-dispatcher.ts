import type {
  OutboxEvent,
  WorkflowStateStore,
} from './contracts.js';
import { StoreProtocolError } from './errors.js';

export interface OutboxPublisher {
  publish(event: OutboxEvent): Promise<void>;
}

export interface OutboxDispatcherOptions {
  readonly store: WorkflowStateStore;
  readonly publisher: OutboxPublisher;
  readonly destination?: string;
  readonly workerId: string;
  readonly leaseDurationMs?: number;
  readonly maxAttempts?: number;
  readonly now?: () => Date;
}

export interface DispatchOutboxCommand {
  readonly limit?: number;
}

export interface DispatchOutboxResult {
  readonly claimed: number;
  readonly dispatched: number;
  readonly failed: number;
}

export interface OutboxDispatcher {
  dispatch(command?: DispatchOutboxCommand): Promise<DispatchOutboxResult>;
}

export function createOutboxDispatcher(
  options: OutboxDispatcherOptions,
): OutboxDispatcher {
  const now = options.now ?? (() => new Date());

  return {
    async dispatch(command = {}) {
      const claimedAt = now();
      const events = await options.store.claimOutbox({
        workerId: options.workerId,
        ...(options.destination === undefined
          ? {}
          : { destination: options.destination }),
        limit: command.limit ?? 32,
        now: claimedAt,
        leaseExpiresAt: new Date(
          claimedAt.getTime() + (options.leaseDurationMs ?? 60_000),
        ),
      });
      let dispatched = 0;
      let failed = 0;

      for (const event of events) {
        if (!event.leaseToken) {
          throw new StoreProtocolError(
            `Claimed outbox event has no lease token: ${event.id}`,
          );
        }
        try {
          await options.publisher.publish(event);
          await options.store.markOutboxDispatched({
            eventId: event.id,
            leaseToken: event.leaseToken,
            now: now(),
          });
          dispatched += 1;
        } catch (error) {
          const attempts = event.attempts + 1;
          const failedAt = now();
          await options.store.markOutboxFailed({
            eventId: event.id,
            leaseToken: event.leaseToken,
            error: {
              message: error instanceof Error ? error.message : String(error),
            },
            retryAt: new Date(
              failedAt.getTime() + Math.min(2 ** attempts, 3600) * 1000,
            ),
            maxAttempts: options.maxAttempts ?? 10,
            now: failedAt,
          });
          failed += 1;
        }
      }

      return { claimed: events.length, dispatched, failed };
    },
  };
}
