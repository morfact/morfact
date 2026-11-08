// postgres-store.ts — PostgreSQL WorkflowStateStore (protocol v1).
//
// Reference implementation used to prove the engine behaves identically on a
// durable store: coordinator subtask dispatch, loop continue with a persisted
// delay, lease fencing, and command idempotency all live in SQL so they
// survive a process restart.
//
// Kept out of the published package surface: it is a conformance fixture, not
// a supported production store. Tests import it by relative path.

import { Pool, type PoolClient } from 'pg';
import type {
  AppendTasksCommand,
  CancelRunCommand,
  CancelTaskCommand,
  ClaimedTask,
  ClaimOutboxCommand,
  ClaimTaskCommand,
  CommitTaskContinueCommand,
  CommitTaskFailureCommand,
  CommitTaskSuccessCommand,
  CompletePollingTaskCommand,
  CreateChildRunCommand,
  CreateInteractionCommand,
  CreateRunCommand,
  FailPollingTaskCommand,
  HeartbeatTaskCommand,
  ListRunsFilter,
  MarkOutboxDispatchedCommand,
  MarkOutboxFailedCommand,
  OutboxEvent,
  ResumeTaskCommand,
  StoreCapabilities,
  SubmitInteractionCommand,
  SuspendTaskCommand,
  TaskCommitResult,
  TaskInteraction,
  TaskRun,
  TaskRunStatus,
  WorkflowRun,
  WorkflowRunStatus,
  WorkflowStateStore,
} from '@morfact/core';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS wf_runs (
  id text PRIMARY KEY,
  tenant_key text NOT NULL,
  name text,
  type text,
  resource_ref text,
  correlation_key text,
  definition jsonb NOT NULL,
  input jsonb NOT NULL,
  context jsonb NOT NULL,
  status text NOT NULL,
  parent_run_id text,
  parent_task_id text,
  root_run_id text,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS wf_tasks (
  id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES wf_runs(id),
  tenant_key text NOT NULL,
  logical_key text NOT NULL,
  hook text NOT NULL,
  input jsonb NOT NULL,
  output jsonb,
  error jsonb,
  status text NOT NULL,
  dependencies_remaining integer NOT NULL,
  ancestor_id text,
  attempt integer NOT NULL,
  max_attempts integer NOT NULL,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  not_before timestamptz,
  waiting_reason text,
  resume_token text,
  meta jsonb,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS wf_tasks_run_logical
  ON wf_tasks (run_id, logical_key);
CREATE TABLE IF NOT EXISTS wf_task_deps (
  task_id text NOT NULL REFERENCES wf_tasks(id),
  depends_on text NOT NULL,
  PRIMARY KEY (task_id, depends_on)
);
CREATE TABLE IF NOT EXISTS wf_outbox (
  id text PRIMARY KEY,
  tenant_key text NOT NULL,
  run_id text NOT NULL,
  task_id text NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  destination text NOT NULL,
  dedup_key text NOT NULL UNIQUE,
  status text NOT NULL,
  available_at timestamptz NOT NULL,
  attempts integer NOT NULL,
  last_error jsonb,
  lease_owner text,
  lease_token text,
  lease_expires_at timestamptz,
  created_at timestamptz NOT NULL,
  dispatched_at timestamptz
);
CREATE TABLE IF NOT EXISTS wf_interactions (
  id text PRIMARY KEY,
  tenant_key text NOT NULL,
  run_id text NOT NULL,
  task_id text NOT NULL,
  kind text NOT NULL,
  schema jsonb,
  ui_hints jsonb,
  draft_payload jsonb,
  submitted_payload jsonb,
  status text NOT NULL,
  token text NOT NULL,
  expire_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS wf_commands (
  command_id text PRIMARY KEY,
  result jsonb NOT NULL
);
`;

interface TaskRow {
  id: string;
  run_id: string;
  tenant_key: string;
  logical_key: string;
  hook: string;
  input: Record<string, unknown>;
  output: unknown;
  error: unknown;
  status: TaskRunStatus;
  dependencies_remaining: number;
  ancestor_id: string | null;
  attempt: number;
  max_attempts: number;
  lease_owner: string | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
  not_before: Date | null;
  waiting_reason: 'external' | 'interaction' | 'child_run' | null;
  resume_token: string | null;
  meta: Record<string, unknown> | null;
  created_at: Date;
  updated_at: Date;
}

function toTask(row: TaskRow): TaskRun {
  return {
    id: row.id,
    runId: row.run_id,
    tenantKey: row.tenant_key,
    logicalKey: row.logical_key,
    hook: row.hook,
    input: row.input,
    status: row.status,
    dependenciesRemaining: row.dependencies_remaining,
    attempt: row.attempt,
    maxAttempts: row.max_attempts,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(row.output !== null ? { output: row.output } : {}),
    ...(row.error !== null ? { error: row.error } : {}),
    ...(row.ancestor_id !== null ? { ancestorId: row.ancestor_id } : {}),
    ...(row.lease_owner !== null ? { leaseOwner: row.lease_owner } : {}),
    ...(row.lease_token !== null ? { leaseToken: row.lease_token } : {}),
    ...(row.lease_expires_at !== null
      ? { leaseExpiresAt: row.lease_expires_at }
      : {}),
    ...(row.not_before !== null ? { notBefore: row.not_before } : {}),
    ...(row.waiting_reason !== null ? { waitingReason: row.waiting_reason } : {}),
    ...(row.resume_token !== null ? { resumeToken: row.resume_token } : {}),
    ...(row.meta !== null ? { meta: row.meta } : {}),
  };
}

interface RunRow {
  id: string;
  tenant_key: string;
  name: string | null;
  type: string | null;
  resource_ref: string | null;
  correlation_key: string | null;
  definition: WorkflowRun['definition'];
  input: Record<string, unknown>;
  context: Record<string, unknown>;
  status: WorkflowRunStatus;
  parent_run_id: string | null;
  parent_task_id: string | null;
  root_run_id: string | null;
  created_at: Date;
  updated_at: Date;
}

function toRun(row: RunRow): WorkflowRun {
  return {
    id: row.id,
    tenantKey: row.tenant_key,
    definitionName: row.definition.name,
    definitionVersion: row.definition.version,
    definition: row.definition,
    input: row.input,
    context: row.context,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(row.name !== null ? { name: row.name } : {}),
    ...(row.type !== null ? { type: row.type } : {}),
    ...(row.resource_ref !== null ? { resourceRef: row.resource_ref } : {}),
    ...(row.correlation_key !== null
      ? { correlationKey: row.correlation_key }
      : {}),
    ...(row.parent_run_id !== null ? { parentRunId: row.parent_run_id } : {}),
    ...(row.parent_task_id !== null ? { parentTaskId: row.parent_task_id } : {}),
    ...(row.root_run_id !== null ? { rootRunId: row.root_run_id } : {}),
  };
}

const TASK_COLUMNS = `id, run_id, tenant_key, logical_key, hook, input, output, error,
  status, dependencies_remaining, ancestor_id, attempt, max_attempts, lease_owner,
  lease_token, lease_expires_at, not_before, waiting_reason, resume_token, meta,
  created_at, updated_at`;

export class PostgresWorkflowStateStore implements WorkflowStateStore {
  readonly protocolVersion = 1 as const;

  constructor(private readonly pool: Pool) {}

  async migrate(): Promise<void> {
    await this.pool.query(SCHEMA);
    // Columns added after the first migration of an existing database.
    await this.pool.query('ALTER TABLE wf_runs ADD COLUMN IF NOT EXISTS name text');
    await this.pool.query('ALTER TABLE wf_runs ADD COLUMN IF NOT EXISTS type text');
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async getCapabilities(): Promise<StoreCapabilities> {
    return {
      atomicTaskCommit: true,
      transactionalOutbox: true,
      taskLeases: true,
    };
  }

  async createRun(command: CreateRunCommand): Promise<WorkflowRun> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO wf_runs (id, tenant_key, name, type, resource_ref, correlation_key,
           definition, input, context, status, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10,$10)`,
        [
          command.runId,
          command.tenantKey,
          command.name ?? null,
          command.type ?? null,
          command.resourceRef ?? null,
          command.correlationKey ?? null,
          JSON.stringify(command.definition),
          JSON.stringify(command.input),
          JSON.stringify(command.context),
          command.now,
        ],
      );
      for (const task of command.tasks) {
        if (task.dependsOn.length === 0) {
          await enqueueTaskReady(client, {
            task: {
              id: task.id,
              runId: command.runId,
              tenantKey: command.tenantKey,
            },
            now: command.now,
            availableAt: command.now,
          });
        }
        await insertTask(client, {
          task,
          runId: command.runId,
          tenantKey: command.tenantKey,
          ancestorId: null,
          now: command.now,
        });
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    const run = await this.getRun(command.runId);
    if (!run) throw new Error(`Run not found after create: ${command.runId}`);
    return run;
  }

  async claimTask(command: ClaimTaskCommand): Promise<ClaimedTask | null> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const params: unknown[] = [command.now];
      let taskFilter = '';
      if (command.taskId !== undefined) {
        params.push(command.taskId);
        taskFilter = `AND t.id = $${params.length}`;
      }
      const { rows } = await client.query<TaskRow>(
        `SELECT ${TASK_COLUMNS.split('\n').map((line) => line.replace(/^/, 't.')).join('\n')} FROM wf_tasks t
         WHERE (
             t.status = 'ready'
             OR (t.status = 'leased' AND t.lease_expires_at <= $1)
           )
           AND t.dependencies_remaining = 0
           AND (t.not_before IS NULL OR t.not_before <= $1)
           ${taskFilter}
           AND EXISTS (
             SELECT 1 FROM wf_runs r
             WHERE r.id = t.run_id AND r.status <> 'cancelled'
           )
         ORDER BY t.created_at
         LIMIT 1
         FOR UPDATE SKIP LOCKED`,
        params,
      );
      const row = rows[0];
      if (!row) {
        await client.query('COMMIT');
        return null;
      }
      const token = crypto.randomUUID();
      const { rows: updated } = await client.query<TaskRow>(
        `UPDATE wf_tasks SET status = 'leased', lease_owner = $2, lease_token = $3,
           lease_expires_at = $4, updated_at = $5
         WHERE id = $1 RETURNING ${TASK_COLUMNS}`,
        [row.id, command.workerId, token, command.leaseExpiresAt, command.now],
      );
      await client.query(
        `UPDATE wf_runs SET status = 'running', updated_at = $2
         WHERE id = $1 AND status = 'pending'`,
        [row.run_id, command.now],
      );
      const run = await readRun(client, row.run_id);
      await client.query('COMMIT');
      const task = updated[0];
      if (!task || !run) return null;
      return { run, task: toTask(task) as ClaimedTask['task'] };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async heartbeatTask(command: HeartbeatTaskCommand): Promise<TaskRun> {
    const { rows } = await this.pool.query<TaskRow>(
      `UPDATE wf_tasks SET lease_expires_at = $3, updated_at = $4
       WHERE id = $1 AND lease_token = $2
       RETURNING ${TASK_COLUMNS}`,
      [command.taskId, command.leaseToken, command.leaseExpiresAt, command.now],
    );
    const row = rows[0];
    if (!row) throw new Error(`Task not leased: ${command.taskId}`);
    return toTask(row);
  }

  async commitTaskSuccess(
    command: CommitTaskSuccessCommand,
  ): Promise<TaskCommitResult> {
    return this.inTransaction(async (client) => {
      const replayed = await readCommand(client, command.commandId);
      if (replayed) return { ...replayed, replayed: true };
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      if (task.leaseToken !== undefined && task.leaseToken !== command.leaseToken) {
        throw new Error(`Stale lease token for task: ${command.taskId}`);
      }
      await client.query(
        `UPDATE wf_tasks SET status = 'succeeded', output = $2, updated_at = $3
         WHERE id = $1`,
        [task.id, JSON.stringify(command.output ?? null), command.now],
      );
      const unlocked = await unlockDependents(client, task, command.now);
      const runStatus = await recomputeRunStatus(client, task.runId, command.now);
      await resumeCoordinator(client, task, command.now);
      const result: TaskCommitResult = {
        taskId: task.id,
        taskStatus: 'succeeded',
        runStatus,
        unlockedTaskIds: unlocked,
        replayed: false,
      };
      await storeCommand(client, command.commandId, result);
      return result;
    });
  }

  async commitTaskFailure(
    command: CommitTaskFailureCommand,
  ): Promise<TaskCommitResult> {
    return this.inTransaction(async (client) => {
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      await client.query(
        `UPDATE wf_tasks SET status = 'failed', error = $2, updated_at = $3
         WHERE id = $1`,
        [task.id, JSON.stringify(command.error), command.now],
      );
      const runStatus = await recomputeRunStatus(client, task.runId, command.now);
      return {
        taskId: task.id,
        taskStatus: 'failed',
        runStatus,
        unlockedTaskIds: [],
        replayed: false,
      };
    });
  }

  async commitTaskContinue(
    command: CommitTaskContinueCommand,
  ): Promise<TaskCommitResult> {
    return this.inTransaction(async (client) => {
      const replayed = await readCommand(client, command.commandId);
      if (replayed) return { ...replayed, replayed: true };
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      const notBefore =
        command.delayMs !== undefined && command.delayMs > 0
          ? new Date(command.now.getTime() + command.delayMs)
          : null;
      const meta = {
        ...(task.meta ?? {}),
        loopState: {
          iteration: command.iteration,
          checkpoint: command.checkpoint,
        },
      };
      await client.query(
        `UPDATE wf_tasks SET status = 'ready', attempt = 0, output = $2, meta = $3,
           not_before = $4, lease_owner = NULL, lease_token = NULL,
           lease_expires_at = NULL, updated_at = $5
         WHERE id = $1`,
        [
          task.id,
          command.output !== undefined ? JSON.stringify(command.output) : null,
          JSON.stringify(meta),
          notBefore,
          command.now,
        ],
      );
      await enqueueTaskReady(client, {
        task,
        now: command.now,
        availableAt: notBefore ?? command.now,
      });
      await client.query(
        `UPDATE wf_runs SET status = 'running', updated_at = $2
         WHERE id = $1 AND status NOT IN ('running', 'cancelled')`,
        [task.runId, command.now],
      );
      const result: TaskCommitResult = {
        taskId: task.id,
        taskStatus: 'ready',
        runStatus: 'running',
        unlockedTaskIds: [],
        replayed: false,
      };
      await storeCommand(client, command.commandId, result);
      return result;
    });
  }

  async suspendTask(command: SuspendTaskCommand): Promise<TaskCommitResult> {
    return this.inTransaction(async (client) => {
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      await client.query(
        `UPDATE wf_tasks SET status = 'waiting', waiting_reason = $2,
           resume_token = $3, updated_at = $4 WHERE id = $1`,
        [task.id, command.reason, command.resumeToken, command.now],
      );
      const runStatus = await recomputeRunStatus(client, task.runId, command.now);
      return {
        taskId: task.id,
        taskStatus: 'waiting',
        runStatus,
        unlockedTaskIds: [],
        replayed: false,
      };
    });
  }

  async resumeTask(command: ResumeTaskCommand): Promise<TaskRun> {
    return this.inTransaction(async (client) => {
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      if (task.resumeToken !== command.resumeToken) {
        throw new Error('resume token mismatch');
      }
      const input =
        command.input !== undefined
          ? { ...task.input, ...command.input }
          : task.input;
      const { rows } = await client.query<TaskRow>(
        `UPDATE wf_tasks SET status = 'ready', input = $2, updated_at = $3
         WHERE id = $1 RETURNING ${TASK_COLUMNS}`,
        [task.id, JSON.stringify(input), command.now],
      );
      const row = rows[0];
      if (!row) throw new Error(`Task not found: ${command.taskId}`);
      await enqueueTaskReady(client, { task, now: command.now, availableAt: command.now });
      return toTask(row);
    });
  }

  async cancelTask(command: CancelTaskCommand): Promise<TaskRun> {
    return this.inTransaction(async (client) => {
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      const { rows } = await client.query<TaskRow>(
        `UPDATE wf_tasks SET status = 'cancelled', updated_at = $2
         WHERE id = $1 RETURNING ${TASK_COLUMNS}`,
        [task.id, command.now],
      );
      await client.query(
        `UPDATE wf_tasks SET status = 'cancelled', updated_at = $2
         WHERE ancestor_id = $1
           AND status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')`,
        [task.id, command.now],
      );
      const row = rows[0];
      if (!row) throw new Error(`Task not found: ${command.taskId}`);
      return toTask(row);
    });
  }

  async cancelRun(command: CancelRunCommand): Promise<WorkflowRun> {
    return this.inTransaction(async (client) => {
      await client.query(
        `UPDATE wf_runs SET status = 'cancelled', updated_at = $2 WHERE id = $1`,
        [command.runId, command.now],
      );
      await client.query(
        `UPDATE wf_tasks SET status = 'cancelled', updated_at = $2
         WHERE run_id = $1
           AND status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')`,
        [command.runId, command.now],
      );
      const run = await readRun(client, command.runId);
      if (!run) throw new Error(`Run not found: ${command.runId}`);
      return run;
    });
  }

  async completePollingTask(
    command: CompletePollingTaskCommand,
  ): Promise<TaskCommitResult> {
    return this.inTransaction(async (client) => {
      const task = await readTask(client, command.taskId);
      if (!task) throw new Error(`Task not found: ${command.taskId}`);
      await client.query(
        `UPDATE wf_tasks SET status = 'succeeded', output = $2, updated_at = $3
         WHERE id = $1`,
        [command.taskId, JSON.stringify(command.output ?? null), command.now],
      );
      const unlocked = await unlockDependents(client, task, command.now);
      const runStatus = await recomputeRunStatus(client, task.runId, command.now);
      await resumeCoordinator(client, task, command.now);
      return {
        taskId: command.taskId,
        taskStatus: 'succeeded',
        runStatus,
        unlockedTaskIds: unlocked,
        replayed: false,
      };
    });
  }

  async failPollingTask(
    command: FailPollingTaskCommand,
  ): Promise<TaskCommitResult> {
    return this.inTransaction(async (client) => {
      await client.query(
        `UPDATE wf_tasks SET status = 'failed', error = $2, updated_at = $3
         WHERE id = $1`,
        [command.taskId, JSON.stringify(command.error), command.now],
      );
      return {
        taskId: command.taskId,
        taskStatus: 'failed',
        runStatus: 'failed',
        unlockedTaskIds: [],
        replayed: false,
      };
    });
  }

  async createChildRun(command: CreateChildRunCommand): Promise<WorkflowRun> {
    const parent = await this.getTask(command.parentTaskId);
    const parentRun = parent ? await this.getRun(parent.runId) : null;
    const created = await this.createRun({
      runId: command.runId,
      tenantKey: command.tenantKey,
      ...(command.resourceRef !== undefined
        ? { resourceRef: command.resourceRef }
        : {}),
      definition: command.definition,
      input: command.input,
      context: command.context,
      tasks: command.tasks,
      now: command.now,
    });
    const parentRunId = command.parentRunId ?? parent?.runId ?? null;
    const rootRunId = command.rootRunId ?? parentRun?.rootRunId ?? parentRun?.id ?? null;
    await this.pool.query(
      `UPDATE wf_runs SET status = 'running', parent_task_id = $2, parent_run_id = $3,
         root_run_id = $4
       WHERE id = $1`,
      [created.id, command.parentTaskId, parentRunId, rootRunId],
    );
    const run = await this.getRun(created.id);
    if (!run) throw new Error(`Run not found: ${created.id}`);
    return run;
  }

  async appendTasks(
    command: AppendTasksCommand,
  ): Promise<{ createdCount: number }> {
    return this.inTransaction(async (client) => {
      const replayed = await readCommand(client, command.commandId);
      if (replayed) return { createdCount: 0 };
      const run = await readRun(client, command.runId);
      if (!run) throw new Error(`Run not found: ${command.runId}`);
      let created = 0;
      for (const task of command.tasks) {
        const inserted = await insertTask(client, {
          task,
          runId: command.runId,
          tenantKey: run.tenantKey,
          ancestorId: command.coordinatorTaskId,
          now: command.now,
          ignoreDuplicate: true,
        });
        if (inserted) created += 1;
      }
      await storeCommand(client, command.commandId, {
        taskId: command.coordinatorTaskId,
        taskStatus: 'waiting',
        runStatus: run.status,
        unlockedTaskIds: [],
        replayed: false,
      });
      return { createdCount: created };
    });
  }

  async claimOutbox(
    command: ClaimOutboxCommand,
  ): Promise<readonly OutboxEvent[]> {
    const params: unknown[] = [command.now, command.limit];
    let destinationFilter = '';
    if (command.destination !== undefined) {
      params.push(command.destination);
      destinationFilter = `AND destination = $${params.length}`;
    }
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const token = crypto.randomUUID();
      const { rows } = await client.query<OutboxRow>(
        `UPDATE wf_outbox SET status = 'leased', lease_owner = $${params.length + 1},
           lease_token = $${params.length + 2},
           lease_expires_at = $${params.length + 3}
         WHERE id IN (
           SELECT id FROM wf_outbox
           WHERE status = 'pending' AND available_at <= $1 ${destinationFilter}
           ORDER BY available_at
           LIMIT $2
           FOR UPDATE SKIP LOCKED
         )
         RETURNING *`,
        [...params, command.workerId, token, command.leaseExpiresAt],
      );
      await client.query('COMMIT');
      return rows.map(toOutbox);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async markOutboxDispatched(
    command: MarkOutboxDispatchedCommand,
  ): Promise<void> {
    const { rowCount } = await this.pool.query(
      `UPDATE wf_outbox SET status = 'dispatched', dispatched_at = $3
       WHERE id = $1 AND lease_token = $2`,
      [command.eventId, command.leaseToken, command.now],
    );
    if (!rowCount) throw new Error(`Outbox event not leased: ${command.eventId}`);
  }

  async markOutboxFailed(command: MarkOutboxFailedCommand): Promise<void> {
    const { rowCount } = await this.pool.query(
      `UPDATE wf_outbox SET
         attempts = attempts + 1,
         status = CASE WHEN attempts + 1 >= $4 THEN 'dead_letter' ELSE 'pending' END,
         available_at = $5, last_error = $6,
         lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL
       WHERE id = $1 AND lease_token = $2`,
      [
        command.eventId,
        command.leaseToken,
        command.now,
        command.maxAttempts,
        command.retryAt,
        JSON.stringify(command.error),
      ],
    );
    if (!rowCount) throw new Error(`Outbox event not leased: ${command.eventId}`);
  }

  async getTask(taskId: string): Promise<TaskRun | null> {
    const { rows } = await this.pool.query<TaskRow>(
      `SELECT ${TASK_COLUMNS} FROM wf_tasks WHERE id = $1`,
      [taskId],
    );
    return rows[0] ? toTask(rows[0]) : null;
  }

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const { rows } = await this.pool.query<RunRow>(
      `SELECT * FROM wf_runs WHERE id = $1`,
      [runId],
    );
    return rows[0] ? toRun(rows[0]) : null;
  }

  async listTasks(runId: string): Promise<readonly TaskRun[]> {
    const { rows } = await this.pool.query<TaskRow>(
      `SELECT ${TASK_COLUMNS} FROM wf_tasks WHERE run_id = $1 ORDER BY created_at`,
      [runId],
    );
    return rows.map(toTask);
  }

  async listRuns(filter?: ListRunsFilter): Promise<readonly WorkflowRun[]> {
    const where: string[] = [];
    const params: unknown[] = [];
    const add = (column: string, value: unknown): void => {
      params.push(value);
      where.push(`${column} = $${params.length}`);
    };
    if (filter?.tenantKey !== undefined) add('tenant_key', filter.tenantKey);
    if (filter?.status !== undefined) add('status', filter.status);
    if (filter?.correlationKey !== undefined) {
      add('correlation_key', filter.correlationKey);
    }
    if (filter?.resourceRef !== undefined) add('resource_ref', filter.resourceRef);
    if (filter?.parentRunId !== undefined) add('parent_run_id', filter.parentRunId);
    if (filter?.parentTaskId !== undefined) add('parent_task_id', filter.parentTaskId);
    if (filter?.rootRunId !== undefined) add('root_run_id', filter.rootRunId);
    if (filter?.type !== undefined) add('type', filter.type);
    const offset = filter?.offset ?? 0;
    const limit = filter?.limit ?? 1000;
    params.push(limit, offset);
    const { rows } = await this.pool.query<RunRow>(
      `SELECT * FROM wf_runs ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY created_at LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    );
    return rows.map(toRun);
  }

  async createInteraction(
    command: CreateInteractionCommand,
  ): Promise<TaskInteraction> {
    const token = crypto.randomUUID();
    const { rows } = await this.pool.query<InteractionRow>(
      `INSERT INTO wf_interactions (id, tenant_key, run_id, task_id, kind, schema,
         ui_hints, draft_payload, status, token, expire_at, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'open',$9,$10,$11,$11) RETURNING *`,
      [
        command.interactionId,
        command.tenantKey,
        command.runId,
        command.taskId,
        command.kind,
        command.schema ? JSON.stringify(command.schema) : null,
        command.uiHints ? JSON.stringify(command.uiHints) : null,
        command.draftPayload !== undefined
          ? JSON.stringify(command.draftPayload)
          : null,
        token,
        command.expireAt ?? null,
        command.now,
      ],
    );
    const row = rows[0];
    if (!row) throw new Error('Failed to create interaction');
    return toInteraction(row);
  }

  async submitInteraction(
    command: SubmitInteractionCommand,
  ): Promise<TaskInteraction> {
    const { rows } = await this.pool.query<InteractionRow>(
      `UPDATE wf_interactions SET submitted_payload = $3, status = 'submitted',
         updated_at = $4
       WHERE id = $1 AND task_id = $2 AND token = $5 RETURNING *`,
      [
        command.interactionId,
        command.taskId,
        JSON.stringify(command.payload ?? null),
        command.now,
        command.token,
      ],
    );
    const row = rows[0];
    if (!row) throw new Error(`Interaction not found: ${command.interactionId}`);
    return toInteraction(row);
  }

  async getInteraction(interactionId: string): Promise<TaskInteraction | null> {
    const { rows } = await this.pool.query<InteractionRow>(
      `SELECT * FROM wf_interactions WHERE id = $1`,
      [interactionId],
    );
    return rows[0] ? toInteraction(rows[0]) : null;
  }

  async listInteractions(taskId: string): Promise<readonly TaskInteraction[]> {
    const { rows } = await this.pool.query<InteractionRow>(
      `SELECT * FROM wf_interactions WHERE task_id = $1`,
      [taskId],
    );
    return rows.map(toInteraction);
  }

  private async inTransaction<T>(
    work: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

async function insertTask(
  client: PoolClient,
  args: {
    task: CreateRunCommand['tasks'][number];
    runId: string;
    tenantKey: string;
    ancestorId: string | null;
    now: Date;
    ignoreDuplicate?: boolean;
  },
): Promise<boolean> {
  const depsRemaining = args.task.dependsOn.length;
  const { rowCount } = await client.query(
    `INSERT INTO wf_tasks (id, run_id, tenant_key, logical_key, hook, input, status,
       dependencies_remaining, ancestor_id, attempt, max_attempts, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,$10,$11,$11)
     ON CONFLICT (run_id, logical_key) DO NOTHING`,
    [
      args.task.id,
      args.runId,
      args.tenantKey,
      args.task.logicalKey,
      args.task.hook,
      JSON.stringify(args.task.input),
      depsRemaining === 0 ? 'ready' : 'pending',
      depsRemaining,
      args.ancestorId,
      args.task.maxAttempts,
      args.now,
    ],
  );
  if (!rowCount) return false;
  for (const dep of args.task.dependsOn) {
    await client.query(
      `INSERT INTO wf_task_deps (task_id, depends_on) VALUES ($1, $2)
       ON CONFLICT DO NOTHING`,
      [args.task.id, dep],
    );
  }
  return true;
}

async function readTask(
  client: PoolClient,
  taskId: string,
): Promise<TaskRun | null> {
  const { rows } = await client.query<TaskRow>(
    `SELECT ${TASK_COLUMNS} FROM wf_tasks WHERE id = $1 FOR UPDATE`,
    [taskId],
  );
  return rows[0] ? toTask(rows[0]) : null;
}

async function readRun(
  client: PoolClient,
  runId: string,
): Promise<WorkflowRun | null> {
  const { rows } = await client.query<RunRow>(
    `SELECT * FROM wf_runs WHERE id = $1`,
    [runId],
  );
  return rows[0] ? toRun(rows[0]) : null;
}

async function readCommand(
  client: PoolClient,
  commandId: string,
): Promise<TaskCommitResult | null> {
  const { rows } = await client.query<{ result: TaskCommitResult }>(
    `SELECT result FROM wf_commands WHERE command_id = $1`,
    [commandId],
  );
  const row = rows[0];
  if (!row) return null;
  return { ...row.result, unlockedTaskIds: row.result.unlockedTaskIds ?? [] };
}

async function storeCommand(
  client: PoolClient,
  commandId: string,
  result: TaskCommitResult,
): Promise<void> {
  await client.query(
    `INSERT INTO wf_commands (command_id, result) VALUES ($1, $2)`,
    [commandId, JSON.stringify(result)],
  );
}

async function unlockDependents(
  client: PoolClient,
  task: TaskRun,
  now: Date,
): Promise<string[]> {
  const { rows } = await client.query<{ id: string }>(
    `UPDATE wf_tasks SET
       dependencies_remaining = dependencies_remaining - 1,
       status = CASE WHEN dependencies_remaining - 1 = 0 THEN 'ready' ELSE status END,
       updated_at = $3
     WHERE run_id = $1
       AND dependencies_remaining > 0
       AND id IN (
         SELECT task_id FROM wf_task_deps WHERE depends_on IN ($2, $4)
       )
     RETURNING id`,
    [task.runId, task.id, now, task.logicalKey],
  );
  const unlocked: string[] = [];
  for (const row of rows) {
    const dependent = await readTask(client, row.id);
    if (dependent && dependent.dependenciesRemaining === 0) {
      unlocked.push(row.id);
      await enqueueTaskReady(client, {
        task: dependent,
        now,
        availableAt: now,
      });
    }
  }
  return unlocked;
}

async function resumeCoordinator(
  client: PoolClient,
  task: TaskRun,
  now: Date,
): Promise<void> {
  if (!task.ancestorId) return;
  const coordinator = await readTask(client, task.ancestorId);
  if (
    !coordinator ||
    coordinator.status !== 'waiting' ||
    coordinator.resumeToken === undefined
  ) {
    return;
  }
  const { rows } = await client.query<{ status: TaskRunStatus }>(
    `SELECT status FROM wf_tasks WHERE ancestor_id = $1`,
    [task.ancestorId],
  );
  const unfinished = rows.filter(
    (row) =>
      row.status !== 'succeeded' &&
      row.status !== 'skipped' &&
      row.status !== 'failed' &&
      row.status !== 'cancelled',
  );
  if (rows.length === 0 || unfinished.length > 0) return;
  await client.query(
    `UPDATE wf_tasks SET status = 'ready', updated_at = $2 WHERE id = $1`,
    [coordinator.id, now],
  );
  await enqueueTaskReady(client, {
    task: coordinator,
    now,
    availableAt: now,
  });
}

async function recomputeRunStatus(
  client: PoolClient,
  runId: string,
  now: Date,
): Promise<WorkflowRunStatus> {
  const { rows } = await client.query<{ status: TaskRunStatus }>(
    `SELECT status FROM wf_tasks WHERE run_id = $1`,
    [runId],
  );
  const statuses = rows.map((row) => row.status);
  let status: WorkflowRunStatus = 'pending';
  if (statuses.some((item) => item === 'failed')) status = 'failed';
  else if (statuses.some((item) => item === 'cancelled')) status = 'cancelled';
  else if (
    statuses.length > 0 &&
    statuses.every((item) => item === 'succeeded' || item === 'skipped')
  ) {
    status = 'succeeded';
  } else if (
    statuses.some((item) => item === 'leased' || item === 'waiting')
  ) {
    status = 'running';
  }
  await client.query(
    `UPDATE wf_runs SET status = $2, updated_at = $3 WHERE id = $1`,
    [runId, status, now],
  );
  return status;
}

async function enqueueTaskReady(
  client: PoolClient,
  args: {
    task: { id: string; runId: string; tenantKey: string; meta?: TaskRun['meta'] };
    now: Date;
    availableAt: Date;
  },
): Promise<void> {
  const dedupKey = `task.ready:${args.task.id}:${args.availableAt.toISOString()}`;
  await client.query(
    `INSERT INTO wf_outbox (id, tenant_key, run_id, task_id, event_type, payload,
       destination, dedup_key, status, available_at, attempts, created_at)
     VALUES ($1,$2,$3,$4,'task.ready',$5,'workflow.tasks',$6,'pending',$7,0,$8)
     ON CONFLICT (dedup_key) DO NOTHING`,
    [
      crypto.randomUUID(),
      args.task.tenantKey,
      args.task.runId,
      args.task.id,
      JSON.stringify({
        taskId: args.task.id,
        runId: args.task.runId,
        tenantKey: args.task.tenantKey,
      }),
      dedupKey,
      args.availableAt,
      args.now,
    ],
  );
}

interface OutboxRow {
  id: string;
  tenant_key: string;
  run_id: string;
  task_id: string;
  event_type: 'task.ready';
  payload: OutboxEvent['payload'];
  destination: string;
  dedup_key: string;
  status: OutboxEvent['status'];
  available_at: Date;
  attempts: number;
  last_error: unknown;
  lease_owner: string | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
  created_at: Date;
  dispatched_at: Date | null;
}

function toOutbox(row: OutboxRow): OutboxEvent {
  return {
    id: row.id,
    tenantKey: row.tenant_key,
    runId: row.run_id,
    taskId: row.task_id,
    eventType: row.event_type,
    payloadSchemaVersion: 1,
    payload: row.payload,
    destination: row.destination,
    dedupKey: row.dedup_key,
    status: row.status,
    availableAt: row.available_at,
    attempts: row.attempts,
    createdAt: row.created_at,
    ...(row.last_error !== null ? { lastError: row.last_error } : {}),
    ...(row.lease_owner !== null ? { leaseOwner: row.lease_owner } : {}),
    ...(row.lease_token !== null ? { leaseToken: row.lease_token } : {}),
    ...(row.lease_expires_at !== null
      ? { leaseExpiresAt: row.lease_expires_at }
      : {}),
    ...(row.dispatched_at !== null ? { dispatchedAt: row.dispatched_at } : {}),
  };
}

interface InteractionRow {
  id: string;
  tenant_key: string;
  run_id: string;
  task_id: string;
  kind: TaskInteraction['kind'];
  schema: Record<string, unknown> | null;
  ui_hints: Record<string, unknown> | null;
  draft_payload: unknown;
  submitted_payload: unknown;
  status: TaskInteraction['status'];
  token: string;
  expire_at: Date | null;
  created_at: Date;
  updated_at: Date;
}

function toInteraction(row: InteractionRow): TaskInteraction {
  return {
    id: row.id,
    tenantKey: row.tenant_key,
    runId: row.run_id,
    taskId: row.task_id,
    kind: row.kind,
    status: row.status,
    token: row.token,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...(row.schema !== null ? { schema: row.schema } : {}),
    ...(row.ui_hints !== null ? { uiHints: row.ui_hints } : {}),
    ...(row.draft_payload !== null ? { draftPayload: row.draft_payload } : {}),
    ...(row.submitted_payload !== null
      ? { submittedPayload: row.submitted_payload }
      : {}),
    ...(row.expire_at !== null ? { expireAt: row.expire_at } : {}),
  };
}
