import type {
  CancelRunCommand,
  CancelTaskCommand,
  ClaimedTask,
  ClaimOutboxCommand,
  ClaimTaskCommand,
  CommitTaskFailureCommand,
  CommitTaskSuccessCommand,
  CommitTaskContinueCommand,
  CompletePollingTaskCommand,
  CreateChildRunCommand,
  CreateInteractionCommand,
  CreateRunCommand,
  AppendTasksCommand,
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

function deepMerge(target: Record<string, unknown>, patch: Record<string, unknown>): Record<string, unknown> {
  const result = { ...target };
  for (const [key, value] of Object.entries(patch)) {
    if (value && typeof value === 'object' && !Array.isArray(value) && target[key] && typeof target[key] === 'object' && !Array.isArray(target[key])) {
      result[key] = deepMerge(target[key] as Record<string, unknown>, value as Record<string, unknown>);
    } else {
      result[key] = value;
    }
  }
  return result;
}

export class MemoryWorkflowStateStore implements WorkflowStateStore {
  readonly protocolVersion = 1 as const;

  private runs: Map<string, WorkflowRun> = new Map();
  private tasks: Map<string, TaskRun> = new Map();
  private outboxEvents: Map<string, OutboxEvent> = new Map();
  private interactions: Map<string, TaskInteraction> = new Map();
  private taskDeps: Map<string, string[]> = new Map();
  /** Applied command ids, so a replayed commit returns the original result. */
  private appliedCommands: Map<string, TaskCommitResult> = new Map();

  async getCapabilities(): Promise<StoreCapabilities> {
    return {
      atomicTaskCommit: true,
      transactionalOutbox: false,
      taskLeases: true,
    };
  }

  async createRun(command: CreateRunCommand): Promise<WorkflowRun> {
    const run: WorkflowRun = {
      id: command.runId,
      tenantKey: command.tenantKey,
      ...(command.name !== undefined ? { name: command.name } : {}),
      ...(command.type !== undefined ? { type: command.type } : {}),
      ...(command.resourceRef !== undefined
        ? { resourceRef: command.resourceRef }
        : {}),
      ...(command.correlationKey !== undefined
        ? { correlationKey: command.correlationKey }
        : {}),
      definitionName: command.definition.name,
      definitionVersion: command.definition.version,
      definition: command.definition,
      input: command.input,
      context: command.context,
      status: 'pending',
      createdAt: command.now,
      updatedAt: command.now,
    };
    this.runs.set(run.id, run);

    for (const t of command.tasks) {
      const depsRemaining = (t.dependsOn?.length) ?? 0;
      if (depsRemaining === 0) {
        this.enqueueTaskReady(
          { id: t.id, runId: run.id, tenantKey: command.tenantKey },
          command.now,
        );
      }
      this.taskDeps.set(t.id, [...t.dependsOn]);
      const task: TaskRun = {
        id: t.id,
        runId: run.id,
        tenantKey: command.tenantKey,
        logicalKey: t.logicalKey,
        hook: t.hook,
        input: t.input,
        status: depsRemaining === 0 ? 'ready' : 'pending',
        dependenciesRemaining: depsRemaining,
        attempt: 0,
        maxAttempts: t.maxAttempts,
        createdAt: command.now,
        updatedAt: command.now,
      };
      this.tasks.set(task.id, task);
    }
    return run;
  }

  async claimTask(command: ClaimTaskCommand): Promise<ClaimedTask | null> {
    this.reclaimExpiredLeases(command.now);
    for (const task of this.tasks.values()) {
      if (task.dependenciesRemaining === 0 && task.status === 'ready') {
        if (command.taskId !== undefined && task.id !== command.taskId) continue;
        // Delayed loop iterations stay ready but unclaimable until notBefore.
        if (task.notBefore && task.notBefore.getTime() > command.now.getTime()) continue;
        const run = this.runs.get(task.runId);
        if (!run) return null;
        if (run.status === 'cancelled') continue;
        const claimedTask: TaskRun = {
          ...task,
          status: 'leased',
          leaseOwner: command.workerId,
          leaseToken: crypto.randomUUID(),
          leaseExpiresAt: command.leaseExpiresAt,
          updatedAt: command.now,
        };
        this.tasks.set(task.id, claimedTask);

        if (run.status === 'pending') {
          const runningRun = { ...run, status: 'running' as WorkflowRunStatus, updatedAt: command.now };
          this.runs.set(run.id, runningRun);
        }

        return { run: run, task: claimedTask as any };
      }
    }
    return null;
  }

  async heartbeatTask(command: HeartbeatTaskCommand): Promise<TaskRun> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    if (task.leaseToken !== command.leaseToken) {
      throw new Error(`Stale lease token for task: ${command.taskId}`);
    }
    const updated = { ...task, leaseExpiresAt: command.leaseExpiresAt, updatedAt: command.now };
    this.tasks.set(task.id, updated);
    return updated;
  }

  async commitTaskSuccess(command: CommitTaskSuccessCommand): Promise<TaskCommitResult> {
    const replayed = this.appliedCommands.get(command.commandId);
    if (replayed) return { ...replayed, replayed: true };
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    if (task.leaseToken !== undefined && task.leaseToken !== command.leaseToken) {
      throw new Error(`Stale lease token for task: ${command.taskId}`);
    }

    const succeeded: TaskRun = {
      ...task,
      status: 'succeeded',
      output: command.output,
      updatedAt: command.now,
    };
    this.tasks.set(task.id, succeeded);

    // Apply contextPatch to run context
    if (command.contextPatch) {
      const run = this.runs.get(task.runId);
      if (run) {
        const mergedContext = deepMerge(run.context as Record<string, unknown>, command.contextPatch);
        this.runs.set(run.id, { ...run, context: mergedContext, updatedAt: command.now });
      }
    }

    const unlockedIds = this.unlockDependents(succeeded, command.now);

    const run = this.runs.get(task.runId);
    const runStatus = run ? this.computeRunStatus(run.id) : 'pending';
    if (run && run.status !== runStatus) {
      this.runs.set(run.id, { ...run, status: runStatus as WorkflowRunStatus, updatedAt: command.now });
    }

    const result: TaskCommitResult = { taskId: task.id, taskStatus: 'succeeded', runStatus, unlockedTaskIds: unlockedIds, replayed: false };
    this.appliedCommands.set(command.commandId, result);
    return result;
  }

  async commitTaskFailure(command: CommitTaskFailureCommand): Promise<TaskCommitResult> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const failed: TaskRun = {
      ...task,
      status: 'failed',
      error: command.error,
      updatedAt: command.now,
    };
    this.tasks.set(task.id, failed);
    const run = this.runs.get(task.runId);
    const runStatus = run ? this.computeRunStatus(run.id) : 'failed';
    if (run && run.status !== runStatus) {
      this.runs.set(run.id, { ...run, status: runStatus as WorkflowRunStatus, updatedAt: command.now });
    }
    return { taskId: task.id, taskStatus: 'failed', runStatus, unlockedTaskIds: [], replayed: false };
  }

  async commitTaskContinue(command: CommitTaskContinueCommand): Promise<TaskCommitResult> {
    const replayed = this.appliedCommands.get(command.commandId);
    if (replayed) return { ...replayed, replayed: true };
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const notBefore =
      command.delayMs !== undefined && command.delayMs > 0
        ? new Date(command.now.getTime() + command.delayMs)
        : undefined;
    const continued: TaskRun = {
      ...task,
      status: 'ready',
      attempt: 0,
      ...(command.output !== undefined ? { output: command.output } : {}),
      // A round without a delay must clear a delay left by an earlier round.
      ...(notBefore !== undefined ? { notBefore } : { notBefore: undefined }),
      meta: {
        ...(task.meta ?? {}),
        loopState: {
          iteration: command.iteration,
          checkpoint: command.checkpoint,
        },
      },
      updatedAt: command.now,
    };
    this.tasks.set(task.id, continued);
    this.enqueueTaskReady(continued, command.now, notBefore);
    const run = this.runs.get(task.runId);
    if (run && run.status !== 'running' && run.status !== 'cancelled') {
      this.runs.set(run.id, { ...run, status: 'running', updatedAt: command.now });
    }
    const result: TaskCommitResult = {
      taskId: task.id,
      taskStatus: 'ready',
      runStatus: 'running',
      unlockedTaskIds: [],
      replayed: false,
    };
    this.appliedCommands.set(command.commandId, result);
    return result;
  }

  async suspendTask(command: SuspendTaskCommand): Promise<TaskCommitResult> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const waiting: TaskRun = { ...task, status: 'waiting', waitingReason: command.reason, resumeToken: command.resumeToken, updatedAt: command.now };
    this.tasks.set(task.id, waiting);
    const run = this.runs.get(task.runId);
    const runStatus = run ? this.computeRunStatus(run.id) : 'running';
    if (run && run.status !== runStatus) {
      this.runs.set(run.id, { ...run, status: runStatus as WorkflowRunStatus, updatedAt: command.now });
    }
    return { taskId: task.id, taskStatus: 'waiting', runStatus, unlockedTaskIds: [], replayed: false };
  }

  async resumeTask(command: ResumeTaskCommand): Promise<TaskRun> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    if (task.resumeToken !== undefined && task.resumeToken !== command.resumeToken) {
      throw new Error('resume token mismatch');
    }
    const resumed: TaskRun = {
      ...task,
      status: 'ready',
      ...(command.input !== undefined
        ? { input: { ...task.input, ...command.input } }
        : {}),
      updatedAt: command.now,
    };
    this.tasks.set(task.id, resumed);
    this.enqueueTaskReady(resumed, command.now);
    return resumed;
  }

  async cancelTask(command: CancelTaskCommand): Promise<TaskRun> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const cancelled: TaskRun = { ...task, status: 'cancelled', updatedAt: command.now };
    this.tasks.set(task.id, cancelled);
    const run = this.runs.get(task.runId);
    if (run) {
      const runStatus = this.computeRunStatus(task.runId);
      this.runs.set(run.id, { ...run, status: runStatus, updatedAt: command.now });
    }
    // Cancelling a coordinator stops its subtasks; they must not keep running.
    for (const child of this.tasks.values()) {
      if (child.ancestorId !== task.id) continue;
      if (isTerminalStatus(child.status)) continue;
      this.tasks.set(child.id, { ...child, status: 'cancelled', updatedAt: command.now });
    }
    return cancelled;
  }

  async cancelRun(command: CancelRunCommand): Promise<WorkflowRun> {
    const run = this.runs.get(command.runId);
    if (!run) throw new Error(`Run not found: ${command.runId}`);
    const cancelled: WorkflowRun = { ...run, status: 'cancelled', updatedAt: command.now };
    this.runs.set(run.id, cancelled);
    for (const task of this.tasks.values()) {
      if (task.runId !== run.id) continue;
      if (isTerminalStatus(task.status)) continue;
      this.tasks.set(task.id, { ...task, status: 'cancelled', updatedAt: command.now });
    }
    return cancelled;
  }

  async completePollingTask(command: CompletePollingTaskCommand): Promise<TaskCommitResult> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const succeeded: TaskRun = { ...task, status: 'succeeded', output: command.output, updatedAt: command.now };
    this.tasks.set(task.id, succeeded);
    const unlocked = this.unlockDependents(succeeded, command.now);
    await this.maybeResumeCoordinator(succeeded, command.now);
    const run = this.runs.get(task.runId);
    const runStatus = run ? this.computeRunStatus(run.id) : 'running';
    if (run) {
      this.runs.set(run.id, { ...run, status: runStatus, updatedAt: command.now });
    }
    return { taskId: task.id, taskStatus: 'succeeded', runStatus, unlockedTaskIds: unlocked, replayed: false };
  }

  async failPollingTask(command: FailPollingTaskCommand): Promise<TaskCommitResult> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const failed: TaskRun = { ...task, status: 'failed', error: command.error, updatedAt: command.now };
    this.tasks.set(task.id, failed);
    return { taskId: task.id, taskStatus: 'failed', runStatus: 'failed', unlockedTaskIds: [], replayed: false };
  }

  async createChildRun(command: CreateChildRunCommand): Promise<WorkflowRun> {
    const parentTask = this.tasks.get(command.parentTaskId);
    const parentRun = parentTask ? this.runs.get(parentTask.runId) : undefined;
    const created = await this.createRun({
      runId: command.runId,
      tenantKey: command.tenantKey,
      ...(command.name !== undefined ? { name: command.name } : {}),
      ...(command.type !== undefined ? { type: command.type } : {}),
      ...(command.resourceRef !== undefined ? { resourceRef: command.resourceRef } : {}),
      definition: command.definition,
      input: command.input,
      context: command.context,
      tasks: command.tasks,
      now: command.now,
    });
    const parentRunId = command.parentRunId ?? parentTask?.runId;
    const rootRunId = command.rootRunId ?? parentRun?.rootRunId ?? parentRun?.id;
    const withParent: WorkflowRun = {
      ...created,
      status: 'running',
      parentTaskId: command.parentTaskId,
      ...(parentRunId !== undefined ? { parentRunId } : {}),
      ...(rootRunId !== undefined ? { rootRunId } : {}),
    };
    this.runs.set(created.id, withParent);
    return withParent;
  }

  async appendTasks(command: AppendTasksCommand): Promise<{ createdCount: number }> {
    const replayed = this.appliedCommands.get(command.commandId);
    if (replayed) return { createdCount: 0 };
    const run = this.runs.get(command.runId);
    if (!run) throw new Error(`Run not found: ${command.runId}`);
    let created = 0;
    for (const t of command.tasks) {
      const duplicate = Array.from(this.tasks.values()).some(
        (existing) =>
          existing.runId === command.runId &&
          existing.ancestorId === command.coordinatorTaskId &&
          existing.logicalKey === t.logicalKey,
      );
      if (duplicate) continue;
      const depsRemaining = t.dependsOn.length;
      this.taskDeps.set(t.id, [...t.dependsOn]);
      const task: TaskRun = {
        id: t.id,
        runId: command.runId,
        tenantKey: run.tenantKey,
        logicalKey: t.logicalKey,
        hook: t.hook,
        input: t.input,
        status: depsRemaining === 0 ? 'ready' : 'pending',
        dependenciesRemaining: depsRemaining,
        ancestorId: command.coordinatorTaskId,
        attempt: 0,
        maxAttempts: t.maxAttempts,
        createdAt: command.now,
        updatedAt: command.now,
      };
      this.tasks.set(t.id, task);
      if (depsRemaining === 0) this.enqueueTaskReady(task, command.now);
      created += 1;
    }
    this.appliedCommands.set(command.commandId, {
      taskId: command.coordinatorTaskId,
      taskStatus: 'waiting',
      runStatus: run.status,
      unlockedTaskIds: [],
      replayed: false,
    });
    return { createdCount: created };
  }

  async claimOutbox(command: ClaimOutboxCommand): Promise<readonly OutboxEvent[]> {
    const claimed: OutboxEvent[] = [];
    for (const event of this.outboxEvents.values()) {
      if (claimed.length >= command.limit) break;
      if (event.status !== 'pending') continue;
      if (event.availableAt.getTime() > command.now.getTime()) continue;
      if (
        command.destination !== undefined &&
        event.destination !== command.destination
      ) {
        continue;
      }
      const leased: OutboxEvent = {
        ...event,
        status: 'leased',
        leaseOwner: command.workerId,
        leaseToken: crypto.randomUUID(),
        leaseExpiresAt: command.leaseExpiresAt,
      };
      this.outboxEvents.set(event.id, leased);
      claimed.push(leased);
    }
    return claimed;
  }

  async markOutboxDispatched(command: MarkOutboxDispatchedCommand): Promise<void> {
    const event = this.outboxEvents.get(command.eventId);
    if (!event || event.leaseToken !== command.leaseToken) {
      throw new Error(`Outbox event not leased: ${command.eventId}`);
    }
    this.outboxEvents.set(event.id, {
      ...event,
      status: 'dispatched',
      dispatchedAt: command.now,
    });
  }

  async markOutboxFailed(command: MarkOutboxFailedCommand): Promise<void> {
    const event = this.outboxEvents.get(command.eventId);
    if (!event || event.leaseToken !== command.leaseToken) {
      throw new Error(`Outbox event not leased: ${command.eventId}`);
    }
    const attempts = event.attempts + 1;
    const exhausted = attempts >= command.maxAttempts;
    this.outboxEvents.set(event.id, {
      ...event,
      status: exhausted ? 'dead_letter' : 'pending',
      attempts,
      availableAt: command.retryAt,
      lastError: command.error,
      leaseOwner: undefined,
      leaseToken: undefined,
      leaseExpiresAt: undefined,
    });
  }

  async getTask(taskId: string): Promise<TaskRun | null> { return this.tasks.get(taskId) ?? null; }
  async getRun(runId: string): Promise<WorkflowRun | null> { return this.runs.get(runId) ?? null; }

  async listTasks(runId: string): Promise<readonly TaskRun[]> {
    return Array.from(this.tasks.values()).filter((t) => t.runId === runId);
  }

  async listRuns(filter?: ListRunsFilter): Promise<readonly WorkflowRun[]> {
    let runs = Array.from(this.runs.values());
    if (!filter) return runs;
    if (filter.tenantKey !== undefined) {
      runs = runs.filter((r) => r.tenantKey === filter.tenantKey);
    }
    if (filter.status !== undefined) {
      runs = runs.filter((r) => r.status === filter.status);
    }
    if (filter.parentTaskId !== undefined) {
      runs = runs.filter((r) => r.parentTaskId === filter.parentTaskId);
    }
    if (filter.parentRunId !== undefined) {
      runs = runs.filter((r) => r.parentRunId === filter.parentRunId);
    }
    if (filter.rootRunId !== undefined) {
      runs = runs.filter((r) => r.rootRunId === filter.rootRunId);
    }
    if (filter.correlationKey !== undefined) {
      runs = runs.filter((r) => r.correlationKey === filter.correlationKey);
    }
    if (filter.resourceRef !== undefined) {
      runs = runs.filter((r) => r.resourceRef === filter.resourceRef);
    }
    if (filter.type !== undefined) {
      runs = runs.filter((r) => r.type === filter.type);
    }
    const offset = filter.offset ?? 0;
    const limit = filter.limit ?? runs.length;
    return runs.slice(offset, offset + limit);
  }

  async createInteraction(command: CreateInteractionCommand): Promise<TaskInteraction> {
    const interaction: TaskInteraction = {
      id: command.interactionId,
      tenantKey: command.tenantKey,
      runId: command.runId,
      taskId: command.taskId,
      kind: command.kind,
      schema: command.schema,
      uiHints: command.uiHints,
      draftPayload: command.draftPayload,
      status: 'open',
      token: crypto.randomUUID(),
      expireAt: command.expireAt,
      createdAt: command.now,
      updatedAt: command.now,
    };
    this.interactions.set(interaction.id, interaction);
    return interaction;
  }
  async submitInteraction(command: SubmitInteractionCommand): Promise<TaskInteraction> {
    const interaction = this.interactions.get(command.interactionId);
    if (!interaction) throw new Error(`Interaction not found: ${command.interactionId}`);
    const submitted: TaskInteraction = {
      ...interaction,
      submittedPayload: command.payload,
      status: 'submitted',
      updatedAt: command.now,
    };
    this.interactions.set(interaction.id, submitted);
    return submitted;
  }
  async getInteraction(interactionId: string): Promise<TaskInteraction | null> {
    return this.interactions.get(interactionId) ?? null;
  }
  async listInteractions(taskId: string): Promise<readonly TaskInteraction[]> {
    return Array.from(this.interactions.values()).filter((i) => i.taskId === taskId);
  }

  /**
   * When the last subtask of a waiting coordinator finishes, the coordinator
   * becomes claimable again. Mirrors the engine's maybeResumeWaiters so a
   * store used without the engine still converges.
   */
  private async maybeResumeCoordinator(task: TaskRun, now: Date): Promise<void> {
    if (!task.ancestorId) return;
    const coordinator = this.tasks.get(task.ancestorId);
    if (
      !coordinator ||
      coordinator.status !== 'waiting' ||
      coordinator.resumeToken === undefined
    ) {
      return;
    }
    const siblings = Array.from(this.tasks.values()).filter(
      (item) => item.ancestorId === task.ancestorId,
    );
    const unfinished = siblings.filter(
      (item) =>
        item.status !== 'succeeded' &&
        item.status !== 'skipped' &&
        item.status !== 'failed' &&
        item.status !== 'cancelled',
    );
    if (siblings.length === 0 || unfinished.length > 0) return;
    await this.resumeTask({
      commandId: `coordinator-resume:${coordinator.id}:${task.id}`,
      taskId: coordinator.id,
      resumeToken: coordinator.resumeToken,
      now,
    });
  }

  private unlockDependents(task: TaskRun, now: Date): string[] {
    const unlocked: string[] = [];
    for (const candidate of this.tasks.values()) {
      if (candidate.id === task.id) continue;
      if (candidate.dependenciesRemaining <= 0) continue;
      const deps = this.getTaskDeps(candidate);
      if (!deps.includes(task.id) && !deps.includes(task.logicalKey)) continue;
      const remaining = candidate.dependenciesRemaining - 1;
      const updated: TaskRun = {
        ...candidate,
        dependenciesRemaining: remaining,
        status: remaining === 0 ? 'ready' : candidate.status,
        updatedAt: now,
      };
      this.tasks.set(candidate.id, updated);
      if (remaining === 0) {
        unlocked.push(candidate.id);
        this.enqueueTaskReady(updated, now);
      }
    }
    return unlocked;
  }

  /** A lease that outlived its expiry is no longer owned; the task is ready. */
  private reclaimExpiredLeases(now: Date): void {
    for (const task of this.tasks.values()) {
      if (task.status !== 'leased' || !task.leaseExpiresAt) continue;
      if (task.leaseExpiresAt.getTime() > now.getTime()) continue;
      this.tasks.set(task.id, {
        ...task,
        status: 'ready',
        leaseOwner: undefined,
        leaseToken: undefined,
        leaseExpiresAt: undefined,
        updatedAt: now,
      });
    }
  }

  /** Record a task.ready outbox event, honoring a delayed availableAt. */
  private enqueueTaskReady(
    task: Pick<TaskRun, 'id' | 'runId' | 'tenantKey'>,
    now: Date,
    notBefore?: Date,
  ): void {
    const availableAt = notBefore ?? now;
    const dedupKey = `task.ready:${task.id}:${availableAt.toISOString()}`;
    for (const existing of this.outboxEvents.values()) {
      if (existing.dedupKey === dedupKey) return;
    }
    const event: OutboxEvent = {
      id: crypto.randomUUID(),
      tenantKey: task.tenantKey,
      runId: task.runId,
      taskId: task.id,
      eventType: 'task.ready',
      payloadSchemaVersion: 1,
      payload: {
        taskId: task.id,
        runId: task.runId,
        tenantKey: task.tenantKey,
      },
      destination: 'workflow.tasks',
      dedupKey,
      status: 'pending',
      availableAt,
      attempts: 0,
      createdAt: now,
    };
    this.outboxEvents.set(event.id, event);
  }

  private getTaskDeps(task: TaskRun): string[] {
    const stored = this.taskDeps.get(task.id);
    if (stored) return stored;
    const run = this.runs.get(task.runId);
    if (!run) return [];
    const node = run.definition.nodes.find(
      (n: { id: string; dependsOn?: readonly string[] }) =>
        n.id === task.logicalKey || n.id === task.id,
    );
    return [...(node?.dependsOn ?? [])];
  }

  private computeRunStatus(runId: string): WorkflowRunStatus {
    const tasks = Array.from(this.tasks.values()).filter((t) => t.runId === runId);
    if (tasks.length === 0) return 'pending';
    if (tasks.some((t) => t.status === 'failed')) return 'failed';
    if (tasks.some((t) => t.status === 'cancelled')) return 'cancelled';
    if (tasks.every((t) => t.status === 'succeeded' || t.status === 'skipped')) return 'succeeded';
    if (tasks.some((t) => t.status === 'leased' || t.status === 'waiting')) return 'running';
    return 'pending';
  }
}

function isTerminalStatus(status: TaskRunStatus): boolean {
  return (
    status === 'succeeded' ||
    status === 'failed' ||
    status === 'skipped' ||
    status === 'cancelled'
  );
}
