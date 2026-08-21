import type {
  CancelRunCommand,
  CancelTaskCommand,
  ClaimedTask,
  ClaimOutboxCommand,
  ClaimTaskCommand,
  CommitTaskFailureCommand,
  CommitTaskSuccessCommand,
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
      resourceRef: command.resourceRef,
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
    for (const task of this.tasks.values()) {
      if (task.dependenciesRemaining === 0 && task.status === 'ready') {
        if (command.taskId !== undefined && task.id !== command.taskId) continue;
        const claimedTask: TaskRun = {
          ...task,
          status: 'leased',
          leaseOwner: command.workerId,
          leaseToken: crypto.randomUUID(),
          leaseExpiresAt: command.leaseExpiresAt,
          updatedAt: command.now,
        };
        this.tasks.set(task.id, claimedTask);

        const run = this.runs.get(task.runId);
        if (!run) return null;
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
    const updated = { ...task, leaseExpiresAt: command.leaseExpiresAt, updatedAt: command.now };
    this.tasks.set(task.id, updated);
    return updated;
  }

  async commitTaskSuccess(command: CommitTaskSuccessCommand): Promise<TaskCommitResult> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);

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

    const unlockedIds: string[] = [];
    const succeededLogicalKey = task.logicalKey;
    for (const t of this.tasks.values()) {
      if (t.id === task.id) continue;
      if (t.dependenciesRemaining > 0) {
        const deps = this.getTaskDeps(t);
        if (deps.includes(succeededLogicalKey) || deps.includes(task.id)) {
          const updated = { ...t, dependenciesRemaining: t.dependenciesRemaining - 1, status: (t.dependenciesRemaining - 1 === 0 ? 'ready' : t.status) as TaskRunStatus, updatedAt: command.now };
          this.tasks.set(t.id, updated);
          if (updated.dependenciesRemaining === 0) unlockedIds.push(t.id);
        }
      }
    }

    const run = this.runs.get(task.runId);
    const runStatus = run ? this.computeRunStatus(run.id) : 'pending';
    if (run && run.status !== runStatus) {
      this.runs.set(run.id, { ...run, status: runStatus as WorkflowRunStatus, updatedAt: command.now });
    }

    return { taskId: task.id, taskStatus: 'succeeded', runStatus, unlockedTaskIds: unlockedIds, replayed: false };
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
    const resumed: TaskRun = {
      ...task,
      status: 'ready',
      ...(command.input !== undefined
        ? { input: { ...task.input, ...command.input } }
        : {}),
      updatedAt: command.now,
    };
    this.tasks.set(task.id, resumed);
    return resumed;
  }

  async cancelTask(command: CancelTaskCommand): Promise<TaskRun> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const cancelled: TaskRun = { ...task, status: 'cancelled', updatedAt: command.now };
    this.tasks.set(task.id, cancelled);
    return cancelled;
  }

  async cancelRun(command: CancelRunCommand): Promise<WorkflowRun> {
    const run = this.runs.get(command.runId);
    if (!run) throw new Error(`Run not found: ${command.runId}`);
    const cancelled: WorkflowRun = { ...run, status: 'cancelled', updatedAt: command.now };
    this.runs.set(run.id, cancelled);
    return cancelled;
  }

  async completePollingTask(command: CompletePollingTaskCommand): Promise<TaskCommitResult> {
    const task = this.tasks.get(command.taskId);
    if (!task) throw new Error(`Task not found: ${command.taskId}`);
    const succeeded: TaskRun = { ...task, status: 'succeeded', output: command.output, updatedAt: command.now };
    this.tasks.set(task.id, succeeded);
    return { taskId: task.id, taskStatus: 'succeeded', runStatus: 'running', unlockedTaskIds: [], replayed: false };
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
      parentTaskId: command.parentTaskId,
      ...(parentRunId !== undefined ? { parentRunId } : {}),
      ...(rootRunId !== undefined ? { rootRunId } : {}),
    };
    this.runs.set(created.id, withParent);
    return withParent;
  }

  async appendTasks(command: AppendTasksCommand): Promise<{ createdCount: number }> {
    const run = this.runs.get(command.runId);
    if (!run) throw new Error(`Run not found: ${command.runId}`);
    for (const t of command.tasks) {
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
    }
    return { createdCount: command.tasks.length };
  }

  async claimOutbox(_command: ClaimOutboxCommand): Promise<readonly OutboxEvent[]> { return []; }
  async markOutboxDispatched(_command: MarkOutboxDispatchedCommand): Promise<void> {}
  async markOutboxFailed(_command: MarkOutboxFailedCommand): Promise<void> {}

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
