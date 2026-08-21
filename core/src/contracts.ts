import type { ChildTemplateRegistryConfig } from './child-template-registry.js';

export interface WorkflowNodeDefinition {
  readonly id: string;
  readonly hook: string;
  readonly dependsOn?: readonly string[];
  readonly input?: Readonly<Record<string, WorkflowInputValue>>;
}

export type WorkflowInputValue =
  | null
  | boolean
  | number
  | string
  | readonly WorkflowInputValue[]
  | { readonly [key: string]: WorkflowInputValue };

export interface WorkflowDefinition {
  readonly schemaVersion: 1;
  readonly name: string;
  readonly version: string;
  readonly nodes: readonly WorkflowNodeDefinition[];
}

export interface HookExecutionContext<TServices = unknown> {
  readonly run: {
    readonly id: string;
    readonly tenantKey: string;
    readonly resourceRef?: string;
    readonly input: Readonly<Record<string, unknown>>;
    readonly context: Readonly<Record<string, unknown>>;
  };
  readonly task: {
    readonly id: string;
    readonly logicalKey: string;
    readonly attempt: number;
  };
  readonly services: TServices;
  readonly signal: AbortSignal;
  readonly now: () => Date;
}

export type HookExecutionResult<TOutput = unknown> =
  | {
      readonly status: 'succeeded';
      readonly output: TOutput;
      readonly contextPatch?: Readonly<Record<string, unknown>>;
    }
  | {
      readonly status: 'waiting';
      readonly reason: 'external' | 'interaction' | 'child_run';
      readonly resumeToken: string;
    }
  | {
      readonly status: 'continue';
      /** Opaque business checkpoint for the next iteration. */
      readonly checkpoint: unknown;
      /** Next iteration index to persist (0-based). */
      readonly iteration: number;
      /** Optional visible progress for this round. */
      readonly output?: unknown;
    };

export interface HookDefinition<
  TInput = unknown,
  TOutput = unknown,
  TServices = unknown,
> {
  readonly name: string;
  execute(
    context: HookExecutionContext<TServices>,
    input: TInput,
  ): Promise<HookExecutionResult<TOutput>>;
}

export interface PluginManifest {
  readonly manifestVersion: 1;
  readonly name: string;
  readonly version: string;
  readonly engine: string;
}

export interface WorkflowPlugin {
  readonly manifest: PluginManifest;
  readonly hooks: readonly HookDefinition[];
  readonly workflows?: readonly WorkflowDefinition[];
}

export interface StoreCapabilities {
  readonly atomicTaskCommit: true;
  readonly transactionalOutbox: boolean;
  readonly taskLeases: true;
}

export type WorkflowRunStatus =
  | 'pending'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export type TaskRunStatus =
  | 'pending'
  | 'queued'
  | 'ready'
  | 'leased'
  | 'running'
  | 'waiting'
  | 'waiting_input'
  | 'polling'
  | 'succeeded'
  | 'failed'
  | 'skipped'
  | 'cancelled';

export interface WorkflowRun {
  readonly id: string;
  readonly tenantKey: string;
  readonly name?: string;
  readonly type?: string;
  readonly resourceRef?: string;
  readonly correlationKey?: string;
  readonly definitionName: string;
  readonly definitionVersion: string;
  readonly definition: WorkflowDefinition;
  readonly input: Readonly<Record<string, unknown>>;
  readonly context: Readonly<Record<string, unknown>>;
  readonly status: WorkflowRunStatus;
  /** 子 Run 链：父 Run ID（spawn 产生） */
  readonly parentRunId?: string;
  /** 子 Run 链：触发此子 Run 的 Task ID */
  readonly parentTaskId?: string;
  /** 子 Run 链：链顶端根 Run ID（级联取消 / 展示用） */
  readonly rootRunId?: string;
  /** 总任务数 */
  readonly totalTasks?: number;
  /** 已完成任务数 */
  readonly doneTasks?: number;
  /** 扩展元数据（运行时上下文、业务自定义字段） */
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly createdAt: Date;
  readonly updatedAt?: Date;
  readonly startedAt?: Date;
  readonly finishedAt?: Date;
}

export interface TaskRun {
  readonly id: string;
  readonly runId: string;
  readonly tenantKey: string;
  readonly logicalKey: string;
  /** 任务类型（通常即 hook 名） */
  readonly type?: string;
  /** 展示名称 */
  readonly name?: string;
  readonly hook: string;
  /** foreach 展开的分组键 */
  readonly groupKey?: string;
  /** 队列名称（多队列分发） */
  readonly queueName?: string;
  /** 资源引用（亲和性调度 / 锁定） */
  readonly resourceRef?: string;
  readonly input: Readonly<Record<string, unknown>>;
  readonly status: TaskRunStatus;
  /** 剩余依赖计数（归零时 status → ready） */
  readonly dependenciesRemaining: number;
  /** 正向依赖：上游 Task ID */
  readonly parentId?: string;
  /** 反向依赖：子 Task → Coordinator 回溯 */
  readonly ancestorId?: string;
  /** 有序扇出：序列键 */
  readonly sequenceKey?: string;
  /** 有序扇出：序列序号 */
  readonly orderIndex?: number;
  /** 幂等键（重复创建检测） */
  readonly idempotencyKey?: string;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly leaseOwner?: string;
  readonly leaseToken?: string;
  readonly leaseExpiresAt?: Date;
  readonly waitingReason?: 'external' | 'interaction' | 'child_run';
  readonly resumeToken?: string;
  readonly output?: unknown;
  readonly error?: unknown;
  /** 执行日志 */
  readonly logs?: Readonly<Record<string, unknown>>;
  /** 扩展元数据（spawn 绑定、coordinator 状态等运行时上下文） */
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly queuedAt?: Date;
  readonly startedAt?: Date;
  readonly finishedAt?: Date;
  readonly completedAt?: Date;
}

export interface MaterializedTask {
  readonly id: string;
  readonly logicalKey: string;
  readonly hook: string;
  readonly input: Readonly<Record<string, unknown>>;
  readonly dependsOn: readonly string[];
  readonly maxAttempts: number;
}

/** 任务依赖边（对应 task_deps 表） */
export interface TaskDep {
  readonly fromTask: string;
  readonly toTask: string;
  readonly runId: string;
  /** forward: 正向依赖（上游完成 → 下游 deps_remaining--）；
   *  reverse: 反向依赖（子 Task 完成 → Coordinator 回溯） */
  readonly direction: 'forward' | 'reverse';
  /** 运行时条件表达式（存在时需评估后才激活依赖） */
  readonly condition?: string;
  readonly createdAt: Date;
}

export interface CreateRunCommand {
  readonly runId: string;
  readonly tenantKey: string;
  readonly name?: string;
  readonly type?: string;
  readonly resourceRef?: string;
  readonly correlationKey?: string;
  readonly definition: WorkflowDefinition;
  readonly input: Readonly<Record<string, unknown>>;
  readonly context: Readonly<Record<string, unknown>>;
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly tasks: readonly MaterializedTask[];
  readonly now: Date;
}

export interface ClaimTaskCommand {
  readonly workerId: string;
  readonly taskId?: string;
  readonly queueName?: string;
  readonly now: Date;
  readonly leaseExpiresAt: Date;
}

export interface HeartbeatTaskCommand {
  readonly taskId: string;
  readonly leaseToken: string;
  readonly now: Date;
  readonly leaseExpiresAt: Date;
}

export interface ClaimedTask {
  readonly run: WorkflowRun;
  readonly task: TaskRun & {
    readonly status: 'leased';
    readonly leaseOwner: string;
    readonly leaseToken: string;
    readonly leaseExpiresAt: Date;
  };
}

export interface CommitTaskSuccessCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly leaseToken: string;
  readonly output: unknown;
  readonly contextPatch?: Readonly<Record<string, unknown>>;
  readonly now: Date;
}

export interface CommitTaskFailureCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly leaseToken: string;
  readonly error: { readonly message: string };
  readonly now: Date;
}

export interface SuspendTaskCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly leaseToken: string;
  readonly reason: 'external' | 'interaction' | 'child_run';
  readonly resumeToken: string;
  readonly now: Date;
}

export interface CommitTaskContinueCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly leaseToken: string;
  readonly checkpoint: unknown;
  readonly iteration: number;
  readonly output?: unknown;
  readonly now: Date;
}

export interface TaskCommitResult {
  readonly taskId: string;
  readonly taskStatus: 'succeeded' | 'failed' | 'waiting' | 'ready';
  readonly runStatus: WorkflowRunStatus;
  readonly unlockedTaskIds: readonly string[];
  readonly replayed: boolean;
}

export interface OutboxEvent {
  readonly id: string;
  readonly tenantKey: string;
  readonly runId: string;
  readonly taskId: string;
  /** 出队后要激活的下一个任务 ID */
  readonly nextTaskId?: string;
  readonly eventType: 'task.ready';
  readonly payloadSchemaVersion: 1;
  readonly payload: {
    readonly taskId: string;
    readonly runId: string;
    readonly tenantKey: string;
  };
  readonly destination: string;
  readonly dedupKey: string;
  readonly queueName?: string;
  readonly status: 'pending' | 'leased' | 'dispatched' | 'dead_letter';
  readonly availableAt: Date;
  readonly attempts: number;
  readonly maxAttempts?: number;
  readonly lastError?: unknown;
  readonly leaseOwner?: string;
  readonly leaseToken?: string;
  readonly leaseExpiresAt?: Date;
  readonly createdAt: Date;
  readonly dispatchedAt?: Date;
}

export interface ClaimOutboxCommand {
  readonly workerId: string;
  readonly destination?: string;
  readonly limit: number;
  readonly now: Date;
  readonly leaseExpiresAt: Date;
}

export interface MarkOutboxDispatchedCommand {
  readonly eventId: string;
  readonly leaseToken: string;
  readonly now: Date;
}

export interface MarkOutboxFailedCommand {
  readonly eventId: string;
  readonly leaseToken: string;
  readonly error: { readonly message: string };
  readonly retryAt: Date;
  readonly maxAttempts: number;
  readonly now: Date;
}

export interface ResumeTaskCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly resumeToken: string;
  readonly input?: Readonly<Record<string, unknown>>;
  readonly now: Date;
}

export interface CancelTaskCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly now: Date;
}

export interface CancelRunCommand {
  readonly commandId: string;
  readonly runId: string;
  readonly now: Date;
}

export interface CompletePollingTaskCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly output: unknown;
  readonly now: Date;
}

export interface FailPollingTaskCommand {
  readonly commandId: string;
  readonly taskId: string;
  readonly error: { readonly message: string };
  readonly now: Date;
}

export interface CreateChildRunCommand {
  readonly runId: string;
  readonly tenantKey: string;
  readonly name?: string;
  readonly type?: string;
  readonly parentTaskId: string;
  readonly parentRunId?: string;
  readonly rootRunId?: string;
  readonly resourceRef?: string;
  readonly definition: WorkflowDefinition;
  readonly input: Readonly<Record<string, unknown>>;
  readonly context: Readonly<Record<string, unknown>>;
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly tasks: readonly MaterializedTask[];
  readonly now: Date;
}

export interface AppendTasksCommand {
  readonly commandId: string;
  readonly runId: string;
  readonly coordinatorTaskId: string;
  readonly tasks: readonly MaterializedTask[];
  readonly now: Date;
}

export interface ListRunsFilter {
  readonly tenantKey?: string;
  readonly status?: WorkflowRunStatus;
  readonly correlationKey?: string;
  readonly resourceRef?: string;
  readonly parentRunId?: string;
  readonly parentTaskId?: string;
  readonly rootRunId?: string;
  readonly type?: string;
  readonly limit?: number;
  readonly offset?: number;
}

export type TaskInteractionStatus = 'open' | 'submitted' | 'expired' | 'cancelled';

export type TaskInteractionKind = 'input' | 'review' | 'approval';

export interface TaskInteraction {
  readonly id: string;
  readonly tenantKey: string;
  readonly runId: string;
  readonly taskId: string;
  /** 交互类型 */
  readonly kind: TaskInteractionKind;
  /** JSON Schema 定义（输入 / 审核的表单结构） */
  readonly schema?: Readonly<Record<string, unknown>>;
  /** UI 渲染提示 */
  readonly uiHints?: Readonly<Record<string, unknown>>;
  /** 草稿 payload（用户编辑中，未提交） */
  readonly draftPayload?: unknown;
  /** 已提交的 payload */
  readonly submittedPayload?: unknown;
  /** 生命周期状态 */
  readonly status: TaskInteractionStatus;
  /** 续期 / 提交凭证 */
  readonly token?: string;
  readonly expireAt?: Date;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

export interface CreateInteractionCommand {
  readonly interactionId: string;
  readonly tenantKey: string;
  readonly runId: string;
  readonly taskId: string;
  readonly kind: TaskInteractionKind;
  readonly schema?: Readonly<Record<string, unknown>>;
  readonly uiHints?: Readonly<Record<string, unknown>>;
  readonly draftPayload?: unknown;
  readonly expireAt?: Date;
  readonly now: Date;
}

export interface SubmitInteractionCommand {
  readonly interactionId: string;
  readonly taskId: string;
  readonly token: string;
  readonly payload: unknown;
  readonly now: Date;
}

export interface WorkflowStateStore {
  readonly protocolVersion: 1;
  getCapabilities(): Promise<StoreCapabilities>;
  createRun(command: CreateRunCommand): Promise<WorkflowRun>;
  claimTask(command: ClaimTaskCommand): Promise<ClaimedTask | null>;
  heartbeatTask(command: HeartbeatTaskCommand): Promise<TaskRun>;
  commitTaskSuccess(
    command: CommitTaskSuccessCommand,
  ): Promise<TaskCommitResult>;
  commitTaskFailure(
    command: CommitTaskFailureCommand,
  ): Promise<TaskCommitResult>;
  /**
   * Optional. Required to run `@morfact/core/loop`.
   * Commits this round's checkpoint/progress, re-queues the same task,
   * and must not unlock downstream nodes.
   */
  commitTaskContinue?(
    command: CommitTaskContinueCommand,
  ): Promise<TaskCommitResult>;
  suspendTask(command: SuspendTaskCommand): Promise<TaskCommitResult>;
  resumeTask(command: ResumeTaskCommand): Promise<TaskRun>;
  cancelTask(command: CancelTaskCommand): Promise<TaskRun>;
  cancelRun(command: CancelRunCommand): Promise<WorkflowRun>;
  completePollingTask(command: CompletePollingTaskCommand): Promise<TaskCommitResult>;
  failPollingTask(command: FailPollingTaskCommand): Promise<TaskCommitResult>;
  createChildRun(command: CreateChildRunCommand): Promise<WorkflowRun>;
  /**
   * Optional. Required to run `@morfact/core/coordinator`.
   * Existing protocol-v1 stores may omit this; the engine throws if a
   * coordinator hook needs it and the method is missing.
   */
  appendTasks?(command: AppendTasksCommand): Promise<{ createdCount: number }>;
  claimOutbox(command: ClaimOutboxCommand): Promise<readonly OutboxEvent[]>;
  markOutboxDispatched(command: MarkOutboxDispatchedCommand): Promise<void>;
  markOutboxFailed(command: MarkOutboxFailedCommand): Promise<void>;
  getTask(taskId: string): Promise<TaskRun | null>;
  getRun(runId: string): Promise<WorkflowRun | null>;
  listTasks(runId: string): Promise<readonly TaskRun[]>;
  listRuns(filter?: ListRunsFilter): Promise<readonly WorkflowRun[]>;
  /** 创建人机交互记录 */
  createInteraction(command: CreateInteractionCommand): Promise<TaskInteraction>;
  /** 用户提交交互 payload */
  submitInteraction(command: SubmitInteractionCommand): Promise<TaskInteraction>;
  /** 查询交互记录 */
  getInteraction(interactionId: string): Promise<TaskInteraction | null>;
  /** 列出某 task 的所有交互 */
  listInteractions(taskId: string): Promise<readonly TaskInteraction[]>;
}

export interface StartRunCommand {
  readonly workflow: string;
  readonly tenantKey: string;
  readonly name?: string;
  readonly resourceRef?: string;
  readonly correlationKey?: string;
  readonly input?: Readonly<Record<string, unknown>>;
  readonly context?: Readonly<Record<string, unknown>>;
}

export interface ExecuteNextTaskCommand {
  readonly workerId: string;
  readonly taskId?: string;
  readonly leaseDurationMs?: number;
  readonly heartbeatIntervalMs?: number;
  readonly signal?: AbortSignal;
}

export interface ExecuteTaskCommand extends ExecuteNextTaskCommand {
  readonly taskId: string;
}

export interface ExecuteTaskResult {
  readonly taskId: string;
  readonly runId: string;
  readonly status: 'succeeded' | 'waiting' | 'continue';
}

export interface EngineOptions {
  readonly store: WorkflowStateStore;
  readonly plugins?: readonly WorkflowPlugin[];
  readonly services?: unknown;
  readonly now?: () => Date;
  readonly idGenerator?: () => string;
  /**
   * In-memory pipeline_id → child template mapping.
   * Required when YAML spawn nodes use `pipelineId` instead of an explicit
   * `childTemplateName`. Hosts must load/parse the registry themselves —
   * the engine never reads `process.cwd()`.
   */
  readonly childTemplateRegistry?: ChildTemplateRegistryConfig;
}

export interface Engine {
  readonly hooks: ReadonlyMap<string, HookDefinition>;
  readonly workflows: ReadonlyMap<string, WorkflowDefinition>;
  validate(): Promise<void>;
  startRun(command: StartRunCommand): Promise<WorkflowRun>;
  executeTask(command: ExecuteTaskCommand): Promise<ExecuteTaskResult | null>;
  executeNextTask(
    command: ExecuteNextTaskCommand,
  ): Promise<ExecuteTaskResult | null>;
  resumeTask(
    taskId: string,
    resumeToken: string,
    input?: Readonly<Record<string, unknown>>,
  ): Promise<TaskRun>;
  cancelTask(taskId: string): Promise<TaskRun>;
  cancelRun(runId: string): Promise<WorkflowRun>;
  completePollingTask(
    taskId: string,
    output: unknown,
  ): Promise<TaskCommitResult>;
  failPollingTask(
    taskId: string,
    error: { readonly message: string },
  ): Promise<TaskCommitResult>;
  spawnChildRun(command: {
    readonly parentTaskId: string;
    readonly workflow: string;
    readonly tenantKey: string;
    readonly resourceRef?: string;
    readonly input?: Readonly<Record<string, unknown>>;
    readonly context?: Readonly<Record<string, unknown>>;
  }): Promise<WorkflowRun>;
  listRuns(filter?: ListRunsFilter): Promise<readonly WorkflowRun[]>;
  getTask(taskId: string): Promise<TaskRun | null>;
  getRun(runId: string): Promise<WorkflowRun | null>;
  listTasks(runId: string): Promise<readonly TaskRun[]>;
}
