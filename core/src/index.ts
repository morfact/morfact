export { defineHook, definePlugin, defineWorkflow } from './definitions.js';
export { createEngine } from './engine.js';
export { createOutboxDispatcher } from './outbox-dispatcher.js';
export {
  materializeWorkflow,
  validateWorkflowDefinition,
} from './compiler.js';
export {
  resolveWorkflowInput,
  resolveWorkflowTaskInput,
} from './input-bindings.js';
export {
  HookRegistrationError,
  StoreProtocolError,
  WorkflowError,
  WorkflowValidationError,
} from './errors.js';

// ─── Flow DSL ─────────────────────────────────────────────────────────────

export {
  validateFlowDefinition,
  compileFlow,
  loadFlowFromYaml,
  loadFlowFromFile,
  loadAndCompileFlow,
  loadAndCompileFlowFromFile,
  compileCoordinatorSubtasks,
} from './flow-compiler.js';

export {
  renderTemplate,
  evaluateWhen,
  parseForeachExpression,
} from './template-engine.js';

export type { RenderContext } from './template-engine.js';

export {
  resolveChildTemplatePin,
  resolveSpawnChildTemplate,
  conventionChildTemplateName,
  resetPipelineRegistryCache,
  parseChildTemplateRegistryYaml,
} from './child-template-registry.js';

export type {
  ChildTemplatePin,
  ResolveSpawnChildTemplateSource,
  ChildTemplateRegistryOptions,
  ChildTemplateRegistryConfig,
} from './child-template-registry.js';

export { FlowRegistry } from './template-registry.js';
export type { FlowRegistryOptions } from './template-registry.js';

// ─── Flow DSL types ───────────────────────────────────────────────────────

export type {
  FlowNodeType,
  RetryPolicy,
  InteractionConfig,
  RouterConfig,
  JoinConfig,
  SpawnConfig,
  CoordinatorConfig,
  ExpandConfig,
  FlowNodeDefinition,
  FlowEdge,
  FlowPolicy,
  FlowSubtaskTemplate,
  FlowSpec,
  FlowMetadata,
  FlowDefinition,
} from './flow-types.js';

// ─── Built-in hooks ───────────────────────────────────────────────────────

export { joinHook } from './hooks/join.js';
export type { JoinOutput } from './hooks/join.js';

export { gateHook } from './hooks/gate.js';

export { completedHook } from './hooks/completed.js';
export type { CompletedOutput } from './hooks/completed.js';

export { coordinatorHook } from './hooks/coordinator.js';
export type {
  CoordinatorServices,
  CoordinatorSubtaskCreateParams,
  CoordinatorTaskDescriptor,
  CoordinatorOutput,
} from './hooks/coordinator.js';

export { spawnHook, createSpawnHook } from './hooks/spawn.js';
export type { SpawnServices, SpawnOutput } from './hooks/spawn.js';

// ─── Core contracts ───────────────────────────────────────────────────────

export type {
  CancelRunCommand,
  CancelTaskCommand,
  ClaimedTask,
  ClaimTaskCommand,
  ClaimOutboxCommand,
  CommitTaskFailureCommand,
  CommitTaskSuccessCommand,
  CompletePollingTaskCommand,
  CreateChildRunCommand,
  AppendTasksCommand,
  CreateInteractionCommand,
  CreateRunCommand,
  Engine,
  EngineOptions,
  ExecuteNextTaskCommand,
  ExecuteTaskCommand,
  ExecuteTaskResult,
  FailPollingTaskCommand,
  HeartbeatTaskCommand,
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
  ListRunsFilter,
  MaterializedTask,
  MarkOutboxDispatchedCommand,
  MarkOutboxFailedCommand,
  OutboxEvent,
  PluginManifest,
  ResumeTaskCommand,
  StartRunCommand,
  StoreCapabilities,
  SubmitInteractionCommand,
  SuspendTaskCommand,
  TaskCommitResult,
  TaskDep,
  TaskInteraction,
  TaskInteractionKind,
  TaskInteractionStatus,
  TaskRun,
  TaskRunStatus,
  WorkflowDefinition,
  WorkflowInputValue,
  WorkflowNodeDefinition,
  WorkflowPlugin,
  WorkflowRun,
  WorkflowRunStatus,
  WorkflowStateStore,
} from './contracts.js';
export type { WorkflowErrorCode } from './errors.js';
export type {
  DispatchOutboxCommand,
  DispatchOutboxResult,
  OutboxDispatcher,
  OutboxDispatcherOptions,
  OutboxPublisher,
} from './outbox-dispatcher.js';
