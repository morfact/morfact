// ─── Flow DSL v1 types ───────────────────────────────────────────────────
// Declarative YAML workflow definition types that compile to
// WorkflowDefinition + MaterializedTask[] for the morfact engine.

export type FlowNodeType =
  | 'business'
  | 'bridge'
  | 'coordinator'
  | 'gate'
  | 'group'
  | 'input'
  | 'merge'
  | 'plan'
  | 'router'
  | 'spawn'
  | 'join'
  | 'loop'
  | 'completed';

/** Allowed inner task types inside a group node. */
export type GroupInnerNodeType =
  | 'business'
  | 'bridge'
  | 'plan'
  | 'input'
  | 'merge'
  | 'completed';

export interface GroupInnerTask {
  id: string;
  type: GroupInnerNodeType;
  name?: string;
  queue?: string;
  maxAttempts?: number;
  inputTemplate?: Record<string, unknown>;
  hooks?: FlowNodeDefinition['hooks'];
  meta?: Record<string, unknown>;
}

export interface GroupConfig {
  queue?: string;
  strategy?: 'parallel' | 'sequential';
  tasks: GroupInnerTask[];
  edges?: FlowEdge[];
  entries?: string[];
  exits?: string[];
}

/** Group definition embedded on coordinator at compile time. */
export interface EmbeddedGroupSpec extends GroupConfig {
  id: string;
}

export interface RetryPolicy {
  maxAttempts?: number;
  backoff?: {
    baseSeconds?: number;
    factor?: number;
    maxMinutes?: number;
  };
}

export interface InteractionConfig {
  kind: 'input' | 'review' | 'approval';
  schema?: {
    required?: string[];
    properties?: Record<string, unknown>;
  };
  uiHints?: Record<string, unknown>;
}

export interface RouterConfig {
  mode?: 'exclusive' | 'inclusive';
  decisionFrom?: string;
  noMatch?: string;
}

export interface JoinConfig {
  mode?: 'exclusive' | 'all' | 'inclusive';
  waitFor?: string[];
  pairedRouter?: string;
}

export interface SpawnConfig {
  pipelineId?: string;
  childTemplateName?: string;
  childTemplateVersion?: string;
  spawnId?: string;
  maxDepth?: number;
  promote?: string[] | Record<string, string>;
  onChildFailure?: 'fail_parent' | 'ignore' | 'wait';
  childInputTemplate?: Record<string, unknown>;
  jobName?: string;
  skipUnlessTruthyParam?: string;
}

export interface CoordinatorConfig {
  /** Reference a type=group node id in the same flow. */
  groupRef?: string;
  /**
   * @deprecated Prefer groupRef + type=group nodes. Coexists during migration.
   */
  taskGroupTemplate?: string;
  groupCount: string | number;
  groupStrategy?: 'sequential' | 'parallel';
  when?: string;
  collectSubtaskOutputKeys?: string[];
  /**
   * Copied from the referenced group or taskGroupTemplates at compile time so
   * the runtime does not need the original YAML / FlowRegistry.
   */
  groupSpec?: EmbeddedGroupSpec;
  /**
   * @deprecated Use groupSpec. Kept for backward compatibility.
   */
  taskGroupTemplateSpec?: { tasks: FlowSubtaskTemplate[] };
}

export interface LoopConfig {
  /** Hook name invoked once per iteration. */
  body: string;
  /** Budget: maximum body executions. Not a correctness signal. */
  maxIterations: number;
  /**
   * When the budget is exhausted without `halt: true`.
   * Default `fail` is a fuse against accidental infinite loops.
   * Precision / agent flows that may legally stop early should set `succeed`.
   */
  onMaxIterations?: 'fail' | 'succeed';
}

export interface ExpandConfig {
  foreach: string;
  as: string;
  keyField?: string;
  alignWith?: string;
}

export interface FlowNodeDefinition {
  id: string;
  type: FlowNodeType;
  name?: string;
  queue?: string;
  maxAttempts?: number;
  when?: string;
  inputTemplate?: Record<string, unknown>;
  hooks?: {
    executor?: { use: string } | string;
    validators?: Array<{ use: string } | string>;
    [key: string]: unknown;
  };
  promoteOutput?: string[] | Record<string, string>;
  interaction?: InteractionConfig;
  expand?: ExpandConfig;
  router?: RouterConfig;
  join?: JoinConfig;
  spawn?: SpawnConfig;
  coordinator?: CoordinatorConfig;
  group?: GroupConfig;
  loop?: LoopConfig;
  tags?: string[];
  meta?: Record<string, unknown>;
}

export interface FlowEdge {
  from: string;
  to: string;
  when?: string;
}

export interface FlowPolicy {
  defaultQueue?: string;
  retry?: RetryPolicy;
  defaultMaxAttempts?: number;
}

export interface FlowSubtaskTemplate {
  id: string;
  type: string;
  name?: string;
  queue?: string;
  inputTemplate?: Record<string, unknown>;
  hooks?: FlowNodeDefinition['hooks'];
  interaction?: InteractionConfig;
  meta?: Record<string, unknown>;
}

export interface FlowSpec {
  policies?: FlowPolicy;
  variables?: Record<string, unknown>;
  nodes: FlowNodeDefinition[];
  edges?: FlowEdge[];
  taskGroupTemplates?: Record<string, { tasks: FlowSubtaskTemplate[] }>;
}

export interface FlowMetadata {
  name: string;
  version: string;
  description?: string;
  tags?: string[];
  changelog?: unknown;
  type?: string;
}

export interface FlowDefinition {
  apiVersion: string;
  kind: 'Flow';
  metadata: FlowMetadata;
  spec: FlowSpec;
}
