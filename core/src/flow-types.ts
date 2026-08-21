// ─── Flow DSL v1 types ───────────────────────────────────────────────────
// Declarative YAML workflow definition types that compile to
// WorkflowDefinition + MaterializedTask[] for the morfact engine.

export type FlowNodeType =
  | 'business'
  | 'bridge'
  | 'coordinator'
  | 'gate'
  | 'input'
  | 'merge'
  | 'plan'
  | 'router'
  | 'spawn'
  | 'join'
  | 'completed';

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
  taskGroupTemplate: string;
  groupCount: string | number;
  groupStrategy?: 'sequential' | 'parallel';
  when?: string;
  collectSubtaskOutputKeys?: string[];
  /**
   * Copied from FlowSpec.taskGroupTemplates at compile time so the runtime
   * does not need the original YAML / FlowRegistry.
   */
  taskGroupTemplateSpec?: { tasks: FlowSubtaskTemplate[] };
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
