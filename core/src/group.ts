// group.ts — first-class group nodes: simple composite subgraphs that compile
// inline in the main DAG or fan out at runtime via coordinator.groupRef.

import type { FlowEdge, FlowSubtaskTemplate, GroupConfig, GroupInnerTask } from './flow-types.js';
import { WorkflowValidationError } from './errors.js';
import { renderTemplate, type RenderContext } from './template-engine.js';

export const FORBIDDEN_GROUP_INNER_TYPES = new Set([
  'coordinator',
  'spawn',
  'loop',
  'gate',
  'router',
  'join',
  'group',
]);

export interface GroupSpec {
  readonly id: string;
  readonly queue?: string;
  readonly strategy?: 'parallel' | 'sequential';
  readonly tasks: readonly GroupInnerTask[];
  readonly edges?: readonly FlowEdge[];
  readonly entries?: readonly string[];
  readonly exits?: readonly string[];
  /** Legacy taskGroupTemplates use `{taskId}[{i}]` keys at runtime. */
  readonly legacyLogicalKeys?: boolean;
}

export interface MaterializedGroupTask {
  logicalKey: string;
  innerTaskId: string;
  groupIndex?: number;
  hook: string;
  queue: string;
  input: Record<string, unknown>;
  maxAttempts: number;
  dependsOnLogical: string[];
  meta: Record<string, unknown>;
}

export function resolveGroupConfig(
  nodeId: string,
  group?: GroupConfig,
): GroupConfig {
  if (!group?.tasks?.length) {
    throw new WorkflowValidationError(
      `Group node '${nodeId}': group.tasks is required and must be non-empty`,
    );
  }
  return group;
}

export function groupSpecFromNode(nodeId: string, group: GroupConfig): GroupSpec {
  const cfg = resolveGroupConfig(nodeId, group);
  return {
    id: nodeId,
    ...(cfg.queue !== undefined ? { queue: cfg.queue } : {}),
    ...(cfg.strategy !== undefined ? { strategy: cfg.strategy } : {}),
    tasks: cfg.tasks,
    ...(cfg.edges !== undefined ? { edges: cfg.edges } : {}),
    ...(cfg.entries !== undefined ? { entries: cfg.entries } : {}),
    ...(cfg.exits !== undefined ? { exits: cfg.exits } : {}),
  };
}

/** Synthesize a GroupSpec from legacy taskGroupTemplates (flat parallel list). */
export function groupSpecFromTaskGroupTemplate(
  templateName: string,
  template: { tasks: FlowSubtaskTemplate[] },
): GroupSpec {
  const tasks: GroupInnerTask[] = template.tasks.map((t) => ({
    id: t.id,
    type: t.type as GroupInnerTask['type'],
    ...(t.name !== undefined ? { name: t.name } : {}),
    ...(t.queue !== undefined ? { queue: t.queue } : {}),
    ...(t.inputTemplate !== undefined ? { inputTemplate: t.inputTemplate } : {}),
    ...(t.hooks !== undefined ? { hooks: t.hooks } : {}),
    ...(t.meta !== undefined ? { meta: t.meta } : {}),
  }));
  return {
    id: templateName,
    strategy: 'parallel',
    tasks,
    legacyLogicalKeys: true,
  };
}

export function validateGroupSpec(
  spec: GroupSpec,
  availableHooks: ReadonlySet<string>,
  label: string,
): void {
  const taskIds = new Set<string>();
  for (const task of spec.tasks) {
    if (taskIds.has(task.id)) {
      throw new WorkflowValidationError(
        `${label}: duplicate inner task id '${task.id}'`,
      );
    }
    taskIds.add(task.id);

    if (FORBIDDEN_GROUP_INNER_TYPES.has(task.type)) {
      throw new WorkflowValidationError(
        `${label}: inner task '${task.id}' uses forbidden type '${task.type}'`,
      );
    }

    if (task.type === 'completed') continue;

    const hook =
      typeof task.hooks?.executor === 'string'
        ? task.hooks.executor
        : task.hooks?.executor?.use;
    if (!hook) {
      throw new WorkflowValidationError(
        `${label}: inner task '${task.id}' requires hooks.executor`,
      );
    }
    if (!availableHooks.has(hook)) {
      throw new WorkflowValidationError(
        `${label}: inner task '${task.id}' references missing hook: ${hook}`,
      );
    }
  }

  const edges = spec.edges ?? [];
  for (const edge of edges) {
    if (!taskIds.has(edge.from)) {
      throw new WorkflowValidationError(
        `${label}: edge references unknown inner task '${edge.from}'`,
      );
    }
    if (!taskIds.has(edge.to)) {
      throw new WorkflowValidationError(
        `${label}: edge references unknown inner task '${edge.to}'`,
      );
    }
  }

  if (spec.entries) {
    for (const id of spec.entries) {
      if (!taskIds.has(id)) {
        throw new WorkflowValidationError(
          `${label}: entries references unknown inner task '${id}'`,
        );
      }
    }
  }
  if (spec.exits) {
    for (const id of spec.exits) {
      if (!taskIds.has(id)) {
        throw new WorkflowValidationError(
          `${label}: exits references unknown inner task '${id}'`,
        );
      }
    }
  }

  detectInnerCycle(spec, label);
}

function detectInnerCycle(spec: GroupSpec, label: string): void {
  const edges = buildInternalEdges(spec);
  const adj = new Map<string, string[]>();
  for (const t of spec.tasks) adj.set(t.id, []);
  for (const [from, to] of edges) {
    adj.get(from)?.push(to);
  }

  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (id: string): void => {
    if (visiting.has(id)) {
      throw new WorkflowValidationError(
        `${label}: inner dependency cycle at task '${id}'`,
      );
    }
    if (visited.has(id)) return;
    visiting.add(id);
    for (const next of adj.get(id) ?? []) visit(next);
    visiting.delete(id);
    visited.add(id);
  };
  for (const t of spec.tasks) visit(t.id);
}

function buildInternalEdges(spec: GroupSpec): Array<[string, string]> {
  const result: Array<[string, string]> = [];
  if (spec.edges?.length) {
    for (const e of spec.edges) result.push([e.from, e.to]);
    return result;
  }
  if (spec.strategy === 'sequential') {
    for (let i = 1; i < spec.tasks.length; i++) {
      const prev = spec.tasks[i - 1];
      const curr = spec.tasks[i];
      if (prev && curr) result.push([prev.id, curr.id]);
    }
  }
  return result;
}

export function resolveGroupEntryTaskIds(spec: GroupSpec): string[] {
  if (spec.entries?.length) return [...spec.entries];
  const hasInbound = new Set<string>();
  for (const [, to] of buildInternalEdges(spec)) hasInbound.add(to);
  return spec.tasks.filter((t) => !hasInbound.has(t.id)).map((t) => t.id);
}

export function resolveGroupExitTaskIds(spec: GroupSpec): string[] {
  if (spec.exits?.length) return [...spec.exits];
  const hasOutbound = new Set<string>();
  for (const [from] of buildInternalEdges(spec)) hasOutbound.add(from);
  return spec.tasks.filter((t) => !hasOutbound.has(t.id)).map((t) => t.id);
}

function logicalKeyFor(
  spec: GroupSpec,
  innerTaskId: string,
  groupIndex?: number,
): string {
  if (groupIndex === undefined) return `${spec.id}.${innerTaskId}`;
  if (spec.legacyLogicalKeys) return `${innerTaskId}[${groupIndex}]`;
  return `${spec.id}.${innerTaskId}[${groupIndex}]`;
}

function resolveInnerHook(task: GroupInnerTask): string {
  if (task.type === 'completed') return '@morfact/core/completed';
  const hook =
    typeof task.hooks?.executor === 'string'
      ? task.hooks.executor
      : task.hooks?.executor?.use;
  if (!hook) {
    throw new WorkflowValidationError(
      `Group inner task '${task.id}' requires hooks.executor`,
    );
  }
  return hook;
}

export interface MaterializeGroupOptions {
  spec: GroupSpec;
  renderCtx: RenderContext;
  defaultQueue?: string;
  defaultMaxAttempts: number;
  groupIndex?: number;
}

export function materializeGroupInstance(
  options: MaterializeGroupOptions,
): MaterializedGroupTask[] {
  const { spec, renderCtx, defaultMaxAttempts, groupIndex } = options;
  const defaultQueue = options.defaultQueue ?? 'default';
  const groupCtx: RenderContext =
    groupIndex === undefined
      ? renderCtx
      : { ...renderCtx, groupIndex, i: groupIndex };

  const internalEdges = buildInternalEdges(spec);
  const inbound = new Map<string, string[]>();
  for (const t of spec.tasks) inbound.set(t.id, []);
  for (const [from, to] of internalEdges) {
    inbound.get(to)?.push(from);
  }

  const materialized: MaterializedGroupTask[] = [];
  const keyByInnerId = new Map<string, string>();

  for (const task of spec.tasks) {
    const logicalKey = logicalKeyFor(spec, task.id, groupIndex);
    keyByInnerId.set(task.id, logicalKey);

    const hook = resolveInnerHook(task);
    const queue = task.queue ?? spec.queue ?? defaultQueue;

    const input: Record<string, unknown> = {};
    if (renderCtx.params) {
      Object.assign(input, renderCtx.params);
    }
    if (task.inputTemplate) {
      const rendered = renderTemplate(task.inputTemplate, groupCtx);
      if (rendered && typeof rendered === 'object') {
        Object.assign(input, rendered);
      }
    }

    const name =
      task.name !== undefined
        ? (() => {
            const rendered = renderTemplate(task.name!, groupCtx);
            return typeof rendered === 'string' && rendered.length > 0
              ? rendered
              : task.id;
          })()
        : task.id;

    materialized.push({
      logicalKey,
      innerTaskId: task.id,
      ...(groupIndex !== undefined ? { groupIndex } : {}),
      hook,
      queue,
      input,
      maxAttempts: task.maxAttempts ?? defaultMaxAttempts,
      dependsOnLogical: [],
      meta: {
        baseNodeType: task.type,
        baseNodeId: task.id,
        groupId: spec.id,
        logicalKey,
        name,
        ...(task.meta ?? {}),
        ...(groupIndex !== undefined ? { groupIndex } : {}),
        coordinatorSubtask: groupIndex !== undefined,
      },
    });
  }

  for (const m of materialized) {
    const inboundIds = inbound.get(m.innerTaskId) ?? [];
    m.dependsOnLogical = inboundIds
      .map((id) => keyByInnerId.get(id))
      .filter((x): x is string => x !== undefined);
  }

  return materialized;
}

export function groupSpecToCoordinatorEmbed(spec: GroupSpec): GroupSpec {
  return spec;
}
