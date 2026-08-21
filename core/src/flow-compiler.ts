// flow-compiler.ts — compiles FlowDefinition (YAML DSL) to
// WorkflowDefinition + MaterializedTask[] for the morfact engine.

import fs from 'node:fs';
import yaml from 'js-yaml';
import type {
  FlowDefinition,
  FlowEdge,
  FlowNodeDefinition,
  FlowSubtaskTemplate,
} from './flow-types.js';
import type {
  MaterializedTask,
  WorkflowDefinition,
  WorkflowNodeDefinition,
  WorkflowInputValue,
} from './contracts.js';
import { WorkflowValidationError } from './errors.js';
import {
  renderTemplate,
  evaluateWhen,
  parseForeachExpression,
  type RenderContext,
} from './template-engine.js';

// ─── Public API ────────────────────────────────────────────────────────────

export function validateFlowDefinition(
  flow: FlowDefinition,
  availableHooks: ReadonlySet<string>,
): void {
  if (!flow?.apiVersion || !flow?.kind || !flow?.metadata || !flow?.spec) {
    throw new WorkflowValidationError('Invalid Flow DSL format');
  }
  if (flow.kind !== 'Flow') {
    throw new WorkflowValidationError(
      `Expected kind=Flow, got ${flow.kind}`,
    );
  }
  if (!flow.spec.nodes || !Array.isArray(flow.spec.nodes)) {
    throw new WorkflowValidationError('Flow spec.nodes is required and must be an array');
  }

  const nodes = flow.spec.nodes;
  const edges = flow.spec.edges ?? [];
  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const outEdges = (id: string) => edges.filter((e) => e.from === id);
  const inEdges = (id: string) => edges.filter((e) => e.to === id);

  // Check duplicates
  const seen = new Set<string>();
  for (const node of nodes) {
    if (seen.has(node.id)) {
      throw new WorkflowValidationError(
        `Flow ${flow.metadata.name}@${flow.metadata.version} contains duplicate node: ${node.id}`,
      );
    }
    seen.add(node.id);
  }

  // Check hooks
  for (const node of nodes) {
    if (node.type === 'completed') continue;
    if (node.type === 'join') continue; // join is a built-in passthrough
    if (node.type === 'loop') {
      if (!availableHooks.has('@morfact/core/loop')) {
        throw new WorkflowValidationError(
          `Flow node ${node.id} references missing hook: @morfact/core/loop`,
        );
      }
      if (!node.loop?.body) {
        throw new WorkflowValidationError(
          `Flow node ${node.id}: loop.body is required`,
        );
      }
      if (!availableHooks.has(node.loop.body)) {
        throw new WorkflowValidationError(
          `Flow node ${node.id} references missing loop body hook: ${node.loop.body}`,
        );
      }
      if (
        typeof node.loop.maxIterations !== 'number' ||
        !Number.isFinite(node.loop.maxIterations) ||
        node.loop.maxIterations < 1
      ) {
        throw new WorkflowValidationError(
          `Flow node ${node.id}: loop.maxIterations must be >= 1`,
        );
      }
      continue;
    }
    const executor = typeof node.hooks?.executor === 'string'
      ? node.hooks.executor
      : node.hooks?.executor?.use;
    if (!executor) {
      throw new WorkflowValidationError(
        `Flow node ${node.id}: hooks.executor is required`,
      );
    }
    if (!availableHooks.has(executor)) {
      throw new WorkflowValidationError(
        `Flow node ${node.id} references missing hook: ${executor}`,
      );
    }
  }

  // Validate edge targets
  for (const edge of edges) {
    if (!nodeById.has(edge.from)) {
      throw new WorkflowValidationError(
        `Flow edge references missing node: ${edge.from}`,
      );
    }
    if (!nodeById.has(edge.to)) {
      throw new WorkflowValidationError(
        `Flow edge references missing node: ${edge.to}`,
      );
    }
  }

  // Router validation
  for (const node of nodes) {
    if (node.type === 'router') {
      const outs = outEdges(node.id);
      if (outs.length < 2) {
        throw new WorkflowValidationError(
          `Router '${node.id}' must have >= 2 outgoing edges (got ${outs.length})`,
        );
      }
      const missingWhen = outs.filter(
        (e) => !e.when || String(e.when).trim() === '',
      );
      if (missingWhen.length > 0) {
        throw new WorkflowValidationError(
          `Router '${node.id}' has outgoing edges without 'when': ${missingWhen
            .map((e) => `${e.from}->${e.to}`)
            .join(', ')}`,
        );
      }
    }
  }

  // Join validation
  for (const node of nodes) {
    if (node.type === 'join') {
      const ins = inEdges(node.id);
      const withWhen = ins.filter(
        (e) => e.when && String(e.when).trim() !== '',
      );
      if (withWhen.length > 0) {
        throw new WorkflowValidationError(
          `Join '${node.id}' incoming edges must not carry 'when'`,
        );
      }
    }
  }

  // Cycle detection
  const adj = new Map<string, string[]>();
  for (const n of nodes) adj.set(n.id, []);
  for (const e of edges) {
    const list = adj.get(e.from);
    if (list) list.push(e.to);
  }
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (id: string): void => {
    if (visiting.has(id)) {
      throw new WorkflowValidationError(
        `Flow contains a dependency cycle at node: ${id}`,
      );
    }
    if (visited.has(id)) return;
    visiting.add(id);
    for (const next of adj.get(id) ?? []) visit(next);
    visiting.delete(id);
    visited.add(id);
  };
  for (const n of nodes) visit(n.id);
}

// ─── expand + compile ─────────────────────────────────────────────────────

interface ExpandedTask {
  logicalKey: string;
  baseNodeId: string;
  hook: string;
  queue: string;
  input: Record<string, unknown>;
  maxAttempts: number;
  dependsOnLogical: string[];
  meta: Record<string, unknown>;
}

type CompileResult = {
  definition: WorkflowDefinition;
  tasks: readonly MaterializedTask[];
};

export function compileFlow(
  flow: FlowDefinition,
  availableHooks: ReadonlySet<string>,
  createId: () => string,
  params?: Record<string, unknown>,
): CompileResult {
  validateFlowDefinition(flow, availableHooks);

  const policies = flow.spec.policies ?? {};
  const defaultQueue = policies.defaultQueue;
  const defaultMaxAttempts = policies.retry?.maxAttempts ?? policies.defaultMaxAttempts ?? 5;
  const edges = flow.spec.edges ?? [];
  const variables = flow.spec.variables ?? {};

  const renderCtx: RenderContext = {
    params: params ?? {},
    flow: { variables },
    vars: variables,
    tasks: {},
  };

  // ── Step 1: Expand nodes ──
  const nodeExpandedKeys = new Map<string, string[]>();
  const expanded: ExpandedTask[] = [];

  for (const node of flow.spec.nodes) {
    // Check when condition
    if (node.when) {
      if (!evaluateWhen(node.when, renderCtx)) continue;
    }

    const instances = expandNode(flow, node, renderCtx);
    const keys = instances.map((inst) => inst.logicalKey);
    nodeExpandedKeys.set(node.id, keys);
    expanded.push(...instances);
  }

  // ── Step 2: Build dependsOn from edges ──
  const expandedMap = new Map<string, ExpandedTask>();
  for (const t of expanded) expandedMap.set(t.logicalKey, t);

  for (const edge of edges) {
    const fromKeys = nodeExpandedKeys.get(edge.from) ?? [];
    const toKeys = nodeExpandedKeys.get(edge.to) ?? [];

    if (fromKeys.length === 0 || toKeys.length === 0) continue;

    // 1:1 → simple dep
    if (fromKeys.length === 1 && toKeys.length === 1) {
      const fromKey = fromKeys[0];
      const toKey = toKeys[0];
      if (fromKey !== undefined && toKey !== undefined) {
        const toTask = expandedMap.get(toKey);
        if (toTask) toTask.dependsOnLogical.push(fromKey);
      }
      continue;
    }

    // 1:N → all N depend on the 1
    if (fromKeys.length === 1 && toKeys.length > 1) {
      const fromKey = fromKeys[0];
      if (fromKey !== undefined) {
        for (const toKey of toKeys) {
          const toTask = expandedMap.get(toKey);
          if (toTask) toTask.dependsOnLogical.push(fromKey);
        }
      }
      continue;
    }

    // N:1 → the 1 depends on all N
    if (fromKeys.length > 1 && toKeys.length === 1) {
      const toKey = toKeys[0];
      if (toKey !== undefined) {
        const toTask = expandedMap.get(toKey);
        if (toTask) toTask.dependsOnLogical.push(...fromKeys);
      }
      continue;
    }

    // N:N — aligned if lengths match, otherwise full mesh
    if (fromKeys.length === toKeys.length) {
      for (let i = 0; i < fromKeys.length; i++) {
        const fromKey = fromKeys[i];
        const toKey = toKeys[i];
        if (fromKey !== undefined && toKey !== undefined) {
          const toTask = expandedMap.get(toKey);
          if (toTask) toTask.dependsOnLogical.push(fromKey);
        }
      }
    } else {
      for (const toKey of toKeys) {
        const toTask = expandedMap.get(toKey);
        if (toTask) toTask.dependsOnLogical.push(...fromKeys);
      }
    }
  }

  // ── Step 3: Build concrete WorkflowNodeDefinition + MaterializedTask[] ──
  const logicalToId = new Map<string, string>();
  for (const t of expanded) logicalToId.set(t.logicalKey, createId());

  const defTasks: MaterializedTask[] = [];
  const defNodes: WorkflowNodeDefinition[] = [];

  for (const t of expanded) {
    const id = logicalToId.get(t.logicalKey)!;
    const dependsOn = t.dependsOnLogical
      .map((lk) => logicalToId.get(lk))
      .filter((x): x is string => x !== undefined);

    const inputWithMeta: Record<string, WorkflowInputValue> = {
      ...(t.input as Record<string, WorkflowInputValue>),
      _meta: t.meta as WorkflowInputValue,
      _type: t.meta.baseNodeType as WorkflowInputValue,
    };

    defNodes.push({
      id,
      hook: t.hook,
      dependsOn,
      ...(t.queue !== undefined ? { queue: t.queue } : {}),
      input: inputWithMeta,
    });

    defTasks.push({
      id,
      logicalKey: t.logicalKey,
      hook: t.hook,
      input: inputWithMeta,
      dependsOn,
      maxAttempts: t.maxAttempts,
    });
  }

  return {
    definition: {
      schemaVersion: 1,
      name: flow.metadata.name,
      version: flow.metadata.version,
      nodes: defNodes,
    },
    tasks: defTasks,
  };
}

// ─── expandNode ───────────────────────────────────────────────────────────

function resolveHook(node: FlowNodeDefinition): string {
  if (node.type === 'join') return '@morfact/core/join';
  if (node.type === 'gate') return '@morfact/core/gate';
  if (node.type === 'completed') return '@morfact/core/completed';
  if (node.type === 'coordinator') return '@morfact/core/coordinator';
  if (node.type === 'spawn') return '@morfact/core/spawn';
  if (node.type === 'loop') return '@morfact/core/loop';
  if (typeof node.hooks?.executor === 'string') return node.hooks.executor;
  if (node.hooks?.executor?.use) return node.hooks.executor.use;
  throw new Error(`Node ${node.id}: hooks.executor is required`);
}

function expandNode(
  flow: FlowDefinition,
  node: FlowNodeDefinition,
  renderCtx: RenderContext,
): ExpandedTask[] {
  const policies = flow.spec.policies ?? {};
  const defaultQueue = policies.defaultQueue;
  const defaultMaxAttempts =
    policies.retry?.maxAttempts ?? policies.defaultMaxAttempts ?? 5;

  const hook = resolveHook(node);
  const queue = node.queue ?? defaultQueue ?? 'default';

  const coordinatorMeta = node.coordinator
    ? {
        ...node.coordinator,
        ...(flow.spec.taskGroupTemplates?.[node.coordinator.taskGroupTemplate]
          ? {
              taskGroupTemplateSpec:
                flow.spec.taskGroupTemplates[node.coordinator.taskGroupTemplate],
            }
          : {}),
      }
    : undefined;

  const baseMeta: Record<string, unknown> = {
    baseNodeType: node.type,
    baseNodeId: node.id,
    ...(node.hooks && { hooks: node.hooks }),
    ...(node.promoteOutput && { promoteOutput: node.promoteOutput }),
    ...(node.interaction && { interaction: node.interaction }),
    ...(node.router && { router: node.router }),
    ...(node.join && { join: node.join }),
    ...(node.spawn && { spawn: node.spawn }),
    ...(coordinatorMeta && { coordinator: coordinatorMeta }),
    ...(node.loop && { loop: node.loop }),
    ...(node.meta ?? {}),
    ...(node.inputTemplate && { inputTemplate: node.inputTemplate }),
  };

  const buildInput = (ctx: RenderContext): Record<string, unknown> => {
    const base: Record<string, unknown> = {};
    if (renderCtx.params) {
      Object.assign(base, renderCtx.params);
    }
    if (node.inputTemplate) {
      const rendered = renderTemplate(node.inputTemplate, ctx);
      if (rendered && typeof rendered === 'object') {
        Object.assign(base, rendered);
      }
    }
    return base;
  };

  const buildName = (ctx: RenderContext, fallback: string): string => {
    if (node.name) {
      const rendered = renderTemplate(node.name, ctx);
      return typeof rendered === 'string' && rendered.length > 0
        ? rendered
        : fallback;
    }
    return fallback;
  };

  // No expand → single instance
  if (!node.expand) {
    const input = buildInput(renderCtx);
    const name = buildName(renderCtx, node.id);
    return [
      {
        logicalKey: node.id,
        baseNodeId: node.id,
        hook,
        queue,
        input,
        maxAttempts: node.maxAttempts ?? defaultMaxAttempts,
        dependsOnLogical: [],
        meta: {
          ...baseMeta,
          logicalKey: node.id,
          name,
        },
      },
    ];
  }

  // Has expand → parse foreach
  const { items, isRange } = parseForeachExpression(
    node.expand.foreach,
    renderCtx,
  );
  const varName = node.expand.as;
  const keyField = node.expand.keyField;

  const instances: ExpandedTask[] = [];
  for (let idx = 0; idx < items.length; idx++) {
    const itemValue = items[idx];
    const ctx: RenderContext = {
      ...renderCtx,
      [varName]: itemValue,
      i: isRange ? (itemValue as number) : idx,
      item: isRange ? itemValue : itemValue,
    };

    const input = buildInput(ctx);
    const logicalKey = `${node.id}[${idx}]`;
    const name = buildName(ctx, logicalKey);

    let idempotencyKey: string;
    if (
      !isRange &&
      keyField &&
      itemValue &&
      typeof itemValue === 'object' &&
      itemValue !== null
    ) {
      const obj = itemValue as Record<string, unknown>;
      idempotencyKey = `${flow.metadata.name}:${node.id}:${obj[keyField]}`;
    } else {
      idempotencyKey = `${flow.metadata.name}:${node.id}:${idx}`;
    }

    instances.push({
      logicalKey,
      baseNodeId: node.id,
      hook,
      queue,
      input,
      maxAttempts: node.maxAttempts ?? defaultMaxAttempts,
      dependsOnLogical: [],
      meta: {
        ...baseMeta,
        logicalKey,
        name,
        index: idx,
        [varName]: itemValue,
        idempotencyKey,
        expandContext: {
          i: isRange ? (itemValue as number) : idx,
          item: isRange ? itemValue : itemValue,
          [varName]: itemValue,
          index: idx,
        },
      },
    });
  }

  return instances;
}

// ─── YAML loading ─────────────────────────────────────────────────────────

export function loadFlowFromYaml(yamlString: string): FlowDefinition {
  const obj = yaml.load(yamlString) as FlowDefinition;
  if (!obj?.apiVersion || !obj?.kind || obj.kind !== 'Flow') {
    throw new WorkflowValidationError(
      'YAML does not represent a valid Flow definition',
    );
  }
  return obj;
}

export function loadFlowFromFile(filePath: string): FlowDefinition {
  const raw = fs.readFileSync(filePath, 'utf8');
  return loadFlowFromYaml(raw);
}

export function loadAndCompileFlow(
  yamlString: string,
  availableHooks: ReadonlySet<string>,
  createId: () => string,
  params?: Record<string, unknown>,
): CompileResult {
  const flow = loadFlowFromYaml(yamlString);
  return compileFlow(flow, availableHooks, createId, params);
}

export function loadAndCompileFlowFromFile(
  filePath: string,
  availableHooks: ReadonlySet<string>,
  createId: () => string,
  params?: Record<string, unknown>,
): CompileResult {
  const flow = loadFlowFromFile(filePath);
  return compileFlow(flow, availableHooks, createId, params);
}

// ─── Coordinator subtask template compilation ─────────────────────────────

export function compileCoordinatorSubtasks(
  flow: FlowDefinition,
  coordinator: { taskGroupTemplate: string; groupCount: number; groupStrategy?: 'sequential' | 'parallel' },
  availableHooks: ReadonlySet<string>,
  createId: () => string,
  renderCtx: RenderContext,
): { tasks: MaterializedTask[]; groupCount: number } {
  const tmpl = flow.spec.taskGroupTemplates?.[coordinator.taskGroupTemplate];
  if (!tmpl || !Array.isArray(tmpl.tasks) || tmpl.tasks.length === 0) {
    return { tasks: [], groupCount: 0 };
  }

  const subtasks = tmpl.tasks as FlowSubtaskTemplate[];
  const tasks: MaterializedTask[] = [];
  for (let gi = 0; gi < coordinator.groupCount; gi++) {
    const groupCtx = { ...renderCtx, groupIndex: gi, i: gi };
    for (const st of subtasks) {
      const logicalKey = `${st.id}[${gi}]`;
      const hook = typeof st.hooks?.executor === 'string'
        ? st.hooks.executor
        : st.hooks?.executor?.use;
      if (!hook) {
        throw new WorkflowValidationError(
          `Coordinator subtask ${st.id} has no executor`,
        );
      }
      if (!availableHooks.has(hook)) {
        throw new WorkflowValidationError(
          `Coordinator subtask ${st.id} references missing hook: ${hook}`,
        );
      }

      const renderedInput: Record<string, WorkflowInputValue> = {};
      if (renderCtx.params) {
        Object.assign(renderedInput, renderCtx.params);
      }
      if (st.inputTemplate) {
        const rendered = renderTemplate(st.inputTemplate, groupCtx);
        if (rendered && typeof rendered === 'object') {
          Object.assign(renderedInput, rendered);
        }
      }

      const id = createId();
      tasks.push({
        id,
        logicalKey,
        hook,
        input: renderedInput,
        dependsOn: [],
        maxAttempts: 5,
      });
    }
  }

  return { tasks, groupCount: coordinator.groupCount };
}
