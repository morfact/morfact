import { materializeWorkflow } from './compiler.js';
import type {
  MaterializedTask,
  WorkflowDefinition,
  WorkflowStateStore,
} from './contracts.js';
import { StoreProtocolError, WorkflowValidationError } from './errors.js';
import type {
  CoordinatorServices,
  CoordinatorTaskDescriptor,
} from './hooks/coordinator.js';
import type { SpawnServices } from './hooks/spawn.js';
import { renderTemplate, type RenderContext } from './template-engine.js';
import type { CoordinatorConfig, FlowSubtaskTemplate } from './flow-types.js';

export type BuiltinRuntimeServices = SpawnServices & CoordinatorServices;

export interface BuiltinRuntimeServicesOptions {
  readonly store: WorkflowStateStore;
  readonly workflows: ReadonlyMap<string, WorkflowDefinition>;
  readonly createId: () => string;
  readonly now: () => Date;
}

function workflowKey(name: string, version: string): string {
  return `${name}@${version}`;
}

function asRenderContext(context: Record<string, unknown>): RenderContext {
  return context as RenderContext;
}

function evaluateGroupCountValue(
  expr: string | number,
  context: Record<string, unknown>,
): number {
  if (typeof expr === 'number') return expr;
  const trimmed = expr.trim();
  if (trimmed === '') return Number.NaN;
  const template = trimmed.includes('{{') ? trimmed : `{{${trimmed}}}`;
  const rendered = renderTemplate(template, asRenderContext(context));
  return typeof rendered === 'number' ? rendered : Number(rendered);
}

function descriptorsFromTemplate(
  spec: { tasks: FlowSubtaskTemplate[] },
  groupCount: number,
  renderContext: Record<string, unknown>,
): CoordinatorTaskDescriptor[] {
  const tasks = spec.tasks;
  const descriptors: CoordinatorTaskDescriptor[] = [];
  for (let gi = 0; gi < groupCount; gi++) {
    const groupCtx: RenderContext = {
      ...asRenderContext(renderContext),
      groupIndex: gi,
      i: gi,
    };
    for (let ti = 0; ti < tasks.length; ti++) {
      const st = tasks[ti];
      if (!st) continue;
      const hook =
        typeof st.hooks?.executor === 'string'
          ? st.hooks.executor
          : st.hooks?.executor?.use;
      if (!hook) {
        throw new WorkflowValidationError(
          `Coordinator subtask ${st.id} has no executor`,
        );
      }
      const renderedInput: Record<string, unknown> = {
        ...((renderContext.params as Record<string, unknown> | undefined) ?? {}),
      };
      if (st.inputTemplate) {
        const rendered = renderTemplate(st.inputTemplate, groupCtx);
        if (rendered && typeof rendered === 'object') {
          Object.assign(renderedInput, rendered);
        }
      }
      descriptors.push({
        logicalId: `${st.id}[${gi}]`,
        type: hook,
        name: st.name ?? `${st.id}[${gi}]`,
        ...(st.queue !== undefined ? { queue: st.queue } : {}),
        input: renderedInput,
        meta: {
          ...(st.meta ?? {}),
          groupIndex: gi,
          orderIndex: ti,
          coordinatorSubtask: true,
        },
        orderIndex: ti,
        groupIndex: gi,
      });
    }
  }
  return descriptors;
}

export function createBuiltinRuntimeServices(
  options: BuiltinRuntimeServicesOptions,
): BuiltinRuntimeServices {
  const { store, workflows, createId, now } = options;

  function resolveChildWorkflow(
    name: string,
    version: string,
  ): { definition: WorkflowDefinition; tasks: readonly MaterializedTask[] } {
    const key = workflowKey(name, version);
    const definition = workflows.get(key);
    if (!definition) {
      throw new WorkflowValidationError(
        `Unknown child workflow: ${key}. Register it on createEngine({ plugins }) before spawning.`,
      );
    }
    return {
      definition,
      tasks: materializeWorkflow(definition, createId),
    };
  }

  const spawn: SpawnServices = {
    async findExistingChildRun(parentTaskId) {
      const runs = await store.listRuns({ parentTaskId });
      return runs[0]?.id ?? null;
    },
    async getChildRunStatus(childRunId) {
      const run = await store.getRun(childRunId);
      if (!run) return null;
      if (run.status === 'succeeded') {
        return { status: 'succeeded' };
      }
      if (run.status === 'failed' || run.status === 'cancelled') {
        return { status: 'failed', error: { status: run.status } };
      }
      return { status: 'running' };
    },
    async createChildRun(params) {
      const { definition, tasks } = resolveChildWorkflow(
        params.childTemplateName,
        params.childTemplateVersion,
      );
      const child = await store.createChildRun({
        runId: createId(),
        tenantKey: params.tenantKey,
        parentTaskId: params.parentTaskId,
        parentRunId: params.parentRunId,
        ...(params.resourceRef !== undefined
          ? { resourceRef: params.resourceRef }
          : {}),
        definition,
        input: params.input,
        context: params.context,
        tasks,
        now: now(),
      });
      return child.id;
    },
    async updateParentTaskMeta() {
      // Parent/child linkage is stored on the child run (parentTaskId).
      // Hosts that persist extra task.meta can override this method.
    },
  };

  const coordinator: CoordinatorServices = {
    async evaluateGroupCount(expr, context) {
      return evaluateGroupCountValue(expr, context);
    },
    async buildRenderContext(runId) {
      const run = await store.getRun(runId);
      if (!run) {
        throw new WorkflowValidationError(`Run not found: ${runId}`);
      }
      const tasks = await store.listTasks(runId);
      const taskBag: Record<string, unknown> = {};
      for (const task of tasks) {
        taskBag[task.logicalKey] = {
          output: task.output,
          input: task.input,
          status: task.status,
        };
      }
      return {
        params: run.input as Record<string, unknown>,
        tasks: taskBag,
        flow: { variables: {} },
        vars: {},
      };
    },
    async compileCoordinatorSubtasks(
      _runId,
      coordinatorConfig,
      groupCount,
      renderContext,
    ) {
      const spec = (coordinatorConfig as CoordinatorConfig)
        .taskGroupTemplateSpec;
      if (!spec?.tasks?.length) {
        throw new WorkflowValidationError(
          `Coordinator template "${coordinatorConfig.taskGroupTemplate}" was not embedded at compile time. ` +
            `Use compileFlow() so taskGroupTemplates are copied onto the node, or implement compileCoordinatorSubtasks in services.`,
        );
      }
      return descriptorsFromTemplate(spec, groupCount, renderContext);
    },
    async createCoordinatorSubtasks(params) {
      if (typeof store.appendTasks !== 'function') {
        throw new StoreProtocolError(
          'Store does not implement appendTasks, which is required for @morfact/core/coordinator',
        );
      }
      const strategy = params.groupStrategy;
      const ids = params.tasks.map(() => createId());
      const materialized: MaterializedTask[] = params.tasks.map((desc, index) => {
        const dependsOn: string[] = [];
        if (strategy === 'sequential' && desc.groupIndex > 0) {
          for (let i = 0; i < params.tasks.length; i++) {
            const other = params.tasks[i];
            if (other && other.groupIndex === desc.groupIndex - 1) {
              const depId = ids[i];
              if (depId) dependsOn.push(depId);
            }
          }
        }
        const id = ids[index];
        if (!id) {
          throw new StoreProtocolError('Failed to allocate coordinator subtask id');
        }
        return {
          id,
          logicalKey: desc.logicalId,
          hook: desc.type,
          input: {
            ...desc.input,
            _meta: desc.meta,
          },
          dependsOn,
          maxAttempts: 5,
        };
      });
      const result = await store.appendTasks({
        commandId: createId(),
        runId: params.runId,
        coordinatorTaskId: params.taskId,
        tasks: materialized,
        now: now(),
      });
      return { createdCount: result.createdCount };
    },
    async areSubtaskGroupsCompleted(runId, taskId) {
      const tasks = await store.listTasks(runId);
      const subs = tasks.filter((t) => t.ancestorId === taskId);
      if (subs.length === 0) {
        return { completed: false, totalCount: 0, unfinishedCount: 0 };
      }
      const unfinished = subs.filter(
        (t) =>
          t.status !== 'succeeded' &&
          t.status !== 'skipped' &&
          t.status !== 'cancelled',
      );
      return {
        completed: unfinished.length === 0,
        totalCount: subs.length,
        unfinishedCount: unfinished.length,
      };
    },
  };

  return { ...spawn, ...coordinator };
}

export function mergeEngineServices(
  defaults: BuiltinRuntimeServices,
  host: unknown,
): BuiltinRuntimeServices {
  if (host && typeof host === 'object' && !Array.isArray(host)) {
    return { ...defaults, ...(host as Partial<BuiltinRuntimeServices>) };
  }
  return defaults;
}
