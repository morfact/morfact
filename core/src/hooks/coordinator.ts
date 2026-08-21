// hooks/coordinator.ts — built-in coordinator hook for task group orchestration.
//
// Reads coordinator config from input._meta.coordinator.
// Uses services.engine to interact with the workflow runtime.
//
// Coordinator lifecycle:
//   1. First invocation → creates subtask groups, suspends self waiting for completion
//   2. Subsequent invocations → checks if all subtasks completed; if so, succeeds
//
// Subtask creation uses services.createCoordinatorSubtasks() which the store
// adapter must implement for DB-specific logic.

import type {
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
} from '../contracts.js';
import type { CoordinatorConfig } from '../flow-types.js';

export interface CoordinatorTaskDescriptor {
  logicalId: string;
  type: string;
  name: string;
  queue?: string;
  input: Record<string, unknown>;
  meta: Record<string, unknown>;
  orderIndex: number;
  groupIndex: number;
}

export interface CoordinatorSubtaskCreateParams {
  runId: string;
  taskId: string;
  coordinatorConfig: CoordinatorConfig;
  groupCount: number;
  groupStrategy: 'sequential' | 'parallel';
  tasks: CoordinatorTaskDescriptor[];
}

export interface CoordinatorServices {
  /** Compute groupCount from the expression */
  evaluateGroupCount(expr: string | number, context: Record<string, unknown>): Promise<number>;
  /** Build render context from run state */
  buildRenderContext(runId: string): Promise<Record<string, unknown>>;
  /** Render subtask templates for each group, returning descriptors */
  compileCoordinatorSubtasks(
    runId: string,
    coordinatorConfig: CoordinatorConfig,
    groupCount: number,
    renderContext: Record<string, unknown>,
  ): Promise<CoordinatorTaskDescriptor[]>;
  /** Create subtasks in the store and set up deps */
  createCoordinatorSubtasks(params: CoordinatorSubtaskCreateParams): Promise<{
    createdCount: number;
  }>;
  /** Check if all subtasks under this coordinator are completed */
  areSubtaskGroupsCompleted(
    runId: string,
    taskId: string,
  ): Promise<{
    completed: boolean;
    totalCount: number;
    unfinishedCount: number;
  }>;
}

export interface CoordinatorOutput {
  coordinatorWaiting?: boolean;
  groupCount?: number;
  totalTaskCount?: number;
  status?: 'waiting_for_subtasks' | 'subtasks_completed';
  message?: string;
  skipped?: boolean;
  reason?: string;
  [key: string]: unknown;
}

const RESUME_TOKEN_PREFIX = 'coordinator:';

export const coordinatorHook: HookDefinition<
  Record<string, unknown>,
  CoordinatorOutput,
  CoordinatorServices
> = {
  name: '@morfact/core/coordinator',
  async execute(
    ctx: HookExecutionContext<CoordinatorServices>,
    input: Record<string, unknown>,
  ): Promise<HookExecutionResult<CoordinatorOutput>> {
    const svc = ctx.services;
    const meta = input._meta as Record<string, unknown> | undefined;
    const coordConfig = meta?.coordinator as CoordinatorConfig | undefined;

    if (!coordConfig) {
      throw new Error('[Coordinator] Missing coordinator config in task meta');
    }

    const { taskGroupTemplate, groupCount: gcExpr, groupStrategy } = coordConfig;

    // Check when condition
    if (coordConfig.when) {
      const { evaluateWhen } = await import('../template-engine.js');
      const context = await svc.buildRenderContext(ctx.run.id);
      if (!evaluateWhen(coordConfig.when, context)) {
        return {
          status: 'succeeded',
          output: {
            skipped: true,
            reason: 'when_condition_not_met',
            coordinatorWaiting: false,
          },
        };
      }
    }

    // Determine group count
    const context = await svc.buildRenderContext(ctx.run.id);
    const groupCount = await svc.evaluateGroupCount(gcExpr, context);

    if (!Number.isFinite(groupCount) || groupCount < 0) {
      throw new Error(
        `[Coordinator] Invalid groupCount: ${gcExpr} → ${groupCount}`,
      );
    }
    if (groupCount < 1) {
      return {
        status: 'succeeded',
        output: {
          skipped: true,
          reason: 'no_task_groups',
          groupCount: 0,
          totalTaskCount: 0,
          taskGroupTemplate,
          status: 'subtasks_completed',
          coordinatorWaiting: false,
        },
      };
    }

    // Check if subtasks already exist
    const check = await svc.areSubtaskGroupsCompleted(
      ctx.run.id,
      ctx.task.id,
    );

    if (check.totalCount > 0) {
      if (check.unfinishedCount > 0) {
        return {
          status: 'waiting',
          reason: 'child_run',
          resumeToken: `${RESUME_TOKEN_PREFIX}${ctx.task.id}`,
        };
      }
      // All done
      return {
        status: 'succeeded',
        output: {
          groupCount,
          totalTaskCount: check.totalCount,
          taskGroupTemplate,
          groupStrategy,
          status: 'subtasks_completed',
          coordinatorWaiting: false,
        },
      };
    }

    // Create subtasks
    const subtaskDescriptors = await svc.compileCoordinatorSubtasks(
      ctx.run.id,
      coordConfig,
      groupCount,
      context,
    );

    if (subtaskDescriptors.length === 0) {
      return {
        status: 'succeeded',
        output: {
          groupCount,
          totalTaskCount: 0,
          taskGroupTemplate,
          groupStrategy,
          status: 'subtasks_completed',
          coordinatorWaiting: false,
        },
      };
    }

    await svc.createCoordinatorSubtasks({
      runId: ctx.run.id,
      taskId: ctx.task.id,
      coordinatorConfig: coordConfig,
      groupCount,
      groupStrategy: groupStrategy ?? 'sequential',
      tasks: subtaskDescriptors,
    });

    return {
      status: 'waiting',
      reason: 'child_run',
      resumeToken: `${RESUME_TOKEN_PREFIX}${ctx.task.id}`,
    };
  },
};
