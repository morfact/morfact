// hooks/spawn.ts — built-in spawn hook for creating child runs.
//
// Reads spawn config from input._meta.spawn.
// Resolves child template via an injected ChildTemplateRegistryConfig.
// Creates child run via SpawnServices (engine supplies a store-backed default).
// Suspends current task waiting for child completion.

import type {
  HookDefinition,
  HookExecutionContext,
  HookExecutionResult,
} from '../contracts.js';
import type { SpawnConfig } from '../flow-types.js';
import type { ChildTemplateRegistryConfig } from '../child-template-registry.js';
import { resolveChildTemplatePin } from '../child-template-registry.js';

export interface SpawnServices {
  /** Check if a child run already exists for this spawn */
  findExistingChildRun(parentTaskId: string): Promise<string | null>;
  /** Get child run status */
  getChildRunStatus(childRunId: string): Promise<
    | { status: 'succeeded'; output?: unknown }
    | { status: 'failed'; error?: unknown }
    | { status: 'running' }
    | null
  >;
  /** Create child run and return its ID */
  createChildRun(params: {
    parentRunId: string;
    parentTaskId: string;
    childTemplateName: string;
    childTemplateVersion: string;
    tenantKey: string;
    resourceRef?: string;
    input: Record<string, unknown>;
    context: Record<string, unknown>;
  }): Promise<string>;
  /** Update parent task meta with child info */
  updateParentTaskMeta(
    taskId: string,
    meta: Record<string, unknown>,
  ): Promise<void>;
}

export interface SpawnOutput {
  childJobWaiting: boolean;
  childJobId?: string;
  spawnId?: string;
  childTemplateResolution?: {
    source: string;
    pipelineId: string | null;
    childTemplateName: string;
    childTemplateVersion: string;
  };
  skipped?: boolean;
  skipReason?: string;
  [key: string]: unknown;
}

const RESUME_TOKEN_PREFIX = 'spawn:';

function requireSpawnServices(
  svc: SpawnServices | undefined,
): SpawnServices {
  if (
    !svc ||
    typeof svc.findExistingChildRun !== 'function' ||
    typeof svc.createChildRun !== 'function'
  ) {
    throw new Error(
      '[Spawn] Engine services are missing SpawnServices. ' +
        'Pass a WorkflowStateStore that implements createChildRun / listRuns({ parentTaskId }), ' +
        'or supply SpawnServices via createEngine({ services }).',
    );
  }
  return svc;
}

export function createSpawnHook(
  registry?: ChildTemplateRegistryConfig,
): HookDefinition<Record<string, unknown>, SpawnOutput, SpawnServices> {
  return {
    name: '@morfact/core/spawn',
    async execute(
      ctx: HookExecutionContext<SpawnServices>,
      input: Record<string, unknown>,
    ): Promise<HookExecutionResult<SpawnOutput>> {
      const svc = requireSpawnServices(ctx.services);
      const meta = input._meta as Record<string, unknown> | undefined;
      const spawnConfig = meta?.spawn as SpawnConfig | undefined;

      if (!spawnConfig) {
        throw new Error('[Spawn] Missing spawn config in task meta');
      }

      const spawnId = String(
        spawnConfig.spawnId ?? meta?.logicalKey ?? ctx.task.id,
      );
      const pipelineId = spawnConfig.pipelineId ?? null;

      let childTemplateName: string;
      let childTemplateVersion: string;
      let source: 'pipeline_id' | 'meta';

      if (pipelineId) {
        const pin = resolveChildTemplatePin(pipelineId, registry);
        childTemplateName = pin.childTemplateName;
        childTemplateVersion = pin.childTemplateVersion;
        source = 'pipeline_id';
      } else if (spawnConfig.childTemplateName) {
        childTemplateName = spawnConfig.childTemplateName;
        childTemplateVersion = spawnConfig.childTemplateVersion ?? '1.0';
        source = 'meta';
      } else {
        throw new Error(
          '[Spawn] Must specify either pipelineId or childTemplateName in spawn config',
        );
      }

      const skipParam = spawnConfig.skipUnlessTruthyParam;
      if (skipParam) {
        const paramKey = skipParam.replace(/^params\./, '');
        if (!input[paramKey] && !ctx.run.input[paramKey]) {
          return {
            status: 'succeeded',
            output: {
              childJobWaiting: false,
              skipped: true,
              skipReason: `skipUnlessTruthyParam:${paramKey}`,
              spawnId,
            },
          };
        }
      }

      const existingChildId = await svc.findExistingChildRun(ctx.task.id);
      if (existingChildId) {
        const status = await svc.getChildRunStatus(existingChildId);
        if (status?.status === 'succeeded') {
          return {
            status: 'succeeded',
            output: {
              childJobWaiting: false,
              childJobId: existingChildId,
              spawnId,
              childTemplateResolution: {
                source,
                pipelineId,
                childTemplateName,
                childTemplateVersion,
              },
            },
          };
        }
        if (status?.status === 'failed') {
          if (spawnConfig.onChildFailure === 'ignore') {
            return {
              status: 'succeeded',
              output: {
                childJobWaiting: false,
                childJobId: existingChildId,
                spawnId,
                skipped: true,
                skipReason: 'onChildFailure:ignore',
                childTemplateResolution: {
                  source,
                  pipelineId,
                  childTemplateName,
                  childTemplateVersion,
                },
              },
            };
          }
          throw new Error(
            `[Spawn] Child run ${existingChildId} failed` +
              (status.error ? `: ${JSON.stringify(status.error)}` : ''),
          );
        }
        return {
          status: 'waiting',
          reason: 'child_run',
          resumeToken: `${RESUME_TOKEN_PREFIX}${ctx.task.id}`,
        };
      }

      const childInputTemplate = spawnConfig.childInputTemplate;
      const childInput = (
        childInputTemplate && typeof childInputTemplate === 'object'
          ? { ...ctx.run.input, ...childInputTemplate }
          : ctx.run.input
      ) as Record<string, unknown>;

      const childJobId = await svc.createChildRun({
        parentRunId: ctx.run.id,
        parentTaskId: ctx.task.id,
        childTemplateName,
        childTemplateVersion,
        tenantKey: ctx.run.tenantKey,
        ...(ctx.run.resourceRef !== undefined
          ? { resourceRef: ctx.run.resourceRef }
          : {}),
        input: childInput,
        context: {},
      });

      await svc.updateParentTaskMeta(ctx.task.id, {
        childJobId,
        spawnId,
        childTemplateName,
        childTemplateVersion,
      });

      return {
        status: 'waiting',
        reason: 'child_run',
        resumeToken: `${RESUME_TOKEN_PREFIX}${ctx.task.id}`,
      };
    },
  };
}

/** Default builtin hook. pipelineId resolution requires createEngine({ childTemplateRegistry }). */
export const spawnHook = createSpawnHook();
