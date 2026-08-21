// child-template-registry.ts — pipeline_id → child Flow template resolution.
//
// Pure in-memory mapping. The engine never reads the filesystem or process.cwd().
// Hosts load YAML (or build the config in code) and pass it to createEngine.
//
// Three-tier resolution:
//   1. Overrides  (exact name + version)
//   2. Aliases    (pipeline_id remapping)
//   3. Convention (pipeline_id dots → underscores, with caller-supplied prefix)

import yaml from 'js-yaml';
import { WorkflowValidationError } from './errors.js';

export type ChildTemplatePin = {
  childTemplateName: string;
  childTemplateVersion: string;
};

export type ResolveSpawnChildTemplateSource = 'pipeline_id' | 'meta';

export interface ChildTemplateRegistryConfig {
  /** Default child template version when an override omits it, or convention is used. */
  readonly defaultsVersion?: string;
  /** pipeline_id remaps. Chains are followed up to 12 hops. */
  readonly aliases?: Readonly<Record<string, string>>;
  /** Exact pins. Win over convention. */
  readonly overrides?: Readonly<Record<string, ChildTemplatePin>>;
  /**
   * Convention prefix, e.g. `"film.branch."`.
   * Required when a pipeline_id has no override after alias expansion.
   */
  readonly conventionTemplateNamePrefix?: string;
}

/** @deprecated Use ChildTemplateRegistryConfig. Kept as a type alias for callers. */
export type ChildTemplateRegistryOptions = ChildTemplateRegistryConfig;

type RegistryYaml = {
  defaults?: { childTemplateVersion?: string };
  aliases?: Record<string, string>;
  overrides?: Record<string, Partial<ChildTemplatePin> & Record<string, unknown>>;
};

const DEFAULT_VERSION = '1.0';
const MAX_ALIAS_HOPS = 12;

function normalizeConfig(
  config?: ChildTemplateRegistryConfig,
): Required<
  Pick<ChildTemplateRegistryConfig, 'defaultsVersion'>
> & {
  aliases: Record<string, string>;
  overrides: Record<string, ChildTemplatePin>;
  conventionTemplateNamePrefix?: string;
} {
  const defaultsVersion =
    String(config?.defaultsVersion ?? DEFAULT_VERSION).trim() || DEFAULT_VERSION;
  const aliases: Record<string, string> = { ...(config?.aliases ?? {}) };
  const overrides: Record<string, ChildTemplatePin> = {
    ...(config?.overrides ?? {}),
  };
  const prefix = config?.conventionTemplateNamePrefix;
  return {
    defaultsVersion,
    aliases,
    overrides,
    ...(prefix !== undefined ? { conventionTemplateNamePrefix: prefix } : {}),
  };
}

/**
 * Parse a workflow-pipeline-registry YAML document into an in-memory config.
 * The host is responsible for reading the file.
 */
export function parseChildTemplateRegistryYaml(
  yamlString: string,
  extra?: Pick<ChildTemplateRegistryConfig, 'conventionTemplateNamePrefix'>,
): ChildTemplateRegistryConfig {
  let file: RegistryYaml | null = null;
  try {
    const loaded = yaml.load(yamlString);
    if (loaded && typeof loaded === 'object' && !Array.isArray(loaded)) {
      file = loaded as RegistryYaml;
    }
  } catch (error) {
    throw new WorkflowValidationError(
      `Invalid child-template registry YAML: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  const defaultsVersion =
    String(file?.defaults?.childTemplateVersion ?? DEFAULT_VERSION).trim() ||
    DEFAULT_VERSION;

  const aliases: Record<string, string> = { ...(file?.aliases ?? {}) };
  const overrides: Record<string, ChildTemplatePin> = {};
  for (const [k, v] of Object.entries(file?.overrides ?? {})) {
    const id = k.trim();
    if (!id || !v || typeof v !== 'object') continue;
    const name =
      typeof v.childTemplateName === 'string' ? v.childTemplateName.trim() : '';
    const ver =
      String(v.childTemplateVersion ?? defaultsVersion).trim() || defaultsVersion;
    if (name) {
      overrides[id] = { childTemplateName: name, childTemplateVersion: ver };
    }
  }

  const prefix = extra?.conventionTemplateNamePrefix;
  return {
    defaultsVersion,
    aliases,
    overrides,
    ...(prefix !== undefined ? { conventionTemplateNamePrefix: prefix } : {}),
  };
}

/**
 * Convention-derived template name: dots → underscores, with caller-supplied prefix.
 * If no prefix is given, throws — there is no default convention.
 */
export function conventionChildTemplateName(
  pipelineId: string,
  prefix?: string,
): string {
  const id = pipelineId.trim();
  if (!id) throw new Error('pipeline_id empty');
  if (!prefix || prefix.trim().length === 0) {
    throw new Error(
      'conventionChildTemplateName requires a prefix (e.g. "film.branch."). ' +
        'Pass conventionTemplateNamePrefix in ChildTemplateRegistryConfig or provide an alias/override.',
    );
  }
  return `${prefix}${id.replace(/\./g, '_')}`;
}

/**
 * Resolve a pipeline_id to a ChildTemplatePin.
 * `config` must be provided by the host (createEngine({ childTemplateRegistry }) or a direct call).
 */
export function resolveChildTemplatePin(
  pipelineIdRaw: string,
  config?: ChildTemplateRegistryConfig,
): ChildTemplatePin {
  const pipelineId = pipelineIdRaw.trim();
  if (!pipelineId) {
    throw new Error('spawn_child: pipeline_id is empty');
  }

  const { defaultsVersion, aliases, overrides, conventionTemplateNamePrefix } =
    normalizeConfig(config);

  let resolved = pipelineId;
  const seen = new Set<string>([pipelineId]);
  for (let i = 0; i < MAX_ALIAS_HOPS; i++) {
    const next = aliases[resolved]?.trim();
    if (!next) break;
    if (seen.has(next)) {
      throw new Error(
        `spawn_child: pipeline_id alias cycle involving "${pipelineId}"`,
      );
    }
    seen.add(next);
    resolved = next;
  }

  const pinOverride = overrides[resolved];
  if (pinOverride) {
    return {
      childTemplateName: pinOverride.childTemplateName,
      childTemplateVersion:
        String(pinOverride.childTemplateVersion ?? defaultsVersion).trim() ||
        defaultsVersion,
    };
  }

  const prefix = conventionTemplateNamePrefix;
  if (!prefix || prefix.trim().length === 0) {
    throw new Error(
      `spawn_child: no override or alias for pipeline_id "${resolved}", and no conventionTemplateNamePrefix configured. ` +
        `Pass childTemplateRegistry to createEngine (parseChildTemplateRegistryYaml + conventionTemplateNamePrefix).`,
    );
  }

  return {
    childTemplateName: conventionChildTemplateName(resolved, prefix),
    childTemplateVersion: defaultsVersion,
  };
}

export function resolveSpawnChildTemplate(opts: {
  taskInput: unknown;
  metaSpawnChild: Record<string, unknown>;
  taskMeta?: Record<string, unknown> | null;
  registryOpts?: ChildTemplateRegistryConfig;
}): {
  childTemplateName: string;
  childTemplateVersion: string;
  source: ResolveSpawnChildTemplateSource;
  pipelineId: string | null;
} {
  const inp =
    opts.taskInput &&
    typeof opts.taskInput === 'object' &&
    !Array.isArray(opts.taskInput)
      ? (opts.taskInput as Record<string, unknown>)
      : {};
  const rawPid = inp.pipeline_id;
  const pipelineId =
    typeof rawPid === 'string' && rawPid.trim().length > 0
      ? rawPid.trim()
      : null;

  if (pipelineId) {
    const pin = resolveChildTemplatePin(pipelineId, opts.registryOpts);
    return {
      childTemplateName: pin.childTemplateName,
      childTemplateVersion: pin.childTemplateVersion,
      source: 'pipeline_id',
      pipelineId,
    };
  }

  const m = opts.metaSpawnChild;
  const tm = opts.taskMeta || {};
  const name =
    (typeof m.childTemplateName === 'string' && m.childTemplateName) ||
    (typeof tm.childTemplateName === 'string' && tm.childTemplateName) ||
    '';
  if (!name) {
    throw new Error(
      'spawn_child: set input.pipeline_id (registered pipeline) or meta.spawnChild.childTemplateName',
    );
  }
  const ver = String(
    m.childTemplateVersion ?? tm.childTemplateVersion ?? '1.0',
  );
  return {
    childTemplateName: name,
    childTemplateVersion: ver,
    source: 'meta',
    pipelineId: null,
  };
}

/**
 * @deprecated Registry config is now immutable and passed per call / Engine instance.
 * Kept so existing tests that reset a process-wide cache still compile.
 */
export function resetPipelineRegistryCache(): void {
  // no-op
}
