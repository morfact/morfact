import { describe, expect, it } from 'vitest';

import {
  conventionChildTemplateName,
  parseChildTemplateRegistryYaml,
  resolveChildTemplatePin,
  resolveSpawnChildTemplate,
} from './child-template-registry.js';

describe('child-template-registry', () => {
  it('resolves aliases then overrides without touching cwd', () => {
    const config = parseChildTemplateRegistryYaml(
      `
defaults:
  childTemplateVersion: "2.0"
aliases:
  environment.marble: environment.routing
overrides:
  environment.routing:
    childTemplateName: film.branch.environment_routing
    childTemplateVersion: "1.0"
`,
      { conventionTemplateNamePrefix: 'film.branch.' },
    );

    expect(resolveChildTemplatePin('environment.marble', config)).toEqual({
      childTemplateName: 'film.branch.environment_routing',
      childTemplateVersion: '1.0',
    });
  });

  it('falls back to convention prefix when no override exists', () => {
    const pin = resolveChildTemplatePin('shot.asset', {
      conventionTemplateNamePrefix: 'film.branch.',
      defaultsVersion: '1.0',
    });
    expect(pin).toEqual({
      childTemplateName: 'film.branch.shot_asset',
      childTemplateVersion: '1.0',
    });
    expect(conventionChildTemplateName('shot.asset', 'film.branch.')).toBe(
      'film.branch.shot_asset',
    );
  });

  it('throws when pipeline_id has no override and no convention prefix', () => {
    expect(() => resolveChildTemplatePin('shot.asset', { aliases: {} })).toThrow(
      /conventionTemplateNamePrefix/,
    );
  });

  it('resolves spawn from pipeline_id or explicit template name', () => {
    const fromPipeline = resolveSpawnChildTemplate({
      taskInput: { pipeline_id: 'shot.asset' },
      metaSpawnChild: {},
      registryOpts: { conventionTemplateNamePrefix: 'film.branch.' },
    });
    expect(fromPipeline.source).toBe('pipeline_id');
    expect(fromPipeline.childTemplateName).toBe('film.branch.shot_asset');

    const fromMeta = resolveSpawnChildTemplate({
      taskInput: {},
      metaSpawnChild: {
        childTemplateName: 'demo.child',
        childTemplateVersion: '3.0',
      },
    });
    expect(fromMeta).toMatchObject({
      source: 'meta',
      childTemplateName: 'demo.child',
      childTemplateVersion: '3.0',
      pipelineId: null,
    });
  });
});
