// template-registry.ts — loads and caches FlowDefinitions from a templates directory.
//
// Host apps MUST pass `templatesDir`. The engine never hardcodes application paths.

import fs from 'node:fs';
import path from 'node:path';
import yaml from 'js-yaml';
import type { FlowDefinition } from './flow-types.js';
import { WorkflowValidationError } from './errors.js';

export interface FlowRegistryOptions {
  /**
   * Directory containing `.yml` Flow definition files.
   * Convention filename: `<name>.<version>.yml`
   * Required — inject from the host (e.g. resolveFilmTemplatesDir).
   */
  templatesDir: string;
  /**
   * If true, create the templates directory if it doesn't exist.
   * Default: false (throws if missing).
   */
  autoCreate?: boolean;
  /**
   * File matching pattern (glob-style extension filter).
   * Default: ".yml"
   */
  fileExtension?: string;
}

type TemplateKey = string; // "name@version"

export class FlowRegistry {
  private templatesDir: string;
  private templates: Map<TemplateKey, FlowDefinition>;
  private templateVersions: Map<string, FlowDefinition[]>;

  constructor(options: FlowRegistryOptions) {
    if (!options?.templatesDir?.trim()) {
      throw new WorkflowValidationError(
        'FlowRegistry requires options.templatesDir (host must inject the templates path).',
      );
    }
    this.templatesDir = options.templatesDir;
    this.templates = new Map();
    this.templateVersions = new Map();
    if (options.autoCreate) {
      fs.mkdirSync(this.templatesDir, { recursive: true });
    }
    const ext = options.fileExtension ?? '.yml';
    this.loadTemplates(ext);
  }

  private loadTemplates(ext: string): void {
    if (!fs.existsSync(this.templatesDir)) {
      throw new WorkflowValidationError(
        `Templates directory not found: ${this.templatesDir}. ` +
        `Set templatesDir in options or create the directory.`,
      );
    }

    const stat = fs.statSync(this.templatesDir);
    if (!stat.isDirectory()) {
      throw new WorkflowValidationError(
        `Templates path is not a directory: ${this.templatesDir}`,
      );
    }

    const files = fs.readdirSync(this.templatesDir);
    for (const file of files) {
      if (file.startsWith('_') || file.startsWith('.')) continue;
      if (!file.endsWith(ext)) continue;

      try {
        const raw = fs.readFileSync(
          path.join(this.templatesDir, file),
          'utf8',
        );
        const flow = yaml.load(raw) as FlowDefinition;

        if (
          !flow?.apiVersion ||
          !flow?.kind ||
          flow.kind !== 'Flow' ||
          !flow?.metadata?.name ||
          !flow?.metadata?.version
        ) {
          console.warn(`[FlowRegistry] Skipping invalid Flow file: ${file}`);
          continue;
        }

        const key = this.makeKey(flow.metadata.name, flow.metadata.version);
        this.templates.set(key, flow);

        if (!this.templateVersions.has(flow.metadata.name)) {
          this.templateVersions.set(flow.metadata.name, []);
        }
        this.templateVersions.get(flow.metadata.name)!.push(flow);
      } catch (err) {
        console.warn(
          `[FlowRegistry] Failed to load ${file}: ${(err as Error)?.message ?? String(err)}`,
        );
      }
    }
  }

  private makeKey(name: string, version: string): TemplateKey {
    return `${name}@${version}`;
  }

  /**
   * Get a Flow definition by name and version.
   */
  get(name: string, version: string): FlowDefinition | undefined {
    return this.templates.get(this.makeKey(name, version));
  }

  /**
   * Get the latest version of a Flow by name.
   */
  getLatest(name: string): FlowDefinition | undefined {
    const versions = this.templateVersions.get(name);
    if (!versions || versions.length === 0) return undefined;
    // Sort by semver-ish version
    return versions.sort((a, b) =>
      b.metadata.version.localeCompare(a.metadata.version, undefined, {
        numeric: true,
      }),
    )[0];
  }

  /**
   * List all loaded Flow definitions.
   */
  list(): FlowDefinition[] {
    return Array.from(this.templates.values());
  }

  /**
   * Get all available names.
   */
  names(): string[] {
    return Array.from(this.templateVersions.keys());
  }

  /**
   * Reload all templates from disk.
   */
  reload(): void {
    this.templates.clear();
    this.templateVersions.clear();
    this.loadTemplates('.yml');
  }

  /**
   * The directory being monitored.
   */
  get directory(): string {
    return this.templatesDir;
  }

  /**
   * Number of loaded templates.
   */
  get size(): number {
    return this.templates.size;
  }
}
