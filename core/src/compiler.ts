import type {
  MaterializedTask,
  WorkflowDefinition,
} from './contracts.js';
import { WorkflowValidationError } from './errors.js';

export function validateWorkflowDefinition(
  definition: WorkflowDefinition,
  availableHooks: ReadonlySet<string>,
): void {
  const nodeIds = new Set<string>();
  const dependencies = new Map<string, readonly string[]>();

  for (const node of definition.nodes) {
    if (nodeIds.has(node.id)) {
      throw new WorkflowValidationError(
        `Workflow ${definition.name}@${definition.version} contains duplicate node: ${node.id}`,
      );
    }
    nodeIds.add(node.id);
    dependencies.set(node.id, node.dependsOn ?? []);

    if (!availableHooks.has(node.hook)) {
      throw new WorkflowValidationError(
        `Workflow ${definition.name}@${definition.version} references missing hook: ${node.hook}`,
      );
    }
  }

  for (const node of definition.nodes) {
    for (const dependency of node.dependsOn ?? []) {
      if (!nodeIds.has(dependency)) {
        throw new WorkflowValidationError(
          `Workflow ${definition.name}@${definition.version} node ${node.id} references missing dependency: ${dependency}`,
        );
      }
      if (dependency === node.id) {
        throw new WorkflowValidationError(
          `Workflow ${definition.name}@${definition.version} node ${node.id} depends on itself`,
        );
      }
    }
  }

  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (nodeId: string): void => {
    if (visiting.has(nodeId)) {
      throw new WorkflowValidationError(
        `Workflow ${definition.name}@${definition.version} contains a dependency cycle at node: ${nodeId}`,
      );
    }
    if (visited.has(nodeId)) {
      return;
    }

    visiting.add(nodeId);
    for (const dependency of dependencies.get(nodeId) ?? []) {
      visit(dependency);
    }
    visiting.delete(nodeId);
    visited.add(nodeId);
  };

  for (const nodeId of nodeIds) {
    visit(nodeId);
  }
}

export function materializeWorkflow(
  definition: WorkflowDefinition,
  createId: () => string,
): readonly MaterializedTask[] {
  return definition.nodes.map((node) => ({
    id: createId(),
    logicalKey: node.id,
    hook: node.hook,
    input: node.input ?? {},
    dependsOn: node.dependsOn ?? [],
    maxAttempts: 1,
  }));
}
