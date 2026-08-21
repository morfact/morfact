import { WorkflowValidationError } from './errors.js';

const EXACT_BINDING = /^\$\{(run\.(?:input|context)(?:\.[A-Za-z0-9_-]+)*)\}$/;

function readPath(
  root: Readonly<Record<string, unknown>>,
  path: string,
): unknown {
  if (!path) {
    return root;
  }
  let current: unknown = root;
  for (const segment of path.split('.')) {
    if (
      typeof current !== 'object' ||
      current === null ||
      Array.isArray(current) ||
      !(segment in current)
    ) {
      throw new WorkflowValidationError(`Input binding path not found: ${path}`);
    }
    current = (current as Readonly<Record<string, unknown>>)[segment];
  }
  return current;
}

export function resolveWorkflowInput(
  value: unknown,
  run: {
    readonly input: Readonly<Record<string, unknown>>;
    readonly context: Readonly<Record<string, unknown>>;
  },
): unknown {
  if (typeof value === 'string') {
    const match = EXACT_BINDING.exec(value);
    if (!match?.[1]) {
      return value;
    }
    const segments = match[1].split('.');
    const scope = segments[1];
    const path = segments.slice(2);
    const root = scope === 'input' ? run.input : run.context;
    return structuredClone(readPath(root, path.join('.')));
  }
  if (Array.isArray(value)) {
    return value.map((item) => resolveWorkflowInput(item, run));
  }
  if (typeof value === 'object' && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        resolveWorkflowInput(item, run),
      ]),
    );
  }
  return value;
}

export function resolveWorkflowTaskInput(
  input: Readonly<Record<string, unknown>>,
  run: {
    readonly input: Readonly<Record<string, unknown>>;
    readonly context: Readonly<Record<string, unknown>>;
  },
): Readonly<Record<string, unknown>> {
  return Object.fromEntries(
    Object.entries(input).map(([key, value]) => [
      key,
      resolveWorkflowInput(value, run),
    ]),
  );
}
