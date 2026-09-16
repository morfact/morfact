export class AgentHarnessError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'AgentHarnessError';
  }
}

export class AgentToolError extends AgentHarnessError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'AgentToolError';
  }
}

export function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
