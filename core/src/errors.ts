export type WorkflowErrorCode =
  | 'HOOK_REGISTRATION_ERROR'
  | 'PLUGIN_COMPATIBILITY_ERROR'
  | 'STORE_PROTOCOL_ERROR'
  | 'WORKFLOW_VALIDATION_ERROR';

export class WorkflowError extends Error {
  constructor(
    readonly code: WorkflowErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = new.target.name;
  }
}

export class HookRegistrationError extends WorkflowError {
  constructor(message: string) {
    super('HOOK_REGISTRATION_ERROR', message);
  }
}

export class StoreProtocolError extends WorkflowError {
  constructor(message: string, options?: ErrorOptions) {
    super('STORE_PROTOCOL_ERROR', message, options);
  }
}

export class WorkflowValidationError extends WorkflowError {
  constructor(message: string) {
    super('WORKFLOW_VALIDATION_ERROR', message);
  }
}
