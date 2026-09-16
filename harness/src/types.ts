export type AgentMessage =
  | { readonly role: 'system' | 'user'; readonly content: string }
  | {
      readonly role: 'assistant';
      readonly content: string | null;
      readonly toolCalls?: readonly AgentToolCall[];
    }
  | {
      readonly role: 'tool';
      readonly toolCallId: string;
      readonly content: string;
    };

export interface AgentToolCall {
  readonly id: string;
  readonly name: string;
  readonly arguments: string;
}

export interface AgentToolDefinition {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: unknown;
}

export interface ModelRequest {
  readonly messages: readonly AgentMessage[];
  readonly tools: readonly AgentToolDefinition[];
  readonly signal?: AbortSignal;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export type ModelEvent =
  | { readonly type: 'text_delta'; readonly delta: string }
  | {
      readonly type: 'tool_call_delta';
      readonly index: number;
      readonly id?: string;
      readonly name?: string;
      readonly argumentsDelta?: string;
    }
  | { readonly type: 'done' };

export interface ChatModel {
  stream(request: ModelRequest): AsyncIterable<ModelEvent>;
}

export interface AgentToolContext {
  readonly signal?: AbortSignal;
  readonly iteration: number;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface AgentToolResult {
  readonly output: unknown;
  readonly display?: {
    readonly summary?: string;
    readonly data?: unknown;
  };
}

export interface AgentTool extends AgentToolDefinition {
  execute(args: unknown, context: AgentToolContext): Promise<AgentToolResult>;
}

export interface AgentRunInput {
  readonly model: ChatModel;
  readonly messages: readonly AgentMessage[];
  readonly tools?: readonly AgentTool[];
  readonly signal?: AbortSignal;
  readonly maxIterations?: number;
  readonly metadata?: Readonly<Record<string, unknown>>;
}

export type AgentEvent =
  | { readonly type: 'text_delta'; readonly delta: string }
  | { readonly type: 'tool_start'; readonly toolName: string; readonly args: unknown; readonly callId: string }
  | { readonly type: 'tool_end'; readonly toolName: string; readonly result: unknown; readonly callId: string }
  | { readonly type: 'iteration'; readonly index: number }
  | { readonly type: 'done'; readonly message: string }
  | { readonly type: 'error'; readonly error: Error };

export interface AgentHarness {
  run(input: AgentRunInput): AsyncIterable<AgentEvent>;
}
