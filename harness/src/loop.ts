import { AgentHarnessError, asError } from './errors.js';
import { AgentToolRegistry } from './tools.js';
import type { AgentEvent, AgentMessage, AgentRunInput, AgentToolCall, ModelEvent } from './types.js';

const DEFAULT_MAX_ITERATIONS = 5;

interface PartialToolCall { id: string; name: string; arguments: string }

function parseArguments(raw: string): unknown {
  const trimmed = raw.trim();
  if (!trimmed) return {};
  const withoutFence = trimmed.replace(/^```(?:json)?\s*/, '').replace(/\s*```$/, '');
  return JSON.parse(withoutFence);
}

function serialize(value: unknown): string {
  try { return JSON.stringify(value) ?? 'null'; } catch { return JSON.stringify({ error: 'Tool result is not serializable' }); }
}

function toolCallMessage(calls: readonly AgentToolCall[], content: string): AgentMessage {
  return { role: 'assistant', content: content || null, toolCalls: calls };
}

export async function* runAgentLoop(input: AgentRunInput): AsyncIterable<AgentEvent> {
  const maxIterations = input.maxIterations ?? DEFAULT_MAX_ITERATIONS;
  if (!Number.isInteger(maxIterations) || maxIterations < 1) {
    throw new AgentHarnessError('maxIterations must be an integer >= 1');
  }

  const registry = new AgentToolRegistry(input.tools ?? []);
  const messages: AgentMessage[] = [...input.messages];
  let finalText = '';

  for (let iteration = 1; iteration <= maxIterations; iteration += 1) {
    if (input.signal?.aborted) return;
    yield { type: 'iteration', index: iteration };

    const calls = new Map<number, PartialToolCall>();
    let turnText = '';
    try {
      const request = {
        messages,
        tools: registry.definitions(),
        ...(input.signal ? { signal: input.signal } : {}),
        ...(input.metadata ? { metadata: input.metadata } : {}),
      };
      for await (const event of input.model.stream(request)) {
        if (input.signal?.aborted) return;
        consumeModelEvent(event, calls, (delta) => {
          turnText += delta;
          finalText += delta;
        });
        if (event.type === 'text_delta') yield { type: 'text_delta', delta: event.delta };
      }
    } catch (error) {
      yield { type: 'error', error: asError(error) };
      return;
    }

    const toolCalls = [...calls.values()].map((call, index) => ({
      id: call.id || `call_${iteration}_${index + 1}`,
      name: call.name,
      arguments: call.arguments,
    }));
    if (toolCalls.length === 0) {
      yield { type: 'done', message: finalText };
      return;
    }

    messages.push(toolCallMessage(toolCalls, turnText));
    for (const call of toolCalls) {
      if (input.signal?.aborted) return;
      let result: unknown;
      try {
        const args = parseArguments(call.arguments);
        yield { type: 'tool_start', toolName: call.name, args, callId: call.id };
        const toolResult = await registry.execute(call.name, args, {
          ...(input.signal ? { signal: input.signal } : {}),
          iteration,
          ...(input.metadata ? { metadata: input.metadata } : {}),
        });
        result = toolResult.output;
      } catch (error) {
        result = { error: asError(error).message };
      }
      yield { type: 'tool_end', toolName: call.name, result, callId: call.id };
      messages.push({ role: 'tool', toolCallId: call.id, content: serialize(result) });
    }
  }

  yield { type: 'error', error: new AgentHarnessError(`Agent exceeded maxIterations (${String(maxIterations)})`) };
}

function consumeModelEvent(event: ModelEvent, calls: Map<number, PartialToolCall>, appendText: (delta: string) => void): void {
  if (event.type === 'text_delta') {
    appendText(event.delta);
    return;
  }
  if (event.type !== 'tool_call_delta') return;
  const current = calls.get(event.index) ?? { id: '', name: '', arguments: '' };
  if (event.id) current.id = event.id;
  if (event.name) current.name = event.name;
  if (event.argumentsDelta) current.arguments += event.argumentsDelta;
  calls.set(event.index, current);
}
