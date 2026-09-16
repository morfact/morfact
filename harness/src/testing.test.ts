import { describe, expect, it } from 'vitest';
import { createAgentHarness } from './runtime.js';
import { collectAgentEvents, createScriptedModel } from './testing.js';
import type { AgentMessage, AgentTool } from './types.js';

const messages: readonly AgentMessage[] = [{ role: 'user', content: 'hello' }];

describe('@morfact/harness', () => {
  it('completes a text-only model turn', async () => {
    const model = createScriptedModel([[{ type: 'text_delta', delta: 'hi' }, { type: 'done' }]]);
    const events = await collectAgentEvents(createAgentHarness().run({ model, messages }));
    expect(events).toEqual([
      { type: 'iteration', index: 1 },
      { type: 'text_delta', delta: 'hi' },
      { type: 'done', message: 'hi' },
    ]);
  });

  it('aggregates streamed tool calls and feeds the result into the next turn', async () => {
    const seen: unknown[] = [];
    const tool: AgentTool = {
      name: 'echo', description: 'Echo input', inputSchema: { type: 'object' },
      async execute(args) { seen.push(args); return { output: { ok: true } }; },
    };
    const model = createScriptedModel([
      [
        { type: 'tool_call_delta', index: 0, id: 'call-1', name: 'echo', argumentsDelta: '{"x":' },
        { type: 'tool_call_delta', index: 0, argumentsDelta: '1}' },
      ],
      [{ type: 'text_delta', delta: 'finished' }],
    ]);
    const events = await collectAgentEvents(createAgentHarness().run({ model, messages, tools: [tool] }));
    expect(seen).toEqual([{ x: 1 }]);
    expect(events.map((event) => event.type)).toEqual(['iteration', 'tool_start', 'tool_end', 'iteration', 'text_delta', 'done']);
    expect(model.requests[1]?.messages.at(-1)).toEqual({ role: 'tool', toolCallId: 'call-1', content: '{"ok":true}' });
  });

  it('returns a normalized result for invalid JSON and unknown tools', async () => {
    const model = createScriptedModel([[{ type: 'tool_call_delta', index: 0, name: 'missing', argumentsDelta: '{bad' }], [{ type: 'text_delta', delta: 'retry' }]]);
    const events = await collectAgentEvents(createAgentHarness().run({ model, messages }));
    const toolEnd = events.find((event) => event.type === 'tool_end');
    expect(toolEnd).toMatchObject({ result: { error: expect.any(String) } });
    expect(toolEnd && 'result' in toolEnd ? toolEnd.result.error.length : 0).toBeGreaterThan(0);
    expect(events.at(-1)).toEqual({ type: 'done', message: 'retry' });
  });

  it('emits an error when the iteration budget is exhausted', async () => {
    const model = createScriptedModel([[{ type: 'tool_call_delta', index: 0, name: 'missing', argumentsDelta: '{}' }]]);
    const events = await collectAgentEvents(createAgentHarness().run({ model, messages, maxIterations: 1 }));
    expect(events.at(-1)).toMatchObject({ type: 'error', error: expect.objectContaining({ message: 'Agent exceeded maxIterations (1)' }) });
  });
});
