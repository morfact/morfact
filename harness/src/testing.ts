import type { AgentEvent, ChatModel, ModelEvent, ModelRequest } from './types.js';

export function createScriptedModel(turns: readonly (readonly ModelEvent[])[]): ChatModel & { readonly requests: readonly ModelRequest[] } {
  const requests: ModelRequest[] = [];
  let turn = 0;
  return {
    requests,
    stream(request) {
      requests.push(request);
      const events = turns[turn++] ?? [];
      return (async function* () {
        for (const event of events) yield event;
      })();
    },
  };
}

export async function collectAgentEvents(events: AsyncIterable<AgentEvent>): Promise<AgentEvent[]> {
  const collected: AgentEvent[] = [];
  for await (const event of events) collected.push(event);
  return collected;
}

export interface AgentRecording {
  readonly turns: readonly (readonly ModelEvent[])[];
}

export function replayAgentRun(recording: AgentRecording): ChatModel {
  return createScriptedModel(recording.turns);
}
