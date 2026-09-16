import { runAgentLoop } from './loop.js';
import type { AgentHarness, AgentRunInput, AgentEvent } from './types.js';

export class DefaultAgentHarness implements AgentHarness {
  run(input: AgentRunInput): AsyncIterable<AgentEvent> {
    return runAgentLoop(input);
  }
}

export function createAgentHarness(): AgentHarness {
  return new DefaultAgentHarness();
}
