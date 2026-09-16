import { AgentToolError, asError } from './errors.js';
import type { AgentTool, AgentToolContext, AgentToolResult } from './types.js';

export class AgentToolRegistry {
  private readonly tools: ReadonlyMap<string, AgentTool>;

  constructor(tools: readonly AgentTool[] = []) {
    const registry = new Map<string, AgentTool>();
    for (const tool of tools) {
      if (!tool.name.trim()) throw new AgentToolError('Tool name must not be empty');
      if (registry.has(tool.name)) throw new AgentToolError(`Duplicate tool: ${tool.name}`);
      registry.set(tool.name, tool);
    }
    this.tools = registry;
  }

  definitions() {
    return [...this.tools.values()].map(({ name, description, inputSchema }) => ({
      name,
      description,
      inputSchema,
    }));
  }

  async execute(name: string, args: unknown, context: AgentToolContext): Promise<AgentToolResult> {
    const tool = this.tools.get(name);
    if (!tool) {
      throw new AgentToolError(`Unknown tool: ${name}`);
    }
    try {
      return await tool.execute(args, context);
    } catch (error) {
      throw new AgentToolError(`Tool ${name} failed: ${asError(error).message}`, { cause: error });
    }
  }
}
