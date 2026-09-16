# @morfact/harness

`@morfact/harness` 是一个轻量、Provider-neutral、Event-first 的 Agent 执行内核。
它负责驱动一次有限的模型—工具循环，不拥有 Agent 角色、业务状态或工作流状态。

```text
model stream
  → tool call aggregation
  → tool execution
  → tool result feedback
  → next model turn
```

## 适用场景

当应用需要让模型在生成文本的过程中调用业务工具，并在工具结果返回后继续生成时，可以使用 Harness。
典型场景包括书稿出版总监、修图 Agent、媒体审核 Agent 和轻量研究 Agent。

推荐的组合关系是：

```text
AgentProfile + AgentHarness + Host/State = 具体 Agent
```

- Agent Profile：由应用定义角色指令、上下文和模型选择。
- Host/State：由应用提供业务状态、权限、资源访问和持久化。
- Agent Harness：只执行本次模型—工具循环。
- `@morfact/core`：负责跨任务、跨进程、跨时间的可靠 Workflow 编排。

## 安装

```bash
pnpm add @morfact/harness
```

Harness 不依赖 `@morfact/core`，也不内置 OpenAI、Anthropic、Shell、文件、搜索、数据库或图片工具。
模型供应商 SDK 和业务工具由宿主应用提供。

## 五分钟开始

### 1. 提供一个 ChatModel

`ChatModel` 是 Harness 与模型供应商之间的唯一边界。宿主将供应商的流式响应映射为 `ModelEvent`：

```ts
import type { ChatModel, ModelEvent, ModelRequest } from '@morfact/harness';

const model: ChatModel = {
  stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    return providerStream({
      messages: request.messages,
      tools: request.tools,
      signal: request.signal,
    });
  },
};
```

供应商适配器应完成以下工作：

1. 将 `AgentMessage` 转换为供应商的消息格式。
2. 将 `AgentToolDefinition` 转换为供应商的工具定义格式。
3. 将文本增量转换为 `{ type: 'text_delta', delta }`。
4. 将工具调用增量转换为 `{ type: 'tool_call_delta', ... }`。
5. 保留 `request.signal` 的取消行为。

供应商 SDK 的类型只能存在于适配器中，不应出现在应用之外的 Harness 公共协议里。

### 2. 注册工具并运行

```ts
import {
  createAgentHarness,
  type AgentTool,
} from '@morfact/harness';

const tools: AgentTool[] = [
  {
    name: 'lookup_book',
    description: 'Look up a book by id.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id'],
    },
    async execute(args, context) {
      const input = args as { id: string };
      const book = await bookHost.lookup(input.id, context.signal);
      return {
        output: book,
        display: { summary: 'Book loaded' },
      };
    },
  },
];

const harness = createAgentHarness();

for await (const event of harness.run({
  model,
  messages: [{ role: 'user', content: 'Review this book.' }],
  tools,
  maxIterations: 5,
  signal,
  metadata: { requestId },
})) {
  // SSE、CLI、日志或 Morfact Hook Adapter 在这里消费事件。
  consume(event);
}
```

## 公共协议

### `AgentRunInput`

```ts
interface AgentRunInput {
  model: ChatModel;
  messages: readonly AgentMessage[];
  tools?: readonly AgentTool[];
  signal?: AbortSignal;
  maxIterations?: number;
  metadata?: Readonly<Record<string, unknown>>;
}
```

- `model`：宿主提供的模型适配器。
- `messages`：本次运行的初始消息，Harness 会在内部追加助手工具调用和工具结果消息。
- `tools`：显式注册给模型的工具，省略时表示没有工具。
- `signal`：同时传给模型和工具，用于取消运行。
- `maxIterations`：模型轮次预算，默认值为 `5`，必须是大于等于 `1` 的整数。
- `metadata`：透传给模型和工具的只读请求元数据。

### `AgentMessage`

支持四类消息：

```ts
type AgentMessage =
  | { role: 'system' | 'user'; content: string }
  | { role: 'assistant'; content: string | null; toolCalls?: readonly AgentToolCall[] }
  | { role: 'tool'; toolCallId: string; content: string };
```

应用通常只需要提供 `system`、历史 `user`/`assistant` 和当前 `user` 消息。
`assistant` 的工具调用消息以及 `tool` 结果消息由 Harness 自动维护。

### `ChatModel` 与 `ModelEvent`

```ts
interface ChatModel {
  stream(request: ModelRequest): AsyncIterable<ModelEvent>;
}

type ModelEvent =
  | { type: 'text_delta'; delta: string }
  | {
      type: 'tool_call_delta';
      index: number;
      id?: string;
      name?: string;
      argumentsDelta?: string;
    }
  | { type: 'done' };
```

工具调用可能被供应商拆成多个增量。Harness 按 `index` 聚合同一调用的 `id`、工具名和参数字符串；模型流结束后才开始执行工具。
`done` 是供应商适配器可以发出的本轮结束标记，Harness 也会把 AsyncIterable 结束视为本轮结束。

### `AgentTool`

```ts
interface AgentTool extends AgentToolDefinition {
  execute(
    args: unknown,
    context: AgentToolContext,
  ): Promise<AgentToolResult>;
}

interface AgentToolDefinition {
  name: string;
  description: string;
  inputSchema: unknown;
}

interface AgentToolContext {
  signal?: AbortSignal;
  iteration: number;
  metadata?: Readonly<Record<string, unknown>>;
}

interface AgentToolResult {
  output: unknown;
  display?: {
    summary?: string;
    data?: unknown;
  };
}
```

`inputSchema` 会原样提供给模型适配器。当前 Harness 负责 JSON 参数解析，不绑定 Zod 或某一种 JSON Schema 实现；工具如需语义校验，应在 `execute` 开始时完成。

工具名称在一次运行内必须唯一且不能为空。工具必须由宿主显式注册，Harness 不会自动发现或扩展权限。

## 事件与运行语义

### `AgentEvent`

```ts
type AgentEvent =
  | { type: 'iteration'; index: number }
  | { type: 'text_delta'; delta: string }
  | { type: 'tool_start'; toolName: string; args: unknown; callId: string }
  | { type: 'tool_end'; toolName: string; result: unknown; callId: string }
  | { type: 'done'; message: string }
  | { type: 'error'; error: Error };
```

一个典型的运行顺序是：

```text
iteration(1)
  → text_delta*
  → tool_start / tool_end
  → iteration(2)
  → text_delta*
  → done
```

没有工具调用时，当前轮次直接以 `done` 结束。发生工具调用时，Harness 会：

1. 将助手消息和聚合后的工具调用追加到内部消息列表。
2. 按模型返回顺序依次执行同一轮的工具调用。
3. 将每个工具输出序列化为 `tool` 消息。
4. 继续下一次模型轮次。

`tool_end.result` 是工具的 `output`，而不是完整的 `AgentToolResult`；`display` 由宿主自行处理或映射到应用层事件。

### 错误行为

- 工具参数不是有效 JSON：生成错误结果并回填模型，不会调用工具。
- 工具未注册：生成错误结果并回填模型。
- 工具执行抛错：转换为 `{ error: string }` 结果并回填模型。
- 模型流抛错：发出一个 `error` 事件并结束运行。
- 迭代预算耗尽：发出 `error` 事件并结束运行。
- `AbortSignal` 已取消：停止运行，不再执行后续模型轮次或工具调用。

Harness 不会自动重试模型或工具。跨轮次重试、持久化和恢复应由应用或 `@morfact/core` 负责。

## 与 `@morfact/core` 集成

Harness 适合嵌入一个普通 Morfact Hook：

```ts
import { createAgentHarness } from '@morfact/harness';
import { defineHook } from '@morfact/core';

export const repairAgent = defineHook({
  name: 'media.repair_agent@1',
  async execute(ctx, input) {
    const events = [];
    for await (const event of createAgentHarness().run({
      model: createModel(ctx.services),
      messages: buildRepairMessages(input),
      tools: createRepairTools(ctx.services),
      signal: ctx.signal,
    })) {
      events.push(event);
    }
    return collectAgentResult(events);
  },
});
```

Hook 可以收集事件并返回结果，但 Harness 本身不保存 Run、Task、Checkpoint 或 Outbox 状态。
长任务应由 Morfact Workflow/Loop 拆成多个有边界的 Harness 运行：

```text
Morfact Workflow / Loop
  └── one bounded Harness run
        └── model ↔ tools
```

## Tale 接入建议

Tale 是 Morfact 的客户和真实应用场景。Tale 侧应保留：

- `BookDirectorProfile` 和出版 Prompt；
- `BookBlueprint` 上下文装配；
- `DRAFT_COPILOT_TOOLS` 与 `executeDraftTool`；
- `BookDraftRepository` 持久化；
- SSE 与书房 UI 事件映射。

Tale 只需要实现：

1. OpenAI 到 `ChatModel`/`ModelEvent` 的 Adapter。
2. 将书房工具转换为 `AgentTool`。
3. 将 `AgentEvent` 映射为现有书房回调和 SSE 事件。

这样 `runDraftCopilotChat` 可以成为应用层 Adapter，而不再承担通用模型循环的实现。

## 确定性测试

从测试子路径导入辅助工具：

```ts
import {
  collectAgentEvents,
  createScriptedModel,
} from '@morfact/harness/testing';

const model = createScriptedModel([
  [{ type: 'text_delta', delta: 'hello' }],
]);

const events = await collectAgentEvents(
  harness.run({
    model,
    messages: [{ role: 'user', content: 'Say hello.' }],
  }),
);
```

`createScriptedModel` 接收按模型轮次排列的 `ModelEvent` 数组，并公开 `requests` 供测试检查消息回填情况。
`replayAgentRun` 可使用相同的事件记录重新驱动模型；`collectAgentEvents` 可将异步事件流收集为数组。

Harness 测试应覆盖：

- 普通文本完成；
- 单次、多轮及同轮多个工具调用；
- 增量 Tool Call 聚合；
- 无效 JSON 参数和未注册工具；
- 工具抛错；
- `AbortSignal`；
- 最大迭代次数；
- 文本、工具和完成事件的顺序。

业务工具的正确性由工具包测试；Agent Profile 的 Prompt 和上下文正确性由具体宿主测试。

## 非目标

以下能力明确不属于 Harness：

- 通用 Agent 角色模型；
- 内置 Web Search、Shell、File、Image 或 Database Tool；
- 长期 Memory、Session Store 或跨请求恢复；
- 多 Agent handoff 和 delegation graph；
- Sandbox 或 workspace isolation；
- 业务级 guardrail、计费、权限和审批；
- `@morfact/core` 的 Workflow、Task、Retry、Wait、Loop 和 Outbox；
- SSE 或任何 Web 框架运行时依赖；
- 对某一个模型供应商的公共类型依赖。

## 开发

在 Morfact workspace 根目录执行：

```bash
pnpm --filter @morfact/harness typecheck
pnpm --filter @morfact/harness test
pnpm --filter @morfact/harness build
```

包的公共导出来自 `@morfact/harness`；测试辅助工具来自 `@morfact/harness/testing`。
