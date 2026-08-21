# @morfact/memory

In-memory `WorkflowStateStore` for `@morfact/core`. Use it in tests, examples, and local development.

It does not persist across process restarts. **Do not use it as a production store.**

## Install

```bash
npm install -D @morfact/memory
```

`@morfact/core` is a peer dependency.

## Usage

```ts
import { createEngine, defineHook, definePlugin, defineWorkflow } from '@morfact/core';
import { MemoryWorkflowStateStore } from '@morfact/memory';

const store = new MemoryWorkflowStateStore();
const engine = createEngine({ store, plugins: [/* ... */] });
```

Implements protocol v1 claim / lease heartbeat / DAG unblocking, plus:

- `createChildRun` (writes `parentTaskId` / `parentRunId` / `rootRunId`)
- `listRuns({ parentTaskId })`
- `appendTasks` (required by `@morfact/core/coordinator`)
- `resumeTask` sets the task to `ready` and merges `input`

For hooks, YAML, gates, spawn, outbox, and Store contracts, see the [`@morfact/core` README](../core/README.md).
