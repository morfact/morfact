# @morfact/core

Composable, storage-agnostic workflow orchestration runtime.

Define DAGs in code or YAML, register hooks through plugins, and keep all state in a `WorkflowStateStore` you provide. The engine never reads `process.cwd()`, environment variables, or a database.

This is the **usage guide after install**. See [`../memory/README.md`](../memory/README.md) for `@morfact/memory`.

---

## 1. Install

```bash
npm install @morfact/core
npm install -D @morfact/memory
```

- Node `>=20`
- ESM only (`"type": "module"`)
- TypeScript: set `module` / `moduleResolution` to `NodeNext`

If the package is not on your registry yet, use a workspace or git dependency:

```json
{
  "dependencies": {
    "@morfact/core": "workspace:*"
  }
}
```

A host app only does three things: **choose or implement a Store, register plugins, and run a worker loop**.

---

## 2. Concepts

```text
createEngine({ store, plugins, lifecycle? })
        │
        ▼
   startRun(workflow@version)
        │  create Run + materialize Tasks
        ▼
   executeNextTask / executeTask
        │  claim → lifecycle → hook → lifecycle → commit / suspend
        ▼
   succeeded | waiting (human / child run / external poll)
   continue  (same task, next iteration; downstream stays locked)
```

| Term | Meaning |
|---|---|
| **Hook** | One executable unit. `name` is globally unique, e.g. `demo.greet@1` |
| **Lifecycle** | Cross-cutting observer on *every* task (`createEngine({ lifecycle })`). Not a business hook |
| **Workflow** | An immutable DAG snapshot, `name@version` |
| **Plugin** | A set of hooks + workflows, passed explicitly to `createEngine` |
| **Run** | One execution instance |
| **Task** | One node instance on a run |
| **Store** | Source of truth for state. The engine does not write a database itself |

Domain names (film, orders, …) belong in *your* hooks, not in the engine.

---

## 3. Five-minute start

```ts
import {
  createEngine,
  defineHook,
  definePlugin,
  defineWorkflow,
} from '@morfact/core';
import { MemoryWorkflowStateStore } from '@morfact/memory';

const greet = defineHook({
  name: 'demo.greet@1',
  async execute(_ctx, input: { name: string }) {
    return { status: 'succeeded', output: { hello: input.name } };
  },
});

const store = new MemoryWorkflowStateStore();
const engine = createEngine({
  store,
  plugins: [
    definePlugin({
      manifest: {
        manifestVersion: 1,
        name: 'demo',
        version: '1.0.0',
        engine: '^0.1.0',
      },
      hooks: [greet],
      workflows: [
        defineWorkflow({
          schemaVersion: 1,
          name: 'demo.hello',
          version: '1.0.0',
          nodes: [
            {
              id: 'greet',
              hook: greet.name,
              input: { name: '${run.input.name}' },
            },
          ],
        }),
      ],
    }),
  ],
});

await engine.validate();

const run = await engine.startRun({
  workflow: 'demo.hello@1.0.0',
  tenantKey: 'user-1',
  input: { name: 'Ada' },
});

const result = await engine.executeNextTask({ workerId: 'worker-1' });
// result.status === 'succeeded'
```

In production, do not call `executeNextTask` once. The worker should keep claiming until nothing is ready:

```ts
while (true) {
  const result = await engine.executeNextTask({
    workerId: process.env.WORKER_ID ?? 'worker-1',
  });
  if (!result) break; // no ready task right now
}
```

After a queue delivery, run a specific task:

```ts
await engine.executeTask({
  workerId: 'worker-1',
  taskId: event.taskId,
});
```

---

## 4. Writing hooks

```ts
const hook = defineHook({
  name: 'orders.charge@1',
  async execute(ctx, input: { orderRef: string }) {
    const charge = await ctx.services.payments.charge(input.orderRef);
    return {
      status: 'succeeded',
      output: charge,
      contextPatch: { lastChargeId: charge.id }, // optional; merged into run.context
    };
  },
});
```

`execute` may only return:

| `status` | Meaning | What the engine does next |
|---|---|---|
| `succeeded` | Done | `commitTaskSuccess`, unlock downstream |
| `waiting` + `reason: 'interaction'` | Wait for a human | Suspend and create a `TaskInteraction` (gate path) |
| `waiting` + `reason: 'child_run'` | Wait for a child run | Suspend; resume automatically when the child reaches a terminal state |
| `waiting` + `reason: 'external'` | Wait for an external system | Suspend; later `completePollingTask` / `failPollingTask` |

Thrown errors become `commitTaskFailure`. Do not return the legacy `{ success: true }` shape.

Hooks may use `ctx.run` / `ctx.task` / `ctx.services` / `ctx.signal` / `ctx.now()`.  
Hooks must not: mutate the Store directly, guess canonical business IDs, or read `process.env` (inject config through a plugin factory).

Put business dependencies on `createEngine({ services })` or in a plugin factory closure:

```ts
export function createOrdersPlugin(deps: { payments: PaymentsPort }) {
  return definePlugin({
    manifest: { manifestVersion: 1, name: 'orders', version: '1.0.0', engine: '^0.1.0' },
    hooks: [
      defineHook({
        name: 'orders.charge@1',
        async execute(_ctx, input: { orderRef: string }) {
          const charge = await deps.payments.charge(input.orderRef);
          return { status: 'succeeded', output: charge };
        },
      }),
    ],
    workflows: [/* ... */],
  });
}
```

---

## 5. Defining flows in YAML

```yaml
apiVersion: flow.v1
kind: Flow
metadata:
  name: demo.order
  version: 1.0.0
spec:
  nodes:
    - id: charge
      type: business
      hooks:
        executor:
          use: orders.charge@1
      inputTemplate:
        orderRef: "{{params.orderRef}}"
    - id: done
      type: completed
      hooks:
        executor:
          use: "@morfact/core/completed"
  edges:
    - from: charge
      to: done
```

The host loads and registers flows (**you** read the file and pass an absolute path or string into the engine):

```ts
import fs from 'node:fs';
import {
  loadFlowFromYaml,
  compileFlow,
  definePlugin,
  FlowRegistry,
} from '@morfact/core';

const availableHooks = new Set([
  'orders.charge@1',
  '@morfact/core/join',
  '@morfact/core/gate',
  '@morfact/core/completed',
  '@morfact/core/spawn',
  '@morfact/core/coordinator',
]);

// Single file
const flow = loadFlowFromYaml(fs.readFileSync('/abs/flows/demo.order.1.0.0.yml', 'utf8'));
const { definition } = compileFlow(flow, availableHooks, () => crypto.randomUUID());

// Or scan a directory (`templatesDir` must be an absolute path the host resolved)
const registry = new FlowRegistry({ templatesDir: '/abs/path/to/templates' });

const flowsPlugin = definePlugin({
  manifest: { manifestVersion: 1, name: 'flows', version: '1.0.0', engine: '^0.1.0' },
  hooks: [],
  workflows: [definition],
});
```

`type: spawn | gate | coordinator | join | completed` binds to the matching built-in hook automatically. You do not need `hooks.executor.use` for those types (the type wins if both are set).

Template expressions:

- `{{params.xxx}}` / `${run.input.xxx}` — run input at `startRun`
- `{{tasks.nodeId.output.xxx}}` — upstream output (runtime only; compile-time `when` cannot see executed tasks)

---

## 6. Built-in hooks

| Hook | YAML `type` | Behavior |
|---|---|---|
| `@morfact/core/spawn` | `spawn` | Creates a child run; parent task is `waiting` / `child_run`; the engine resumes the parent when the child finishes |
| `@morfact/core/coordinator` | `coordinator` | Appends subtasks from `taskGroupTemplates`. The Store **must** implement `appendTasks` |
| `@morfact/core/join` | `join` | Pass-through once inbound dependencies are cleared |
| `@morfact/core/gate` | `gate` | Waits for humans while `interaction.schema.required` is missing, and calls `createInteraction` |
| `@morfact/core/completed` | `completed` | Terminal marker |
| `@morfact/core/loop` | `loop` | Runs one body hook per iteration on the **same** task. Re-queues via `commitTaskContinue`; does not add graph cycles |

### 6.1 Gate (human confirmation)

YAML:

```yaml
- id: review
  type: gate
  interaction:
    kind: approval
    schema:
      required: [approved]
      properties:
        approved: { type: boolean }
  hooks:
    executor:
      use: "@morfact/core/gate"
```

After the user submits:

```ts
await store.submitInteraction({
  interactionId,
  taskId,
  token,
  payload: { approved: true },
  now: new Date(),
});
await engine.resumeTask(taskId, `gate:${taskId}`, { approved: true });
await engine.executeTask({ workerId: 'worker-1', taskId });
```

The third argument to `resumeTask` is merged into task input. The gate proceeds once it sees `approved`.

### 6.2 Spawn (child flow)

The child workflow must be registered on the **same** engine. Spawn resolves `name@version` only.

Prefer an explicit template name:

```yaml
- id: child
  type: spawn
  spawn:
    childTemplateName: demo.child
    childTemplateVersion: "1.0"
```

If you map `pipelineId` values, **the host loads the registry and injects it** (the engine never reads the disk):

```ts
import {
  createEngine,
  parseChildTemplateRegistryYaml,
} from '@morfact/core';

const childTemplateRegistry = parseChildTemplateRegistryYaml(
  fs.readFileSync('/abs/config/workflow-pipeline-registry.yaml', 'utf8'),
  { conventionTemplateNamePrefix: 'app.branch.' },
);

createEngine({ store, plugins, childTemplateRegistry });
```

You can skip the file and pass an object: `{ aliases, overrides, conventionTemplateNamePrefix, defaultsVersion }`.

### 6.3 Loop (unknown-N iteration)

Use a loop when N is not known at compile time (pagination, agent turns). It is **one task / one `logicalKey`**, not N coordinator subtasks and not a graph back-edge. `compileFlow` still rejects cycles.

YAML:

```yaml
- id: paginate
  type: loop
  loop:
    body: book.paginate_page@1
    maxIterations: 500
    onMaxIterations: fail   # default; use succeed when a cap is a valid stop
```

The body hook receives `input.loop = { iteration, checkpoint }` and must return:

```ts
{ halt: false, checkpoint, output? }  // next round; checkpoint is opaque to the engine
{ halt: true, output?, checkpoint? }  // node completes
```

Throw retries the **same** iteration through existing `maxAttempts`. The next iteration starts only after `{ halt: false, checkpoint }`.

`maxIterations` is a budget, not a correctness signal. Default `onMaxIterations: fail` is a fuse. Precision / agent flows that may legally stop at the cap should set `succeed`. Final output always includes `haltReason` (`halt` | `max_iterations`) and `truncated` when the cap was hit.

This is not polling: each continue is a real commit of progress, the node stays incomplete, and the engine re-enqueues **the same task**. Do not reuse `completePollingTask`. Production stores must implement `commitTaskContinue` (re-queue, persist `meta.loopState`, do **not** unlock downstream).

Body hook example (pagination). Register this hook on the same engine; `loop.body` is its `name`:

```ts
import { defineHook } from '@morfact/core';

const paginatePage = defineHook({
  name: 'book.paginate_page@1',
  async execute(_ctx, input: {
    bookId: string;
    loop?: { iteration: number; checkpoint?: { cursor: string | null } };
  }) {
    const cursor = input.loop?.checkpoint?.cursor ?? null;
    const page = await fetchPage({ bookId: input.bookId, cursor });

    if (!page.nextCursor) {
      return {
        status: 'succeeded' as const,
        output: {
          halt: true,
          checkpoint: { cursor: null },
          output: { items: page.items },
        },
      };
    }

    return {
      status: 'succeeded' as const,
      output: {
        halt: false,
        checkpoint: { cursor: page.nextCursor },
        output: { items: page.items },
      },
    };
  },
});
```

Without an outbox worker, drain continues in-process:

```ts
let result;
do {
  result = await engine.executeNextTask({ workerId: 'w1' });
} while (result?.status === 'continue');
```

`executeTask` / `executeNextTask` returns `status: 'continue'` when the node is not done. A production worker should treat that like any other re-queued ready task (the Store writes outbox on `commitTaskContinue`).

---

## 7. Queue integration (outbox)

The engine does not bundle Cloud Tasks or Redis. The pattern is: the Store writes an outbox row when a task becomes ready → the dispatcher publishes an envelope that **only contains `taskId`** → the worker `claim`s / `executeTask`s.

```ts
import { createOutboxDispatcher } from '@morfact/core';

const dispatcher = createOutboxDispatcher({
  store,
  workerId: 'dispatcher-1',
  destination: 'workflow.tasks',
  publisher: {
    async publish(event) {
      await queue.send({
        schemaVersion: 1,
        taskId: event.payload.taskId,
        availableAt: event.availableAt,
      });
    },
  },
});

await dispatcher.dispatch(); // tick on an interval
```

Do not put a full Task/Run snapshot in the message body and treat it as authoritative.

---

## 8. Task lifecycle

Engine-level callbacks that run for **every** claimed task. Use them for billing, metrics, audit, quotas — not for node business logic (that stays in `defineHook`).

Register only on the engine (plugins cannot attach lifecycle):

```ts
import { createEngine, defineLifecycle } from '@morfact/core';

const billing = defineLifecycle({
  name: 'billing',
  order: 10, // lower runs first; default 0
  async beforeExecute(event) {
    // Hold / reject before the business hook runs.
    // May return { input } to rewrite hook input, or { shortCircuit: result }
    // to skip the hook and go straight to commit.
  },
  async beforeCommit(event) {
    // Business result is known: event.result + event.commitKind
    // ('success' | 'waiting' | 'continue'). Throw to fail the task
    // (commitTaskFailure + onTaskFailed) instead of committing success.
  },
  async onTaskFailed(event) {
    // Release a hold. Soft phase: errors are swallowed via onLifecycleError.
  },
  async afterCommit(event) {
    // Metrics / audit only. Soft phase — must not be the source of truth
    // for money or other critical side effects.
  },
});

createEngine({ store, plugins, lifecycle: [billing] });
```

Pipeline (hard phases throw → task failure):

```text
claim
  → onTaskClaimed          (soft)
  → beforeExecute          (hard; may rewrite input / shortCircuit)
  → hook.execute           (skipped if shortCircuit)
  → afterExecute           (hard; may rewrite result)
  → beforeCommit           (hard; sees commitKind)
  → store commit / suspend
  → afterCommit            (soft)
```

On any hard-phase / heartbeat failure before a successful store write: `commitTaskFailure`, then `onTaskFailed` (soft).

Multiple lifecycles run in `order` ascending (registration order ties). Rewrite results pipeline: later handlers see earlier rewrites; `shortCircuit` stops further `beforeExecute` handlers and skips the business hook (still runs `afterExecute` → commit).

---

## 9. Engine API

```ts
await engine.validate();

const run = await engine.startRun({
  workflow: 'demo.hello@1.0.0',
  tenantKey: 'user-1',
  input: { /* ... */ },
});

await engine.executeNextTask({ workerId: 'w1' });
await engine.executeTask({ workerId: 'w1', taskId });

await engine.resumeTask(taskId, resumeToken, { /* merged into task.input */ });
await engine.cancelTask(taskId);
await engine.cancelRun(runId);
await engine.completePollingTask(taskId, output);
await engine.failPollingTask(taskId, { message: 'timeout' });

await engine.getRun(runId);
await engine.listTasks(runId);
await engine.listRuns({ tenantKey: 'user-1', status: 'running' });
```

`createEngine` options: `store` (required), `plugins`, `lifecycle`, `services`, `childTemplateRegistry`, `now`, `idGenerator`.

---

## 10. Implementing a production Store

Implement `WorkflowStateStore` with `protocolVersion: 1`. Claim, commit, and downstream unlock must be atomic.

```ts
import { storeConformanceCases } from '@morfact/core/testing';

for (const testCase of storeConformanceCases) {
  await testCase.run(async () => new MyPostgresStore());
}
```

| Capability | Notes |
|---|---|
| 24 required methods | Run / Task / Outbox / Interaction |
| `appendTasks?` | Required for `@morfact/core/coordinator`; missing it throws `StoreProtocolError` |
| `commitTaskContinue?` | Required for `@morfact/core/loop`; re-queue the same task and persist checkpoint; must not unlock downstream |
| `listRuns({ parentTaskId })` | Used by spawn to find an existing child run |

For local/tests, use `new MemoryWorkflowStateStore()`. It already implements `appendTasks`, `commitTaskContinue`, parent/child runs, and merging `input` on resume.

---

## 11. Public entrypoints

Only these are supported:

```ts
import { createEngine, defineLifecycle } from '@morfact/core';
import { storeConformanceCases } from '@morfact/core/testing';
```

Do not `import '@morfact/core/dist/...'`.

Current version is `0.1.2`. Store protocol and definition `schemaVersion` are both `1`. Incompatible combinations must fail in `createEngine` / `validate()` — no silent fallback.
