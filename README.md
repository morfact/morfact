# morfact

Composable, storage-agnostic workflow engine. Consumers install these packages:

| Package | Role |
|---|---|
| [`@morfact/core`](./core/README.md) | Runtime: compile DAGs, execute hooks, lease / retry / outbox, built-in spawn / gate / coordinator / loop |
| [`@morfact/memory`](./memory/README.md) | In-memory Store for tests and local use. **Not for production.** |
| [`@morfact/harness`](./harness/README.md) | Provider-neutral, event-first model and tool loop for bounded Agent runs. It does not own business state or workflow persistence. |

There is no official `@morfact/postgres`. A production host implements `WorkflowStateStore` and checks it with `@morfact/core/testing`.

`@morfact/harness` is orthogonal to `@morfact/core`: use Harness for the model/tool loop inside one run, and use Core for durable workflows, retries, waits, loops, and outbox semantics. Application-specific Agent Profiles, tools, permissions, and persistence remain in the consuming application.

Usage docs for each package live in that package's README and are shipped in its npm tarball:

- [`@morfact/core`](./core/README.md)
- [`@morfact/memory`](./memory/README.md)
- [`@morfact/harness`](./harness/README.md)
