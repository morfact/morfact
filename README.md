# morfact

Composable, storage-agnostic workflow engine. Consumers install these two packages:

| Package | Role |
|---|---|
| [`@morfact/core`](./core/README.md) | Runtime: compile DAGs, execute hooks, lease / retry / outbox, built-in spawn / gate / coordinator / loop |
| [`@morfact/memory`](./memory/README.md) | In-memory Store for tests and local use. **Not for production.** |

There is no official `@morfact/postgres`. A production host implements `WorkflowStateStore` and checks it with `@morfact/core/testing`.

**Usage docs (what to write after install) live in [`core/README.md`](./core/README.md).** That file is also shipped in the npm tarball.
