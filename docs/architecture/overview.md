# Architecture and contracts

`orbit-saga` owns an application-facing step model, immutable bounded execution snapshots, a
provider-neutral asynchronous `SagaStore` contract, and the coordinator state machine. Provider SDKs,
database schema, connection settings, and provider errors belong in separately installed adapters.
Core owns application composition and lifecycle; the application supplies the selected store.

## State transitions

```text
pending → running → completed
                  ↘ compensating → failed
                                  ↘ compensation_required → compensating
```

The coordinator persists `running` intent before invoking a side effect, then stores its bounded
JSON output before moving to the next step. Compensation runs in reverse completion order and saves
progress after each successful action. Every write uses optimistic version checking. The store
uniquely owns `(definition, idempotency_key)` across revisions. Increment the explicit definition
revision when changing step meaning or order; an existing key then fails closed instead of starting
a second workflow under the new code.

## Delivery and recovery semantics

The coordinator offers at-least-once invocation. An operation can have succeeded remotely even if
the process was cancelled or stopped before the result checkpoint. Forward and compensation
handlers must therefore use `SagaStepContext.idempotency_key` or their provider's equivalent. The
package cannot atomically commit an external side effect and a local database row.
`SagaRecoveryWorker` scans bounded pages and resumes only revisions the application explicitly
registers. Its poll interval and per-instance deadline are finite. The store page is not an
exclusive claim: concurrent workers may read a candidate, and compare-and-swap rejects stale state
transitions before their actions run. A crash after a remote effect can still cause a repeat, so
handlers must honor their idempotency key. Unknown revisions remain unchanged for operator review;
failed compensation remains durable for inspection and retry.

## Persistence

`SagaStore` requires a unique idempotency key and compare-and-swap updates. `InMemorySagaStore` is
bounded but process-local and is not suitable for production. `orbit-saga-sql` implements the
contract over `orbit-sql`; its SQL schema uses portable scalar columns and bounded JSON state. The
SQL adapter implements deterministic, bounded scans of resumable states. Run
the checked-in migration with `orbit-migrations` or a reviewed deployment migration. The explicit
`initialize_schema()` helper is for local development, not automatic production startup.

The capability does not encrypt state itself. Store only the minimum required state and apply
database encryption at rest, access policy, backup, and retention controls. Applications choose
whether to run the optional recovery worker; no operator UI is included.
