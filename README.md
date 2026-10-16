# Orbit Saga

`orbit-saga` coordinates ordered asynchronous side effects with durable checkpoints and reverse
compensation. It is a capability package: Core remains responsible for application lifecycle and
dependency injection, while a separately installed store adapter owns persistence.

```bash
python -m pip install orbit-saga orbit-saga-sql orbit-sql orbit-sql-sqlite
```

```python
from orbit import Application, ApplicationConfig
from orbit_saga import SagaCoordinator, SagaDefinition, SagaRecoveryWorker, SagaStep
from orbit_saga_sql import SQLSagaStore

async def reserve(data, context):
    reservation_id = await inventory.reserve(
        data["order_id"], idempotency_key=context.idempotency_key
    )
    return {"reservation_id": reservation_id}

async def release(data, output, context):
    await inventory.release(
        output["reservation_id"], idempotency_key=context.idempotency_key
    )

definition = SagaDefinition(
    "checkout",
    (SagaStep("reserve", reserve, release),),
    revision="1",
)
store = SQLSagaStore(sql_database)
await store.initialize_schema()  # local development; use a reviewed migration in production
coordinator = SagaCoordinator(store)
application = Application(ApplicationConfig(name="checkout"))
result = await coordinator.start(
    definition, {"order_id": "order-42"}, idempotency_key="request-42"
)

recovery = SagaRecoveryWorker(
    coordinator,
    store,
    {(definition.name, definition.revision): definition},
    poll_interval_seconds=5,
    operation_timeout_seconds=60,
    batch_size=100,
)
application.tasks.register("saga-recovery", recovery.run)
```

The SQL adapter is needed for durable multi-process recovery. `InMemorySagaStore` is bounded and
intended only for tests or single-process development. Every forward and compensation action must
be idempotent: the process may stop after a side effect succeeds but before its result is saved.
The context supplies a stable per-step idempotency key for that recovery case. Delivery is at least
once; this package does not promise exactly-once effects or distributed ACID transactions.

When a step fails, already completed steps are compensated in reverse order. A failed compensation
is stored as `compensation_required`; `SagaRecoveryWorker` retries it using the exact registered
definition revision. The worker scans bounded pages and enforces per-instance timeouts. Multiple
workers may see the same candidate; optimistic compare-and-swap selects the state-transition winner.
Operations must remain idempotent because effects can repeat across crash windows. The original input and step outputs
are persisted as bounded JSON. Keep credentials and unnecessary personal data out of that state,
and use the database's approved encryption, access-control, backup, and retention policies.

## Documentation

See the [architecture](docs/architecture/overview.md),
[operations and recovery](docs/operations/README.md), [security](docs/security/overview.md), and
[development](docs/development/README.md) guides. Public APIs are alpha and may change before a
stable release.

## Local checks

```bash
python -m pip install -e '.[dev]'
pytest
ruff check src tests
ruff format --check src tests
mypy
```

Licensed under Apache-2.0.
