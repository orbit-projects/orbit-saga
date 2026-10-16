# Operations and recovery

## Store selection

Use a durable adapter such as `orbit-saga-sql` for production. `InMemorySagaStore` loses all active
instances on process restart, is limited to one process, and is provided for tests and development
only. The coordinator does not own or close its store. Register the database and store with the
application lifecycle and close the database only after in-flight saga work has drained.

## Recovery runbook

1. Register `SagaRecoveryWorker` with Core's `TaskSupervisor`, using a durable `SagaStore` and only
   the definition revisions deployed by the application.
2. Set bounded page size, poll interval, and per-instance timeout values that fit downstream service
   deadlines and idempotency retention.
3. Alert on generic failed-recovery and unknown-revision warning counts. Query individual records
   through a restricted operator identity when diagnosis is needed.
4. For manual recovery, load the exact deployed definition revision and check downstream idempotency
   records and service health before calling `resume()`.
5. Escalate repeated compensation failures; do not delete state to hide unfinished effects.

The worker polls bounded pages and retries incomplete states, including failed compensation. It does
not lease rows: duplicate scans are expected, and compare-and-swap rejects stale transitions. This
does not provide exactly-once behavior or protect a non-idempotent remote side effect. Tune polling
to avoid hammering an unhealthy dependency and provide operator alerting for persistent failures.

## Bounds and availability

Saga input and outputs are finite JSON objects with a 1 MiB encoded state limit, nesting depth 64,
and at most 100,000 values. Definitions contain at most 128 uniquely named steps. Store outages
prevent state transitions; do not continue the workflow after a failed persistence call. Downstream
actions may still have completed, so recover using their stable idempotency keys.

No live PostgreSQL or MySQL validation is claimed. Run provider-specific concurrency, failover,
backup, restore, and retention checks before production use.
