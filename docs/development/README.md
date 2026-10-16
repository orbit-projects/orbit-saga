# Development and validation

The capability package has no runtime dependency beyond Python 3.11–3.14. Its tests exercise
bounded immutable state, durable transition sequencing, idempotency, cancellation recovery,
compensation order, compare-and-swap recovery scans, unknown definition revisions, and cooperative
operation timeouts with `InMemorySagaStore`.

The SQL adapter tests use the local SQLite provider. PostgreSQL and MySQL behavior needs a live
provider matrix before publication. Tests do not claim exactly-once effects or prove application
handlers are idempotent.

```bash
python -m pip install -e '.[dev]'
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Keep Core, capability, and provider boundaries intact. Add regression tests for state transitions,
resource ownership, cancellation, and sanitized failures, and update these guides when the public
contract changes.
