# Roadmap

This roadmap records areas for review without delivery dates or release promises.

- Validate SQL compare-and-swap behavior against live PostgreSQL and MySQL providers.
- Design an operator-facing recovery workflow after real deployments define their authorization and
  retention requirements.
- Consider a lease-based recovery worker only after the store contract has production evidence.

The current alpha does not include automatic scanning/retry, exactly-once execution, a schema
registry, encryption, or an admin UI.
