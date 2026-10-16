# Security guidance

Saga state is application data and may outlive the request that created it. Never put passwords,
access tokens, secret material, payment credentials, or unnecessary personal data into input or step
outputs. Store opaque references where a later step needs a secret, then resolve it through
`orbit-secrets` at execution time.

Use a least-privilege database identity, encrypted transport, database encryption at rest, reviewed
backup access, and a retention policy that matches the business recovery window. The SQL adapter
does not encrypt payloads or rotate credentials. Keep handler exceptions and state JSON out of logs;
the coordinator emits safe errors without copying provider messages. Audit access to the durable
table using platform database controls.

This guidance does not certify a deployment, database, key-management setup, or operator workflow.
