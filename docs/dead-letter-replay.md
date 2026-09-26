# Dead-letter replay admission

Dead-letter queues are evidence, not ordinary work queues. Blind redrive can reintroduce stale
schemas, repeat a poison event indefinitely, or release a large backlog as a replay storm. The
`DeadLetterReplayer` therefore requeues only an explicit, bounded set of events in one database
transaction.

## Workflow

1. An operator inspects named event IDs. Inspection returns payload and failure SHA-256 digests,
   not the failure text or payload.
2. The operator records those digests in a change-reviewed `ReplayRequest`.
3. Admission verifies the dead-letter/outbox pair, current allowed schema, exact payload and failure
   evidence, cooldown/expiry window, attempt ceiling, and per-event/aggregate byte budgets.
4. Every target is requeued atomically or none are. Accepted dead letters become immutable replay
   evidence and an audit receipt binds hashed actor and change-ticket identities to the request.

```python
from datetime import timedelta

from event_platform.replay import DeadLetterReplayer, ReplayPolicy, ReplayRequest, ReplayTarget

policy = ReplayPolicy(
    allowed_event_schemas={"order.created.v1": frozenset({1})},
    min_dead_age=timedelta(minutes=10),
    max_dead_age=timedelta(days=7),
)
replayer = DeadLetterReplayer(database, policy)
evidence = replayer.inspect([event_id])[0]
report = replayer.replay(
    ReplayRequest(
        actor="oncall@example.invalid",
        change_ticket="INC-1234",
        targets=(
            ReplayTarget(
                event_id=event_id,
                expected_payload_sha256=evidence.payload_sha256,
                expected_failure_sha256=evidence.failure_sha256,
            ),
        ),
    )
)
```

`ReplayRejected.findings` contains stable codes and hashed event references. Malformed requests and
policies raise `ReplayArtifactError`; policy or state failures raise `ReplayRejected`. A repeated
request is rejected rather than silently resetting a live or already-published event.

## Operational boundaries

- Authentication and approval live outside this package. The caller must authorize the actor and
  change ticket before admission.
- The allowlist states which schema versions remain deployable; it does not prove semantic
  compatibility. Pair it with consumer contract tests and historical-payload replay.
- A payload digest proves the reviewed bytes are unchanged, not that the event is safe or correct.
- This SQLite implementation demonstrates atomic state transitions. A production broker needs an
  equivalent transaction/fencing mechanism, rate-limited release, and monitoring for replay age,
  rejection reason, throughput, and renewed dead-lettering.
- Replaying retains the original event ID. Consumers still need durable inbox idempotency because a
  publish may have succeeded before the relay observed a failure.
- Receipt rows are tamper-evident only relative to the database boundary. Sign and export them to an
  append-only evidence store when stronger provenance is required.
- Plain SHA-256 references reduce accidental disclosure but do not anonymize low-entropy actor,
  ticket, or failure values. Use keyed digests or governed opaque identifiers where dictionary
  inference is in scope.
