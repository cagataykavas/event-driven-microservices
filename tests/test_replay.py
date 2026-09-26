from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from event_platform.domain import CreateOrder, EventType
from event_platform.orders import OrderService
from event_platform.relay import OutboxRelay, RelayPolicy
from event_platform.replay import (
    DeadLetterReplayer,
    ReplayArtifactError,
    ReplayPolicy,
    ReplayRejected,
    ReplayRequest,
    ReplayTarget,
)
from event_platform.storage import Database


@dataclass
class Clock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def dead_letter(database: Database, clock: Clock, *, order_id: str = "order-1") -> str:
    receipt = OrderService(database, clock=clock).create(
        CreateOrder.build(
            order_id=order_id,
            amount="10.00",
            currency="EUR",
            customer_id=f"customer-{order_id}",
        ),
        idempotency_key=f"command-{order_id}",
    )
    relay = OutboxRelay(
        database,
        lambda *_: (_ for _ in ()).throw(RuntimeError("broker unavailable")),
        worker_id="relay-1",
        clock=clock,
        policy=RelayPolicy(max_attempts=1),
    )
    assert relay.run_once().dead_lettered == 1
    return receipt.event_id


def policy(**overrides: object) -> ReplayPolicy:
    values: dict[str, object] = {
        "allowed_event_schemas": {EventType.ORDER_CREATED: frozenset({1})},
        "min_dead_age": timedelta(minutes=5),
    }
    values.update(overrides)
    return ReplayPolicy(**values)  # type: ignore[arg-type]


def request_for(replayer: DeadLetterReplayer, event_ids: list[str]) -> ReplayRequest:
    evidence = replayer.inspect(event_ids)
    return ReplayRequest(
        actor="oncall@example.invalid",
        change_ticket="INC-1234",
        targets=tuple(
            ReplayTarget(
                event_id=event_id,
                expected_payload_sha256=item.payload_sha256,
                expected_failure_sha256=item.failure_sha256,
            )
            for event_id, item in zip(event_ids, evidence, strict=True)
        ),
    )


def codes(error: ReplayRejected) -> set[str]:
    return {finding.code for finding in error.findings}


def test_replay_atomically_requeues_reviewed_dead_letter(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)

    report = replayer.replay(request_for(replayer, [event_id]))

    assert report.event_count == 1
    assert report.event_refs == (f"evt_{hashlib.sha256(event_id.encode()).hexdigest()[:16]}",)
    assert report.to_dict()["status"] == "accepted"
    outbox = database.connection.execute(
        "SELECT status, attempts, available_at, last_error FROM outbox WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    assert tuple(outbox) == ("pending", 0, clock.value.isoformat(), None)
    dead = database.connection.execute(
        "SELECT status, replay_id, replayed_at FROM dead_letters WHERE event_id = ?", (event_id,)
    ).fetchone()
    assert tuple(dead) == ("replayed", report.replay_id, clock.value.isoformat())
    receipt = database.connection.execute("SELECT * FROM dead_letter_replays").fetchone()
    assert receipt["actor_hash"] == hashlib.sha256(b"oncall@example.invalid").hexdigest()
    assert receipt["change_ticket_hash"] == hashlib.sha256(b"INC-1234").hexdigest()
    stored = json.loads(receipt["evidence_json"])
    assert stored["evidence_sha256"] == report.evidence_sha256
    assert "order-1" not in receipt["evidence_json"]


def test_replayed_event_retains_id_and_can_be_published(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    replayer.replay(request_for(replayer, [event_id]))
    published: list[str] = []

    result = OutboxRelay(
        database,
        lambda _topic, envelope: published.append(envelope.event_id),
        worker_id="relay-2",
        clock=clock,
    ).run_once()

    assert result.published == 1
    assert published == [event_id]


def test_replay_is_all_or_nothing_when_one_target_is_stale(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    first = dead_letter(database, clock, order_id="one")
    second = dead_letter(database, clock, order_id="two")
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    request = request_for(replayer, [first, second])
    with database.transaction() as connection:
        connection.execute(
            "UPDATE dead_letters SET reason = 'different failure' WHERE event_id = ?", (second,)
        )

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request)

    assert codes(captured.value) == {"EXPECTED_FAILURE_MISMATCH"}
    assert {
        row["status"] for row in database.connection.execute("SELECT status FROM outbox").fetchall()
    } == {"dead"}
    assert (
        database.connection.execute("SELECT COUNT(*) FROM dead_letter_replays").fetchone()[0] == 0
    )


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(60, "COOLDOWN_NOT_ELAPSED"), (31 * 86400, "DEAD_LETTER_EXPIRED")],
)
def test_replay_enforces_age_window(database: Database, seconds: int, expected: str) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    request = request_for(replayer, [event_id])
    clock.advance(seconds)

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request)

    assert codes(captured.value) == {expected}


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        ("schema", "SCHEMA_VERSION_NOT_ALLOWED"),
        ("type", "EVENT_TYPE_MISMATCH"),
        ("attempts", "ATTEMPT_COUNT_MISMATCH"),
        ("payload-copy", "PAYLOAD_COPY_MISMATCH"),
        ("payload-hash", "PAYLOAD_HASH_MISMATCH"),
        ("lease", "DEAD_LETTER_HAS_LEASE"),
    ],
)
def test_replay_fails_closed_on_corrupt_or_inconsistent_state(
    database: Database, mutate: str, expected: str
) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    request = request_for(replayer, [event_id])
    statements = {
        "schema": ("UPDATE outbox SET schema_version = 2 WHERE event_id = ?",),
        "type": ("UPDATE dead_letters SET event_type = 'other.v1' WHERE event_id = ?",),
        "attempts": ("UPDATE dead_letters SET attempts = 9 WHERE event_id = ?",),
        "payload-copy": ("UPDATE dead_letters SET payload_json = '{}' WHERE event_id = ?",),
        "payload-hash": ("UPDATE outbox SET payload_hash = 'bad' WHERE event_id = ?",),
        "lease": ("UPDATE outbox SET lease_owner = 'unexpected' WHERE event_id = ?",),
    }
    with database.transaction() as connection:
        connection.execute(statements[mutate][0], (event_id,))

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request)

    assert codes(captured.value) == {expected}


def test_replay_rejects_changed_reviewed_digests(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    evidence = replayer.inspect([event_id])[0]
    changed = ReplayRequest(
        actor="oncall@example.invalid",
        change_ticket="INC-1234",
        targets=(
            ReplayTarget(
                event_id=event_id,
                expected_payload_sha256="0" * 64,
                expected_failure_sha256=evidence.failure_sha256,
            ),
        ),
    )

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(changed)

    assert codes(captured.value) == {"EXPECTED_PAYLOAD_MISMATCH"}


def test_repeated_request_cannot_reset_published_event(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    request = request_for(replayer, [event_id])
    replayer.replay(request)

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request)

    assert codes(captured.value) == {"REQUEST_ALREADY_APPLIED"}


def test_batch_and_payload_budgets_are_enforced(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(
        database,
        policy(max_payload_bytes=10, max_total_payload_bytes=10),
        clock=clock,
    )
    request = request_for(replayer, [event_id])

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request)

    assert codes(captured.value) == {"PAYLOAD_LIMIT_EXCEEDED"}


def test_aggregate_payload_budget_is_enforced(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_ids = [
        dead_letter(database, clock, order_id="budget-one"),
        dead_letter(database, clock, order_id="budget-two"),
    ]
    clock.advance(301)
    row_bytes = database.connection.execute(
        "SELECT length(CAST(payload_json AS BLOB)) FROM outbox WHERE event_id = ?", (event_ids[0],)
    ).fetchone()[0]
    replayer = DeadLetterReplayer(
        database,
        policy(max_payload_bytes=row_bytes + 1, max_total_payload_bytes=row_bytes + 1),
        clock=clock,
    )

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request_for(replayer, event_ids))

    assert codes(captured.value) == {"TOTAL_PAYLOAD_LIMIT_EXCEEDED"}


def test_invalid_request_and_policy_are_rejected_before_database_access() -> None:
    with pytest.raises(ReplayArtifactError, match="duplicate event_id"):
        target = ReplayTarget("event-1", "0" * 64, "1" * 64)
        ReplayRequest("actor", "ticket", (target, target))
    with pytest.raises(ReplayArtifactError, match="timezone-aware"):
        database = Database()
        database.initialize()
        try:
            DeadLetterReplayer(
                database,
                policy(min_dead_age=timedelta(0)),
                clock=lambda: datetime(2026, 9, 26),  # noqa: DTZ001 - intentional naive clock.
            ).replay(ReplayRequest("actor", "ticket", (ReplayTarget("x", "0" * 64, "1" * 64),)))
        finally:
            database.close()


def test_noncanonical_payload_numbers_fail_closed(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 26, tzinfo=UTC))
    event_id = dead_letter(database, clock)
    clock.advance(301)
    replayer = DeadLetterReplayer(database, policy(), clock=clock)
    request = request_for(replayer, [event_id])
    with database.transaction() as connection:
        connection.execute(
            "UPDATE outbox SET payload_json = ?, payload_hash = ? WHERE event_id = ?",
            ('{"value":NaN}', "irrelevant", event_id),
        )
        connection.execute(
            "UPDATE dead_letters SET payload_json = ? WHERE event_id = ?",
            ('{"value":NaN}', event_id),
        )

    with pytest.raises(ReplayRejected) as captured:
        replayer.replay(request)

    assert codes(captured.value) == {"PAYLOAD_INVALID"}


def test_initialize_additively_migrates_legacy_dead_letter_table(tmp_path: object) -> None:
    path = str(tmp_path) + "/legacy.sqlite3"
    legacy = sqlite3.connect(path)
    legacy.execute(
        "CREATE TABLE dead_letters (event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, "
        "payload_json TEXT NOT NULL, attempts INTEGER NOT NULL, reason TEXT NOT NULL, "
        "failed_at TEXT NOT NULL)"
    )
    legacy.commit()
    legacy.close()

    with Database(path) as database:
        columns = {
            row["name"] for row in database.connection.execute("PRAGMA table_info(dead_letters)")
        }
        assert {"status", "replay_id", "replayed_at"} <= columns
        assert (
            database.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'dead_letter_replays'"
            ).fetchone()
            is not None
        )
