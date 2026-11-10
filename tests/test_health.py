from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from event_platform.domain import CreateOrder
from event_platform.health import OutboxHealthPolicy, audit_outbox_health, main
from event_platform.orders import OrderService
from event_platform.relay import OutboxRelay, RelayPolicy
from event_platform.storage import Database


@dataclass
class Clock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value


def seed(database: Database, clock: Clock, order_id: str = "order-1") -> str:
    receipt = OrderService(database, clock=clock).create(
        CreateOrder.build(
            order_id=order_id,
            amount="10.00",
            currency="EUR",
            customer_id="customer-1",
        ),
        idempotency_key=f"command-{order_id}",
    )
    return receipt.event_id


def test_empty_outbox_is_healthy_and_privacy_reduced(database: Database) -> None:
    now = datetime(2026, 11, 10, tzinfo=UTC)

    first = audit_outbox_health(database, observed_at=now)
    second = audit_outbox_health(database, observed_at=now)
    payload = first.to_dict()

    assert first == second
    assert first.schema_version == 1
    assert first.healthy
    assert first.pending == first.ready == first.dead_letter_records == 0
    assert len(first.evidence_sha256) == 64
    assert "event_id" not in json.dumps(payload)
    assert "payload" not in json.dumps(payload)


def test_pending_age_and_ready_thresholds_are_enforced(database: Database) -> None:
    created = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)
    seed(database, Clock(created))
    policy = OutboxHealthPolicy(max_pending=0, max_ready=0, max_oldest_pending_seconds=60)

    report = audit_outbox_health(
        database,
        observed_at=created + timedelta(seconds=120),
        policy=policy,
    )

    assert not report.healthy
    assert report.pending == report.ready == 1
    assert report.oldest_pending_age_seconds == 120
    assert set(report.findings) >= {
        "PENDING_LIMIT_EXCEEDED",
        "READY_LIMIT_EXCEEDED",
        "PENDING_AGE_LIMIT_EXCEEDED",
    }


def test_active_and_expired_leases_are_distinguished(database: Database) -> None:
    now = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)
    event_id = seed(database, Clock(now))
    with database.transaction() as connection:
        connection.execute(
            "UPDATE outbox SET lease_owner = 'relay-1', lease_until = ? WHERE event_id = ?",
            ((now + timedelta(seconds=30)).isoformat(), event_id),
        )

    active = audit_outbox_health(database, observed_at=now)
    expired = audit_outbox_health(database, observed_at=now + timedelta(seconds=31))

    assert active.actively_leased == 1
    assert active.ready == active.expired_leases == 0
    assert expired.actively_leased == 0
    assert expired.ready == expired.expired_leases == 1
    assert "EXPIRED_LEASE_LIMIT_EXCEEDED" in expired.findings


def test_dead_letter_counts_match_terminal_outbox_state(database: Database) -> None:
    now = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)
    seed(database, Clock(now))

    def fail_publish(*_: object) -> None:
        raise RuntimeError("broker unavailable")

    relay = OutboxRelay(
        database,
        fail_publish,
        worker_id="relay-1",
        policy=RelayPolicy(max_attempts=1),
        clock=Clock(now),
    )

    assert relay.run_once().dead_lettered == 1
    report = audit_outbox_health(database, observed_at=now)

    assert report.dead == report.dead_letter_records == 1
    assert "DEAD_LETTER_LIMIT_EXCEEDED" in report.findings
    assert "DEAD_LETTER_STATE_MISMATCH" not in report.findings


def test_inconsistent_durable_state_fails_closed(database: Database) -> None:
    now = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)
    event_id = seed(database, Clock(now))
    with database.transaction() as connection:
        connection.execute(
            "UPDATE outbox SET lease_until = ?, published_at = ? WHERE event_id = ?",
            ((now + timedelta(seconds=30)).isoformat(), now.isoformat(), event_id),
        )

    report = audit_outbox_health(database, observed_at=now)

    assert not report.healthy
    assert set(report.findings) >= {
        "INCOMPLETE_LEASE_STATE",
        "INVALID_PUBLICATION_TIMESTAMP_STATE",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"max_pending": -1},
        {"max_ready": True},
        {"max_oldest_pending_seconds": 1.5},
    ],
)
def test_invalid_policy_is_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="non-negative integer"):
        OutboxHealthPolicy(**changes)


def test_naive_observation_time_is_rejected(database: Database) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        audit_outbox_health(database, observed_at=datetime(2026, 11, 10))


def test_cli_writes_atomic_report_and_enforces_health(tmp_path) -> None:
    database_path = tmp_path / "events.db"
    output_path = tmp_path / "health.json"
    now = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)
    with Database(database_path) as database:
        seed(database, Clock(now))

    exit_code = main(
        [
            "--database",
            str(database_path),
            "--observed-at",
            (now + timedelta(seconds=301)).isoformat(),
            "--output",
            str(output_path),
            "--require-healthy",
        ]
    )

    assert exit_code == 2
    payload = json.loads(output_path.read_text())
    assert payload["healthy"] is False
    assert "PENDING_AGE_LIMIT_EXCEEDED" in payload["findings"]


def test_cli_does_not_create_a_missing_database(tmp_path) -> None:
    database_path = tmp_path / "missing.db"

    exit_code = main(["--database", str(database_path)])

    assert exit_code == 1
    assert not database_path.exists()


def test_read_only_database_cannot_mutate_health_source(tmp_path) -> None:
    database_path = tmp_path / "events.db"
    now = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)
    with Database(database_path) as database:
        seed(database, Clock(now))

    with Database(database_path, read_only=True) as database:
        assert audit_outbox_health(database, observed_at=now).pending == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            database.connection.execute("DELETE FROM outbox")
