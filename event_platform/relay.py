from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from event_platform.domain import EventEnvelope, utc_now
from event_platform.storage import Database


@dataclass(frozen=True, slots=True)
class RelayPolicy:
    batch_size: int = 50
    lease_seconds: int = 30
    max_attempts: int = 5
    base_backoff_seconds: int = 2
    max_backoff_seconds: int = 300

    def __post_init__(self) -> None:
        if (
            min(self.batch_size, self.lease_seconds, self.max_attempts, self.base_backoff_seconds)
            <= 0
        ):
            raise ValueError("relay policy values must be positive")

    def backoff(self, attempt: int) -> int:
        return min(self.base_backoff_seconds * (2 ** max(0, attempt - 1)), self.max_backoff_seconds)


@dataclass(frozen=True, slots=True)
class RelayResult:
    claimed: int
    published: int
    retried: int
    dead_lettered: int


class OutboxRelay:
    """Claims outbox rows with leases and publishes them with bounded retries."""

    def __init__(
        self,
        database: Database,
        publish: Callable[[str, EventEnvelope], None],
        *,
        worker_id: str,
        policy: RelayPolicy | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id is required")
        self.database = database
        self.publish = publish
        self.worker_id = worker_id
        self.policy = policy or RelayPolicy()
        self.clock = clock

    def _claim(self, now: datetime) -> list[EventEnvelope]:
        lease_until = now + timedelta(seconds=self.policy.lease_seconds)
        with self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM outbox WHERE status = 'pending' AND available_at <= ? "
                "AND (lease_until IS NULL OR lease_until <= ?) ORDER BY occurred_at, event_id LIMIT ?",
                (now.isoformat(), now.isoformat(), self.policy.batch_size),
            ).fetchall()
            ids = [row["event_id"] for row in rows]
            for event_id in ids:
                connection.execute(
                    "UPDATE outbox SET lease_owner = ?, lease_until = ? WHERE event_id = ?",
                    (self.worker_id, lease_until.isoformat(), event_id),
                )
        return [
            EventEnvelope(
                event_id=row["event_id"],
                aggregate_id=row["aggregate_id"],
                event_type=row["event_type"],
                schema_version=row["schema_version"],
                payload=json.loads(row["payload_json"]),
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
            )
            for row in rows
        ]

    def run_once(self) -> RelayResult:
        now = self.clock()
        events = self._claim(now)
        published = retried = dead_lettered = 0
        for event in events:
            try:
                self.publish(event.event_type, event)
            except Exception as exc:  # noqa: BLE001 - transport boundary records every failure.
                outcome = self._mark_failure(event, str(exc), now)
                retried += outcome == "retry"
                dead_lettered += outcome == "dead"
            else:
                self._mark_published(event.event_id, now)
                published += 1
        return RelayResult(len(events), published, retried, dead_lettered)

    def _mark_published(self, event_id: str, now: datetime) -> None:
        with self.database.transaction() as connection:
            cursor = connection.execute(
                "UPDATE outbox SET status = 'published', published_at = ?, lease_owner = NULL, "
                "lease_until = NULL, attempts = attempts + 1 WHERE event_id = ? AND lease_owner = ?",
                (now.isoformat(), event_id, self.worker_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("outbox lease ownership was lost before acknowledgement")

    def _mark_failure(self, event: EventEnvelope, reason: str, now: datetime) -> str:
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT attempts, payload_json FROM outbox WHERE event_id = ? AND lease_owner = ?",
                (event.event_id, self.worker_id),
            ).fetchone()
            if row is None:
                raise RuntimeError("outbox lease ownership was lost after publish failure")
            attempts = int(row["attempts"]) + 1
            if attempts >= self.policy.max_attempts:
                connection.execute(
                    "UPDATE outbox SET status = 'dead', attempts = ?, last_error = ?, "
                    "lease_owner = NULL, lease_until = NULL WHERE event_id = ?",
                    (attempts, reason[:1000], event.event_id),
                )
                connection.execute(
                    "INSERT OR REPLACE INTO dead_letters(event_id, event_type, payload_json, "
                    "attempts, reason, failed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.event_type,
                        row["payload_json"],
                        attempts,
                        reason[:1000],
                        now.isoformat(),
                    ),
                )
                return "dead"
            available_at = now + timedelta(seconds=self.policy.backoff(attempts))
            connection.execute(
                "UPDATE outbox SET attempts = ?, available_at = ?, last_error = ?, "
                "lease_owner = NULL, lease_until = NULL WHERE event_id = ?",
                (attempts, available_at.isoformat(), reason[:1000], event.event_id),
            )
            return "retry"
