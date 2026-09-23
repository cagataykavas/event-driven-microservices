from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from numbers import Real

from event_platform.delivery import DeliveryDisposition, classify_delivery_failure
from event_platform.domain import EventEnvelope, utc_now
from event_platform.storage import Database


@dataclass(frozen=True, slots=True)
class RelayPolicy:
    batch_size: int = 50
    lease_seconds: int = 30
    max_attempts: int = 5
    base_backoff_seconds: int = 2
    max_backoff_seconds: int = 300
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        integer_fields = (
            "batch_size",
            "lease_seconds",
            "max_attempts",
            "base_backoff_seconds",
            "max_backoff_seconds",
        )
        for field in integer_fields:
            value = getattr(self, field)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{field} must be a positive integer")
        if self.max_backoff_seconds < self.base_backoff_seconds:
            raise ValueError("max_backoff_seconds cannot be below base_backoff_seconds")
        if (
            not isinstance(self.jitter_ratio, Real)
            or isinstance(self.jitter_ratio, bool)
            or not math.isfinite(float(self.jitter_ratio))
            or not 0 <= self.jitter_ratio <= 1
        ):
            raise ValueError("jitter_ratio must be finite and between zero and one")

    def backoff(self, attempt: int) -> int:
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt <= 0:
            raise ValueError("attempt must be a positive integer")
        exponent = attempt - 1
        saturation_exponent = math.ceil(
            math.log2(self.max_backoff_seconds / self.base_backoff_seconds)
        )
        if exponent >= saturation_exponent:
            return self.max_backoff_seconds
        return self.base_backoff_seconds * (2**exponent)

    def retry_delay(
        self,
        *,
        attempt: int,
        event_id: str,
        retry_after_seconds: float | None = None,
    ) -> int:
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event_id is required for deterministic jitter")
        if retry_after_seconds is not None and (
            not isinstance(retry_after_seconds, Real)
            or isinstance(retry_after_seconds, bool)
            or not math.isfinite(float(retry_after_seconds))
            or retry_after_seconds < 0
        ):
            raise ValueError("retry_after_seconds must be finite and non-negative")

        floor = max(float(self.backoff(attempt)), float(retry_after_seconds or 0))
        digest = sha256(f"{event_id}:{attempt}".encode()).digest()
        unit_interval = int.from_bytes(digest[:8], "big") / ((1 << 64) - 1)
        jitter = floor * float(self.jitter_ratio) * unit_interval
        return min(math.ceil(floor + jitter), self.max_backoff_seconds)


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
            except Exception as exc:  # noqa: BLE001 - transport boundary classifies every failure.
                outcome = self._mark_failure(event, exc, now)
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

    def _mark_failure(self, event: EventEnvelope, error: Exception, now: datetime) -> str:
        failure = classify_delivery_failure(error)
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT attempts, payload_json FROM outbox WHERE event_id = ? AND lease_owner = ?",
                (event.event_id, self.worker_id),
            ).fetchone()
            if row is None:
                raise RuntimeError("outbox lease ownership was lost after publish failure")
            attempts = int(row["attempts"]) + 1
            if (
                failure.disposition is DeliveryDisposition.PERMANENT
                or attempts >= self.policy.max_attempts
            ):
                connection.execute(
                    "UPDATE outbox SET status = 'dead', attempts = ?, last_error = ?, "
                    "lease_owner = NULL, lease_until = NULL WHERE event_id = ?",
                    (attempts, failure.reason, event.event_id),
                )
                connection.execute(
                    "INSERT OR REPLACE INTO dead_letters(event_id, event_type, payload_json, "
                    "attempts, reason, failed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.event_type,
                        row["payload_json"],
                        attempts,
                        failure.reason,
                        now.isoformat(),
                    ),
                )
                return "dead"
            delay = self.policy.retry_delay(
                attempt=attempts,
                event_id=event.event_id,
                retry_after_seconds=failure.retry_after_seconds,
            )
            available_at = now + timedelta(seconds=delay)
            connection.execute(
                "UPDATE outbox SET attempts = ?, available_at = ?, last_error = ?, "
                "lease_owner = NULL, lease_until = NULL WHERE event_id = ?",
                (attempts, available_at.isoformat(), failure.reason, event.event_id),
            )
            return "retry"
