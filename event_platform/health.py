from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from event_platform.domain import utc_now
from event_platform.storage import Database


@dataclass(frozen=True, slots=True)
class OutboxHealthPolicy:
    max_pending: int = 1_000
    max_ready: int = 100
    max_oldest_pending_seconds: int = 300
    max_dead_letters: int = 0
    max_expired_leases: int = 0
    max_pending_attempts: int = 4

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class OutboxHealthReport:
    schema_version: int
    observed_at: str
    healthy: bool
    findings: tuple[str, ...]
    pending: int
    ready: int
    delayed: int
    actively_leased: int
    expired_leases: int
    published: int
    dead: int
    dead_letter_records: int
    oldest_pending_age_seconds: float | None
    maximum_pending_attempts: int
    policy: OutboxHealthPolicy
    evidence_sha256: str

    def body(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("evidence_sha256")
        payload["findings"] = list(self.findings)
        return payload

    def to_dict(self) -> dict[str, Any]:
        return {**self.body(), "evidence_sha256": self.evidence_sha256}


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _count(connection: sqlite3.Connection, query: str, parameters: tuple[object, ...] = ()) -> int:
    return int(connection.execute(query, parameters).fetchone()[0])


def audit_outbox_health(
    database: Database,
    *,
    policy: OutboxHealthPolicy | None = None,
    observed_at: datetime | None = None,
) -> OutboxHealthReport:
    """Return a privacy-reduced operational audit of durable outbox state."""
    if not isinstance(database, Database):
        raise TypeError("database must be a Database")
    active_policy = policy or OutboxHealthPolicy()
    now = observed_at or utc_now()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    now = now.astimezone(UTC)
    timestamp = now.isoformat()
    with database.transaction(immediate=False) as connection:
        return _audit_snapshot(connection, active_policy, now, timestamp)


def _audit_snapshot(
    connection: sqlite3.Connection,
    policy: OutboxHealthPolicy,
    now: datetime,
    timestamp: str,
) -> OutboxHealthReport:
    pending = _count(connection, "SELECT COUNT(*) FROM outbox WHERE status = 'pending'")
    ready = _count(
        connection,
        "SELECT COUNT(*) FROM outbox WHERE status = 'pending' AND available_at <= ? "
        "AND (lease_until IS NULL OR lease_until <= ?)",
        (timestamp, timestamp),
    )
    delayed = _count(
        connection,
        "SELECT COUNT(*) FROM outbox WHERE status = 'pending' AND available_at > ? "
        "AND (lease_until IS NULL OR lease_until <= ?)",
        (timestamp, timestamp),
    )
    actively_leased = _count(
        connection,
        "SELECT COUNT(*) FROM outbox WHERE status = 'pending' AND lease_until > ?",
        (timestamp,),
    )
    expired_leases = _count(
        connection,
        "SELECT COUNT(*) FROM outbox WHERE status = 'pending' "
        "AND lease_owner IS NOT NULL AND lease_until <= ?",
        (timestamp,),
    )
    published = _count(connection, "SELECT COUNT(*) FROM outbox WHERE status = 'published'")
    dead = _count(connection, "SELECT COUNT(*) FROM outbox WHERE status = 'dead'")
    dead_letters = _count(connection, "SELECT COUNT(*) FROM dead_letters")
    maximum_attempts = int(
        connection.execute(
            "SELECT COALESCE(MAX(attempts), 0) FROM outbox WHERE status = 'pending'"
        ).fetchone()[0]
    )

    findings: list[str] = []
    oldest = connection.execute(
        "SELECT MIN(occurred_at) FROM outbox WHERE status = 'pending'"
    ).fetchone()[0]
    oldest_age: float | None = None
    if oldest is not None:
        try:
            oldest_timestamp = datetime.fromisoformat(str(oldest))
            if oldest_timestamp.tzinfo is None or oldest_timestamp.utcoffset() is None:
                raise ValueError
            oldest_age = (now - oldest_timestamp.astimezone(UTC)).total_seconds()
            if oldest_age < 0:
                findings.append("PENDING_TIMESTAMP_IN_FUTURE")
                oldest_age = 0.0
        except ValueError:
            findings.append("INVALID_PENDING_TIMESTAMP")

    consistency_queries = {
        "UNKNOWN_OUTBOX_STATUS": (
            "SELECT COUNT(*) FROM outbox WHERE status NOT IN ('pending', 'published', 'dead')"
        ),
        "INCOMPLETE_LEASE_STATE": (
            "SELECT COUNT(*) FROM outbox WHERE status = 'pending' AND "
            "((lease_owner IS NULL AND lease_until IS NOT NULL) OR "
            "(lease_owner IS NOT NULL AND lease_until IS NULL))"
        ),
        "TERMINAL_ROW_RETAINS_LEASE": (
            "SELECT COUNT(*) FROM outbox WHERE status IN ('published', 'dead') "
            "AND (lease_owner IS NOT NULL OR lease_until IS NOT NULL)"
        ),
        "INVALID_PUBLICATION_TIMESTAMP_STATE": (
            "SELECT COUNT(*) FROM outbox WHERE "
            "(status = 'published' AND published_at IS NULL) OR "
            "(status != 'published' AND published_at IS NOT NULL)"
        ),
        "DEAD_LETTER_STATE_MISMATCH": (
            "SELECT COUNT(*) FROM outbox o LEFT JOIN dead_letters d ON d.event_id = o.event_id "
            "WHERE (o.status = 'dead' AND d.event_id IS NULL) OR "
            "(o.status != 'dead' AND d.event_id IS NOT NULL) OR "
            "(o.status = 'dead' AND o.attempts != d.attempts)"
        ),
        "ORPHAN_DEAD_LETTER": (
            "SELECT COUNT(*) FROM dead_letters d LEFT JOIN outbox o ON o.event_id = d.event_id "
            "WHERE o.event_id IS NULL"
        ),
    }
    findings.extend(
        code for code, query in consistency_queries.items() if _count(connection, query) > 0
    )
    threshold_checks = (
        (pending > policy.max_pending, "PENDING_LIMIT_EXCEEDED"),
        (ready > policy.max_ready, "READY_LIMIT_EXCEEDED"),
        (dead_letters > policy.max_dead_letters, "DEAD_LETTER_LIMIT_EXCEEDED"),
        (expired_leases > policy.max_expired_leases, "EXPIRED_LEASE_LIMIT_EXCEEDED"),
        (maximum_attempts > policy.max_pending_attempts, "PENDING_ATTEMPT_LIMIT_EXCEEDED"),
        (
            oldest_age is not None and oldest_age > policy.max_oldest_pending_seconds,
            "PENDING_AGE_LIMIT_EXCEEDED",
        ),
    )
    findings.extend(code for failed, code in threshold_checks if failed)
    stable_findings = tuple(sorted(set(findings)))
    body = {
        "schema_version": 1,
        "observed_at": timestamp,
        "healthy": not stable_findings,
        "findings": list(stable_findings),
        "pending": pending,
        "ready": ready,
        "delayed": delayed,
        "actively_leased": actively_leased,
        "expired_leases": expired_leases,
        "published": published,
        "dead": dead,
        "dead_letter_records": dead_letters,
        "oldest_pending_age_seconds": oldest_age,
        "maximum_pending_attempts": maximum_attempts,
        "policy": asdict(policy),
    }
    return OutboxHealthReport(
        schema_version=1,
        observed_at=timestamp,
        healthy=not stable_findings,
        findings=stable_findings,
        pending=pending,
        ready=ready,
        delayed=delayed,
        actively_leased=actively_leased,
        expired_leases=expired_leases,
        published=published,
        dead=dead,
        dead_letter_records=dead_letters,
        oldest_pending_age_seconds=oldest_age,
        maximum_pending_attempts=maximum_attempts,
        policy=policy,
        evidence_sha256=hashlib.sha256(_canonical_json(body)).hexdigest(),
    )


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            json.dump(payload, handle, allow_nan=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary = handle.name
        os.replace(temporary, path)
    except OSError:
        if temporary:
            Path(temporary).unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit durable outbox operational health")
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--observed-at", type=datetime.fromisoformat)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-healthy", action="store_true")
    parser.add_argument("--max-pending", type=int, default=1_000)
    parser.add_argument("--max-ready", type=int, default=100)
    parser.add_argument("--max-oldest-pending-seconds", type=int, default=300)
    parser.add_argument("--max-dead-letters", type=int, default=0)
    parser.add_argument("--max-expired-leases", type=int, default=0)
    parser.add_argument("--max-pending-attempts", type=int, default=4)
    args = parser.parse_args(argv)
    try:
        policy = OutboxHealthPolicy(
            max_pending=args.max_pending,
            max_ready=args.max_ready,
            max_oldest_pending_seconds=args.max_oldest_pending_seconds,
            max_dead_letters=args.max_dead_letters,
            max_expired_leases=args.max_expired_leases,
            max_pending_attempts=args.max_pending_attempts,
        )
        with Database(args.database, read_only=True) as database:
            report = audit_outbox_health(
                database,
                observed_at=args.observed_at,
                policy=policy,
            )
        payload = report.to_dict()
        if args.output:
            _write_atomic(args.output, payload)
        print(json.dumps(payload, allow_nan=False, indent=2, sort_keys=True))
    except (OSError, sqlite3.Error, TypeError, ValueError):
        print(json.dumps({"error": "outbox_health_audit_failed"}))
        return 1
    return 2 if args.require_healthy and not report.healthy else 0


if __name__ == "__main__":
    raise SystemExit(main())
