from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

from event_platform.domain import canonical_json, fingerprint, utc_now
from event_platform.storage import Database

_SHA256 = re.compile(r"[0-9a-f]{64}")


class ReplayArtifactError(ValueError):
    """The replay request or policy is malformed and cannot be evaluated."""


class ReplayRejected(RuntimeError):
    """The replay request is well formed but failed one or more safety checks."""

    def __init__(self, findings: Sequence[ReplayFinding]) -> None:
        self.findings = tuple(findings)
        super().__init__(",".join(finding.code for finding in self.findings))


@dataclass(frozen=True, slots=True, order=True)
class ReplayFinding:
    code: str
    event_ref: str


@dataclass(frozen=True, slots=True)
class ReplayTarget:
    event_id: str
    expected_payload_sha256: str
    expected_failure_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.event_id, str)
            or not self.event_id
            or len(self.event_id) > 200
            or _has_control(self.event_id)
        ):
            raise ReplayArtifactError("event_id must be 1-200 printable characters")
        for name, value in (
            ("expected_payload_sha256", self.expected_payload_sha256),
            ("expected_failure_sha256", self.expected_failure_sha256),
        ):
            if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
                raise ReplayArtifactError(f"{name} must be a lowercase SHA-256 digest")


@dataclass(frozen=True, slots=True)
class ReplayRequest:
    actor: str
    change_ticket: str
    targets: tuple[ReplayTarget, ...]

    def __post_init__(self) -> None:
        _validate_label("actor", self.actor)
        _validate_label("change_ticket", self.change_ticket)
        if isinstance(self.targets, (str, bytes)):
            raise ReplayArtifactError("targets must be a sequence of ReplayTarget values")
        targets = tuple(self.targets)
        if any(not isinstance(target, ReplayTarget) for target in targets):
            raise ReplayArtifactError("targets must contain only ReplayTarget values")
        object.__setattr__(self, "targets", targets)
        if not self.targets:
            raise ReplayArtifactError("at least one replay target is required")
        ids = [target.event_id for target in self.targets]
        if len(ids) != len(set(ids)):
            raise ReplayArtifactError("duplicate event_id in replay request")


@dataclass(frozen=True, slots=True)
class ReplayPolicy:
    allowed_event_schemas: Mapping[str, frozenset[int]]
    min_dead_age: timedelta = timedelta(minutes=5)
    max_dead_age: timedelta = timedelta(days=30)
    max_batch_size: int = 25
    max_payload_bytes: int = 256 * 1024
    max_total_payload_bytes: int = 1024 * 1024
    max_original_attempts: int = 20
    max_future_skew: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if not isinstance(self.allowed_event_schemas, Mapping) or not self.allowed_event_schemas:
            raise ReplayArtifactError("allowed_event_schemas must not be empty")
        normalized: dict[str, frozenset[int]] = {}
        for event_type, versions in self.allowed_event_schemas.items():
            if (
                not isinstance(event_type, str)
                or not event_type
                or len(event_type) > 200
                or _has_control(event_type)
            ):
                raise ReplayArtifactError("event types must be 1-200 printable characters")
            if isinstance(versions, (str, bytes)):
                raise ReplayArtifactError("schema versions must be positive integers")
            try:
                normalized_versions = frozenset(versions)
            except TypeError as exc:
                raise ReplayArtifactError("schema versions must be an iterable") from exc
            if not normalized_versions or any(
                type(version) is not int or version <= 0 for version in normalized_versions
            ):
                raise ReplayArtifactError("schema versions must be positive integers")
            normalized[event_type] = normalized_versions
        object.__setattr__(self, "allowed_event_schemas", MappingProxyType(normalized))
        for name, value in (
            ("max_batch_size", self.max_batch_size),
            ("max_payload_bytes", self.max_payload_bytes),
            ("max_total_payload_bytes", self.max_total_payload_bytes),
            ("max_original_attempts", self.max_original_attempts),
        ):
            if type(value) is not int or value <= 0:
                raise ReplayArtifactError(f"{name} must be a positive integer")
        for name, value in (
            ("min_dead_age", self.min_dead_age),
            ("max_dead_age", self.max_dead_age),
            ("max_future_skew", self.max_future_skew),
        ):
            if not isinstance(value, timedelta):
                raise ReplayArtifactError(f"{name} must be a timedelta")
        if self.min_dead_age < timedelta(0):
            raise ReplayArtifactError("min_dead_age must not be negative")
        if self.max_dead_age <= self.min_dead_age:
            raise ReplayArtifactError("max_dead_age must exceed min_dead_age")
        if self.max_future_skew < timedelta(0):
            raise ReplayArtifactError("max_future_skew must not be negative")
        if self.max_total_payload_bytes < self.max_payload_bytes:
            raise ReplayArtifactError("total payload budget must cover one maximum-size payload")


@dataclass(frozen=True, slots=True)
class ReplayReport:
    replay_id: str
    replayed_at: str
    event_count: int
    event_refs: tuple[str, ...]
    request_sha256: str
    evidence_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence_sha256": self.evidence_sha256,
            "event_count": self.event_count,
            "event_refs": list(self.event_refs),
            "replay_id": self.replay_id,
            "replayed_at": self.replayed_at,
            "request_sha256": self.request_sha256,
            "status": "accepted",
        }


@dataclass(frozen=True, slots=True)
class DeadLetterEvidence:
    event_ref: str
    payload_sha256: str
    failure_sha256: str
    failed_at: str
    attempts: int
    event_type: str
    schema_version: int


class DeadLetterReplayer:
    """Atomically admits explicit dead-letter events back into the outbox."""

    def __init__(
        self,
        database: Database,
        policy: ReplayPolicy,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.database = database
        self.policy = policy
        self.clock = clock

    def inspect(self, event_ids: Sequence[str]) -> tuple[DeadLetterEvidence, ...]:
        """Return bounded digests operators can copy into an explicit replay request."""
        if isinstance(event_ids, (str, bytes)):
            raise ReplayArtifactError("event_ids must be a sequence of identifiers")
        if not event_ids or len(event_ids) > self.policy.max_batch_size:
            raise ReplayArtifactError("inspection batch is empty or exceeds max_batch_size")
        if len(event_ids) != len(set(event_ids)):
            raise ReplayArtifactError("duplicate event_id in inspection request")
        evidence: list[DeadLetterEvidence] = []
        for event_id in event_ids:
            if (
                not isinstance(event_id, str)
                or not event_id
                or len(event_id) > 200
                or _has_control(event_id)
            ):
                raise ReplayArtifactError("event_id must be 1-200 printable characters")
            row = self.database.connection.execute(
                "SELECT d.payload_json, d.reason, d.failed_at, d.attempts, d.event_type, "
                "o.schema_version FROM dead_letters d JOIN outbox o ON o.event_id = d.event_id "
                "WHERE d.event_id = ? AND d.status = 'active' AND o.status = 'dead'",
                (event_id,),
            ).fetchone()
            if row is None:
                raise ReplayRejected((ReplayFinding("DEAD_LETTER_NOT_ACTIVE", _ref(event_id)),))
            evidence.append(
                DeadLetterEvidence(
                    event_ref=_ref(event_id),
                    payload_sha256=_hash(row["payload_json"]),
                    failure_sha256=_hash(row["reason"]),
                    failed_at=row["failed_at"],
                    attempts=int(row["attempts"]),
                    event_type=row["event_type"],
                    schema_version=int(row["schema_version"]),
                )
            )
        return tuple(evidence)

    def replay(self, request: ReplayRequest) -> ReplayReport:
        if len(request.targets) > self.policy.max_batch_size:
            raise ReplayRejected((ReplayFinding("BATCH_LIMIT_EXCEEDED", "batch"),))
        now = _aware_utc(self.clock(), "clock")
        request_body = {
            "actor_hash": _hash(request.actor),
            "change_ticket_hash": _hash(request.change_ticket),
            "targets": [
                {
                    "event_ref": _ref(target.event_id),
                    "expected_failure_sha256": target.expected_failure_sha256,
                    "expected_payload_sha256": target.expected_payload_sha256,
                }
                for target in sorted(request.targets, key=lambda item: item.event_id)
            ],
        }
        request_hash = _digest(request_body)
        replay_id = (
            f"rpl_{_digest({'request_sha256': request_hash, 'replayed_at': now.isoformat()})[:24]}"
        )

        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT replay_id FROM dead_letter_replays WHERE request_hash = ?", (request_hash,)
            ).fetchone()
            if existing is not None:
                raise ReplayRejected((ReplayFinding("REQUEST_ALREADY_APPLIED", "batch"),))

            findings: list[ReplayFinding] = []
            accepted: list[dict[str, Any]] = []
            total_payload_bytes = 0
            for target in sorted(request.targets, key=lambda item: item.event_id):
                result = self._evaluate(connection, target, now)
                if isinstance(result, ReplayFinding):
                    findings.append(result)
                    continue
                accepted.append(result)
                total_payload_bytes += result["payload_bytes"]
            if total_payload_bytes > self.policy.max_total_payload_bytes:
                findings.append(ReplayFinding("TOTAL_PAYLOAD_LIMIT_EXCEEDED", "batch"))
            if findings:
                raise ReplayRejected(tuple(sorted(findings)))

            event_refs = tuple(item["event_ref"] for item in accepted)
            evidence = {
                **request_body,
                "event_count": len(accepted),
                "original_attempts": [item["attempts"] for item in accepted],
                "policy_sha256": self._policy_hash(),
                "replay_id": replay_id,
                "replayed_at": now.isoformat(),
                "status": "accepted",
            }
            evidence_hash = _digest(evidence)
            evidence_json = canonical_json({**evidence, "evidence_sha256": evidence_hash})
            for target in request.targets:
                cursor = connection.execute(
                    "UPDATE outbox SET status = 'pending', attempts = 0, available_at = ?, "
                    "lease_owner = NULL, lease_until = NULL, last_error = NULL "
                    "WHERE event_id = ? AND status = 'dead' AND lease_owner IS NULL",
                    (now.isoformat(), target.event_id),
                )
                if cursor.rowcount != 1:
                    raise ReplayRejected(
                        (ReplayFinding("OUTBOX_STATE_CHANGED", _ref(target.event_id)),)
                    )
                connection.execute(
                    "UPDATE dead_letters SET status = 'replayed', replay_id = ?, replayed_at = ? "
                    "WHERE event_id = ? AND status = 'active'",
                    (replay_id, now.isoformat(), target.event_id),
                )
            connection.execute(
                "INSERT INTO dead_letter_replays(replay_id, request_hash, actor_hash, "
                "change_ticket_hash, replayed_at, event_count, evidence_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    replay_id,
                    request_hash,
                    _hash(request.actor),
                    _hash(request.change_ticket),
                    now.isoformat(),
                    len(accepted),
                    evidence_json,
                ),
            )
        return ReplayReport(
            replay_id=replay_id,
            replayed_at=now.isoformat(),
            event_count=len(accepted),
            event_refs=event_refs,
            request_sha256=request_hash,
            evidence_sha256=evidence_hash,
        )

    def _evaluate(
        self, connection: sqlite3.Connection, target: ReplayTarget, now: datetime
    ) -> dict[str, Any] | ReplayFinding:
        event_ref = _ref(target.event_id)
        row = connection.execute(
            "SELECT o.status AS outbox_status, o.event_type, o.schema_version, o.payload_json, "
            "o.payload_hash, o.attempts AS outbox_attempts, o.lease_owner, o.lease_until, "
            "d.status AS dead_status, d.event_type AS dead_event_type, "
            "d.payload_json AS dead_payload_json, d.attempts AS dead_attempts, "
            "d.reason, d.failed_at FROM outbox o LEFT JOIN dead_letters d "
            "ON d.event_id = o.event_id WHERE o.event_id = ?",
            (target.event_id,),
        ).fetchone()
        if row is None or row["dead_status"] is None:
            return ReplayFinding("DEAD_LETTER_NOT_FOUND", event_ref)
        if row["outbox_status"] != "dead" or row["dead_status"] != "active":
            return ReplayFinding("DEAD_LETTER_NOT_ACTIVE", event_ref)
        if row["lease_owner"] is not None or row["lease_until"] is not None:
            return ReplayFinding("DEAD_LETTER_HAS_LEASE", event_ref)
        if row["event_type"] != row["dead_event_type"]:
            return ReplayFinding("EVENT_TYPE_MISMATCH", event_ref)
        versions = self.policy.allowed_event_schemas.get(row["event_type"])
        if versions is None:
            return ReplayFinding("EVENT_TYPE_NOT_ALLOWED", event_ref)
        if int(row["schema_version"]) not in versions:
            return ReplayFinding("SCHEMA_VERSION_NOT_ALLOWED", event_ref)
        attempts = int(row["dead_attempts"])
        if attempts != int(row["outbox_attempts"]):
            return ReplayFinding("ATTEMPT_COUNT_MISMATCH", event_ref)
        if attempts > self.policy.max_original_attempts:
            return ReplayFinding("ATTEMPT_LIMIT_EXCEEDED", event_ref)
        if row["payload_json"] != row["dead_payload_json"]:
            return ReplayFinding("PAYLOAD_COPY_MISMATCH", event_ref)
        try:
            payload = _strict_json(row["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return ReplayFinding("PAYLOAD_INVALID", event_ref)
        if not isinstance(payload, dict) or fingerprint(payload) != row["payload_hash"]:
            return ReplayFinding("PAYLOAD_HASH_MISMATCH", event_ref)
        payload_bytes = len(row["payload_json"].encode("utf-8"))
        if payload_bytes > self.policy.max_payload_bytes:
            return ReplayFinding("PAYLOAD_LIMIT_EXCEEDED", event_ref)
        if _hash(row["payload_json"]) != target.expected_payload_sha256:
            return ReplayFinding("EXPECTED_PAYLOAD_MISMATCH", event_ref)
        if _hash(row["reason"]) != target.expected_failure_sha256:
            return ReplayFinding("EXPECTED_FAILURE_MISMATCH", event_ref)
        try:
            failed_at = _aware_utc(datetime.fromisoformat(row["failed_at"]), "failed_at")
        except (TypeError, ValueError):
            return ReplayFinding("FAILED_AT_INVALID", event_ref)
        if failed_at > now + self.policy.max_future_skew:
            return ReplayFinding("FAILED_AT_IN_FUTURE", event_ref)
        age = now - failed_at
        if age < self.policy.min_dead_age:
            return ReplayFinding("COOLDOWN_NOT_ELAPSED", event_ref)
        if age > self.policy.max_dead_age:
            return ReplayFinding("DEAD_LETTER_EXPIRED", event_ref)
        return {"attempts": attempts, "event_ref": event_ref, "payload_bytes": payload_bytes}

    def _policy_hash(self) -> str:
        return _digest(
            {
                "allowed_event_schemas": {
                    key: sorted(value)
                    for key, value in sorted(self.policy.allowed_event_schemas.items())
                },
                "max_batch_size": self.policy.max_batch_size,
                "max_dead_age_seconds": self.policy.max_dead_age.total_seconds(),
                "max_future_skew_seconds": self.policy.max_future_skew.total_seconds(),
                "max_original_attempts": self.policy.max_original_attempts,
                "max_payload_bytes": self.policy.max_payload_bytes,
                "max_total_payload_bytes": self.policy.max_total_payload_bytes,
                "min_dead_age_seconds": self.policy.min_dead_age.total_seconds(),
            }
        )


def _strict_json(value: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite number: {value}")

    return json.loads(value, object_pairs_hook=pairs, parse_constant=reject_constant)


def _validate_label(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 128 or _has_control(value):
        raise ReplayArtifactError(f"{name} must be 1-128 printable characters")


def _has_control(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ReplayArtifactError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ref(event_id: str) -> str:
    return f"evt_{_hash(event_id)[:16]}"


def _digest(value: dict[str, Any]) -> str:
    return _hash(canonical_json(value))
