from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PASS_EXIT_CODE = 0
POLICY_FAILURE_EXIT_CODE = 2
INVALID_EVIDENCE_EXIT_CODE = 3
MAX_INPUT_BYTES = 2_000_000
MAX_PARTITIONS_LIMIT = 4_096


class EvidenceError(ValueError):
    """Broker evidence is malformed or internally inconsistent."""


def _integer(name: str, value: int, *, minimum: int = 0) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise EvidenceError(f"{name} must be an integer")
    if value < minimum:
        raise EvidenceError(f"{name} must be at least {minimum}")


def _bounded_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise EvidenceError(f"{name} must be a non-empty string")
    if value != value.strip() or len(value) > 128:
        raise EvidenceError(f"{name} must be trimmed and at most 128 characters")
    if any(ord(character) < 32 for character in value):
        raise EvidenceError(f"{name} must not contain control characters")


def _parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise EvidenceError("captured_at must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceError("captured_at must be a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvidenceError("captured_at must include a timezone")
    return parsed.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class PartitionOffset:
    partition: int
    high_watermark: int
    committed_offset: int

    def __post_init__(self) -> None:
        _integer("partition", self.partition)
        _integer("high_watermark", self.high_watermark)
        _integer("committed_offset", self.committed_offset)
        if self.committed_offset > self.high_watermark:
            raise EvidenceError("committed_offset must not exceed high_watermark")

    @property
    def lag(self) -> int:
        return self.high_watermark - self.committed_offset


@dataclass(frozen=True, slots=True)
class OffsetSnapshot:
    captured_at: datetime
    stream: str
    consumer_group: str
    partitions: tuple[PartitionOffset, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.captured_at, datetime):
            raise EvidenceError("captured_at must be a datetime")
        if self.captured_at.tzinfo is None or self.captured_at.utcoffset() is None:
            raise EvidenceError("captured_at must include a timezone")
        _bounded_text("stream", self.stream)
        _bounded_text("consumer_group", self.consumer_group)
        if not self.partitions:
            raise EvidenceError("partitions must not be empty")
        if not all(isinstance(item, PartitionOffset) for item in self.partitions):
            raise EvidenceError("partitions must contain PartitionOffset values")
        identifiers = [item.partition for item in self.partitions]
        if len(set(identifiers)) != len(identifiers):
            raise EvidenceError("partition identifiers must be unique")


@dataclass(frozen=True, slots=True)
class ConsumerLagPolicy:
    max_snapshot_age_seconds: int = 300
    max_future_skew_seconds: int = 5
    min_observation_interval_seconds: int = 30
    max_observation_interval_seconds: int = 900
    max_total_lag: int = 10_000
    max_partition_lag: int = 2_000
    max_lag_growth: int = 1_000
    min_progress_ratio: float = 0.05
    max_stalled_partitions: int = 0
    max_partitions: int = 1_024

    def __post_init__(self) -> None:
        for name in (
            "max_snapshot_age_seconds",
            "max_future_skew_seconds",
            "min_observation_interval_seconds",
            "max_observation_interval_seconds",
            "max_total_lag",
            "max_partition_lag",
            "max_lag_growth",
            "max_stalled_partitions",
            "max_partitions",
        ):
            _integer(name, getattr(self, name))
        if self.max_snapshot_age_seconds == 0:
            raise EvidenceError("max_snapshot_age_seconds must be positive")
        if self.min_observation_interval_seconds == 0:
            raise EvidenceError("min_observation_interval_seconds must be positive")
        if self.max_observation_interval_seconds < self.min_observation_interval_seconds:
            raise EvidenceError(
                "max_observation_interval_seconds must be at least the minimum interval"
            )
        if not isinstance(self.min_progress_ratio, (int, float)) or isinstance(
            self.min_progress_ratio, bool
        ):
            raise EvidenceError("min_progress_ratio must be numeric")
        if not math.isfinite(self.min_progress_ratio) or not 0 <= self.min_progress_ratio <= 1:
            raise EvidenceError("min_progress_ratio must be finite and between 0 and 1")
        if not 1 <= self.max_partitions <= MAX_PARTITIONS_LIMIT:
            raise EvidenceError(f"max_partitions must be between 1 and {MAX_PARTITIONS_LIMIT}")


@dataclass(frozen=True, slots=True)
class PartitionLag:
    partition_ref: str
    lag: int
    lag_delta: int
    consumed: int
    stalled: bool


@dataclass(frozen=True, slots=True)
class LagViolation:
    code: str
    partition_ref: str | None = None


@dataclass(frozen=True, slots=True)
class ConsumerLagReport:
    passed: bool
    observed_at: str
    snapshot_age_seconds: float
    observation_interval_seconds: float
    partition_count: int
    previous_total_lag: int
    current_total_lag: int
    lag_growth: int
    progress_ratio: float | None
    stalled_partitions: int
    partitions: tuple[PartitionLag, ...]
    violations: tuple[LagViolation, ...]
    evidence_digest: str
    policy: ConsumerLagPolicy

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema_version"] = "consumer-lag-audit-v1"
        return payload


def _partition_ref(stream: str, consumer_group: str, partition: int) -> str:
    material = f"{stream}\0{consumer_group}\0{partition}".encode()
    return hashlib.sha256(material).hexdigest()[:16]


def _snapshot_payload(snapshot: OffsetSnapshot) -> dict[str, Any]:
    return {
        "captured_at": snapshot.captured_at.astimezone(UTC).isoformat(),
        "consumer_group": snapshot.consumer_group,
        "partitions": [
            asdict(item) for item in sorted(snapshot.partitions, key=lambda item: item.partition)
        ],
        "stream": snapshot.stream,
    }


def _evidence_digest(
    previous: OffsetSnapshot,
    current: OffsetSnapshot,
    policy: ConsumerLagPolicy,
) -> str:
    payload = {
        "current": _snapshot_payload(current),
        "policy": asdict(policy),
        "previous": _snapshot_payload(previous),
        "schema_version": "consumer-lag-evidence-v1",
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def audit_consumer_lag(
    previous: OffsetSnapshot,
    current: OffsetSnapshot,
    *,
    policy: ConsumerLagPolicy | None = None,
    observed_at: datetime | None = None,
) -> ConsumerLagReport:
    active_policy = policy or ConsumerLagPolicy()
    now = observed_at or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise EvidenceError("observed_at must include a timezone")
    now = now.astimezone(UTC)
    if previous.stream != current.stream or previous.consumer_group != current.consumer_group:
        raise EvidenceError("snapshots must describe the same stream and consumer group")
    if len(previous.partitions) > active_policy.max_partitions:
        raise EvidenceError("partition count exceeds policy maximum")
    if len(current.partitions) > active_policy.max_partitions:
        raise EvidenceError("partition count exceeds policy maximum")

    previous_by_id = {item.partition: item for item in previous.partitions}
    current_by_id = {item.partition: item for item in current.partitions}
    if previous_by_id.keys() != current_by_id.keys():
        raise EvidenceError("partition sets must match across snapshots")

    interval = (current.captured_at - previous.captured_at).total_seconds()
    if interval <= 0:
        raise EvidenceError("current snapshot must be newer than previous snapshot")
    age = (now - current.captured_at).total_seconds()

    violations: list[LagViolation] = []
    if age > active_policy.max_snapshot_age_seconds:
        violations.append(LagViolation("current_snapshot_stale"))
    if age < -active_policy.max_future_skew_seconds:
        violations.append(LagViolation("current_snapshot_from_future"))
    if interval < active_policy.min_observation_interval_seconds:
        violations.append(LagViolation("observation_interval_too_short"))
    if interval > active_policy.max_observation_interval_seconds:
        violations.append(LagViolation("observation_interval_too_long"))

    partition_results: list[PartitionLag] = []
    previous_total_lag = 0
    current_total_lag = 0
    total_consumed = 0
    stalled_partitions = 0
    for identifier in sorted(previous_by_id):
        before = previous_by_id[identifier]
        after = current_by_id[identifier]
        if after.high_watermark < before.high_watermark:
            raise EvidenceError(f"high watermark regressed for partition {identifier}")
        if after.committed_offset < before.committed_offset:
            raise EvidenceError(f"committed offset regressed for partition {identifier}")
        consumed = after.committed_offset - before.committed_offset
        stalled = before.lag > 0 and consumed == 0
        if stalled:
            stalled_partitions += 1
        previous_total_lag += before.lag
        current_total_lag += after.lag
        total_consumed += consumed
        reference = _partition_ref(current.stream, current.consumer_group, identifier)
        partition_results.append(
            PartitionLag(
                partition_ref=reference,
                lag=after.lag,
                lag_delta=after.lag - before.lag,
                consumed=consumed,
                stalled=stalled,
            )
        )
        if after.lag > active_policy.max_partition_lag:
            violations.append(LagViolation("partition_lag_exceeded", reference))

    lag_growth = current_total_lag - previous_total_lag
    progress_ratio = min(1.0, total_consumed / previous_total_lag) if previous_total_lag else None
    if current_total_lag > active_policy.max_total_lag:
        violations.append(LagViolation("total_lag_exceeded"))
    if lag_growth > active_policy.max_lag_growth:
        violations.append(LagViolation("lag_growth_exceeded"))
    if progress_ratio is not None and progress_ratio < active_policy.min_progress_ratio:
        violations.append(LagViolation("consumption_progress_below_minimum"))
    if stalled_partitions > active_policy.max_stalled_partitions:
        violations.append(LagViolation("stalled_partitions_exceeded"))

    return ConsumerLagReport(
        passed=not violations,
        observed_at=now.isoformat(),
        snapshot_age_seconds=age,
        observation_interval_seconds=interval,
        partition_count=len(current_by_id),
        previous_total_lag=previous_total_lag,
        current_total_lag=current_total_lag,
        lag_growth=lag_growth,
        progress_ratio=progress_ratio,
        stalled_partitions=stalled_partitions,
        partitions=tuple(partition_results),
        violations=tuple(violations),
        evidence_digest=_evidence_digest(previous, current, active_policy),
        policy=active_policy,
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _exact_keys(value: dict[str, Any], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise EvidenceError(f"{name} must contain exactly {', '.join(sorted(expected))}")


def _snapshot_from_dict(value: Any, name: str) -> OffsetSnapshot:
    if not isinstance(value, dict):
        raise EvidenceError(f"{name} must be an object")
    _exact_keys(value, {"captured_at", "consumer_group", "partitions", "stream"}, name)
    rows = value["partitions"]
    if not isinstance(rows, list):
        raise EvidenceError(f"{name}.partitions must be an array")
    partitions: list[PartitionOffset] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise EvidenceError(f"{name}.partitions[{index}] must be an object")
        _exact_keys(row, {"committed_offset", "high_watermark", "partition"}, "partition")
        partitions.append(PartitionOffset(**row))
    return OffsetSnapshot(
        captured_at=_parse_timestamp(value["captured_at"]),
        stream=value["stream"],
        consumer_group=value["consumer_group"],
        partitions=tuple(partitions),
    )


def load_evidence(path: Path) -> tuple[OffsetSnapshot, OffsetSnapshot]:
    if path.is_symlink():
        raise EvidenceError("input must not be a symlink")
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise EvidenceError(f"input exceeds {MAX_INPUT_BYTES} bytes")
    payload = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=lambda value: (_ for _ in ()).throw(
            EvidenceError(f"non-finite JSON value: {value}")
        ),
    )
    if not isinstance(payload, dict):
        raise EvidenceError("input must be an object")
    _exact_keys(payload, {"current", "previous"}, "input")
    return (
        _snapshot_from_dict(payload["previous"], "previous"),
        _snapshot_from_dict(payload["current"], "current"),
    )


def _write_atomic(path: Path, content: str) -> None:
    if path.is_symlink():
        raise EvidenceError("output must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit broker consumer-lag evidence.")
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-snapshot-age-seconds", type=int, default=300)
    parser.add_argument("--min-observation-interval-seconds", type=int, default=30)
    parser.add_argument("--max-observation-interval-seconds", type=int, default=900)
    parser.add_argument("--max-total-lag", type=int, default=10_000)
    parser.add_argument("--max-partition-lag", type=int, default=2_000)
    parser.add_argument("--max-lag-growth", type=int, default=1_000)
    parser.add_argument("--min-progress-ratio", type=float, default=0.05)
    parser.add_argument("--max-stalled-partitions", type=int, default=0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        policy = ConsumerLagPolicy(
            max_snapshot_age_seconds=arguments.max_snapshot_age_seconds,
            min_observation_interval_seconds=arguments.min_observation_interval_seconds,
            max_observation_interval_seconds=arguments.max_observation_interval_seconds,
            max_total_lag=arguments.max_total_lag,
            max_partition_lag=arguments.max_partition_lag,
            max_lag_growth=arguments.max_lag_growth,
            min_progress_ratio=arguments.min_progress_ratio,
            max_stalled_partitions=arguments.max_stalled_partitions,
        )
        previous, current = load_evidence(arguments.input)
        report = audit_consumer_lag(previous, current, policy=policy)
        rendered = json.dumps(report.as_dict(), indent=2, sort_keys=True) + "\n"
        if arguments.output:
            _write_atomic(arguments.output, rendered)
        else:
            print(rendered, end="")
    except (EvidenceError, json.JSONDecodeError, OSError, TypeError) as exc:
        print(f"consumer lag audit failed: {exc}", file=sys.stderr)
        return INVALID_EVIDENCE_EXIT_CODE
    return PASS_EXIT_CODE if report.passed else POLICY_FAILURE_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
