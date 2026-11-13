from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from event_platform.consumer_lag import (
    INVALID_EVIDENCE_EXIT_CODE,
    PASS_EXIT_CODE,
    POLICY_FAILURE_EXIT_CODE,
    ConsumerLagPolicy,
    EvidenceError,
    OffsetSnapshot,
    PartitionOffset,
    audit_consumer_lag,
    main,
)

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)


def snapshot(
    captured_at: datetime,
    rows: list[tuple[int, int, int]],
) -> OffsetSnapshot:
    return OffsetSnapshot(
        captured_at=captured_at,
        stream="orders.v1",
        consumer_group="billing-v1",
        partitions=tuple(PartitionOffset(*row) for row in rows),
    )


def healthy_pair() -> tuple[OffsetSnapshot, OffsetSnapshot]:
    return (
        snapshot(NOW - timedelta(minutes=6), [(0, 100, 80), (1, 200, 190)]),
        snapshot(NOW - timedelta(minutes=1), [(0, 120, 115), (1, 220, 215)]),
    )


def evidence_payload(previous: OffsetSnapshot, current: OffsetSnapshot) -> dict[str, object]:
    def encode(value: OffsetSnapshot) -> dict[str, object]:
        return {
            "captured_at": value.captured_at.isoformat(),
            "stream": value.stream,
            "consumer_group": value.consumer_group,
            "partitions": [asdict(item) for item in value.partitions],
        }

    return {"previous": encode(previous), "current": encode(current)}


def test_healthy_progress_passes_with_privacy_reduced_evidence() -> None:
    previous, current = healthy_pair()

    report = audit_consumer_lag(previous, current, observed_at=NOW)

    assert report.passed
    assert report.previous_total_lag == 30
    assert report.current_total_lag == 10
    assert report.progress_ratio == 1.0
    assert report.stalled_partitions == 0
    assert all(len(item.partition_ref) == 16 for item in report.partitions)
    rendered = json.dumps(report.as_dict())
    assert "orders.v1" not in rendered
    assert "billing-v1" not in rendered


def test_stalled_backlog_fails_progress_and_partition_policy() -> None:
    previous = snapshot(NOW - timedelta(minutes=6), [(0, 100, 80), (1, 200, 190)])
    current = snapshot(NOW - timedelta(minutes=1), [(0, 125, 80), (1, 220, 190)])

    report = audit_consumer_lag(previous, current, observed_at=NOW)

    assert not report.passed
    assert {violation.code for violation in report.violations} == {
        "consumption_progress_below_minimum",
        "stalled_partitions_exceeded",
    }
    assert report.stalled_partitions == 2


def test_lag_thresholds_report_aggregate_and_partition_failures() -> None:
    previous, current = healthy_pair()
    policy = ConsumerLagPolicy(max_total_lag=5, max_partition_lag=4)

    report = audit_consumer_lag(previous, current, policy=policy, observed_at=NOW)

    assert not report.passed
    assert [violation.code for violation in report.violations].count("partition_lag_exceeded") == 2
    assert "total_lag_exceeded" in {violation.code for violation in report.violations}


@pytest.mark.parametrize(
    ("previous_rows", "current_rows", "message"),
    [
        ([(0, 100, 80)], [(0, 99, 90)], "high watermark regressed"),
        ([(0, 100, 80)], [(0, 110, 79)], "committed offset regressed"),
        ([(0, 100, 80)], [(1, 110, 90)], "partition sets"),
    ],
)
def test_inconsistent_offset_evidence_fails_closed(
    previous_rows: list[tuple[int, int, int]],
    current_rows: list[tuple[int, int, int]],
    message: str,
) -> None:
    previous = snapshot(NOW - timedelta(minutes=6), previous_rows)
    current = snapshot(NOW - timedelta(minutes=1), current_rows)

    with pytest.raises(EvidenceError, match=message):
        audit_consumer_lag(previous, current, observed_at=NOW)


def test_stale_snapshot_is_a_policy_failure() -> None:
    previous = snapshot(NOW - timedelta(minutes=20), [(0, 100, 80)])
    current = snapshot(NOW - timedelta(minutes=10), [(0, 120, 115)])

    report = audit_consumer_lag(previous, current, observed_at=NOW)

    assert not report.passed
    assert {violation.code for violation in report.violations} == {"current_snapshot_stale"}


def test_partition_order_does_not_change_evidence_digest() -> None:
    previous, current = healthy_pair()
    reversed_previous = snapshot(previous.captured_at, [(1, 200, 190), (0, 100, 80)])
    reversed_current = snapshot(current.captured_at, [(1, 220, 215), (0, 120, 115)])

    first = audit_consumer_lag(previous, current, observed_at=NOW)
    second = audit_consumer_lag(reversed_previous, reversed_current, observed_at=NOW)

    assert first.evidence_digest == second.evidence_digest
    assert first.partitions == second.partitions


def test_cli_distinguishes_pass_policy_failure_and_invalid_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    previous, current = healthy_pair()
    passing = tmp_path / "passing.json"
    failing = tmp_path / "failing.json"
    invalid = tmp_path / "invalid.json"
    output = tmp_path / "report.json"
    passing.write_text(json.dumps(evidence_payload(previous, current)), encoding="utf-8")
    failing.write_text(json.dumps(evidence_payload(previous, current)), encoding="utf-8")
    invalid.write_text('{"previous": {}, "previous": {}}', encoding="utf-8")

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return NOW

    monkeypatch.setattr("event_platform.consumer_lag.datetime", FrozenDateTime)

    assert main([str(passing), "--output", str(output)]) == PASS_EXIT_CODE
    assert json.loads(output.read_text(encoding="utf-8"))["passed"] is True
    assert main([str(failing), "--max-total-lag", "5"]) == POLICY_FAILURE_EXIT_CODE
    assert main([str(invalid)]) == INVALID_EVIDENCE_EXIT_CODE
