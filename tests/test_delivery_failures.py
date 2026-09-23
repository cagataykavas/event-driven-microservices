from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from event_platform.delivery import PermanentDeliveryError, RetryableDeliveryError
from event_platform.domain import CreateOrder
from event_platform.orders import OrderService
from event_platform.relay import OutboxRelay, RelayPolicy
from event_platform.storage import Database


@dataclass
class Clock:
    value: datetime

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def seed(database: Database, clock: Clock, *, order_id: str = "order-1") -> str:
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


def test_permanent_failure_is_dead_lettered_without_retry(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 23, tzinfo=UTC))
    event_id = seed(database, clock)

    def reject(*_: object) -> None:
        raise PermanentDeliveryError("destination rejected the event schema")

    result = OutboxRelay(database, reject, worker_id="relay-1", clock=clock).run_once()

    assert result.dead_lettered == 1
    row = database.connection.execute(
        "SELECT status, attempts, last_error FROM outbox WHERE event_id = ?", (event_id,)
    ).fetchone()
    assert tuple(row) == (
        "dead",
        1,
        "PERMANENT_DELIVERY: destination rejected the event schema",
    )


def test_retry_hint_is_honored_and_capped(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 23, tzinfo=UTC))
    event_id = seed(database, clock)

    def throttle(*_: object) -> None:
        raise RetryableDeliveryError("upstream throttled", retry_after_seconds=600)

    relay = OutboxRelay(
        database,
        throttle,
        worker_id="relay-1",
        clock=clock,
        policy=RelayPolicy(max_backoff_seconds=90, jitter_ratio=0.5),
    )
    assert relay.run_once().retried == 1
    available_at = database.connection.execute(
        "SELECT available_at FROM outbox WHERE event_id = ?", (event_id,)
    ).fetchone()[0]
    assert datetime.fromisoformat(available_at) == clock.value + timedelta(seconds=90)
    clock.advance(89)
    assert relay.run_once().claimed == 0


def test_deterministic_jitter_spreads_events_without_random_state() -> None:
    policy = RelayPolicy(base_backoff_seconds=100, max_backoff_seconds=1000, jitter_ratio=0.5)

    first = policy.retry_delay(attempt=1, event_id="event-a")

    assert first == policy.retry_delay(attempt=1, event_id="event-a")
    assert first != policy.retry_delay(attempt=1, event_id="event-b")
    assert 100 <= first <= 150


def test_retry_hint_sets_a_floor_before_jitter() -> None:
    policy = RelayPolicy(base_backoff_seconds=2, max_backoff_seconds=100, jitter_ratio=0.25)

    delay = policy.retry_delay(
        attempt=1,
        event_id="event-a",
        retry_after_seconds=40,
    )

    assert 40 <= delay <= 50


@pytest.mark.parametrize("value", [-1, math.inf, math.nan, True])
def test_retryable_error_rejects_unsafe_retry_hints(value: object) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        RetryableDeliveryError("bad hint", retry_after_seconds=value)  # type: ignore[arg-type]


def test_permanent_failure_rejects_retry_hint() -> None:
    with pytest.raises(ValueError, match="cannot carry"):
        PermanentDeliveryError("not retryable", retry_after_seconds=1)


def test_policy_rejects_invalid_attempts() -> None:
    policy = RelayPolicy(base_backoff_seconds=2, max_backoff_seconds=2)
    with pytest.raises(ValueError, match="positive integer"):
        policy.retry_delay(attempt=0, event_id="event-a")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": True},
        {"lease_seconds": 0},
        {"max_attempts": 1.5},
        {"base_backoff_seconds": -1},
        {"max_backoff_seconds": 0},
    ],
)
def test_policy_rejects_invalid_integer_fields(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        RelayPolicy(**kwargs)  # type: ignore[arg-type]


def test_backoff_saturates_without_unbounded_exponentiation() -> None:
    policy = RelayPolicy(base_backoff_seconds=2, max_backoff_seconds=300)

    assert policy.backoff(10_000_000) == 300
