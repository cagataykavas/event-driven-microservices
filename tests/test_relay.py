from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from event_platform.broker import InMemoryBroker
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


def seed(database: Database, clock: Clock) -> str:
    receipt = OrderService(database, clock=clock).create(
        CreateOrder.build(
            order_id="order-1", amount="10.00", currency="EUR", customer_id="customer-1"
        ),
        idempotency_key="command-1",
    )
    return receipt.event_id


def test_successful_publish_acknowledges_owned_outbox_row(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 8, tzinfo=UTC))
    event_id = seed(database, clock)
    broker = InMemoryBroker()
    result = OutboxRelay(database, broker.publish, worker_id="relay-1", clock=clock).run_once()

    assert result.published == 1
    assert broker.published[0].envelope.event_id == event_id
    row = database.connection.execute(
        "SELECT * FROM outbox WHERE event_id = ?", (event_id,)
    ).fetchone()
    assert row["status"] == "published"
    assert row["attempts"] == 1
    assert row["lease_owner"] is None


def test_failed_publish_is_delayed_by_exponential_backoff(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 8, tzinfo=UTC))
    event_id = seed(database, clock)
    broker = InMemoryBroker(fail_first={event_id})
    relay = OutboxRelay(
        database,
        broker.publish,
        worker_id="relay-1",
        clock=clock,
        policy=RelayPolicy(base_backoff_seconds=4, jitter_ratio=0),
    )

    failed = relay.run_once()
    assert failed.retried == 1
    assert relay.run_once().claimed == 0
    clock.advance(4)
    recovered = relay.run_once()
    assert recovered.published == 1
    assert database.connection.execute("SELECT attempts FROM outbox").fetchone()[0] == 2


def test_poison_event_moves_to_dead_letter_after_bounded_attempts(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 8, tzinfo=UTC))
    seed(database, clock)

    def always_fail(topic: str, envelope: object) -> None:
        raise RuntimeError("broker unavailable")

    relay = OutboxRelay(
        database,
        always_fail,
        worker_id="relay-1",
        clock=clock,
        policy=RelayPolicy(max_attempts=2, base_backoff_seconds=1, jitter_ratio=0),
    )
    assert relay.run_once().retried == 1
    clock.advance(1)
    assert relay.run_once().dead_lettered == 1
    dead = database.connection.execute("SELECT * FROM dead_letters").fetchone()
    assert dead["attempts"] == 2
    assert dead["reason"] == "UNCLASSIFIED_DELIVERY: broker unavailable"


def test_active_lease_prevents_second_worker_from_claiming(database: Database) -> None:
    clock = Clock(datetime(2026, 9, 8, tzinfo=UTC))
    seed(database, clock)
    with database.transaction() as connection:
        connection.execute(
            "UPDATE outbox SET lease_owner = 'relay-1', lease_until = ?",
            ((clock.value + timedelta(seconds=30)).isoformat(),),
        )
    second = OutboxRelay(database, lambda *_: None, worker_id="relay-2", clock=clock)
    assert second.run_once().claimed == 0
    clock.advance(31)
    assert second.run_once().published == 1
