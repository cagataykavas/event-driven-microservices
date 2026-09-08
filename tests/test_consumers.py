from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest

from event_platform.consumers import BillingHandler, IdempotentConsumer
from event_platform.domain import CreateOrder, EventEnvelope, EventType, MessageMutation
from event_platform.orders import OrderService
from event_platform.storage import Database

NOW = datetime(2026, 9, 8, tzinfo=UTC)


def order_event(*, event_id: str = "event-1", amount_minor: int = 1000) -> EventEnvelope:
    return EventEnvelope.create(
        event_id=event_id,
        aggregate_id="order-1",
        event_type=EventType.ORDER_CREATED,
        payload={"order_id": "order-1", "amount_minor": amount_minor, "currency": "EUR"},
        correlation_id="correlation-1",
        now=NOW,
    )


def seed_order(database: Database) -> None:
    OrderService(database, clock=lambda: NOW).create(
        CreateOrder.build(
            order_id="order-1", amount="10.00", currency="EUR", customer_id="customer-1"
        ),
        idempotency_key="seed",
    )


def test_duplicate_delivery_commits_side_effect_exactly_once(database: Database) -> None:
    seed_order(database)
    consumer = IdempotentConsumer(
        database, name="billing", handler=BillingHandler(), clock=lambda: NOW
    )
    envelope = order_event()

    assert consumer(envelope)
    assert not consumer(envelope)
    assert database.connection.execute("SELECT COUNT(*) FROM payments").fetchone()[0] == 1
    assert database.connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 1
    assert (
        database.connection.execute(
            "SELECT COUNT(*) FROM outbox WHERE event_type = 'payment.authorized.v1'"
        ).fetchone()[0]
        == 1
    )


def test_same_message_id_with_mutated_payload_is_quarantined(database: Database) -> None:
    seed_order(database)
    consumer = IdempotentConsumer(database, name="billing", handler=BillingHandler())
    consumer(order_event())
    with pytest.raises(MessageMutation, match="different payload"):
        consumer(order_event(amount_minor=999))


def test_failed_handler_rolls_back_inbox_receipt(database: Database) -> None:
    def fail(connection: sqlite3.Connection, envelope: EventEnvelope, now: datetime) -> None:
        connection.execute(
            "INSERT INTO inbox VALUES ('temporary', 'side-effect', 'hash', ?)",
            (now.isoformat(),),
        )
        raise RuntimeError("poison message")

    consumer = IdempotentConsumer(database, name="failing", handler=fail)
    with pytest.raises(RuntimeError, match="poison"):
        consumer(order_event())
    assert database.connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0] == 0


def test_billing_declines_amount_above_policy_limit(database: Database) -> None:
    seed_order(database)
    consumer = IdempotentConsumer(
        database,
        name="billing",
        handler=BillingHandler(authorization_limit_minor=500),
    )
    consumer(order_event(amount_minor=1000))
    payment = database.connection.execute("SELECT status FROM payments").fetchone()
    assert payment["status"] == "declined"
