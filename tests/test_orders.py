from __future__ import annotations

import pytest

from event_platform.domain import (
    CreateOrder,
    DomainError,
    IdempotencyConflict,
    money_to_minor_units,
)
from event_platform.orders import OrderService
from event_platform.storage import Database


def command(*, amount: str = "149.90") -> CreateOrder:
    return CreateOrder.build(
        order_id="order-1", amount=amount, currency="eur", customer_id="customer-1"
    )


def test_money_uses_decimal_minor_units_without_float_rounding() -> None:
    assert money_to_minor_units("10.015") == 1002
    with pytest.raises(DomainError, match="positive"):
        money_to_minor_units("0")
    with pytest.raises(DomainError, match="finite"):
        money_to_minor_units("NaN")


def test_create_order_commits_aggregate_outbox_and_receipt_atomically(database: Database) -> None:
    receipt = OrderService(database).create(command(), idempotency_key="request-1")
    order = database.connection.execute("SELECT * FROM orders").fetchone()
    event = database.connection.execute("SELECT * FROM outbox").fetchone()
    stored_receipt = database.connection.execute("SELECT * FROM command_receipts").fetchone()

    assert receipt.order_id == "order-1"
    assert not receipt.replayed
    assert order["amount_minor"] == 14990
    assert order["currency"] == "EUR"
    assert event["aggregate_id"] == order["order_id"]
    assert event["event_type"] == "order.created.v1"
    assert stored_receipt["aggregate_id"] == order["order_id"]


def test_same_idempotency_key_replays_original_response(database: Database) -> None:
    service = OrderService(database)
    original = service.create(command(), idempotency_key="request-1")
    replay = service.create(command(), idempotency_key="request-1")

    assert replay.replayed
    assert replay.event_id == original.event_id
    assert database.connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
    assert database.connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1


def test_key_reuse_with_changed_command_is_rejected(database: Database) -> None:
    service = OrderService(database)
    service.create(command(), idempotency_key="request-1")
    with pytest.raises(IdempotencyConflict, match="different command"):
        service.create(command(amount="150.00"), idempotency_key="request-1")


def test_existing_order_under_other_key_is_not_silently_replayed(database: Database) -> None:
    service = OrderService(database)
    service.create(command(), idempotency_key="request-1")
    with pytest.raises(IdempotencyConflict, match="order_id"):
        service.create(command(), idempotency_key="request-2")
    assert database.connection.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 1
