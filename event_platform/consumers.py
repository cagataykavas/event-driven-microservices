from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import datetime

from event_platform.domain import (
    EventEnvelope,
    EventType,
    MessageMutation,
    OrderStatus,
    utc_now,
)
from event_platform.storage import Database

TransactionalHandler = Callable[[sqlite3.Connection, EventEnvelope, datetime], None]


def append_event(connection: sqlite3.Connection, event: EventEnvelope) -> None:
    connection.execute(
        "INSERT INTO outbox(event_id, aggregate_id, event_type, schema_version, payload_json, "
        "payload_hash, correlation_id, causation_id, occurred_at, available_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event.event_id,
            event.aggregate_id,
            event.event_type,
            event.schema_version,
            event.payload_json(),
            event.payload_hash(),
            event.correlation_id,
            event.causation_id,
            event.occurred_at.isoformat(),
            event.occurred_at.isoformat(),
        ),
    )


class IdempotentConsumer:
    """Commits a business side effect and inbox receipt in one transaction."""

    def __init__(
        self,
        database: Database,
        *,
        name: str,
        handler: TransactionalHandler,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.database = database
        self.name = name
        self.handler = handler
        self.clock = clock

    def __call__(self, envelope: EventEnvelope) -> bool:
        now = self.clock()
        payload_hash = envelope.payload_hash()
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT payload_hash FROM inbox WHERE consumer_name = ? AND message_id = ?",
                (self.name, envelope.event_id),
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    raise MessageMutation(
                        "message identifier was replayed with different payload content"
                    )
                return False
            self.handler(connection, envelope, now)
            connection.execute(
                "INSERT INTO inbox(consumer_name, message_id, payload_hash, processed_at) "
                "VALUES (?, ?, ?, ?)",
                (self.name, envelope.event_id, payload_hash, now.isoformat()),
            )
        return True


class BillingHandler:
    """Authorizes orders under a configurable minor-unit risk limit."""

    def __init__(self, *, authorization_limit_minor: int = 100_000) -> None:
        if authorization_limit_minor <= 0:
            raise ValueError("authorization_limit_minor must be positive")
        self.authorization_limit_minor = authorization_limit_minor

    def __call__(
        self, connection: sqlite3.Connection, envelope: EventEnvelope, now: datetime
    ) -> None:
        if envelope.event_type != EventType.ORDER_CREATED:
            raise ValueError(f"billing cannot process {envelope.event_type}")
        amount_minor = int(envelope.payload["amount_minor"])
        authorized = amount_minor <= self.authorization_limit_minor
        status = "authorized" if authorized else "declined"
        connection.execute(
            "INSERT INTO payments(order_id, event_id, status, amount_minor, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                envelope.aggregate_id,
                envelope.event_id,
                status,
                amount_minor,
                now.isoformat(),
            ),
        )
        result_type = EventType.PAYMENT_AUTHORIZED if authorized else EventType.PAYMENT_DECLINED
        append_event(
            connection,
            EventEnvelope.create(
                aggregate_id=envelope.aggregate_id,
                event_type=result_type,
                payload={
                    "order_id": envelope.aggregate_id,
                    "amount_minor": amount_minor,
                    "currency": envelope.payload["currency"],
                },
                correlation_id=envelope.correlation_id,
                causation_id=envelope.event_id,
                now=now,
            ),
        )


class OrderStatusHandler:
    """Projects payment results back into the order aggregate."""

    def __call__(
        self, connection: sqlite3.Connection, envelope: EventEnvelope, now: datetime
    ) -> None:
        transitions = {
            EventType.PAYMENT_AUTHORIZED: OrderStatus.CONFIRMED,
            EventType.PAYMENT_DECLINED: OrderStatus.REJECTED,
        }
        try:
            status = transitions[EventType(envelope.event_type)]
        except (KeyError, ValueError) as exc:
            raise ValueError(f"orders projection cannot process {envelope.event_type}") from exc
        cursor = connection.execute(
            "UPDATE orders SET status = ?, version = version + 1, updated_at = ? "
            "WHERE order_id = ? AND status = ?",
            (status, now.isoformat(), envelope.aggregate_id, OrderStatus.PENDING),
        )
        if cursor.rowcount != 1:
            row = connection.execute(
                "SELECT status FROM orders WHERE order_id = ?", (envelope.aggregate_id,)
            ).fetchone()
            if row is None:
                raise ValueError("payment result references an unknown order")
            if row["status"] != status:
                raise ValueError(f"illegal order transition from {row['status']} to {status}")
