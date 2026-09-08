from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime

from event_platform.domain import (
    CreateOrder,
    EventEnvelope,
    EventType,
    IdempotencyConflict,
    OrderStatus,
    utc_now,
)
from event_platform.storage import Database


@dataclass(frozen=True, slots=True)
class OrderReceipt:
    order_id: str
    event_id: str
    status: str
    replayed: bool


class OrderService:
    """Creates an order and its integration event in one database commit."""

    def __init__(self, database: Database, *, clock: Callable[[], datetime] = utc_now) -> None:
        self.database = database
        self.clock = clock

    def create(self, command: CreateOrder, *, idempotency_key: str) -> OrderReceipt:
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        request_hash = command.fingerprint()
        now: datetime = self.clock()
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT request_fingerprint, response_json FROM command_receipts "
                "WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != request_hash:
                    raise IdempotencyConflict("idempotency key was reused with a different command")
                response = json.loads(existing["response_json"])
                return OrderReceipt(**{**response, "replayed": True})

            event = EventEnvelope.create(
                aggregate_id=command.order_id,
                event_type=EventType.ORDER_CREATED,
                payload={
                    "order_id": command.order_id,
                    "customer_id": command.customer_id,
                    "amount_minor": command.amount_minor,
                    "currency": command.currency,
                },
                correlation_id=command.order_id,
                now=now,
            )
            try:
                connection.execute(
                    "INSERT INTO orders(order_id, customer_id, amount_minor, currency, status, "
                    "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        command.order_id,
                        command.customer_id,
                        command.amount_minor,
                        command.currency,
                        OrderStatus.PENDING,
                        now.isoformat(),
                        now.isoformat(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise IdempotencyConflict("order_id already exists under another command") from exc
            connection.execute(
                "INSERT INTO outbox(event_id, aggregate_id, event_type, schema_version, "
                "payload_json, payload_hash, correlation_id, causation_id, occurred_at, "
                "available_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            receipt = OrderReceipt(
                order_id=command.order_id,
                event_id=event.event_id,
                status=OrderStatus.PENDING,
                replayed=False,
            )
            stored_response = {**asdict(receipt), "replayed": False}
            connection.execute(
                "INSERT INTO command_receipts(idempotency_key, request_fingerprint, "
                "aggregate_id, response_json, created_at) VALUES (?, ?, ?, ?, ?)",
                (
                    idempotency_key,
                    request_hash,
                    command.order_id,
                    json.dumps(stored_response, sort_keys=True),
                    now.isoformat(),
                ),
            )
        return receipt
