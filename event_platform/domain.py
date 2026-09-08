from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from typing import Any


class DomainError(ValueError):
    """Base error for rejected commands and corrupted messages."""


class IdempotencyConflict(DomainError):
    """An idempotency key was reused with different command content."""


class MessageMutation(DomainError):
    """A known message identifier arrived with a different payload."""


class OrderStatus(StrEnum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"


class EventType(StrEnum):
    ORDER_CREATED = "order.created.v1"
    PAYMENT_AUTHORIZED = "payment.authorized.v1"
    PAYMENT_DECLINED = "payment.declined.v1"
    INVENTORY_RESERVED = "inventory.reserved.v1"


def utc_now() -> datetime:
    return datetime.now(UTC)


def canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def money_to_minor_units(value: str | Decimal) -> int:
    try:
        amount = Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise DomainError("amount must be a decimal number") from exc
    if not amount.is_finite() or amount <= 0:
        raise DomainError("amount must be finite and positive")
    return int(amount * 100)


@dataclass(frozen=True, slots=True)
class CreateOrder:
    order_id: str
    amount_minor: int
    currency: str
    customer_id: str

    @classmethod
    def build(
        cls, *, order_id: str, amount: str | Decimal, currency: str, customer_id: str
    ) -> CreateOrder:
        normalized_currency = currency.strip().upper()
        if len(normalized_currency) != 3 or not normalized_currency.isalpha():
            raise DomainError("currency must be a three-letter code")
        if not order_id.strip() or not customer_id.strip():
            raise DomainError("order_id and customer_id are required")
        return cls(
            order_id.strip(), money_to_minor_units(amount), normalized_currency, customer_id.strip()
        )

    def fingerprint(self) -> str:
        return fingerprint(
            {
                "amount_minor": self.amount_minor,
                "currency": self.currency,
                "customer_id": self.customer_id,
                "order_id": self.order_id,
            }
        )


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event_id: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime
    correlation_id: str
    causation_id: str | None = None
    schema_version: int = 1

    @classmethod
    def create(
        cls,
        *,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
        correlation_id: str,
        causation_id: str | None = None,
        now: datetime | None = None,
        event_id: str | None = None,
    ) -> EventEnvelope:
        return cls(
            event_id=event_id or str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            occurred_at=now or utc_now(),
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    def payload_json(self) -> str:
        return canonical_json(self.payload)

    def payload_hash(self) -> str:
        return fingerprint(self.payload)
