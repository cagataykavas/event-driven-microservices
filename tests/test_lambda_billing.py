from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lambda_handlers.billing import deterministic_output_id, process_message


@dataclass
class FakeStore:
    inserted: bool = True
    calls: list[dict[str, Any]] = field(default_factory=list)

    def commit_once(self, **values: Any) -> bool:
        self.calls.append(values)
        return self.inserted


@dataclass
class FakePublisher:
    events: list[dict[str, Any]] = field(default_factory=list)

    def publish(self, event: dict[str, Any]) -> None:
        self.events.append(event)


def envelope(amount_minor: int = 1000) -> dict[str, Any]:
    return {
        "event_id": "event-1",
        "event_type": "order.created.v1",
        "aggregate_id": "order-1",
        "correlation_id": "correlation-1",
        "payload": {"order_id": "order-1", "amount_minor": amount_minor, "currency": "EUR"},
    }


def test_lambda_boundary_uses_deterministic_output_identity() -> None:
    store = FakeStore()
    publisher = FakePublisher()
    assert process_message(
        "sqs-message-1",
        envelope(),
        store=store,
        publisher=publisher,
        authorization_limit_minor=5000,
    )
    assert publisher.events[0]["event_id"] == deterministic_output_id("sqs-message-1")
    assert publisher.events[0]["event_type"] == "payment.authorized.v1"
    assert store.calls[0]["status"] == "authorized"


def test_replay_republishes_same_identity_for_downstream_deduplication() -> None:
    store = FakeStore(inserted=False)
    publisher = FakePublisher()
    inserted = process_message(
        "sqs-message-1",
        envelope(6000),
        store=store,
        publisher=publisher,
        authorization_limit_minor=5000,
    )
    assert not inserted
    assert publisher.events == [
        {
            "event_id": deterministic_output_id("sqs-message-1"),
            "event_type": "payment.declined.v1",
            "aggregate_id": "order-1",
            "correlation_id": "correlation-1",
            "causation_id": "event-1",
            "schema_version": 1,
            "payload": {"order_id": "order-1", "amount_minor": 6000, "currency": "EUR"},
        }
    ]
