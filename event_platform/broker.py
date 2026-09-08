from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from event_platform.domain import EventEnvelope


class PublishError(RuntimeError):
    """The broker did not acknowledge an event."""


@dataclass(frozen=True, slots=True)
class PublishedMessage:
    topic: str
    envelope: EventEnvelope


class InMemoryBroker:
    """Deterministic at-least-once transport for tests and local demonstrations."""

    def __init__(
        self, *, fail_first: set[str] | None = None, duplicate_deliveries: int = 1
    ) -> None:
        if duplicate_deliveries <= 0:
            raise ValueError("duplicate_deliveries must be positive")
        self.fail_first = set(fail_first or ())
        self.duplicate_deliveries = duplicate_deliveries
        self.published: list[PublishedMessage] = []
        self._subscribers: dict[str, list[Callable[[EventEnvelope], None]]] = defaultdict(list)
        self._attempts: dict[str, int] = defaultdict(int)

    def subscribe(self, topic: str, handler: Callable[[EventEnvelope], None]) -> None:
        self._subscribers[topic].append(handler)

    def publish(self, topic: str, envelope: EventEnvelope) -> None:
        self._attempts[envelope.event_id] += 1
        if envelope.event_id in self.fail_first and self._attempts[envelope.event_id] == 1:
            raise PublishError("injected broker acknowledgement failure")
        self.published.append(PublishedMessage(topic, envelope))
        for handler in self._subscribers[topic]:
            for _ in range(self.duplicate_deliveries):
                handler(envelope)
