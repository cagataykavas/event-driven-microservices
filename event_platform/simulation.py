from __future__ import annotations

from dataclasses import asdict, dataclass

from event_platform.broker import InMemoryBroker
from event_platform.consumers import BillingHandler, IdempotentConsumer, OrderStatusHandler
from event_platform.domain import CreateOrder, EventType
from event_platform.orders import OrderService
from event_platform.relay import OutboxRelay, RelayResult
from event_platform.storage import Database


@dataclass(frozen=True, slots=True)
class SimulationReport:
    orders_created: int
    confirmed: int
    rejected: int
    published_messages: int
    inbox_receipts: int
    dead_letters: int
    relay_cycles: int
    relay_results: tuple[dict[str, int], ...]


def run_reference_scenario(
    database: Database, *, duplicate_deliveries: int = 2
) -> SimulationReport:
    broker = InMemoryBroker(duplicate_deliveries=duplicate_deliveries)
    billing = IdempotentConsumer(
        database,
        name="billing-v1",
        handler=BillingHandler(authorization_limit_minor=100_000),
    )
    orders_projection = IdempotentConsumer(
        database,
        name="orders-payment-projection-v1",
        handler=OrderStatusHandler(),
    )
    broker.subscribe(EventType.ORDER_CREATED, billing)
    broker.subscribe(EventType.PAYMENT_AUTHORIZED, orders_projection)
    broker.subscribe(EventType.PAYMENT_DECLINED, orders_projection)

    service = OrderService(database)
    commands = [
        CreateOrder.build(
            order_id="order-1001", amount="149.90", currency="EUR", customer_id="customer-1"
        ),
        CreateOrder.build(
            order_id="order-1002", amount="1250.00", currency="EUR", customer_id="customer-2"
        ),
    ]
    for index, command in enumerate(commands, start=1):
        service.create(command, idempotency_key=f"reference-command-{index}")

    relay = OutboxRelay(database, broker.publish, worker_id="reference-relay")
    results: list[RelayResult] = []
    for _ in range(10):
        result = relay.run_once()
        results.append(result)
        pending = database.connection.execute(
            "SELECT COUNT(*) FROM outbox WHERE status = 'pending'"
        ).fetchone()[0]
        if pending == 0:
            break

    statuses = dict(
        database.connection.execute(
            "SELECT status, COUNT(*) AS count FROM orders GROUP BY status"
        ).fetchall()
    )
    return SimulationReport(
        orders_created=len(commands),
        confirmed=int(statuses.get("confirmed", 0)),
        rejected=int(statuses.get("rejected", 0)),
        published_messages=len(broker.published),
        inbox_receipts=database.connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0],
        dead_letters=database.connection.execute("SELECT COUNT(*) FROM dead_letters").fetchone()[0],
        relay_cycles=len(results),
        relay_results=tuple(asdict(result) for result in results),
    )
