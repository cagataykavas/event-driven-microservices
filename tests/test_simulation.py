from __future__ import annotations

from event_platform.simulation import run_reference_scenario
from event_platform.storage import Database


def test_reference_scenario_converges_under_duplicate_delivery(database: Database) -> None:
    report = run_reference_scenario(database, duplicate_deliveries=3)
    assert report.orders_created == 2
    assert report.confirmed == 1
    assert report.rejected == 1
    assert report.published_messages == 4
    assert report.inbox_receipts == 4
    assert report.dead_letters == 0
    assert report.relay_cycles == 2


def test_reference_scenario_is_persisted_not_mocked(database: Database) -> None:
    run_reference_scenario(database)
    rows = database.connection.execute(
        "SELECT order_id, status, version FROM orders ORDER BY order_id"
    ).fetchall()
    assert [(row["order_id"], row["status"], row["version"]) for row in rows] == [
        ("order-1001", "confirmed", 2),
        ("order-1002", "rejected", 2),
    ]
