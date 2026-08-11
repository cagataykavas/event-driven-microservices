from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY, status TEXT NOT NULL, amount REAL NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, published INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS processed_messages(consumer TEXT NOT NULL, message_id TEXT NOT NULL, processed_at TEXT NOT NULL, PRIMARY KEY(consumer, message_id));
"""


def create_order(conn: sqlite3.Connection, order_id: str, amount: float) -> str:
    event_id = str(uuid.uuid4())
    payload = json.dumps({"order_id": order_id, "amount": amount})
    with conn:
        conn.execute("INSERT INTO orders(id, status, amount) VALUES (?, 'created', ?)", (order_id, amount))
        conn.execute(
            "INSERT INTO outbox(id, aggregate_id, event_type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_id, order_id, "OrderCreated", payload, datetime.now(timezone.utc).isoformat()),
        )
    return event_id


def consume_once(conn: sqlite3.Connection, consumer: str, message_id: str, handler) -> bool:
    with conn:
        exists = conn.execute("SELECT 1 FROM processed_messages WHERE consumer=? AND message_id=?", (consumer, message_id)).fetchone()
        if exists:
            return False
        handler()
        conn.execute(
            "INSERT INTO processed_messages(consumer, message_id, processed_at) VALUES (?, ?, ?)",
            (consumer, message_id, datetime.now(timezone.utc).isoformat()),
        )
    return True


if __name__ == "__main__":
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    event = create_order(db, "order-1001", 149.90)
    print("outbox event:", db.execute("SELECT event_type, payload FROM outbox WHERE id=?", (event,)).fetchone())
    print("first delivery:", consume_once(db, "billing", event, lambda: print("charged exactly once")))
    print("duplicate delivery:", consume_once(db, "billing", event, lambda: print("should not execute")))
