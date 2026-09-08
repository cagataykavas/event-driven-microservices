from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from typing import Any, Protocol


class AtomicBillingStore(Protocol):
    def commit_once(
        self,
        *,
        input_message_id: str,
        payload_hash: str,
        order_id: str,
        amount_minor: int,
        status: str,
        output_event: dict[str, Any],
    ) -> bool: ...


class EventPublisher(Protocol):
    def publish(self, event: dict[str, Any]) -> None: ...


class MutatedMessage(ValueError):
    """A transport message ID was reused with different content."""


def canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def message_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def deterministic_output_id(input_message_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"billing-result:{input_message_id}"))


def process_message(
    message_id: str,
    envelope: dict[str, Any],
    *,
    store: AtomicBillingStore,
    publisher: EventPublisher,
    authorization_limit_minor: int,
) -> bool:
    if envelope.get("event_type") != "order.created.v1":
        raise ValueError("billing accepts only order.created.v1")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise TypeError("event payload must be an object")
    order_id = str(payload["order_id"])
    amount_minor = int(payload["amount_minor"])
    if amount_minor <= 0:
        raise ValueError("amount_minor must be positive")
    status = "authorized" if amount_minor <= authorization_limit_minor else "declined"
    event_type = f"payment.{status}.v1"
    output = {
        "event_id": deterministic_output_id(message_id),
        "event_type": event_type,
        "aggregate_id": order_id,
        "correlation_id": envelope.get("correlation_id", order_id),
        "causation_id": envelope.get("event_id", message_id),
        "schema_version": 1,
        "payload": {
            "order_id": order_id,
            "amount_minor": amount_minor,
            "currency": payload["currency"],
        },
    }
    inserted = store.commit_once(
        input_message_id=message_id,
        payload_hash=message_hash(envelope),
        order_id=order_id,
        amount_minor=amount_minor,
        status=status,
        output_event=output,
    )
    # Publish on both first delivery and replay. If SNS acknowledged but Lambda
    # crashed before SQS acknowledgement, downstream inboxes absorb the duplicate.
    publisher.publish(output)
    return inserted


@dataclass(slots=True)
class DynamoBillingStore:
    client: Any
    inbox_table: str
    payments_table: str
    outbox_table: str

    def commit_once(
        self,
        *,
        input_message_id: str,
        payload_hash: str,
        order_id: str,
        amount_minor: int,
        status: str,
        output_event: dict[str, Any],
    ) -> bool:
        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": self.inbox_table,
                            "Item": {
                                "message_id": {"S": input_message_id},
                                "payload_hash": {"S": payload_hash},
                            },
                            "ConditionExpression": "attribute_not_exists(message_id)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self.payments_table,
                            "Item": {
                                "order_id": {"S": order_id},
                                "amount_minor": {"N": str(amount_minor)},
                                "status": {"S": status},
                                "input_message_id": {"S": input_message_id},
                            },
                            "ConditionExpression": "attribute_not_exists(order_id)",
                        }
                    },
                    {
                        "Put": {
                            "TableName": self.outbox_table,
                            "Item": {
                                "event_id": {"S": str(output_event["event_id"])},
                                "payload": {"S": canonical_json(output_event)},
                            },
                            "ConditionExpression": "attribute_not_exists(event_id)",
                        }
                    },
                ]
            )
            return True
        except self.client.exceptions.TransactionCanceledException:
            existing = self.client.get_item(
                TableName=self.inbox_table,
                Key={"message_id": {"S": input_message_id}},
                ConsistentRead=True,
            ).get("Item")
            if existing is None or existing["payload_hash"]["S"] != payload_hash:
                raise MutatedMessage("message ID replayed with different payload") from None
            return False


@dataclass(slots=True)
class SnsPublisher:
    client: Any
    topic_arn: str

    def publish(self, event: dict[str, Any]) -> None:
        self.client.publish(
            TopicArn=self.topic_arn,
            Message=canonical_json(event),
            MessageAttributes={
                "event_type": {"DataType": "String", "StringValue": str(event["event_type"])}
            },
        )


def handle(event: dict[str, Any], _: object) -> dict[str, list[dict[str, str]]]:
    import boto3

    dynamodb = boto3.client("dynamodb")
    store = DynamoBillingStore(
        dynamodb,
        inbox_table=os.environ["INBOX_TABLE"],
        payments_table=os.environ["PAYMENTS_TABLE"],
        outbox_table=os.environ["OUTBOX_TABLE"],
    )
    publisher = SnsPublisher(boto3.client("sns"), os.environ["PAYMENT_EVENTS_TOPIC_ARN"])
    limit = int(os.getenv("AUTHORIZATION_LIMIT_MINOR", "100000"))
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        message_id = str(record["messageId"])
        try:
            process_message(
                message_id,
                json.loads(record["body"]),
                store=store,
                publisher=publisher,
                authorization_limit_minor=limit,
            )
        except Exception:  # noqa: BLE001 - Lambda partial-batch contract retries only this record.
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
