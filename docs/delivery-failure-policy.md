# Delivery failure and retry policy

The outbox relay separates transport failures by operational meaning instead of
retrying every exception until the dead-letter threshold.

Transport adapters should raise:

- `RetryableDeliveryError` for timeouts, connection resets, throttling and
  temporary upstream unavailability. A parsed server retry hint can be supplied
  as `retry_after_seconds`.
- `PermanentDeliveryError` for authentication, authorization, schema, routing or
  policy failures that cannot succeed without changing the message or deployment.

Unknown exception types remain retryable for backward compatibility and are
persisted with the `UNCLASSIFIED_DELIVERY` reason code. Teams should remove that
ambiguity by mapping their client library's exceptions at the transport adapter.

## Retry schedule

The next attempt uses the greater of exponential backoff and the server hint,
adds deterministic event-specific positive jitter, then caps the result at
`max_backoff_seconds`. The same event and attempt always produce the same delay,
which makes tests and incident replay reproducible without synchronizing separate
events into a retry storm.

Server hints are treated as untrusted input: negative, boolean, infinite and NaN
values fail closed, while excessive finite hints are capped. Permanent failures
move to the dead-letter table after their first failed delivery and do not consume
the transient retry budget.

## Example adapter

```python
from event_platform.delivery import PermanentDeliveryError, RetryableDeliveryError


def publish(topic, envelope):
    response = client.send(topic, envelope.payload)
    if response.status_code == 429:
        raise RetryableDeliveryError(
            "broker throttled the publish",
            retry_after_seconds=parse_retry_after(response.headers),
        )
    if 400 <= response.status_code < 500:
        raise PermanentDeliveryError("broker rejected the publish contract")
    response.raise_for_status()
```

## Trust boundary and limitations

This module consumes an already parsed numeric retry hint; HTTP-date parsing and
clock-skew handling belong in the protocol adapter. Error classification is a
caller-owned policy decision: an incorrect permanent classification can discard a
recoverable delivery, while an incorrect retryable classification delays poison
message quarantine. The SQLite reference relay is process-local and does not
replace broker-native visibility timeouts, redrive policies, rate limits or
cross-region coordination.

The next step is to expose per-reason retry and dead-letter counters, then alert
on `UNCLASSIFIED_DELIVERY` so production adapters converge on an explicit failure
taxonomy.
