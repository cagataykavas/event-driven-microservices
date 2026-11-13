# Consumer-lag admission audit

Broker dashboards can show a small current backlog while hiding stale samples, offset resets, a
single hot partition, or a consumer that has stopped making progress. The
`event_platform.consumer_lag` audit evaluates two bounded offset snapshots before their health
claim is admitted into deployment or incident automation.

It validates that both snapshots describe the same stream, consumer group, and partition set;
requires monotonically increasing high watermarks and committed offsets; checks sample freshness
and spacing; and gates total lag, per-partition lag, lag growth, progress, and stalled partitions.
Reports contain aggregate counts and hashed partition references rather than infrastructure names.
The SHA-256 evidence digest binds the complete normalized snapshots and policy.

Input is strict JSON with `previous` and `current` snapshots:

```json
{
  "previous": {
    "captured_at": "2026-11-13T08:00:00+00:00",
    "stream": "orders.v1",
    "consumer_group": "billing-v1",
    "partitions": [
      {"partition": 0, "high_watermark": 100, "committed_offset": 80}
    ]
  },
  "current": {
    "captured_at": "2026-11-13T08:05:00+00:00",
    "stream": "orders.v1",
    "consumer_group": "billing-v1",
    "partitions": [
      {"partition": 0, "high_watermark": 120, "committed_offset": 115}
    ]
  }
}
```

```bash
python -m event_platform.consumer_lag offsets.json \
  --max-total-lag 10000 \
  --max-partition-lag 2000 \
  --min-progress-ratio 0.05 \
  --output consumer-lag-report.json
```

Exit code `0` means the policy passed, `2` means valid evidence failed policy, and `3` means the
evidence or operation was invalid. Inputs are size-bounded, reject symlinks, duplicate JSON keys,
unknown fields, non-finite values, duplicate partitions, and excessive partition counts. File
output uses an fsynced temporary file and atomic replacement.

## Limitations

Offset lag counts records, not elapsed processing time or business impact. Monotonic offsets do not
prove message correctness, sink durability, idempotency, or end-to-end completion. A partition can
make offset progress while repeatedly failing a subset of events, and retention or repartitioning
requires a new evidence identity rather than appearing as an offset regression. Thresholds require
calibration by traffic tier and message cost. The next step is a least-privilege Kafka or cloud
queue collector that signs snapshot provenance and correlates offset lag with event-time age and
end-to-end processing receipts.
