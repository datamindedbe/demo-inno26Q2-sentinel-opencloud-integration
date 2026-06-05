"""Push benchmark results to a Prometheus Pushgateway.

Each pod is a short-lived Job, so Prometheus can't scrape it directly — instead
we push a one-shot set of gauges to a Pushgateway, which Prometheus then scrapes
and retains. Concurrent pods don't overwrite each other because the grouping key
(instance + run_id + identity) makes every pod's push a distinct metric group.

No-op unless PUSHGATEWAY_URL is set, so local / standard-AWS runs are unaffected.
"""

from __future__ import annotations

import socket

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Job label all benchmark metrics share. Grafana queries filter on this.
JOB = "s3_benchmark"


def push_results(
    pushgateway_url: str,
    identity: str,
    run_id: str,
    summary: dict[str, dict],
) -> None:
    """Push the per-operation summary for this pod to the Pushgateway.

    `summary` maps an operation key (e.g. "write_big") to a dict with:
        ok    : bool   — did the operation succeed (False on access-denied/error)
        wall_s: float  — wall-clock seconds (optional)
        mbps  : float  — throughput in MB/s, for data ops (optional)
        rate  : float  — files/objects per second, for many-object ops (optional)
    """
    registry = CollectorRegistry()
    wall = Gauge(
        "s3_benchmark_op_wall_seconds",
        "Wall-clock time for a benchmark operation",
        ["operation"],
        registry=registry,
    )
    mbps = Gauge(
        "s3_benchmark_op_throughput_mbps",
        "Throughput in MB/s for data operations (big file)",
        ["operation"],
        registry=registry,
    )
    rate = Gauge(
        "s3_benchmark_op_rate_per_second",
        "Files/objects per second for many-object operations",
        ["operation"],
        registry=registry,
    )
    success = Gauge(
        "s3_benchmark_op_success",
        "1 if the operation succeeded, 0 if it failed (e.g. access denied)",
        ["operation"],
        registry=registry,
    )
    ts = Gauge(
        "s3_benchmark_last_push_timestamp_seconds",
        "Unix time at which this pod last pushed results",
        registry=registry,
    )

    for op, m in summary.items():
        success.labels(op).set(1 if m.get("ok") else 0)
        if m.get("wall_s") is not None:
            wall.labels(op).set(m["wall_s"])
        if m.get("mbps") is not None:
            mbps.labels(op).set(m["mbps"])
        if m.get("rate") is not None:
            rate.labels(op).set(m["rate"])
    ts.set_to_current_time()

    push_to_gateway(
        pushgateway_url,
        job=JOB,
        registry=registry,
        grouping_key={
            "instance": socket.gethostname(),
            "run_id": run_id,
            "identity": identity,
        },
    )
