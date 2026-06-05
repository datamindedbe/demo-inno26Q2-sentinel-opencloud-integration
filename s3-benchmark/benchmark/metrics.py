"""Push benchmark results to a Prometheus Pushgateway.

Each pod is a short-lived Job, so Prometheus can't scrape it directly — instead
we push a one-shot set of gauges to a Pushgateway, which Prometheus then scrapes
and retains. Concurrent pods don't overwrite each other because the grouping key
(instance + run_id + identity) makes every pod's push a distinct metric group.

No-op unless PUSHGATEWAY_URL is set, so local / standard-AWS runs are unaffected.
"""

from __future__ import annotations

import re
import socket

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Job label all benchmark metrics share. Grafana queries filter on this.
JOB = "s3_benchmark"

# Prometheus label names must match this; we sanitise experiment label keys to it.
_LABEL_NAME_RE = re.compile(r"[^a-zA-Z0-9_]")


def parse_labels(spec: str | None) -> dict[str, str]:
    """Parse "k1=v1,k2=v2" into a label dict, sanitising keys to valid names.

    Tolerant of blanks/whitespace and missing values; skips malformed pairs so a
    typo in EXPERIMENT_LABELS can never abort the run.
    """
    labels: dict[str, str] = {}
    for pair in (spec or "").split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        key, _, value = pair.partition("=")
        key = _LABEL_NAME_RE.sub("_", key.strip())
        if key and not key[0].isdigit():
            labels[key] = value.strip()
    return labels


def push_results(
    pushgateway_url: str,
    identity: str,
    run_id: str,
    summary: dict[str, dict],
    expected_denied: bool = False,
    experiment_id: str = "",
    experiment_labels: dict[str, str] | None = None,
) -> None:
    """Push the per-operation summary for this pod to the Pushgateway.

    `summary` maps an operation key (e.g. "write_big") to a dict with:
        ok    : bool   — did the operation succeed (False on access-denied/error)
        wall_s: float  — wall-clock seconds (optional)
        mbps  : float  — throughput in MB/s, for data ops (optional)
        rate  : float  — files/objects per second, for many-object ops (optional)

    `expected_denied` flags a run that was deliberately pointed at a forbidden
    path, so Grafana can tell an expected denial from a policy-bug one.

    `experiment_id` and `experiment_labels` (the run's knobs) are attached to the
    grouping key, so they become labels on *every* pushed series — letting you
    filter/group a whole experiment in Grafana.
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
    expected_denied_g = Gauge(
        "s3_benchmark_expected_denied",
        "1 if this run deliberately targeted a forbidden path (expected denial)",
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
    expected_denied_g.set(1 if expected_denied else 0)

    # The grouping key labels every series in this push. instance + run_id keep
    # each pod's group distinct; experiment_id + the run's knobs let Grafana
    # filter/group a whole experiment. Reserved keys win over experiment labels.
    grouping_key: dict[str, str] = dict(experiment_labels or {})
    if experiment_id:
        grouping_key["experiment_id"] = experiment_id
    grouping_key.update(
        {
            "instance": socket.gethostname(),
            "run_id": run_id,
            "identity": identity,
        }
    )

    push_to_gateway(
        pushgateway_url,
        job=JOB,
        registry=registry,
        grouping_key=grouping_key,
    )
