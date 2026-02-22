from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from metadata.store import load_query_traces


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def build_metrics(traces: List[Dict[str, Any]]) -> Dict[str, Any]:
    analyze_events = [t for t in traces if t.get("stage") in (None, "analyze")]
    insight_events = [t for t in traces if t.get("stage") == "insight_generation"]

    total = len(analyze_events)
    success = sum(1 for t in analyze_events if t.get("evaluation_status") == "ok" and not t.get("error"))
    retries = sum(1 for t in analyze_events if int(t.get("retries_used") or 0) > 0)
    cache_hits = sum(1 for t in analyze_events if bool(t.get("cache_hit")))

    grounded = 0
    total_insight = 0
    for t in insight_events:
        if not t.get("enabled"):
            continue
        total_insight += 1
        if not t.get("error"):
            grounded += 1

    total_latency = 0.0
    latency_count = 0
    for t in analyze_events:
        timing = t.get("timing_ms") or {}
        try:
            total_latency += float(timing.get("total") or 0.0)
            latency_count += 1
        except (TypeError, ValueError):
            continue

    return {
        "totals": {
            "analyze_requests": total,
            "insight_requests_enabled": total_insight,
        },
        "rates": {
            "execution_success_rate": _safe_ratio(success, total),
            "retry_rate": _safe_ratio(retries, total),
            "cache_hit_rate": _safe_ratio(cache_hits, total),
            "insight_groundedness_rate": _safe_ratio(grounded, total_insight),
        },
        "latency_ms": {
            "avg_total": round(total_latency / latency_count, 3) if latency_count else 0.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute autonomy metrics from query trace logs.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of most-recent traces to read")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()

    traces = load_query_traces(limit=args.limit)
    metrics = build_metrics(traces)
    if args.pretty:
        print(json.dumps(metrics, indent=2))
    else:
        print(json.dumps(metrics))


if __name__ == "__main__":
    main()
