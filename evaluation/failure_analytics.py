from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from typing import Any, Dict, List

from metadata.store import load_query_traces


def build_failure_analytics(traces: List[Dict[str, Any]]) -> Dict[str, Any]:
    analyze_events = [t for t in traces if t.get("stage") in (None, "analyze")]
    failures = [t for t in analyze_events if t.get("error") or t.get("evaluation_status") not in {None, "ok"}]

    error_counter: Counter[str] = Counter()
    dataset_failures: defaultdict[str, int] = defaultdict(int)
    dataset_totals: defaultdict[str, int] = defaultdict(int)
    retry_counter: Counter[str] = Counter()

    for event in analyze_events:
        ds = str(event.get("dataset_id") or "global")
        dataset_totals[ds] += 1
        retries = int(event.get("retries_used") or 0)
        retry_counter[ds] += retries

    for event in failures:
        ds = str(event.get("dataset_id") or "global")
        dataset_failures[ds] += 1
        err = str(event.get("error") or event.get("evaluation_reason") or "unknown_error").strip()
        if not err:
            err = "unknown_error"
        prefix = err.split("::", 1)[0]
        error_counter[prefix] += 1

    per_dataset = []
    for ds, total in sorted(dataset_totals.items()):
        failed = dataset_failures.get(ds, 0)
        per_dataset.append(
            {
                "dataset_id": ds,
                "total_requests": total,
                "failed_requests": failed,
                "failure_rate": round(failed / total, 4) if total else 0.0,
                "total_retries": retry_counter.get(ds, 0),
            }
        )

    return {
        "summary": {
            "total_analyze_requests": len(analyze_events),
            "total_failed_requests": len(failures),
            "overall_failure_rate": round(len(failures) / len(analyze_events), 4) if analyze_events else 0.0,
        },
        "error_taxonomy": dict(error_counter.most_common()),
        "dataset_breakdown": per_dataset,
        "top_failure_examples": [
            {
                "trace_id": event.get("trace_id"),
                "dataset_id": event.get("dataset_id"),
                "question": event.get("question"),
                "error": event.get("error"),
                "evaluation_reason": event.get("evaluation_reason"),
                "retries_used": event.get("retries_used"),
            }
            for event in failures[:10]
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build failure analytics from query traces.")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    traces = load_query_traces(limit=args.limit)
    report = build_failure_analytics(traces)
    if args.pretty:
        print(json.dumps(report, indent=2))
    else:
        print(json.dumps(report))


if __name__ == "__main__":
    main()
