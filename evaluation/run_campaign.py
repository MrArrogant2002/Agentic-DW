from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List
from urllib import error, request

from agent.planner import Plan
from api import routes
from api.schemas import AnalyzeRequest


def _mock_dataset_metadata() -> Dict[str, Dict[str, Any]]:
    return {
        "ds_ecommerce": {
            "tables": [{"table_name": "sales", "columns": [{"column_name": "country"}, {"column_name": "order_date"}, {"column_name": "amount"}]}],
            "entities": [{"table": "sales", "column": "country"}],
            "measures": [{"table": "sales", "column": "amount"}],
            "time_columns": [{"table": "sales", "column": "order_date"}],
            "relationships": [],
        },
        "ds_finance": {
            "tables": [{"table_name": "ledger", "columns": [{"column_name": "region"}, {"column_name": "txn_date"}, {"column_name": "net_income"}]}],
            "entities": [{"table": "ledger", "column": "region"}],
            "measures": [{"table": "ledger", "column": "net_income"}],
            "time_columns": [{"table": "ledger", "column": "txn_date"}],
            "relationships": [],
        },
        "ds_scores": {
            "tables": [{"table_name": "scores", "columns": [{"column_name": "school"}, {"column_name": "exam_date"}, {"column_name": "score"}]}],
            "entities": [{"table": "scores", "column": "school"}],
            "measures": [{"table": "scores", "column": "score"}],
            "time_columns": [{"table": "scores", "column": "exam_date"}],
            "relationships": [],
        },
    }


def _mock_plan(question: str) -> Plan:
    q = question.lower()
    if "trend" in q:
        return Plan(
            question=question,
            requires_mining=True,
            intent="trend_analysis",
            planner_source="mock",
            task_type="trend_analysis",
            entity_scope="top_n" if "top" in q else "all",
            entity_dimension="country",
            n=5 if "top" in q else None,
            metric=None,
            time_grain="month",
            compare_against="global",
        )
    if "segment" in q:
        return Plan(
            question=question,
            requires_mining=True,
            intent="customer_segmentation",
            planner_source="mock",
            task_type="segmentation",
            entity_scope="all",
            entity_dimension=None,
            n=None,
            metric=None,
            time_grain="month",
            compare_against="none",
        )
    return Plan(
        question=question,
        requires_mining=False,
        intent="country_revenue",
        planner_source="mock",
        task_type="sql_retrieval",
        entity_scope="top_n",
        entity_dimension="country",
        n=5,
        metric=None,
        time_grain=None,
        compare_against="none",
    )


def _run_mock_campaign(limit: int) -> Dict[str, Any]:
    metadata_by_dataset = _mock_dataset_metadata()

    def fake_get_dataset(dataset_id: str):
        if dataset_id not in metadata_by_dataset:
            return None
        return {
            "dataset_id": dataset_id,
            "source_type": "db_connection",
            "db_engine": "postgres",
            "schema_hash": f"hash_{dataset_id}",
            "source_config": {},
        }

    def fake_load_schema_metadata(dataset_id: str):
        return metadata_by_dataset.get(dataset_id)

    def fake_build_plan(question: str, dataset_metadata=None, trace_id=None, prompt_version=None):
        return _mock_plan(question)

    def fake_generate_sql_from_plan(question, plan, dataset_metadata, previous_sql=None, error_message=None, trace_id=None, prompt_version=None):
        if plan.intent == "country_revenue":
            return 'SELECT "country", 100.0 AS revenue'
        return "SELECT 1"

    def fake_execute_safe_query(sql, row_limit=100, timeout_ms=15000, db_engine=None, source_config=None):
        if "country" in sql.lower():
            return [{"country": "A", "revenue": 100.0}, {"country": "B", "revenue": 80.0}]
        return [{"value": 1}]

    def fake_get_snapshot(snapshot_type: str, refresh_if_stale: bool = True, **kwargs):
        if snapshot_type == "trend_analysis":
            return {
                "snapshot_type": "trend_analysis",
                "dataset_id": kwargs.get("dataset_id"),
                "scope_key": "mock",
                "snapshot_json": {
                    "monthly_revenue": [{"month_key": "2025-01", "revenue": 100.0}, {"month_key": "2025-02", "revenue": 120.0}],
                    "trend": {"status": "ok", "points": 2, "slope_per_month": 20.0, "intercept": 100.0, "r2": 1.0, "direction": "upward"},
                },
                "source_max_date": "2025-02-01",
                "snapshot_version": 1,
                "run_id": "mock-run-trend",
                "generated_at": "2026-02-22T00:00:00+00:00",
                "refreshed": False,
            }
        return {
            "snapshot_type": "customer_segmentation",
            "dataset_id": kwargs.get("dataset_id"),
            "scope_key": "mock",
            "snapshot_json": {
                "rfm_summary": {"status": "ok", "customers": 10},
                "clustering": {"status": "ok", "k": 3, "silhouette_score": 0.5, "clusters": []},
            },
            "source_max_date": "2025-02-01",
            "snapshot_version": 1,
            "run_id": "mock-run-seg",
            "generated_at": "2026-02-22T00:00:00+00:00",
            "refreshed": False,
        }

    routes.get_dataset = fake_get_dataset
    routes.load_schema_metadata = fake_load_schema_metadata
    routes.build_plan = fake_build_plan
    routes.generate_sql_from_plan = fake_generate_sql_from_plan
    routes.execute_safe_query = fake_execute_safe_query
    routes.get_snapshot = fake_get_snapshot

    questions = [
        ("ds_ecommerce", "Top 5 countries by revenue"),
        ("ds_ecommerce", "show trend analysis"),
        ("ds_finance", "Top 5 regions by net income"),
        ("ds_finance", "show trend analysis"),
        ("ds_scores", "Top 5 schools by score"),
        ("ds_scores", "show trend analysis"),
    ]

    for dataset_id, question in questions:
        request = AnalyzeRequest(dataset_id=dataset_id, question=question, row_limit=10, timeout_ms=15000)
        routes.analyze(request)
        routes.analyze_report(request)

    return routes.evaluation_metrics(limit=limit)


def _http_post_json(base_url: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=f"{base_url.rstrip('/')}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_get_json(base_url: str, path: str) -> Dict[str, Any]:
    req = request.Request(url=f"{base_url.rstrip('/')}{path}", method="GET")
    with request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _check_thresholds(metrics: Dict[str, Any], thresholds: Dict[str, float]) -> Dict[str, Any]:
    rates = metrics.get("rates", {})
    verdict = {
        "execution_success_rate_ok": float(rates.get("execution_success_rate", 0.0)) >= thresholds["min_execution_success_rate"],
        "retry_rate_ok": float(rates.get("retry_rate", 1.0)) <= thresholds["max_retry_rate"],
        "avg_latency_ok": float(metrics.get("latency_ms", {}).get("avg_total", 1e9)) <= thresholds["max_avg_latency_ms"],
    }
    verdict["all_ok"] = all(verdict.values())
    return verdict


def _write_evaluation_markdown(out_path: Path, mode: str, metrics: Dict[str, Any], thresholds: Dict[str, float], verdict: Dict[str, Any]) -> None:
    lines = []
    lines.append("# Evaluation Report")
    lines.append("")
    lines.append(f"- Mode: `{mode}`")
    lines.append(f"- Analyze requests: `{metrics.get('totals', {}).get('analyze_requests')}`")
    lines.append(f"- Execution success rate: `{metrics.get('rates', {}).get('execution_success_rate')}`")
    lines.append(f"- Retry rate: `{metrics.get('rates', {}).get('retry_rate')}`")
    lines.append(f"- Cache hit rate: `{metrics.get('rates', {}).get('cache_hit_rate')}`")
    lines.append(f"- Avg latency (ms): `{metrics.get('latency_ms', {}).get('avg_total')}`")
    lines.append("")
    lines.append("## Thresholds")
    lines.append("")
    lines.append(f"- min_execution_success_rate: `{thresholds['min_execution_success_rate']}`")
    lines.append(f"- max_retry_rate: `{thresholds['max_retry_rate']}`")
    lines.append(f"- max_avg_latency_ms: `{thresholds['max_avg_latency_ms']}`")
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(f"- all_ok: `{verdict['all_ok']}`")
    lines.append(f"- execution_success_rate_ok: `{verdict['execution_success_rate_ok']}`")
    lines.append(f"- retry_rate_ok: `{verdict['retry_rate_ok']}`")
    lines.append(f"- avg_latency_ok: `{verdict['avg_latency_ok']}`")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def _run_live_campaign(base_url: str, datasets_file: Path, limit: int) -> Dict[str, Any]:
    datasets = json.loads(datasets_file.read_text(encoding="utf-8"))
    if not isinstance(datasets, list) or len(datasets) < 3:
        raise ValueError("Live campaign requires at least 3 dataset entries in datasets file.")

    for item in datasets:
        dataset_id = item.get("dataset_id")
        questions = item.get("questions", [])
        if not dataset_id or not isinstance(questions, list):
            continue
        for question in questions:
            payload = {"dataset_id": dataset_id, "question": str(question), "row_limit": 50}
            _http_post_json(base_url, "/analyze", payload)
            _http_post_json(base_url, "/analyze/report", payload)

    return _http_get_json(base_url, f"/evaluation/metrics?limit={int(limit)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run 3-dataset evaluation campaign and publish /evaluation/metrics output.")
    parser.add_argument("--limit", type=int, default=5000, help="Trace limit for metrics endpoint.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print output.")
    parser.add_argument("--mode", choices=["mock", "live"], default="mock", help="Campaign mode.")
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000", help="Base URL for live API campaign.")
    parser.add_argument("--datasets-file", default="evaluation/live_datasets.json", help="Live datasets file path.")
    parser.add_argument("--json-out", default="docs/evaluation_report.json")
    parser.add_argument("--md-out", default="docs/evaluation.md")
    args = parser.parse_args()

    os.environ.setdefault("SQL_LLM_ENABLED", "1")
    os.environ["INSIGHT_MODEL_ENABLED"] = "0"

    if args.mode == "mock":
        metrics = _run_mock_campaign(limit=args.limit)
    else:
        try:
            metrics = _run_live_campaign(
                base_url=args.api_base_url,
                datasets_file=Path(args.datasets_file),
                limit=args.limit,
            )
        except (error.URLError, TimeoutError, ValueError) as exc:
            raise SystemExit(f"Live campaign failed: {exc}") from exc

    thresholds = {
        "min_execution_success_rate": float(os.getenv("EVAL_MIN_EXECUTION_SUCCESS_RATE", "0.90")),
        "max_retry_rate": float(os.getenv("EVAL_MAX_RETRY_RATE", "0.30")),
        "max_avg_latency_ms": float(os.getenv("EVAL_MAX_AVG_LATENCY_MS", "2000")),
    }
    verdict = _check_thresholds(metrics, thresholds)
    report_payload = {"mode": args.mode, "metrics": metrics, "thresholds": thresholds, "verdict": verdict}
    Path(args.json_out).write_text(json.dumps(report_payload, indent=2), encoding="utf-8")
    _write_evaluation_markdown(Path(args.md_out), args.mode, metrics, thresholds, verdict)

    if args.pretty:
        print(json.dumps(report_payload, indent=2))
    else:
        print(json.dumps(report_payload))


if __name__ == "__main__":
    main()
