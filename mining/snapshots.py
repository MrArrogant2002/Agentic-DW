import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from agent.planner import Plan
from mining.clustering import run_kmeans
from mining.feature_builder import feature_builder
from mining.common import db_cursor
from mining.rfm import normalize_rfm_rows, run as run_rfm, summarize_rfm
from mining.trend import run as run_trend, run_from_rows
from utils.env_loader import load_environments

SNAPSHOT_TYPES = {"trend_analysis", "customer_segmentation"}
DEFAULT_DATASET_ID = "__default__"
DEFAULT_SCOPE_KEY = "all"


def ensure_snapshot_table() -> None:
    sql = """
        CREATE TABLE IF NOT EXISTS mining_snapshots (
            snapshot_type VARCHAR(64) NOT NULL,
            dataset_id VARCHAR(128) NOT NULL DEFAULT '__default__',
            scope_key VARCHAR(256) NOT NULL DEFAULT 'all',
            snapshot_json JSONB NOT NULL,
            source_max_date DATE,
            snapshot_version INTEGER NOT NULL DEFAULT 1,
            run_id VARCHAR(64),
            generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_type, dataset_id, scope_key)
        )
    """
    with db_cursor(write=True) as cur:
        cur.execute(sql)
        cur.execute("ALTER TABLE mining_snapshots ADD COLUMN IF NOT EXISTS dataset_id VARCHAR(128) NOT NULL DEFAULT '__default__'")
        cur.execute("ALTER TABLE mining_snapshots ADD COLUMN IF NOT EXISTS scope_key VARCHAR(256) NOT NULL DEFAULT 'all'")
        cur.execute("ALTER TABLE mining_snapshots ADD COLUMN IF NOT EXISTS snapshot_version INTEGER NOT NULL DEFAULT 1")
        cur.execute("ALTER TABLE mining_snapshots ADD COLUMN IF NOT EXISTS run_id VARCHAR(64)")
        cur.execute("ALTER TABLE mining_snapshots DROP CONSTRAINT IF EXISTS mining_snapshots_pkey")
        cur.execute("ALTER TABLE mining_snapshots ADD CONSTRAINT mining_snapshots_pkey PRIMARY KEY (snapshot_type, dataset_id, scope_key)")


def _get_fact_max_date():
    with db_cursor() as cur:
        cur.execute("SELECT MAX(date_id) FROM fact_sales")
        return cur.fetchone()[0]


def _build_scope_key(plan: Optional[Plan]) -> str:
    if not plan:
        return DEFAULT_SCOPE_KEY
    payload = {
        "entity_scope": plan.entity_scope,
        "entity_dimension": plan.entity_dimension,
        "n": plan.n,
        "metric": plan.metric,
        "time_grain": plan.time_grain,
        "compare_against": plan.compare_against,
    }
    return json.dumps(payload, sort_keys=True)


def _extract_source_max_date(rows: List[Dict[str, Any]]) -> Optional[str]:
    candidates: List[str] = []
    for row in rows:
        raw = row.get("period_start") or row.get("latest_event_date")
        if raw is None:
            continue
        raw_str = str(raw)
        candidates.append(raw_str[:10])
    if not candidates:
        return None
    return max(candidates)


def _build_snapshot_payload(
    snapshot_type: str,
    plan: Optional[Plan] = None,
    dataset_metadata: Optional[Dict[str, Any]] = None,
    db_engine: str = "postgres",
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if dataset_metadata and not plan:
        if snapshot_type == "trend_analysis":
            plan = Plan(
                question="snapshot trend analysis",
                requires_mining=True,
                intent="trend_analysis",
                planner_source="system",
                task_type="trend_analysis",
                entity_scope="all",
                entity_dimension=None,
                n=None,
                metric=None,
                time_grain="month",
                compare_against="none",
            )
        elif snapshot_type == "customer_segmentation":
            plan = Plan(
                question="snapshot customer segmentation",
                requires_mining=True,
                intent="customer_segmentation",
                planner_source="system",
                task_type="segmentation",
                entity_scope="all",
                entity_dimension=None,
                n=None,
                metric=None,
                time_grain="month",
                compare_against="none",
            )

    if dataset_metadata and plan:
        if snapshot_type == "trend_analysis":
            built = feature_builder(
                schema_metadata=dataset_metadata,
                plan=plan,
                db_engine=db_engine,
                source_config=source_config,
            )
            if built.get("status") != "ok":
                return {"status": built.get("status"), "reason": built.get("reason"), "trend": {"status": "insufficient_data"}}

            scoped_rows = built.get("rows", [])
            global_rows = None
            if plan.compare_against == "global" and plan.entity_scope == "top_n":
                global_plan = Plan(
                    question=plan.question,
                    requires_mining=plan.requires_mining,
                    intent=plan.intent,
                    planner_source=plan.planner_source,
                    task_type=plan.task_type,
                    entity_scope="all",
                    entity_dimension=None,
                    n=None,
                    metric=plan.metric,
                    time_grain=plan.time_grain,
                    compare_against="none",
                )
                global_built = feature_builder(
                    schema_metadata=dataset_metadata,
                    plan=global_plan,
                    db_engine=db_engine,
                    source_config=source_config,
                )
                if global_built.get("status") == "ok":
                    global_rows = global_built.get("rows", [])

            trend_payload = run_from_rows(scoped_rows, global_rows=global_rows)
            trend_payload["feature_sql"] = built.get("sql")
            trend_payload["feature_row_count"] = len(scoped_rows)
            return trend_payload

        if snapshot_type == "customer_segmentation":
            built = feature_builder(
                schema_metadata=dataset_metadata,
                plan=plan,
                db_engine=db_engine,
                source_config=source_config,
            )
            if built.get("status") != "ok":
                return {"status": built.get("status"), "reason": built.get("reason"), "clustering": {"status": "insufficient_data"}}
            rfm_rows = normalize_rfm_rows(built.get("rows", []))
            rfm_summary = summarize_rfm(rfm_rows)
            clustering = run_kmeans(rfm_rows=rfm_rows)
            return {
                "rfm_summary": rfm_summary,
                "clustering": clustering,
                "feature_sql": built.get("sql"),
                "feature_row_count": len(built.get("rows", [])),
            }

    if snapshot_type == "trend_analysis":
        return run_trend()
    if snapshot_type == "customer_segmentation":
        rfm = run_rfm()
        clustering = run_kmeans()
        return {"rfm_summary": rfm.get("summary"), "clustering": clustering}
    raise ValueError(f"Unsupported snapshot_type: {snapshot_type}")


def refresh_snapshot(
    snapshot_type: str,
    dataset_id: Optional[str] = None,
    plan: Optional[Plan] = None,
    dataset_metadata: Optional[Dict[str, Any]] = None,
    db_engine: str = "postgres",
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if snapshot_type not in SNAPSHOT_TYPES:
        raise ValueError(f"snapshot_type must be one of {sorted(SNAPSHOT_TYPES)}")

    ensure_snapshot_table()
    dataset_key = dataset_id or DEFAULT_DATASET_ID
    scope_key = _build_scope_key(plan)
    payload = _build_snapshot_payload(
        snapshot_type=snapshot_type,
        plan=plan,
        dataset_metadata=dataset_metadata,
        db_engine=db_engine,
        source_config=source_config,
    )

    if dataset_metadata and plan:
        source_max_date_str = _extract_source_max_date(payload.get("monthly_revenue", []) or payload.get("global_monthly_revenue", []))
    else:
        source_max_date_str = str(_get_fact_max_date()) if _get_fact_max_date() else None

    source_max_date = source_max_date_str
    generated_at = datetime.now(timezone.utc).isoformat()
    run_id = str(uuid4())

    with db_cursor(write=True) as cur:
        cur.execute(
            """
            INSERT INTO mining_snapshots (
                snapshot_type, dataset_id, scope_key, snapshot_json, source_max_date, snapshot_version, run_id, generated_at
            )
            VALUES (%s, %s, %s, %s::jsonb, %s, 1, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (snapshot_type, dataset_id, scope_key)
            DO UPDATE SET
                snapshot_json = EXCLUDED.snapshot_json,
                source_max_date = EXCLUDED.source_max_date,
                snapshot_version = mining_snapshots.snapshot_version + 1,
                run_id = EXCLUDED.run_id,
                generated_at = CURRENT_TIMESTAMP
            RETURNING snapshot_version, generated_at
            """,
            (snapshot_type, dataset_key, scope_key, json.dumps(payload), source_max_date, run_id),
        )
        row = cur.fetchone()
    snapshot_version = int(row[0]) if row and row[0] is not None else 1
    generated_at_db = row[1].isoformat() if row and row[1] else generated_at

    return {
        "snapshot_type": snapshot_type,
        "dataset_id": dataset_key,
        "scope_key": scope_key,
        "snapshot_json": payload,
        "source_max_date": str(source_max_date) if source_max_date else None,
        "snapshot_version": snapshot_version,
        "run_id": run_id,
        "generated_at": generated_at_db,
        "refreshed": True,
    }


def _read_snapshot(snapshot_type: str, dataset_id: Optional[str] = None, plan: Optional[Plan] = None) -> Dict[str, Any] | None:
    ensure_snapshot_table()
    dataset_key = dataset_id or DEFAULT_DATASET_ID
    scope_key = _build_scope_key(plan)
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_json, source_max_date, snapshot_version, run_id, generated_at
            FROM mining_snapshots
            WHERE snapshot_type = %s
              AND dataset_id = %s
              AND scope_key = %s
            """,
            (snapshot_type, dataset_key, scope_key),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "snapshot_type": snapshot_type,
        "dataset_id": dataset_key,
        "scope_key": scope_key,
        "snapshot_json": row[0],
        "source_max_date": str(row[1]) if row[1] else None,
        "snapshot_version": int(row[2]) if row[2] is not None else None,
        "run_id": row[3],
        "generated_at": row[4].isoformat() if row[4] else None,
        "refreshed": False,
    }


def _is_stale(snapshot: Dict[str, Any], dataset_metadata: Optional[Dict[str, Any]] = None, plan: Optional[Plan] = None) -> bool:
    load_environments()
    ttl_hours = int(os.getenv("MINING_SNAPSHOT_TTL_HOURS", "24"))
    snapshot_generated_at = snapshot.get("generated_at")
    if not snapshot_generated_at:
        return True

    try:
        generated = datetime.fromisoformat(str(snapshot_generated_at).replace("Z", "+00:00"))
    except ValueError:
        return True

    age_hours = (datetime.now(generated.tzinfo) - generated).total_seconds() / 3600.0
    if age_hours > ttl_hours:
        return True

    if dataset_metadata and plan:
        return False

    current_max = _get_fact_max_date()
    current_max_str = str(current_max) if current_max else None
    return snapshot.get("source_max_date") != current_max_str


def get_snapshot(
    snapshot_type: str,
    refresh_if_stale: bool = True,
    dataset_id: Optional[str] = None,
    plan: Optional[Plan] = None,
    dataset_metadata: Optional[Dict[str, Any]] = None,
    db_engine: str = "postgres",
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    snapshot = _read_snapshot(snapshot_type, dataset_id=dataset_id, plan=plan)
    if snapshot is None:
        return refresh_snapshot(
            snapshot_type,
            dataset_id=dataset_id,
            plan=plan,
            dataset_metadata=dataset_metadata,
            db_engine=db_engine,
            source_config=source_config,
        )

    if refresh_if_stale and _is_stale(snapshot, dataset_metadata=dataset_metadata, plan=plan):
        return refresh_snapshot(
            snapshot_type,
            dataset_id=dataset_id,
            plan=plan,
            dataset_metadata=dataset_metadata,
            db_engine=db_engine,
            source_config=source_config,
        )

    return snapshot


def refresh_all() -> List[Dict[str, Any]]:
    return [refresh_snapshot(snapshot_type) for snapshot_type in sorted(SNAPSHOT_TYPES)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh or read mining snapshots.")
    parser.add_argument("--type", choices=sorted(SNAPSHOT_TYPES), default=None, help="Snapshot type to refresh/read.")
    parser.add_argument("--refresh", action="store_true", help="Force refresh.")
    parser.add_argument("--all", action="store_true", help="Refresh all snapshot types.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    if args.all:
        result: Any = refresh_all()
    elif args.type and args.refresh:
        result = refresh_snapshot(args.type)
    elif args.type:
        result = get_snapshot(args.type, refresh_if_stale=True)
    else:
        raise SystemExit("Provide --type <snapshot_type> or --all")

    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result))

