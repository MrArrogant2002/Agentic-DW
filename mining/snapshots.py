import argparse
import json
import os
from datetime import datetime
from typing import Any, Dict, List

from mining.clustering import run_kmeans
from mining.common import db_cursor
from mining.rfm import run as run_rfm
from mining.trend import run as run_trend
from utils.env_loader import load_environments

SNAPSHOT_TYPES = {"trend_analysis", "customer_segmentation"}


def ensure_snapshot_table() -> None:
    sql = """
        CREATE TABLE IF NOT EXISTS mining_snapshots (
            snapshot_type VARCHAR(64) PRIMARY KEY,
            snapshot_json JSONB NOT NULL,
            source_max_date DATE,
            generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """
    with db_cursor(write=True) as cur:
        cur.execute(sql)


def _get_fact_max_date():
    with db_cursor() as cur:
        cur.execute("SELECT MAX(date_id) FROM fact_sales")
        return cur.fetchone()[0]


def _build_snapshot_payload(snapshot_type: str) -> Dict[str, Any]:
    if snapshot_type == "trend_analysis":
        return run_trend()
    if snapshot_type == "customer_segmentation":
        rfm = run_rfm()
        clustering = run_kmeans()
        return {"rfm_summary": rfm.get("summary"), "clustering": clustering}
    raise ValueError(f"Unsupported snapshot_type: {snapshot_type}")


def refresh_snapshot(snapshot_type: str) -> Dict[str, Any]:
    if snapshot_type not in SNAPSHOT_TYPES:
        raise ValueError(f"snapshot_type must be one of {sorted(SNAPSHOT_TYPES)}")

    ensure_snapshot_table()
    payload = _build_snapshot_payload(snapshot_type)
    source_max_date = _get_fact_max_date()
    generated_at = datetime.utcnow().isoformat() + "Z"

    with db_cursor(write=True) as cur:
        cur.execute(
            """
            INSERT INTO mining_snapshots (snapshot_type, snapshot_json, source_max_date, generated_at)
            VALUES (%s, %s::jsonb, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (snapshot_type)
            DO UPDATE SET
                snapshot_json = EXCLUDED.snapshot_json,
                source_max_date = EXCLUDED.source_max_date,
                generated_at = CURRENT_TIMESTAMP
            """,
            (snapshot_type, json.dumps(payload), source_max_date),
        )

    return {
        "snapshot_type": snapshot_type,
        "snapshot_json": payload,
        "source_max_date": str(source_max_date) if source_max_date else None,
        "generated_at": generated_at,
        "refreshed": True,
    }


def _read_snapshot(snapshot_type: str) -> Dict[str, Any] | None:
    ensure_snapshot_table()
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_json, source_max_date, generated_at
            FROM mining_snapshots
            WHERE snapshot_type = %s
            """,
            (snapshot_type,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "snapshot_type": snapshot_type,
        "snapshot_json": row[0],
        "source_max_date": str(row[1]) if row[1] else None,
        "generated_at": row[2].isoformat() if row[2] else None,
        "refreshed": False,
    }


def _is_stale(snapshot: Dict[str, Any]) -> bool:
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

    current_max = _get_fact_max_date()
    current_max_str = str(current_max) if current_max else None
    return snapshot.get("source_max_date") != current_max_str


def get_snapshot(snapshot_type: str, refresh_if_stale: bool = True) -> Dict[str, Any]:
    snapshot = _read_snapshot(snapshot_type)
    if snapshot is None:
        return refresh_snapshot(snapshot_type)

    if refresh_if_stale and _is_stale(snapshot):
        return refresh_snapshot(snapshot_type)

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

