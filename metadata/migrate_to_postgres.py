import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from metadata.store import (
    append_query_trace,
    ensure_metadata_tables,
    save_ingestion_run,
    save_quality_report,
    save_schema_metadata,
    save_semantic_map,
    set_cached_sql,
)


BASE_DIR = Path("metadata")
REGISTRY_FILE = BASE_DIR / "dataset_registry.json"
SCHEMA_CACHE_DIR = BASE_DIR / "schema_cache"
SEMANTIC_MAPS_DIR = BASE_DIR / "semantic_maps"
INGESTION_RUNS_DIR = BASE_DIR / "ingestion_runs"
QUALITY_REPORTS_DIR = BASE_DIR / "quality_reports"
PLAN_SQL_CACHE_FILE = BASE_DIR / "plan_sql_cache.json"
QUERY_TRACES_FILE = BASE_DIR / "query_traces.jsonl"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _read_registry() -> List[Dict[str, Any]]:
    return _read_json(REGISTRY_FILE, [])


def _migrate_datasets() -> None:
    from schema.introspector.db import connect

    items = _read_registry()
    conn = connect()
    try:
        with conn.cursor() as cur:
            for item in items:
                dataset_id = str(item.get("dataset_id", ""))
                if not dataset_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO agent_datasets (
                        dataset_id, name, source_type, db_engine, schema_name, description, status, source_config,
                        metadata_path, schema_hash, semantic_map_path, last_ingested_at, row_count, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::timestamptz, %s, %s::timestamptz, %s::timestamptz)
                    ON CONFLICT (dataset_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        source_type = EXCLUDED.source_type,
                        db_engine = EXCLUDED.db_engine,
                        schema_name = EXCLUDED.schema_name,
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        source_config = EXCLUDED.source_config,
                        metadata_path = EXCLUDED.metadata_path,
                        schema_hash = EXCLUDED.schema_hash,
                        semantic_map_path = EXCLUDED.semantic_map_path,
                        last_ingested_at = EXCLUDED.last_ingested_at,
                        row_count = EXCLUDED.row_count,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        dataset_id,
                        item.get("name") or dataset_id,
                        item.get("source_type") or "db_connection",
                        item.get("db_engine") or "postgres",
                        item.get("schema_name") or "public",
                        item.get("description"),
                        item.get("status") or "registered",
                        json.dumps(item.get("source_config") or {}),
                        item.get("metadata_path"),
                        item.get("schema_hash"),
                        item.get("semantic_map_path"),
                        item.get("last_ingested_at"),
                        item.get("row_count"),
                        item.get("created_at"),
                        item.get("updated_at"),
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def _migrate_schema_and_semantic() -> None:
    for path in SCHEMA_CACHE_DIR.glob("*.json"):
        dataset_id = path.stem
        payload = _read_json(path, {})
        metadata = payload.get("metadata")
        if metadata:
            save_schema_metadata(dataset_id, metadata)

    for path in SEMANTIC_MAPS_DIR.glob("*.json"):
        dataset_id = path.stem
        payload = _read_json(path, {})
        semantic_map = payload.get("semantic_map")
        if semantic_map:
            save_semantic_map(dataset_id, semantic_map)


def _migrate_ingestion_and_quality() -> None:
    for path in INGESTION_RUNS_DIR.glob("*.json"):
        dataset_id = path.stem
        runs = _read_json(path, [])
        if isinstance(runs, list):
            for run in runs:
                if isinstance(run, dict):
                    save_ingestion_run(dataset_id, run)

    for path in QUALITY_REPORTS_DIR.glob("*.json"):
        dataset_id = path.stem
        report = _read_json(path, {})
        if isinstance(report, dict) and report:
            save_quality_report(dataset_id, report)


def _migrate_plan_cache() -> None:
    cache = _read_json(PLAN_SQL_CACHE_FILE, {})
    for key, value in cache.items():
        if not isinstance(value, dict):
            continue
        sql = value.get("sql")
        if not sql:
            continue
        dataset_id = None
        plan_key = key
        schema_hash = value.get("schema_hash")
        if "::" in key:
            parts = key.split("::", 2)
            if len(parts) == 3:
                dataset_id = None if parts[0] == "global" else parts[0]
                schema_hash = None if parts[1] in {"", "no_schema_hash"} else parts[1]
                plan_key = parts[2]
        set_cached_sql(dataset_id=dataset_id, plan_key=plan_key, sql=sql, schema_hash=schema_hash)


def _migrate_traces() -> None:
    if not QUERY_TRACES_FILE.exists():
        return
    lines = QUERY_TRACES_FILE.read_text(encoding="utf-8").splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            trace = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(trace, dict):
            append_query_trace(trace)


def run_migration() -> None:
    os.environ["METADATA_BACKEND"] = "postgres"
    ensure_metadata_tables()
    _migrate_datasets()
    _migrate_schema_and_semantic()
    _migrate_ingestion_and_quality()
    _migrate_plan_cache()
    _migrate_traces()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate metadata JSON files into PostgreSQL metadata tables.")
    parser.add_argument("--pretty", action="store_true", help="Print summary JSON")
    args = parser.parse_args()

    run_migration()
    summary = {"status": "ok", "message": "Metadata migration to PostgreSQL completed."}
    if args.pretty:
        print(json.dumps(summary, indent=2))
    else:
        print(json.dumps(summary))
