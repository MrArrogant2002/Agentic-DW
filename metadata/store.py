import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from utils.env_loader import load_environments


BASE_DIR = Path("metadata")
REGISTRY_FILE = BASE_DIR / "dataset_registry.json"
SCHEMA_CACHE_DIR = BASE_DIR / "schema_cache"
SEMANTIC_MAPS_DIR = BASE_DIR / "semantic_maps"
INGESTION_RUNS_DIR = BASE_DIR / "ingestion_runs"
QUALITY_REPORTS_DIR = BASE_DIR / "quality_reports"
PLAN_SQL_CACHE_FILE = BASE_DIR / "plan_sql_cache.json"
QUERY_TRACES_FILE = BASE_DIR / "query_traces.jsonl"


def _ensure_dirs() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SEMANTIC_MAPS_DIR.mkdir(parents=True, exist_ok=True)
    INGESTION_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    QUALITY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if not REGISTRY_FILE.exists():
        REGISTRY_FILE.write_text("[]", encoding="utf-8")
    if not PLAN_SQL_CACHE_FILE.exists():
        PLAN_SQL_CACHE_FILE.write_text("{}", encoding="utf-8")
    if not QUERY_TRACES_FILE.exists():
        QUERY_TRACES_FILE.write_text("", encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _has_db_env() -> bool:
    load_environments()
    return bool(os.getenv("DB_HOST") and os.getenv("DB_NAME") and os.getenv("DB_USER") and os.getenv("DB_PASSWORD"))


def _backend() -> str:
    load_environments()
    mode = (os.getenv("METADATA_BACKEND", "auto") or "auto").strip().lower()
    if mode in {"postgres", "file"}:
        return mode
    return "postgres" if _has_db_env() else "file"


def _jsonify(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _compose_plan_cache_key(dataset_id: Optional[str], plan_key: str, schema_hash: Optional[str] = None) -> str:
    dataset_key = dataset_id or "global"
    schema_key = schema_hash or "no_schema_hash"
    return f"{dataset_key}::{schema_key}::{plan_key}"


# ----------------------------
# Postgres backend
# ----------------------------
def _pg_connect():
    from schema.introspector.db import connect

    return connect()


def ensure_metadata_tables() -> None:
    if _backend() != "postgres":
        return
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_datasets (
                    dataset_id VARCHAR(64) PRIMARY KEY,
                    name VARCHAR(256) NOT NULL,
                    source_type VARCHAR(64) NOT NULL,
                    db_engine VARCHAR(64) NOT NULL,
                    schema_name VARCHAR(256) NOT NULL,
                    description TEXT,
                    status VARCHAR(64) NOT NULL,
                    source_config JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata_path TEXT,
                    schema_hash VARCHAR(128),
                    semantic_map_path TEXT,
                    last_ingested_at TIMESTAMPTZ,
                    row_count BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_schema_metadata (
                    dataset_id VARCHAR(64) PRIMARY KEY REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
                    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    schema_hash VARCHAR(128) NOT NULL,
                    metadata JSONB NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_semantic_maps (
                    dataset_id VARCHAR(64) PRIMARY KEY REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
                    generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    semantic_map JSONB NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_ingestion_runs (
                    id BIGSERIAL PRIMARY KEY,
                    dataset_id VARCHAR(64) NOT NULL REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
                    run_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_quality_reports (
                    dataset_id VARCHAR(64) PRIMARY KEY REFERENCES agent_datasets(dataset_id) ON DELETE CASCADE,
                    report_json JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_plan_sql_cache (
                    cache_key TEXT PRIMARY KEY,
                    dataset_id VARCHAR(64),
                    schema_hash VARCHAR(128),
                    plan_key TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_query_traces (
                    id BIGSERIAL PRIMARY KEY,
                    trace_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_query_traces_created_at ON agent_query_traces(created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_agent_ingestion_runs_dataset_created ON agent_ingestion_runs(dataset_id, created_at DESC)")
        conn.commit()
    finally:
        conn.close()


def _pg_register_dataset(
    name: str,
    source_type: str,
    db_engine: str,
    schema_name: str,
    description: Optional[str] = None,
    status: str = "registered",
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_metadata_tables()
    dataset_id = str(uuid4())
    created_at = _now_iso()
    payload = source_config or {}
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_datasets (
                    dataset_id, name, source_type, db_engine, schema_name, description, status, source_config, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::timestamptz, %s::timestamptz)
                """,
                (dataset_id, name, source_type, db_engine, schema_name, description, status, json.dumps(payload), created_at, created_at),
            )
        conn.commit()
    finally:
        conn.close()
    return _pg_get_dataset(dataset_id) or {}


def _pg_list_datasets() -> List[Dict[str, Any]]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dataset_id, name, source_type, db_engine, schema_name, description, status, source_config,
                       metadata_path, schema_hash, semantic_map_path, last_ingested_at, row_count, created_at, updated_at
                FROM agent_datasets
                ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "dataset_id": row[0],
                "name": row[1],
                "source_type": row[2],
                "db_engine": row[3],
                "schema_name": row[4],
                "description": row[5],
                "status": row[6],
                "source_config": _jsonify(row[7]) or {},
                "metadata_path": row[8],
                "schema_hash": row[9],
                "semantic_map_path": row[10],
                "last_ingested_at": row[11].isoformat() if row[11] else None,
                "row_count": int(row[12]) if row[12] is not None else None,
                "created_at": row[13].isoformat() if row[13] else None,
                "updated_at": row[14].isoformat() if row[14] else None,
            }
        )
    return out


def _pg_get_dataset(dataset_id: str) -> Optional[Dict[str, Any]]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dataset_id, name, source_type, db_engine, schema_name, description, status, source_config,
                       metadata_path, schema_hash, semantic_map_path, last_ingested_at, row_count, created_at, updated_at
                FROM agent_datasets
                WHERE dataset_id = %s
                """,
                (dataset_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "dataset_id": row[0],
        "name": row[1],
        "source_type": row[2],
        "db_engine": row[3],
        "schema_name": row[4],
        "description": row[5],
        "status": row[6],
        "source_config": _jsonify(row[7]) or {},
        "metadata_path": row[8],
        "schema_hash": row[9],
        "semantic_map_path": row[10],
        "last_ingested_at": row[11].isoformat() if row[11] else None,
        "row_count": int(row[12]) if row[12] is not None else None,
        "created_at": row[13].isoformat() if row[13] else None,
        "updated_at": row[14].isoformat() if row[14] else None,
    }


def _pg_update_dataset(dataset_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    ensure_metadata_tables()
    existing = _pg_get_dataset(dataset_id)
    if not existing:
        raise ValueError(f"Unknown dataset_id: {dataset_id}")

    merged = dict(existing)
    merged.update(patch)
    merged["updated_at"] = _now_iso()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agent_datasets
                SET name = %s,
                    source_type = %s,
                    db_engine = %s,
                    schema_name = %s,
                    description = %s,
                    status = %s,
                    source_config = %s::jsonb,
                    metadata_path = %s,
                    schema_hash = %s,
                    semantic_map_path = %s,
                    last_ingested_at = %s::timestamptz,
                    row_count = %s,
                    updated_at = %s::timestamptz
                WHERE dataset_id = %s
                """,
                (
                    merged.get("name"),
                    merged.get("source_type"),
                    merged.get("db_engine"),
                    merged.get("schema_name"),
                    merged.get("description"),
                    merged.get("status"),
                    json.dumps(merged.get("source_config") or {}),
                    merged.get("metadata_path"),
                    merged.get("schema_hash"),
                    merged.get("semantic_map_path"),
                    merged.get("last_ingested_at"),
                    merged.get("row_count"),
                    merged.get("updated_at"),
                    dataset_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return _pg_get_dataset(dataset_id) or {}


def _pg_save_schema_metadata(dataset_id: str, metadata: Dict[str, Any]) -> Path:
    ensure_metadata_tables()
    metadata_hash = hashlib.sha256(json.dumps(metadata, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    generated_at = _now_iso()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_schema_metadata (dataset_id, generated_at, schema_hash, metadata)
                VALUES (%s, %s::timestamptz, %s, %s::jsonb)
                ON CONFLICT (dataset_id)
                DO UPDATE SET
                    generated_at = EXCLUDED.generated_at,
                    schema_hash = EXCLUDED.schema_hash,
                    metadata = EXCLUDED.metadata
                """,
                (dataset_id, generated_at, metadata_hash, json.dumps(metadata)),
            )
        conn.commit()
    finally:
        conn.close()

    _pg_update_dataset(dataset_id, {"schema_hash": metadata_hash, "metadata_path": f"db://agent_schema_metadata/{dataset_id}"})
    return SCHEMA_CACHE_DIR / f"{dataset_id}.json"


def _pg_load_schema_metadata(dataset_id: str) -> Optional[Dict[str, Any]]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT metadata FROM agent_schema_metadata WHERE dataset_id = %s", (dataset_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return _jsonify(row[0])


def _pg_load_schema_hash(dataset_id: str) -> Optional[str]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT schema_hash FROM agent_schema_metadata WHERE dataset_id = %s", (dataset_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def _pg_save_semantic_map(dataset_id: str, semantic_map: Dict[str, Any]) -> Path:
    ensure_metadata_tables()
    generated_at = _now_iso()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_semantic_maps (dataset_id, generated_at, semantic_map)
                VALUES (%s, %s::timestamptz, %s::jsonb)
                ON CONFLICT (dataset_id)
                DO UPDATE SET
                    generated_at = EXCLUDED.generated_at,
                    semantic_map = EXCLUDED.semantic_map
                """,
                (dataset_id, generated_at, json.dumps(semantic_map)),
            )
        conn.commit()
    finally:
        conn.close()

    _pg_update_dataset(dataset_id, {"semantic_map_path": f"db://agent_semantic_maps/{dataset_id}"})
    return SEMANTIC_MAPS_DIR / f"{dataset_id}.json"


def _pg_load_semantic_map(dataset_id: str) -> Optional[Dict[str, Any]]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT semantic_map FROM agent_semantic_maps WHERE dataset_id = %s", (dataset_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return _jsonify(row[0])


def _pg_save_ingestion_run(dataset_id: str, run: Dict[str, Any]) -> Dict[str, Any]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO agent_ingestion_runs (dataset_id, run_json) VALUES (%s, %s::jsonb)",
                (dataset_id, json.dumps(run)),
            )
        conn.commit()
    finally:
        conn.close()
    return run


def _pg_load_latest_ingestion_run(dataset_id: str) -> Optional[Dict[str, Any]]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_json
                FROM agent_ingestion_runs
                WHERE dataset_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (dataset_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    return _jsonify(row[0]) if row else None


def _pg_save_quality_report(dataset_id: str, report: Dict[str, Any]) -> Dict[str, Any]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_quality_reports (dataset_id, report_json, updated_at)
                VALUES (%s, %s::jsonb, %s::timestamptz)
                ON CONFLICT (dataset_id)
                DO UPDATE SET
                    report_json = EXCLUDED.report_json,
                    updated_at = EXCLUDED.updated_at
                """,
                (dataset_id, json.dumps(report), _now_iso()),
            )
        conn.commit()
    finally:
        conn.close()
    return report


def _pg_load_quality_report(dataset_id: str) -> Optional[Dict[str, Any]]:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT report_json FROM agent_quality_reports WHERE dataset_id = %s", (dataset_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    return _jsonify(row[0]) if row else None


def _pg_get_cached_sql(dataset_id: Optional[str], plan_key: str, schema_hash: Optional[str] = None) -> Optional[str]:
    ensure_metadata_tables()
    key = _compose_plan_cache_key(dataset_id, plan_key, schema_hash)
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sql_text FROM agent_plan_sql_cache WHERE cache_key = %s", (key,))
            row = cur.fetchone()
            if not row:
                legacy_key = f"{dataset_id or 'global'}::{plan_key}"
                cur.execute("SELECT sql_text FROM agent_plan_sql_cache WHERE cache_key = %s", (legacy_key,))
                row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def _pg_set_cached_sql(dataset_id: Optional[str], plan_key: str, sql: str, schema_hash: Optional[str] = None) -> None:
    ensure_metadata_tables()
    key = _compose_plan_cache_key(dataset_id, plan_key, schema_hash)
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_plan_sql_cache (cache_key, dataset_id, schema_hash, plan_key, sql_text, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::timestamptz)
                ON CONFLICT (cache_key)
                DO UPDATE SET
                    sql_text = EXCLUDED.sql_text,
                    updated_at = EXCLUDED.updated_at,
                    schema_hash = EXCLUDED.schema_hash,
                    dataset_id = EXCLUDED.dataset_id,
                    plan_key = EXCLUDED.plan_key
                """,
                (key, dataset_id, schema_hash, plan_key, sql, _now_iso()),
            )
        conn.commit()
    finally:
        conn.close()


def _pg_append_query_trace(trace: Dict[str, Any]) -> None:
    ensure_metadata_tables()
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO agent_query_traces (trace_json) VALUES (%s::jsonb)", (json.dumps(trace, default=str),))
        conn.commit()
    finally:
        conn.close()


def _pg_load_query_traces(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    ensure_metadata_tables()
    query = "SELECT trace_json FROM agent_query_traces ORDER BY created_at DESC, id DESC"
    params: List[Any] = []
    if limit is not None and limit > 0:
        query += " LIMIT %s"
        params.append(limit)
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
    finally:
        conn.close()
    traces = [_jsonify(row[0]) for row in rows]
    traces.reverse()
    return [t for t in traces if isinstance(t, dict)]


# ----------------------------
# File backend (existing logic)
# ----------------------------
def _read_registry() -> List[Dict[str, Any]]:
    _ensure_dirs()
    return json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))


def _write_registry(items: List[Dict[str, Any]]) -> None:
    _ensure_dirs()
    REGISTRY_FILE.write_text(json.dumps(items, indent=2), encoding="utf-8")


def _file_register_dataset(
    name: str,
    source_type: str,
    db_engine: str,
    schema_name: str,
    description: Optional[str] = None,
    status: str = "registered",
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    items = _read_registry()
    dataset_id = str(uuid4())
    record = {
        "dataset_id": dataset_id,
        "name": name,
        "source_type": source_type,
        "db_engine": db_engine,
        "schema_name": schema_name,
        "description": description,
        "status": status,
        "source_config": source_config or {},
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
    }
    items.append(record)
    _write_registry(items)
    return record


def _file_list_datasets() -> List[Dict[str, Any]]:
    return _read_registry()


def _file_get_dataset(dataset_id: str) -> Optional[Dict[str, Any]]:
    for item in _read_registry():
        if item.get("dataset_id") == dataset_id:
            return item
    return None


def _file_update_dataset(dataset_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    items = _read_registry()
    for item in items:
        if item.get("dataset_id") == dataset_id:
            item.update(patch)
            item["updated_at"] = _now_iso()
            _write_registry(items)
            return item
    raise ValueError(f"Unknown dataset_id: {dataset_id}")


def _file_save_schema_metadata(dataset_id: str, metadata: Dict[str, Any]) -> Path:
    _ensure_dirs()
    out_path = SCHEMA_CACHE_DIR / f"{dataset_id}.json"
    metadata_hash = hashlib.sha256(json.dumps(metadata, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    payload = {
        "dataset_id": dataset_id,
        "generated_at": _now_iso(),
        "schema_hash": metadata_hash,
        "metadata": metadata,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    _file_update_dataset(dataset_id, {"metadata_path": str(out_path), "schema_hash": metadata_hash})
    return out_path


def _file_load_schema_metadata(dataset_id: str) -> Optional[Dict[str, Any]]:
    _ensure_dirs()
    path = SCHEMA_CACHE_DIR / f"{dataset_id}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("metadata")


def _file_load_schema_hash(dataset_id: str) -> Optional[str]:
    _ensure_dirs()
    path = SCHEMA_CACHE_DIR / f"{dataset_id}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("schema_hash")


def _file_save_semantic_map(dataset_id: str, semantic_map: Dict[str, Any]) -> Path:
    _ensure_dirs()
    out_path = SEMANTIC_MAPS_DIR / f"{dataset_id}.json"
    payload = {
        "dataset_id": dataset_id,
        "generated_at": _now_iso(),
        "semantic_map": semantic_map,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    _file_update_dataset(dataset_id, {"semantic_map_path": str(out_path)})
    return out_path


def _file_load_semantic_map(dataset_id: str) -> Optional[Dict[str, Any]]:
    _ensure_dirs()
    path = SEMANTIC_MAPS_DIR / f"{dataset_id}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("semantic_map")


def _file_ingestion_run_path(dataset_id: str) -> Path:
    return INGESTION_RUNS_DIR / f"{dataset_id}.json"


def _file_save_ingestion_run(dataset_id: str, run: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_dirs()
    path = _file_ingestion_run_path(dataset_id)
    if path.exists():
        items = json.loads(path.read_text(encoding="utf-8"))
    else:
        items = []
    items.append(run)
    path.write_text(json.dumps(items, indent=2), encoding="utf-8")
    return run


def _file_load_latest_ingestion_run(dataset_id: str) -> Optional[Dict[str, Any]]:
    path = _file_ingestion_run_path(dataset_id)
    if not path.exists():
        return None
    items = json.loads(path.read_text(encoding="utf-8"))
    if not items:
        return None
    return items[-1]


def _file_quality_report_path(dataset_id: str) -> Path:
    return QUALITY_REPORTS_DIR / f"{dataset_id}.json"


def _file_save_quality_report(dataset_id: str, report: Dict[str, Any]) -> Dict[str, Any]:
    _ensure_dirs()
    path = _file_quality_report_path(dataset_id)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def _file_load_quality_report(dataset_id: str) -> Optional[Dict[str, Any]]:
    path = _file_quality_report_path(dataset_id)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _file_read_plan_sql_cache() -> Dict[str, Any]:
    _ensure_dirs()
    return json.loads(PLAN_SQL_CACHE_FILE.read_text(encoding="utf-8"))


def _file_write_plan_sql_cache(cache: Dict[str, Any]) -> None:
    _ensure_dirs()
    PLAN_SQL_CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def _file_get_cached_sql(dataset_id: Optional[str], plan_key: str, schema_hash: Optional[str] = None) -> Optional[str]:
    cache = _file_read_plan_sql_cache()
    key = _compose_plan_cache_key(dataset_id, plan_key, schema_hash)
    item = cache.get(key)
    if not item:
        legacy_key = f"{dataset_id or 'global'}::{plan_key}"
        item = cache.get(legacy_key)
    if not item:
        return None
    return item.get("sql")


def _file_set_cached_sql(dataset_id: Optional[str], plan_key: str, sql: str, schema_hash: Optional[str] = None) -> None:
    cache = _file_read_plan_sql_cache()
    key = _compose_plan_cache_key(dataset_id, plan_key, schema_hash)
    cache[key] = {"sql": sql, "updated_at": _now_iso(), "schema_hash": schema_hash}
    _file_write_plan_sql_cache(cache)


def _file_append_query_trace(trace: Dict[str, Any]) -> None:
    _ensure_dirs()
    with QUERY_TRACES_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(trace, default=str) + "\n")


def _file_load_query_traces(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    _ensure_dirs()
    lines = QUERY_TRACES_FILE.read_text(encoding="utf-8").splitlines()
    if limit is not None and limit > 0:
        lines = lines[-limit:]
    out: List[Dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


# ----------------------------
# Public API
# ----------------------------
def register_dataset(
    name: str,
    source_type: str,
    db_engine: str,
    schema_name: str,
    description: Optional[str] = None,
    status: str = "registered",
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if _backend() == "postgres":
        return _pg_register_dataset(name, source_type, db_engine, schema_name, description, status, source_config)
    return _file_register_dataset(name, source_type, db_engine, schema_name, description, status, source_config)


def list_datasets() -> List[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_list_datasets()
    return _file_list_datasets()


def get_dataset(dataset_id: str) -> Optional[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_get_dataset(dataset_id)
    return _file_get_dataset(dataset_id)


def update_dataset(dataset_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    if _backend() == "postgres":
        return _pg_update_dataset(dataset_id, patch)
    return _file_update_dataset(dataset_id, patch)


def save_schema_metadata(dataset_id: str, metadata: Dict[str, Any]) -> Path:
    if _backend() == "postgres":
        return _pg_save_schema_metadata(dataset_id, metadata)
    return _file_save_schema_metadata(dataset_id, metadata)


def load_schema_metadata(dataset_id: str) -> Optional[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_load_schema_metadata(dataset_id)
    return _file_load_schema_metadata(dataset_id)


def load_schema_hash(dataset_id: str) -> Optional[str]:
    if _backend() == "postgres":
        return _pg_load_schema_hash(dataset_id)
    return _file_load_schema_hash(dataset_id)


def save_semantic_map(dataset_id: str, semantic_map: Dict[str, Any]) -> Path:
    if _backend() == "postgres":
        return _pg_save_semantic_map(dataset_id, semantic_map)
    return _file_save_semantic_map(dataset_id, semantic_map)


def load_semantic_map(dataset_id: str) -> Optional[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_load_semantic_map(dataset_id)
    return _file_load_semantic_map(dataset_id)


def save_ingestion_run(dataset_id: str, run: Dict[str, Any]) -> Dict[str, Any]:
    if _backend() == "postgres":
        return _pg_save_ingestion_run(dataset_id, run)
    return _file_save_ingestion_run(dataset_id, run)


def load_latest_ingestion_run(dataset_id: str) -> Optional[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_load_latest_ingestion_run(dataset_id)
    return _file_load_latest_ingestion_run(dataset_id)


def save_quality_report(dataset_id: str, report: Dict[str, Any]) -> Dict[str, Any]:
    if _backend() == "postgres":
        return _pg_save_quality_report(dataset_id, report)
    return _file_save_quality_report(dataset_id, report)


def load_quality_report(dataset_id: str) -> Optional[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_load_quality_report(dataset_id)
    return _file_load_quality_report(dataset_id)


def get_cached_sql(dataset_id: Optional[str], plan_key: str, schema_hash: Optional[str] = None) -> Optional[str]:
    if _backend() == "postgres":
        return _pg_get_cached_sql(dataset_id, plan_key, schema_hash)
    return _file_get_cached_sql(dataset_id, plan_key, schema_hash)


def set_cached_sql(dataset_id: Optional[str], plan_key: str, sql: str, schema_hash: Optional[str] = None) -> None:
    if _backend() == "postgres":
        _pg_set_cached_sql(dataset_id, plan_key, sql, schema_hash)
        return
    _file_set_cached_sql(dataset_id, plan_key, sql, schema_hash)


def append_query_trace(trace: Dict[str, Any]) -> None:
    if _backend() == "postgres":
        _pg_append_query_trace(trace)
        return
    _file_append_query_trace(trace)


def load_query_traces(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if _backend() == "postgres":
        return _pg_load_query_traces(limit)
    return _file_load_query_traces(limit)
