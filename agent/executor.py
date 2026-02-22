import os
import re
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from adapters.factory import get_adapter
from utils.env_loader import load_environments


class UnsafeSQLError(ValueError):
    pass


_DENYLIST = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "grant",
    "revoke",
    "copy",
    "call",
    "do",
    "vacuum",
    "comment",
)


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip()).lower()


def validate_sql(sql: str) -> str:
    candidate = sql.strip()
    if not candidate:
        raise UnsafeSQLError("SQL is empty")

    semicolons = candidate.count(";")
    if semicolons > 1:
        raise UnsafeSQLError("Multiple SQL statements are not allowed")
    if semicolons == 1 and not candidate.endswith(";"):
        raise UnsafeSQLError("Semicolon is only allowed at the end of SQL")

    normalized = _normalize_sql(candidate.rstrip(";"))
    if not (normalized.startswith("select ") or normalized.startswith("with ")):
        raise UnsafeSQLError("Only SELECT/CTE queries are allowed")

    for keyword in _DENYLIST:
        if re.search(rf"\b{keyword}\b", normalized):
            raise UnsafeSQLError(f"Blocked SQL keyword detected: {keyword}")

    return candidate.rstrip(";")


def _build_db_params() -> Dict[str, Any]:
    load_environments()
    host = os.getenv("DB_HOST")
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    if not host:
        raise ValueError("DB_HOST is required")
    if not dbname:
        raise ValueError("DB_NAME is required")
    if not user:
        raise ValueError("DB_USER is required")
    if not password:
        raise ValueError("DB_PASSWORD is required")

    return {
        "host": host,
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": dbname,
        "user": user,
        "password": password,
    }


def _connect():
    params = _build_db_params()
    try:
        import psycopg  # type: ignore

        return psycopg.connect(**params), "psycopg"
    except ImportError:
        import psycopg2  # type: ignore

        return psycopg2.connect(**params), "psycopg2"


@contextmanager
def db_session():
    conn, driver = _connect()
    try:
        yield conn, driver
    finally:
        conn.close()


def execute_safe_query(
    sql: str,
    row_limit: int = 100,
    timeout_ms: int = 15_000,
    db_engine: Optional[str] = None,
    source_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    if row_limit <= 0:
        raise ValueError("row_limit must be positive")
    if timeout_ms <= 0:
        raise ValueError("timeout_ms must be positive")

    safe_sql = validate_sql(sql)
    selected_engine = (db_engine or os.getenv("DB_ENGINE", "postgres")).strip().lower()

    if selected_engine not in {"postgres", "postgresql"} or source_config:
        adapter = get_adapter(db_engine=selected_engine, source_config=source_config)
        return adapter.execute_select(safe_sql, row_limit=row_limit, timeout_ms=timeout_ms)

    wrapped_sql = f"SELECT * FROM ({safe_sql}) AS guarded_query LIMIT %s"

    with db_session() as (conn, driver):
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{int(timeout_ms)}ms'")
            cur.execute(wrapped_sql, (row_limit,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            result: List[Dict[str, Any]] = []
            for row in rows:
                result.append({columns[i]: row[i] for i in range(len(columns))})
            return result
