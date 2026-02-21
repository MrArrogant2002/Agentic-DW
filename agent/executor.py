import os
import re
from contextlib import contextmanager
from typing import Any, Dict, List


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
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise ValueError("DB_PASSWORD is required")

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME", "agentic_ai_db"),
        "user": os.getenv("DB_USER", "postgres"),
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


def execute_safe_query(sql: str, row_limit: int = 100, timeout_ms: int = 15_000) -> List[Dict[str, Any]]:
    if row_limit <= 0:
        raise ValueError("row_limit must be positive")

    safe_sql = validate_sql(sql)
    wrapped_sql = f"SELECT * FROM ({safe_sql}) AS guarded_query LIMIT %s"

    with db_session() as (conn, driver):
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = %s", (timeout_ms,))
            cur.execute(wrapped_sql, (row_limit,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            result: List[Dict[str, Any]] = []
            for row in rows:
                result.append({columns[i]: row[i] for i in range(len(columns))})
            return result

