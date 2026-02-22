import os
from contextlib import contextmanager
from typing import Any, Dict

from utils.env_loader import load_environments


def build_db_params() -> Dict[str, Any]:
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


def connect():
    params = build_db_params()
    try:
        import psycopg  # type: ignore

        return psycopg.connect(**params)
    except ImportError:
        try:
            import psycopg2  # type: ignore

            return psycopg2.connect(**params)
        except ImportError as exc:
            raise ImportError(
                "No PostgreSQL driver found for mining. Install one of: "
                "`python -m pip install \"psycopg[binary]\"` or "
                "`python -m pip install psycopg2-binary`."
            ) from exc


@contextmanager
def db_cursor(write: bool = False):
    conn = connect()
    try:
        with conn.cursor() as cursor:
            yield cursor
        if write:
            conn.commit()
    except Exception:
        if write:
            conn.rollback()
        raise
    finally:
        conn.close()
