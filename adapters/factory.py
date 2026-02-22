from __future__ import annotations

import os
from typing import Any, Dict, Optional

from adapters.base import AdapterError, DatabaseAdapter
from adapters.mysql import MySQLAdapter
from adapters.postgres import PostgresAdapter
from adapters.sqlite import SQLiteAdapter
from utils.env_loader import load_environments


def get_adapter(db_engine: Optional[str] = None, source_config: Optional[Dict[str, Any]] = None) -> DatabaseAdapter:
    load_environments()
    engine = (db_engine or os.getenv("DB_ENGINE", "postgres")).strip().lower()
    if engine in {"postgres", "postgresql"}:
        return PostgresAdapter(source_config=source_config)
    if engine == "sqlite":
        return SQLiteAdapter(source_config=source_config)
    if engine == "mysql":
        return MySQLAdapter(source_config=source_config)
    raise AdapterError(f"Unsupported db_engine: {engine}")
