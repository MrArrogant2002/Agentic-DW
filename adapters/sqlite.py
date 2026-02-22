from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from adapters.base import DatabaseAdapter
from utils.env_loader import load_environments


def _sqlite_type_to_generic(data_type: str) -> str:
    lowered = (data_type or "").lower()
    if "int" in lowered:
        return "integer"
    if any(tok in lowered for tok in ("real", "floa", "doub", "dec", "num")):
        return "numeric"
    if any(tok in lowered for tok in ("date", "time")):
        return "timestamp without time zone"
    return "text"


class SQLiteAdapter(DatabaseAdapter):
    engine = "sqlite"

    def _db_path(self) -> str:
        load_environments()
        raw = self.source_config.get("db_path") or os.getenv("SQLITE_DB_PATH")
        if not raw:
            raise ValueError("SQLITE_DB_PATH is required for sqlite adapter")
        db_path = Path(str(raw))
        if not db_path.exists():
            raise ValueError(f"SQLite database file does not exist: {db_path}")
        return str(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path())
        conn.row_factory = sqlite3.Row
        return conn

    def execute_select(self, sql: str, row_limit: int, timeout_ms: int) -> List[Dict[str, Any]]:
        wrapped_sql = f"SELECT * FROM ({sql}) AS guarded_query LIMIT ?"
        conn = self._connect()
        try:
            conn.execute(f"PRAGMA busy_timeout = {int(timeout_ms)}")
            cur = conn.cursor()
            cur.execute(wrapped_sql, (row_limit,))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def introspect_schema(self, schema_name: Optional[str] = None) -> Dict[str, Any]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
            table_names = [row[0] for row in cur.fetchall()]

            tables: List[Dict[str, Any]] = []
            relationships: List[Dict[str, Any]] = []
            for table_name in table_names:
                cur.execute(f'PRAGMA table_info("{table_name}")')
                cols = cur.fetchall()
                columns = []
                for col in cols:
                    columns.append(
                        {
                            "column_name": col[1],
                            "data_type": _sqlite_type_to_generic(str(col[2] or "")),
                            "udt_name": str(col[2] or ""),
                            "is_nullable": col[3] == 0,
                            "is_primary_key": col[5] == 1,
                            "ordinal_position": int(col[0]) + 1,
                        }
                    )

                cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                row_count = int(cur.fetchone()[0])

                cur.execute(f'PRAGMA foreign_key_list("{table_name}")')
                for fk in cur.fetchall():
                    relationships.append(
                        {
                            "from_table": table_name,
                            "from_column": fk[3],
                            "to_table": fk[2],
                            "to_column": fk[4],
                        }
                    )

                tables.append({"table_name": table_name, "row_count": row_count, "columns": columns})

            return {
                "source": {"db_engine": "sqlite", "schema_name": schema_name or "main"},
                "profile": {"table_count": len(tables), "relationship_count": len(relationships)},
                "tables": tables,
                "entities": [],
                "measures": [],
                "time_columns": [],
                "relationships": relationships,
            }
        finally:
            conn.close()
