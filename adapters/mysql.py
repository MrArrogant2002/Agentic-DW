from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from adapters.base import DatabaseAdapter
from utils.env_loader import load_environments


class MySQLAdapter(DatabaseAdapter):
    engine = "mysql"

    def _db_params(self) -> Dict[str, Any]:
        load_environments()
        host = self.source_config.get("host") or os.getenv("DB_HOST")
        dbname = self.source_config.get("dbname") or os.getenv("DB_NAME")
        user = self.source_config.get("user") or os.getenv("DB_USER")
        password = self.source_config.get("password") or os.getenv("DB_PASSWORD")
        port_raw = self.source_config.get("port") or os.getenv("DB_PORT", "3306")
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
            "port": int(port_raw),
            "database": dbname,
            "user": user,
            "password": password,
        }

    def _connect(self):
        params = self._db_params()
        try:
            import mysql.connector  # type: ignore

            return mysql.connector.connect(**params), "mysql.connector"
        except ImportError:
            try:
                import pymysql  # type: ignore

                return pymysql.connect(
                    host=params["host"],
                    port=params["port"],
                    user=params["user"],
                    password=params["password"],
                    database=params["database"],
                ), "pymysql"
            except ImportError as exc:
                raise ImportError(
                    "No MySQL driver found. Install one of: "
                    "`python -m pip install mysql-connector-python` or `python -m pip install pymysql`."
                ) from exc

    def execute_select(self, sql: str, row_limit: int, timeout_ms: int) -> List[Dict[str, Any]]:
        wrapped_sql = f"SELECT * FROM ({sql}) AS guarded_query LIMIT %s"
        conn, driver = self._connect()
        try:
            if driver == "mysql.connector":
                cur = conn.cursor(dictionary=True)
            else:
                import pymysql.cursors  # type: ignore

                cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(f"SET SESSION MAX_EXECUTION_TIME={int(timeout_ms)}")
            cur.execute(wrapped_sql, (row_limit,))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def introspect_schema(self, schema_name: Optional[str] = None) -> Dict[str, Any]:
        target_schema = schema_name or self._db_params()["database"]
        conn, driver = self._connect()
        try:
            if driver == "mysql.connector":
                cur = conn.cursor(dictionary=True)
            else:
                import pymysql.cursors  # type: ignore

                cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (target_schema,),
            )
            table_names = [r["table_name"] for r in cur.fetchall()]

            cur.execute(
                """
                SELECT
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    ordinal_position,
                    column_key
                FROM information_schema.columns
                WHERE table_schema = %s
                ORDER BY table_name, ordinal_position
                """,
                (target_schema,),
            )
            column_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    table_name AS from_table,
                    column_name AS from_column,
                    referenced_table_name AS to_table,
                    referenced_column_name AS to_column
                FROM information_schema.key_column_usage
                WHERE table_schema = %s
                  AND referenced_table_name IS NOT NULL
                """,
                (target_schema,),
            )
            fk_rows = cur.fetchall()

            cur.execute(
                """
                SELECT table_name, table_rows
                FROM information_schema.tables
                WHERE table_schema = %s
                """,
                (target_schema,),
            )
            counts = {r["table_name"]: int(r.get("table_rows") or 0) for r in cur.fetchall()}
        finally:
            conn.close()

        columns_by_table: Dict[str, List[Dict[str, Any]]] = {}
        for row in column_rows:
            table_name = row["table_name"]
            columns_by_table.setdefault(table_name, []).append(
                {
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                    "udt_name": row["data_type"],
                    "is_nullable": str(row["is_nullable"]).upper() == "YES",
                    "is_primary_key": row["column_key"] == "PRI",
                    "ordinal_position": int(row["ordinal_position"]),
                }
            )

        relationships = [
            {
                "from_table": row["from_table"],
                "from_column": row["from_column"],
                "to_table": row["to_table"],
                "to_column": row["to_column"],
            }
            for row in fk_rows
        ]

        tables = [
            {
                "table_name": t,
                "row_count": counts.get(t, 0),
                "columns": columns_by_table.get(t, []),
            }
            for t in table_names
        ]

        return {
            "source": {"db_engine": "mysql", "schema_name": target_schema},
            "profile": {"table_count": len(tables), "relationship_count": len(relationships)},
            "tables": tables,
            "entities": [],
            "measures": [],
            "time_columns": [],
            "relationships": relationships,
        }
