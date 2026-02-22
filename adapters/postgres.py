from __future__ import annotations

import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from adapters.base import AdapterError, DatabaseAdapter
from utils.env_loader import load_environments


NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "decimal",
    "real",
    "double precision",
}
TEXT_TYPES = {"text", "character varying", "character", "varchar", "char"}
TIME_TYPES = {"date", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone"}


def _keyword_score(name: str, keywords: List[str], weight: float) -> float:
    lowered = name.lower()
    return weight if any(key in lowered for key in keywords) else 0.0


def _normalize_cardinality(n_distinct: float | None, row_count: int) -> float:
    if n_distinct is None or row_count <= 0:
        return 0.5
    if n_distinct < 0:
        return min(1.0, max(0.0, abs(n_distinct)))
    return min(1.0, max(0.0, float(n_distinct) / float(row_count)))


class PostgresAdapter(DatabaseAdapter):
    engine = "postgres"

    def _db_params(self) -> Dict[str, Any]:
        load_environments()
        host = self.source_config.get("host") or os.getenv("DB_HOST")
        dbname = self.source_config.get("dbname") or os.getenv("DB_NAME")
        user = self.source_config.get("user") or os.getenv("DB_USER")
        password = self.source_config.get("password") or os.getenv("DB_PASSWORD")
        port_raw = self.source_config.get("port") or os.getenv("DB_PORT", "5432")
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
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    def _connect(self):
        params = self._db_params()
        try:
            import psycopg  # type: ignore

            return psycopg.connect(**params), "psycopg"
        except ImportError:
            try:
                import psycopg2  # type: ignore

                return psycopg2.connect(**params), "psycopg2"
            except ImportError as exc:
                raise ImportError(
                    "No PostgreSQL driver found. Install one of: "
                    '`python -m pip install "psycopg[binary]"` or `python -m pip install psycopg2-binary`.'
                ) from exc

    def execute_select(self, sql: str, row_limit: int, timeout_ms: int) -> List[Dict[str, Any]]:
        wrapped_sql = f"SELECT * FROM ({sql}) AS guarded_query LIMIT %s"
        conn, _driver = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = '{int(timeout_ms)}ms'")
                cur.execute(wrapped_sql, (row_limit,))
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                return [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
        finally:
            conn.close()

    def introspect_schema(self, schema_name: Optional[str] = None) -> Dict[str, Any]:
        target_schema = schema_name or "public"
        conn, _driver = self._connect()
        try:
            with conn.cursor() as cur:
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
                table_rows = cur.fetchall()
                table_names = [row[0] for row in table_rows]

                cur.execute(
                    """
                    SELECT
                        table_name,
                        column_name,
                        data_type,
                        udt_name,
                        is_nullable,
                        ordinal_position
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
                        tc.table_name,
                        kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = %s
                      AND tc.constraint_type = 'PRIMARY KEY'
                    """,
                    (target_schema,),
                )
                pk_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        tc.table_name AS source_table,
                        kcu.column_name AS source_column,
                        ccu.table_name AS target_table,
                        ccu.column_name AS target_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                     AND ccu.table_schema = tc.table_schema
                    WHERE tc.table_schema = %s
                      AND tc.constraint_type = 'FOREIGN KEY'
                    """,
                    (target_schema,),
                )
                fk_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT relname AS table_name, COALESCE(n_live_tup::bigint, 0) AS row_count
                    FROM pg_stat_user_tables
                    WHERE schemaname = %s
                    """,
                    (target_schema,),
                )
                count_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT tablename AS table_name, attname AS column_name, n_distinct
                    FROM pg_stats
                    WHERE schemaname = %s
                    """,
                    (target_schema,),
                )
                stats_rows = cur.fetchall()
        finally:
            conn.close()

        pk_lookup = defaultdict(set)
        for table_name, column_name in pk_rows:
            pk_lookup[table_name].add(column_name)

        row_count_lookup: Dict[str, int] = {table_name: int(row_count) for table_name, row_count in count_rows}
        n_distinct_lookup: Dict[Tuple[str, str], float] = {}
        for table_name, column_name, n_distinct in stats_rows:
            try:
                n_distinct_lookup[(table_name, column_name)] = float(n_distinct)
            except (TypeError, ValueError):
                continue

        table_columns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for table_name, column_name, data_type, udt_name, is_nullable, ordinal in column_rows:
            table_columns[table_name].append(
                {
                    "column_name": column_name,
                    "data_type": data_type,
                    "udt_name": udt_name,
                    "is_nullable": is_nullable == "YES",
                    "is_primary_key": column_name in pk_lookup[table_name],
                    "ordinal_position": int(ordinal),
                }
            )

        relationships: List[Dict[str, Any]] = []
        for src_table, src_col, tgt_table, tgt_col in fk_rows:
            relationships.append(
                {
                    "from_table": src_table,
                    "from_column": src_col,
                    "to_table": tgt_table,
                    "to_column": tgt_col,
                }
            )

        entities: List[Dict[str, Any]] = []
        measures: List[Dict[str, Any]] = []
        time_columns: List[Dict[str, Any]] = []

        for table_name in table_names:
            row_count = row_count_lookup.get(table_name, 0)
            for col in table_columns.get(table_name, []):
                col_name = col["column_name"]
                data_type = col["data_type"]
                n_distinct = n_distinct_lookup.get((table_name, col_name))
                cardinality_ratio = _normalize_cardinality(n_distinct, row_count)

                if data_type in TEXT_TYPES or data_type in {"integer", "bigint"}:
                    entity_score = 0.0
                    entity_score += _keyword_score(
                        col_name,
                        ["country", "customer", "product", "category", "region", "segment", "name"],
                        0.5,
                    )
                    if not col.get("is_primary_key"):
                        entity_score += 0.2
                    if 0.0 < cardinality_ratio < 0.9:
                        entity_score += 0.3
                    if entity_score >= 0.45:
                        entities.append(
                            {
                                "table": table_name,
                                "column": col_name,
                                "data_type": data_type,
                                "row_count": row_count,
                                "cardinality_ratio": round(cardinality_ratio, 4),
                                "score": round(entity_score, 4),
                            }
                        )

                if data_type in NUMERIC_TYPES:
                    measure_score = 0.2
                    measure_score += _keyword_score(
                        col_name,
                        ["amount", "revenue", "price", "total", "qty", "quantity", "sales", "value", "score"],
                        0.6,
                    )
                    if cardinality_ratio > 0.01:
                        measure_score += 0.2
                    if measure_score >= 0.45:
                        measures.append(
                            {
                                "table": table_name,
                                "column": col_name,
                                "data_type": data_type,
                                "row_count": row_count,
                                "cardinality_ratio": round(cardinality_ratio, 4),
                                "score": round(measure_score, 4),
                                "default_agg": "sum",
                            }
                        )

                if data_type in TIME_TYPES:
                    time_score = 0.3
                    time_score += _keyword_score(col_name, ["date", "time", "created", "updated", "timestamp"], 0.6)
                    if time_score >= 0.45:
                        time_columns.append(
                            {
                                "table": table_name,
                                "column": col_name,
                                "data_type": data_type,
                                "score": round(time_score, 4),
                                "default_grain": "month",
                            }
                        )

        entities = sorted(entities, key=lambda x: x["score"], reverse=True)
        measures = sorted(measures, key=lambda x: x["score"], reverse=True)
        time_columns = sorted(time_columns, key=lambda x: x["score"], reverse=True)

        tables: List[Dict[str, Any]] = []
        for table_name in table_names:
            tables.append(
                {
                    "table_name": table_name,
                    "row_count": row_count_lookup.get(table_name, 0),
                    "columns": table_columns.get(table_name, []),
                }
            )

        return {
            "source": {"db_engine": "postgres", "schema_name": target_schema},
            "profile": {
                "table_count": len(table_names),
                "relationship_count": len(relationships),
            },
            "tables": tables,
            "entities": entities,
            "measures": measures,
            "time_columns": time_columns,
            "relationships": relationships,
        }


def ensure_postgres_adapter(adapter: DatabaseAdapter) -> PostgresAdapter:
    if not isinstance(adapter, PostgresAdapter):
        raise AdapterError(f"Expected PostgresAdapter, got: {type(adapter).__name__}")
    return adapter
