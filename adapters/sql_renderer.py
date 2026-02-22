from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SQLDialect:
    engine: str
    limit_placeholder: str

    def render_date_bucket(self, column_expr: str, time_grain: str) -> str:
        grain = (time_grain or "month").lower()
        if self.engine in {"postgres", "postgresql"}:
            supported = {"day", "week", "month", "quarter", "year"}
            g = grain if grain in supported else "month"
            return f"date_trunc('{g}', {column_expr})::date"
        if self.engine == "sqlite":
            if grain == "year":
                return f"date(strftime('%Y-01-01', {column_expr}))"
            if grain == "day":
                return f"date({column_expr})"
            return f"date(strftime('%Y-%m-01', {column_expr}))"
        if self.engine == "mysql":
            if grain == "year":
                return f"str_to_date(concat(year({column_expr}), '-01-01'), '%Y-%m-%d')"
            if grain == "day":
                return f"date({column_expr})"
            return f"str_to_date(date_format({column_expr}, '%Y-%m-01'), '%Y-%m-%d')"
        return column_expr


def get_sql_dialect(db_engine: str) -> SQLDialect:
    engine = (db_engine or "postgres").strip().lower()
    if engine in {"postgres", "postgresql"}:
        return SQLDialect(engine="postgres", limit_placeholder="%s")
    if engine == "sqlite":
        return SQLDialect(engine="sqlite", limit_placeholder="?")
    if engine == "mysql":
        return SQLDialect(engine="mysql", limit_placeholder="%s")
    return SQLDialect(engine=engine, limit_placeholder="%s")
