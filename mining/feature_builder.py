from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from adapters.sql_renderer import get_sql_dialect
from agent.executor import execute_safe_query
from agent.planner import Plan


@dataclass
class FeatureBuildResult:
    status: str
    reason: Optional[str]
    sql: Optional[str]
    rows: List[Dict[str, Any]]


def _safe_ident(name: str) -> str:
    return f'"{str(name).replace(chr(34), "")}"'


def _find_candidate(items: List[Dict[str, Any]], keyword: Optional[str], fallback_keywords: List[str]) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    lowered_kw = (keyword or "").strip().lower()
    if lowered_kw:
        exact = [i for i in items if lowered_kw == str(i.get("column", "")).lower()]
        if exact:
            return exact[0]
        contains = [i for i in items if lowered_kw in str(i.get("column", "")).lower()]
        if contains:
            return contains[0]
    for kw in fallback_keywords:
        matches = [i for i in items if kw in str(i.get("column", "")).lower()]
        if matches:
            return matches[0]
    return items[0]


def _find_relationship(relationships: List[Dict[str, Any]], left_table: str, right_table: str) -> Optional[Tuple[str, str]]:
    for rel in relationships:
        if rel.get("from_table") == left_table and rel.get("to_table") == right_table:
            return str(rel.get("from_column")), str(rel.get("to_column"))
        if rel.get("from_table") == right_table and rel.get("to_table") == left_table:
            return str(rel.get("to_column")), str(rel.get("from_column"))
    return None


def _date_trunc_expr(db_engine: str, column: str, time_grain: Optional[str]) -> str:
    dialect = get_sql_dialect(db_engine)
    return dialect.render_date_bucket(column, time_grain or "month")


def _build_trend_sql(plan: Plan, metadata: Dict[str, Any], db_engine: str) -> FeatureBuildResult:
    entities = metadata.get("entities", [])
    measures = metadata.get("measures", [])
    time_columns = metadata.get("time_columns", [])
    relationships = metadata.get("relationships", [])

    measure = _find_candidate(measures, plan.metric, ["amount", "revenue", "total", "price", "value", "score", "sales"])
    time_col = _find_candidate(time_columns, None, ["date", "time", "created", "updated", "timestamp"])
    if not measure or not time_col:
        return FeatureBuildResult(
            status="insufficient_data",
            reason="No suitable measure/time columns found in semantic map.",
            sql=None,
            rows=[],
        )

    m_table = str(measure["table"])
    m_col = str(measure["column"])
    t_table = str(time_col["table"])
    t_col = str(time_col["column"])
    if m_table != t_table:
        return FeatureBuildResult(
            status="insufficient_data",
            reason="Measure and time columns are on different tables; join inference for trend is not available yet.",
            sql=None,
            rows=[],
        )

    period_expr = _date_trunc_expr(db_engine, f"f.{_safe_ident(t_col)}", plan.time_grain)
    value_expr = f"SUM(f.{_safe_ident(m_col)})"
    base_group = "period_start"

    entity = None
    if plan.entity_scope == "top_n":
        entity = _find_candidate(entities, plan.entity_dimension, ["country", "customer", "product", "category", "segment", "region", "name"])

    if entity and str(entity["table"]) != m_table:
        rel = _find_relationship(relationships, m_table, str(entity["table"]))
        if not rel:
            return FeatureBuildResult(
                status="insufficient_data",
                reason="Top-N entity requested but relationship to measure table was not found.",
                sql=None,
                rows=[],
            )
        left_col, right_col = rel
        entity_select = f"e.{_safe_ident(str(entity['column']))} AS entity_key"
        join_clause = (
            f'JOIN {_safe_ident(str(entity["table"]))} e ON '
            f"f.{_safe_ident(left_col)} = e.{_safe_ident(right_col)}"
        )
    elif entity:
        entity_select = f"f.{_safe_ident(str(entity['column']))} AS entity_key"
        join_clause = ""
    else:
        entity_select = None
        join_clause = ""

    if entity_select and plan.entity_scope == "top_n":
        n = plan.n or 5
        sql = f"""
            WITH base AS (
                SELECT {period_expr} AS period_start,
                       {entity_select},
                       {value_expr} AS metric_value
                FROM {_safe_ident(m_table)} f
                {join_clause}
                GROUP BY 1, 2
            ),
            top_entities AS (
                SELECT entity_key
                FROM base
                GROUP BY entity_key
                ORDER BY SUM(metric_value) DESC
                LIMIT {int(n)}
            )
            SELECT b.period_start, b.entity_key, ROUND(SUM(b.metric_value), 4) AS metric_value
            FROM base b
            JOIN top_entities t ON t.entity_key = b.entity_key
            GROUP BY 1, 2
            ORDER BY 1, 3 DESC
        """.strip()
    else:
        sql = f"""
            SELECT {period_expr} AS period_start,
                   ROUND({value_expr}, 4) AS metric_value
            FROM {_safe_ident(m_table)} f
            GROUP BY 1
            ORDER BY 1
        """.strip()

    return FeatureBuildResult(status="ok", reason=None, sql=sql, rows=[])


def _build_segmentation_sql(plan: Plan, metadata: Dict[str, Any], db_engine: str) -> FeatureBuildResult:
    entities = metadata.get("entities", [])
    measures = metadata.get("measures", [])
    time_columns = metadata.get("time_columns", [])
    relationships = metadata.get("relationships", [])

    measure = _find_candidate(measures, plan.metric, ["amount", "revenue", "total", "price", "value", "score", "sales"])
    time_col = _find_candidate(time_columns, None, ["date", "time", "created", "updated", "timestamp"])
    entity = _find_candidate(entities, plan.entity_dimension, ["customer", "account", "user", "student", "country", "product", "name", "id"])
    if not measure or not time_col or not entity:
        return FeatureBuildResult(
            status="insufficient_data",
            reason="No suitable entity/measure/time columns found in semantic map.",
            sql=None,
            rows=[],
        )

    m_table = str(measure["table"])
    m_col = str(measure["column"])
    t_table = str(time_col["table"])
    t_col = str(time_col["column"])
    e_table = str(entity["table"])
    e_col = str(entity["column"])

    if m_table != t_table:
        return FeatureBuildResult(
            status="insufficient_data",
            reason="Measure and time columns are on different tables; segmentation join inference unavailable.",
            sql=None,
            rows=[],
        )

    if e_table != m_table:
        rel = _find_relationship(relationships, m_table, e_table)
        if not rel:
            return FeatureBuildResult(
                status="insufficient_data",
                reason="Entity relationship to measure table was not found for segmentation.",
                sql=None,
                rows=[],
            )
        left_col, right_col = rel
        entity_expr = f"e.{_safe_ident(e_col)}"
        join_clause = f'JOIN {_safe_ident(e_table)} e ON f.{_safe_ident(left_col)} = e.{_safe_ident(right_col)}'
    else:
        entity_expr = f"f.{_safe_ident(e_col)}"
        join_clause = ""

    if db_engine in {"postgres", "postgresql"}:
        recency_expr = f"(MAX(f.{_safe_ident(t_col)})::date)"
        sql = f"""
            WITH entity_rollup AS (
                SELECT
                    {entity_expr} AS entity_id,
                    {recency_expr} AS latest_event_date,
                    COUNT(*)::int AS frequency,
                    ROUND(SUM(f.{_safe_ident(m_col)}), 4) AS monetary
                FROM {_safe_ident(m_table)} f
                {join_clause}
                GROUP BY 1
            ),
            ref AS (
                SELECT MAX(latest_event_date) AS ref_date FROM entity_rollup
            )
            SELECT
                er.entity_id,
                (ref.ref_date - er.latest_event_date)::int AS recency_days,
                er.frequency,
                er.monetary
            FROM entity_rollup er
            CROSS JOIN ref
            ORDER BY er.entity_id
        """.strip()
    else:
        # Generic fallback for sqlite/mysql: return latest date and compute recency in Python.
        sql = f"""
            SELECT
                {entity_expr} AS entity_id,
                MAX(f.{_safe_ident(t_col)}) AS latest_event_date,
                COUNT(*) AS frequency,
                ROUND(SUM(f.{_safe_ident(m_col)}), 4) AS monetary
            FROM {_safe_ident(m_table)} f
            {join_clause}
            GROUP BY 1
            ORDER BY 1
        """.strip()

    return FeatureBuildResult(status="ok", reason=None, sql=sql, rows=[])


def feature_builder(
    schema_metadata: Dict[str, Any],
    plan: Plan,
    db_engine: str = "postgres",
    source_config: Optional[Dict[str, Any]] = None,
    row_limit: int = 200000,
    timeout_ms: int = 60000,
) -> Dict[str, Any]:
    task = plan.task_type
    if task == "trend_analysis":
        spec = _build_trend_sql(plan, schema_metadata, db_engine=db_engine)
    elif task == "segmentation":
        spec = _build_segmentation_sql(plan, schema_metadata, db_engine=db_engine)
    else:
        return {"status": "unsupported_task", "reason": f"feature_builder does not support task_type={task}", "rows": []}

    if spec.status != "ok" or not spec.sql:
        return {"status": spec.status, "reason": spec.reason, "rows": [], "sql": spec.sql}

    rows = execute_safe_query(
        spec.sql,
        row_limit=row_limit,
        timeout_ms=timeout_ms,
        db_engine=db_engine,
        source_config=source_config,
    )
    return {
        "status": "ok",
        "reason": None,
        "sql": spec.sql,
        "rows": rows,
    }
