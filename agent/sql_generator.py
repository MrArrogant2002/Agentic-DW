import re
from typing import Any, Dict, List, Optional

from agent.planner import Plan


INTENT_SQL = {
    "country_revenue": """
        SELECT c.country, ROUND(SUM(f.total_amount), 4) AS revenue
        FROM fact_sales f
        JOIN dim_customer c ON c.customer_id = f.customer_id
        GROUP BY c.country
        ORDER BY revenue DESC
    """,
    "top_customers": """
        SELECT f.customer_id, ROUND(SUM(f.total_amount), 4) AS revenue
        FROM fact_sales f
        GROUP BY f.customer_id
        ORDER BY revenue DESC
    """,
    "top_products": """
        SELECT f.product_id, ROUND(SUM(f.total_amount), 4) AS revenue
        FROM fact_sales f
        GROUP BY f.product_id
        ORDER BY revenue DESC
    """,
    "monthly_revenue": """
        SELECT to_char(date_trunc('month', f.invoice_timestamp), 'YYYY-MM') AS month_key,
               ROUND(SUM(f.total_amount), 4) AS revenue
        FROM fact_sales f
        GROUP BY 1
        ORDER BY 1
    """,
    "trend_analysis": """
        SELECT to_char(date_trunc('month', f.invoice_timestamp), 'YYYY-MM') AS month_key,
               ROUND(SUM(f.total_amount), 4) AS revenue
        FROM fact_sales f
        GROUP BY 1
        ORDER BY 1
    """,
    "customer_segmentation": """
        SELECT customer_id,
               COUNT(*)::int AS frequency,
               ROUND(SUM(total_amount), 4) AS monetary
        FROM fact_sales
        GROUP BY customer_id
        ORDER BY monetary DESC
    """,
    "generic_sales_summary": """
        SELECT COUNT(*) AS rows_loaded, ROUND(SUM(total_amount), 4) AS revenue
        FROM fact_sales
    """,
}


def _safe_ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return f'"{name}"'


def _pick_candidate(candidates: List[Dict[str, Any]], keywords: List[str]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    lowered_keywords = [k.lower() for k in keywords]
    preferred = [c for c in candidates if any(k in str(c.get("column", "")).lower() for k in lowered_keywords)]
    if preferred:
        return preferred[0]
    return candidates[0]


def _find_relationship(measure_table: str, entity_table: str, relationships: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for rel in relationships:
        if rel.get("from_table") == measure_table and rel.get("to_table") == entity_table:
            return {"left_col": rel.get("from_column"), "right_col": rel.get("to_column"), "direction": "forward"}
        if rel.get("from_table") == entity_table and rel.get("to_table") == measure_table:
            return {"left_col": rel.get("to_column"), "right_col": rel.get("from_column"), "direction": "reverse"}
    return None


def _dynamic_group_sql(entity: Dict[str, Any], measure: Dict[str, Any], relationships: List[Dict[str, Any]], alias: str) -> Optional[str]:
    e_table = str(entity.get("table"))
    e_col = str(entity.get("column"))
    m_table = str(measure.get("table"))
    m_col = str(measure.get("column"))

    if e_table == m_table:
        return (
            f"SELECT {_safe_ident(e_col)} AS {alias}, ROUND(SUM({_safe_ident(m_col)}), 4) AS value "
            f"FROM {_safe_ident(m_table)} "
            f"GROUP BY 1 ORDER BY value DESC"
        )

    rel = _find_relationship(m_table, e_table, relationships)
    if not rel:
        return None

    return (
        f"SELECT e.{_safe_ident(e_col)} AS {alias}, ROUND(SUM(m.{_safe_ident(m_col)}), 4) AS value "
        f"FROM {_safe_ident(m_table)} m "
        f"JOIN {_safe_ident(e_table)} e ON m.{_safe_ident(str(rel['left_col']))} = e.{_safe_ident(str(rel['right_col']))} "
        f"GROUP BY 1 ORDER BY value DESC"
    )


def _dynamic_monthly_sql(measure: Dict[str, Any], time_col: Dict[str, Any]) -> Optional[str]:
    m_table = str(measure.get("table"))
    m_col = str(measure.get("column"))
    t_table = str(time_col.get("table"))
    t_col = str(time_col.get("column"))
    if m_table != t_table:
        return None
    return (
        "SELECT to_char(date_trunc('month', "
        f"{_safe_ident(t_col)}), 'YYYY-MM') AS month_key, "
        f"ROUND(SUM({_safe_ident(m_col)}), 4) AS value "
        f"FROM {_safe_ident(m_table)} GROUP BY 1 ORDER BY 1"
    )


def _generate_dynamic_sql(plan: Plan, metadata: Dict[str, Any]) -> Optional[str]:
    entities = metadata.get("entities", [])
    measures = metadata.get("measures", [])
    time_columns = metadata.get("time_columns", [])
    relationships = metadata.get("relationships", [])

    if not measures:
        return None

    measure = _pick_candidate(measures, ["amount", "revenue", "total", "price", "value", "score", "sales"])
    if not measure:
        return None

    if plan.intent == "country_revenue":
        entity = _pick_candidate(entities, ["country", "region", "nation"])
        if not entity:
            return None
        return _dynamic_group_sql(entity, measure, relationships, alias="entity")

    if plan.intent == "top_customers":
        entity = _pick_candidate(entities, ["customer", "client", "account"])
        if not entity:
            return None
        return _dynamic_group_sql(entity, measure, relationships, alias="entity")

    if plan.intent == "top_products":
        entity = _pick_candidate(entities, ["product", "item", "sku"])
        if not entity:
            return None
        return _dynamic_group_sql(entity, measure, relationships, alias="entity")

    if plan.intent in {"monthly_revenue", "trend_analysis"}:
        time_col = _pick_candidate(time_columns, ["date", "time", "created", "updated"])
        if not time_col:
            return None
        return _dynamic_monthly_sql(measure, time_col)

    if plan.intent == "generic_sales_summary":
        m_table = str(measure.get("table"))
        m_col = str(measure.get("column"))
        return f"SELECT COUNT(*) AS rows_loaded, ROUND(SUM({_safe_ident(m_col)}), 4) AS value FROM {_safe_ident(m_table)}"

    return None


def generate_sql(plan: Plan, strict: bool = False, dataset_metadata: Dict[str, Any] | None = None) -> str:
    if strict:
        return INTENT_SQL["generic_sales_summary"].strip()

    if dataset_metadata:
        try:
            dynamic_sql = _generate_dynamic_sql(plan, dataset_metadata)
            if dynamic_sql:
                return dynamic_sql.strip()
        except Exception:
            pass

    return INTENT_SQL.get(plan.intent, INTENT_SQL["generic_sales_summary"]).strip()

