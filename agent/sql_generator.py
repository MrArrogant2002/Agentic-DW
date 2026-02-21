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


def generate_sql(plan: Plan, strict: bool = False) -> str:
    if strict:
        # Strict retry fallback to a guaranteed summary query if first pass returns empty.
        return INTENT_SQL["generic_sales_summary"].strip()
    return INTENT_SQL.get(plan.intent, INTENT_SQL["generic_sales_summary"]).strip()
