-- Validation 05: Monthly revenue profile sanity checks.
-- Expected:
-- month_count = 13
-- top_month = 2011-11
-- top_month_revenue = 1161817.3800

WITH monthly AS (
    SELECT
        to_char(date_trunc('month', invoice_timestamp), 'YYYY-MM') AS month_key,
        ROUND(SUM(total_amount), 4) AS revenue
    FROM fact_sales
    GROUP BY 1
)
SELECT
    (SELECT COUNT(*) FROM monthly) AS month_count,
    (SELECT month_key FROM monthly ORDER BY revenue DESC LIMIT 1) AS top_month,
    (SELECT revenue FROM monthly ORDER BY revenue DESC LIMIT 1) AS top_month_revenue;
