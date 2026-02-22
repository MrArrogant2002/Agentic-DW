-- Baseline benchmark queries for warehouse validation.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*) AS rows_loaded, ROUND(SUM(total_amount), 4) AS revenue
FROM fact_sales;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT c.country, ROUND(SUM(f.total_amount), 4) AS revenue
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 5;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT to_char(date_trunc('month', f.invoice_timestamp), 'YYYY-MM') AS month_key,
       ROUND(SUM(f.total_amount), 4) AS revenue
FROM fact_sales f
GROUP BY 1
ORDER BY 1;
