-- Validation 04: Top 5 countries by revenue should match this ordering and values.
-- Expected order:
-- 1. United Kingdom 7308391.5540
-- 2. Netherlands    285446.3400
-- 3. EIRE           265545.9000
-- 4. Germany        228867.1400
-- 5. France         209024.0500

SELECT
    c.country,
    ROUND(SUM(f.total_amount), 4) AS revenue
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 5;
