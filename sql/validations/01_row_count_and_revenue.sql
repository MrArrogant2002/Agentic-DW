-- Validation 01: Fact row count and total revenue should match ETL-cleaned baseline.
-- Expected:
-- fact_rows = 397884
-- total_revenue = 8911407.9040

SELECT
    COUNT(*) AS fact_rows,
    ROUND(SUM(total_amount), 4) AS total_revenue
FROM fact_sales;
