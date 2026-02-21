-- Validation 03: Guardrail checks should all return 0 after ETL cleaning.
-- Expected:
-- null_customer_fk = 0
-- null_product_fk = 0
-- null_date_fk = 0
-- nonpositive_quantity_rows = 0
-- nonpositive_price_rows = 0
-- bad_total_amount_rows = 0

SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_fk,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_fk,
    SUM(CASE WHEN date_id IS NULL THEN 1 ELSE 0 END) AS null_date_fk,
    SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) AS nonpositive_quantity_rows,
    SUM(CASE WHEN unit_price <= 0 THEN 1 ELSE 0 END) AS nonpositive_price_rows,
    SUM(CASE WHEN total_amount <> ROUND(quantity::numeric * unit_price, 4) THEN 1 ELSE 0 END) AS bad_total_amount_rows
FROM fact_sales;
