-- Validation 02: Distinct dimensions should match ETL-cleaned baseline.
-- Expected:
-- dim_customer_rows = 4338
-- dim_product_rows = 3665
-- dim_date_rows = 305

SELECT
    (SELECT COUNT(*) FROM dim_customer) AS dim_customer_rows,
    (SELECT COUNT(*) FROM dim_product) AS dim_product_rows,
    (SELECT COUNT(*) FROM dim_date) AS dim_date_rows;
