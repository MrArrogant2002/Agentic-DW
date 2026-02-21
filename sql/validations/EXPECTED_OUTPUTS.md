# Validation Expected Outputs

These expected values assume:

- Source file: `data.csv`
- ETL cleaning rules in `etl/transform.py`
- Fresh load after running `agentic_ai_db.sql`

## 1) Fact Row Count + Revenue

- `fact_rows`: `397884`
- `total_revenue`: `8911407.9040`

## 2) Dimension Counts

- `dim_customer_rows`: `4338`
- `dim_product_rows`: `3665`
- `dim_date_rows`: `305`

## 3) Data Quality Assertions

All should be `0`:

- `null_customer_fk`
- `null_product_fk`
- `null_date_fk`
- `nonpositive_quantity_rows`
- `nonpositive_price_rows`
- `bad_total_amount_rows`

## 4) Top 5 Countries by Revenue

1. `United Kingdom` -> `7308391.5540`
2. `Netherlands` -> `285446.3400`
3. `EIRE` -> `265545.9000`
4. `Germany` -> `228867.1400`
5. `France` -> `209024.0500`

## 5) Monthly Revenue Check

- `month_count`: `13`
- `top_month`: `2011-11`
- `top_month_revenue`: `1161817.3800`
