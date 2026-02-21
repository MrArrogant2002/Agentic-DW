DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
    customer_id VARCHAR(16) PRIMARY KEY,
    country VARCHAR(128) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_product (
    product_id VARCHAR(32) PRIMARY KEY,
    description TEXT NOT NULL CHECK (length(trim(description)) > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month_name VARCHAR(12) NOT NULL,
    day_name VARCHAR(12) NOT NULL
);

CREATE TABLE fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    invoice_no VARCHAR(20) NOT NULL,
    invoice_line_no INTEGER NOT NULL,
    customer_id VARCHAR(16) NOT NULL REFERENCES dim_customer(customer_id),
    product_id VARCHAR(32) NOT NULL REFERENCES dim_product(product_id),
    date_id DATE NOT NULL REFERENCES dim_date(date_id),
    invoice_timestamp TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12,4) NOT NULL CHECK (unit_price > 0),
    total_amount NUMERIC(14,4) NOT NULL CHECK (total_amount > 0),
    CONSTRAINT uq_fact_invoice_line UNIQUE (invoice_no, invoice_line_no),
    CONSTRAINT ck_total_amount_consistent CHECK (
        total_amount = round((quantity::numeric * unit_price), 4)
    )
);

CREATE INDEX idx_fact_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_product ON fact_sales(product_id);
CREATE INDEX idx_fact_date ON fact_sales(date_id);
CREATE INDEX idx_fact_invoice_ts ON fact_sales(invoice_timestamp);
CREATE INDEX idx_fact_invoice_no ON fact_sales(invoice_no);
