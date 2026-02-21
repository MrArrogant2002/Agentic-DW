import csv
import os
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Iterator, List, Tuple

from utils.env_loader import load_environments


def _get_connection():
    load_environments()
    db_params = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }
    if not db_params["host"]:
        raise ValueError("DB_HOST is required")
    if not db_params["dbname"]:
        raise ValueError("DB_NAME is required")
    if not db_params["user"]:
        raise ValueError("DB_USER is required")
    if not db_params["password"]:
        raise ValueError("DB_PASSWORD is required")
    if db_params["password"] in {"your_password", "your_password_here", "changeme"}:
        raise ValueError("DB_PASSWORD appears to be a placeholder. Update .env with your real PostgreSQL password.")

    try:
        import psycopg  # type: ignore

        return psycopg.connect(**db_params), "psycopg"
    except ImportError:
        try:
            import psycopg2  # type: ignore

            return psycopg2.connect(**db_params), "psycopg2"
        except ImportError as exc:
            raise ImportError(
                "No PostgreSQL driver found. Install one of: "
                "`pip install \"psycopg[binary]\"` or `pip install psycopg2-binary`."
            ) from exc


@contextmanager
def db_session():
    conn, driver = _get_connection()
    try:
        yield conn, driver
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _read_clean_rows(clean_csv_path: str) -> Iterator[Dict[str, str]]:
    with open(clean_csv_path, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row


def _batch_insert(cur, sql: str, rows: List[Tuple], batch_size: int = 5000) -> int:
    inserted = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        cur.executemany(sql, batch)
        inserted += len(batch)
    return inserted


def load_to_postgres(clean_csv_path: str) -> Dict[str, int]:
    customers: Dict[str, str] = {}
    products: Dict[str, str] = {}
    dates: Dict[str, datetime] = {}

    for row in _read_clean_rows(clean_csv_path):
        customers[row["customer_id"]] = row["country"]
        products[row["product_id"]] = row["description"]
        if row["date_id"] not in dates:
            dates[row["date_id"]] = datetime.strptime(row["date_id"], "%Y-%m-%d")

    line_counter = defaultdict(int)
    facts: List[Tuple] = []
    for row in _read_clean_rows(clean_csv_path):
        invoice_no = row["invoice_no"]
        line_counter[invoice_no] += 1
        facts.append(
            (
                invoice_no,
                line_counter[invoice_no],
                row["customer_id"],
                row["product_id"],
                row["date_id"],
                row["invoice_timestamp"],
                int(row["quantity"]),
                row["unit_price"],
                row["total_amount"],
            )
        )

    customer_rows = [(k, v) for k, v in customers.items()]
    product_rows = [(k, v) for k, v in products.items()]
    date_rows = [
        (
            d.strftime("%Y-%m-%d"),
            d.day,
            d.month,
            (d.month - 1) // 3 + 1,
            d.year,
            int(d.strftime("%V")),
            d.strftime("%B"),
            d.strftime("%A"),
        )
        for d in dates.values()
    ]

    with db_session() as (conn, _driver):
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE fact_sales, dim_date, dim_product, dim_customer RESTART IDENTITY CASCADE;")

            inserted_customers = _batch_insert(
                cur,
                "INSERT INTO dim_customer (customer_id, country) VALUES (%s, %s)",
                customer_rows,
            )
            inserted_products = _batch_insert(
                cur,
                "INSERT INTO dim_product (product_id, description) VALUES (%s, %s)",
                product_rows,
            )
            inserted_dates = _batch_insert(
                cur,
                """
                INSERT INTO dim_date
                (date_id, day, month, quarter, year, week_of_year, month_name, day_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                date_rows,
            )
            inserted_facts = _batch_insert(
                cur,
                """
                INSERT INTO fact_sales
                (invoice_no, invoice_line_no, customer_id, product_id, date_id, invoice_timestamp, quantity, unit_price, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                facts,
            )

    return {
        "dim_customer": inserted_customers,
        "dim_product": inserted_products,
        "dim_date": inserted_dates,
        "fact_sales": inserted_facts,
    }
