import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict

from extract import extract_rows

INPUT_DATETIME_FORMAT = "%m/%d/%Y %H:%M"
MONEY_QUANT = Decimal("0.0001")


@dataclass
class TransformStats:
    total_rows: int = 0
    kept_rows: int = 0
    dropped_null_customer: int = 0
    dropped_nonpositive_quantity: int = 0
    dropped_nonpositive_price: int = 0
    dropped_bad_datetime: int = 0


def _parse_decimal(value: str) -> Decimal:
    try:
        return Decimal(value.strip())
    except (InvalidOperation, AttributeError):
        return Decimal("-1")


def _normalize_row(row: Dict[str, str]) -> Dict[str, str]:
    invoice_dt = datetime.strptime(row["InvoiceDate"].strip(), INPUT_DATETIME_FORMAT)
    quantity = _parse_decimal(row["Quantity"])
    unit_price = _parse_decimal(row["UnitPrice"])
    total_amount = (quantity * unit_price).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)

    return {
        "invoice_no": row["InvoiceNo"].strip(),
        "product_id": row["StockCode"].strip(),
        "description": row["Description"].strip(),
        "quantity": str(int(quantity)),
        "invoice_timestamp": invoice_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "date_id": invoice_dt.strftime("%Y-%m-%d"),
        "unit_price": str(unit_price.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)),
        "customer_id": row["CustomerID"].strip(),
        "country": row["Country"].strip(),
        "total_amount": str(total_amount),
    }


def transform_csv(input_csv: str, clean_output_csv: str, rejected_output_csv: str) -> TransformStats:
    stats = TransformStats()
    Path(clean_output_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(rejected_output_csv).parent.mkdir(parents=True, exist_ok=True)

    clean_columns = [
        "invoice_no",
        "product_id",
        "description",
        "quantity",
        "invoice_timestamp",
        "date_id",
        "unit_price",
        "customer_id",
        "country",
        "total_amount",
    ]
    reject_columns = ["reason", "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

    with (
        Path(clean_output_csv).open("w", encoding="utf-8", newline="") as clean_file,
        Path(rejected_output_csv).open("w", encoding="utf-8", newline="") as rejected_file,
    ):
        clean_writer = csv.DictWriter(clean_file, fieldnames=clean_columns)
        rejected_writer = csv.DictWriter(rejected_file, fieldnames=reject_columns)
        clean_writer.writeheader()
        rejected_writer.writeheader()

        for raw_row in extract_rows(input_csv):
            stats.total_rows += 1

            customer_id = (raw_row.get("CustomerID") or "").strip()
            if not customer_id:
                stats.dropped_null_customer += 1
                rejected_writer.writerow({"reason": "missing_customer_id", **raw_row})
                continue

            quantity = _parse_decimal(raw_row.get("Quantity", ""))
            if quantity <= 0:
                stats.dropped_nonpositive_quantity += 1
                rejected_writer.writerow({"reason": "nonpositive_quantity", **raw_row})
                continue

            price = _parse_decimal(raw_row.get("UnitPrice", ""))
            if price <= 0:
                stats.dropped_nonpositive_price += 1
                rejected_writer.writerow({"reason": "nonpositive_price", **raw_row})
                continue

            try:
                normalized = _normalize_row(raw_row)
            except (ValueError, KeyError):
                stats.dropped_bad_datetime += 1
                rejected_writer.writerow({"reason": "bad_invoice_date", **raw_row})
                continue

            clean_writer.writerow(normalized)
            stats.kept_rows += 1

    return stats
