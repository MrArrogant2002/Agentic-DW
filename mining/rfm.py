import argparse
import json
from datetime import date
from typing import Any, Dict, List

from mining.common import db_cursor


def fetch_rfm(reference_date: date | None = None) -> List[Dict[str, Any]]:
    if reference_date is None:
        with db_cursor() as cur:
            cur.execute("SELECT MAX(date_id) FROM fact_sales")
            max_date = cur.fetchone()[0]
        if max_date is None:
            return []
        reference_date = max_date

    sql = """
        SELECT
            customer_id,
            (%s::date - MAX(date_id))::int AS recency_days,
            COUNT(*)::int AS frequency,
            ROUND(SUM(total_amount), 4) AS monetary
        FROM fact_sales
        GROUP BY customer_id
        ORDER BY customer_id
    """
    with db_cursor() as cur:
        cur.execute(sql, (reference_date,))
        rows = cur.fetchall()

    return [
        {
            "customer_id": row[0],
            "recency_days": int(row[1]),
            "frequency": int(row[2]),
            "monetary": float(row[3]),
        }
        for row in rows
    ]


def summarize_rfm(rfm_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rfm_rows:
        return {"status": "insufficient_data", "customers": 0}

    recencies = [row["recency_days"] for row in rfm_rows]
    freqs = [row["frequency"] for row in rfm_rows]
    monies = [row["monetary"] for row in rfm_rows]

    return {
        "status": "ok",
        "customers": len(rfm_rows),
        "recency_min": min(recencies),
        "recency_max": max(recencies),
        "frequency_min": min(freqs),
        "frequency_max": max(freqs),
        "monetary_min": round(min(monies), 4),
        "monetary_max": round(max(monies), 4),
        "monetary_total": round(sum(monies), 4),
    }


def run(reference_date: date | None = None) -> Dict[str, Any]:
    rfm_rows = fetch_rfm(reference_date=reference_date)
    return {"rfm": rfm_rows, "summary": summarize_rfm(rfm_rows)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build customer RFM features.")
    parser.add_argument("--reference-date", default=None, help="Optional YYYY-MM-DD override for recency reference.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    ref = date.fromisoformat(args.reference_date) if args.reference_date else None
    result = run(reference_date=ref)
    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result))

