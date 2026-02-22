import argparse
import json
from datetime import date, datetime
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


def normalize_rfm_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    # If recency_days is missing but latest_event_date exists, derive recency from max date.
    if "recency_days" not in rows[0] and "latest_event_date" in rows[0]:
        parsed_dates: List[date] = []
        for row in rows:
            raw = row.get("latest_event_date")
            if raw is None:
                continue
            if isinstance(raw, datetime):
                parsed_dates.append(raw.date())
            elif isinstance(raw, date):
                parsed_dates.append(raw)
            else:
                try:
                    parsed_dates.append(datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date())
                except ValueError:
                    continue
        ref_date = max(parsed_dates) if parsed_dates else date.today()
    else:
        ref_date = date.today()

    normalized: List[Dict[str, Any]] = []
    for row in rows:
        entity = row.get("entity_id") if row.get("entity_id") is not None else row.get("customer_id")
        if entity is None:
            continue

        recency_days = row.get("recency_days")
        if recency_days is None and row.get("latest_event_date") is not None:
            raw = row.get("latest_event_date")
            if isinstance(raw, datetime):
                latest = raw.date()
            elif isinstance(raw, date):
                latest = raw
            else:
                try:
                    latest = datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date()
                except ValueError:
                    continue
            recency_days = (ref_date - latest).days

        try:
            frequency = int(row.get("frequency", 0))
            monetary = float(row.get("monetary", 0.0))
            recency = int(recency_days)
        except (TypeError, ValueError):
            continue

        normalized.append(
            {
                "customer_id": str(entity),
                "recency_days": recency,
                "frequency": frequency,
                "monetary": monetary,
            }
        )

    return normalized


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

