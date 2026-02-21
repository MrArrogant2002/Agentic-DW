import argparse
import json
from typing import Any, Dict, List

import numpy as np

from mining.common import db_cursor


def fetch_monthly_revenue() -> List[Dict[str, Any]]:
    sql = """
        SELECT
            to_char(date_trunc('month', invoice_timestamp), 'YYYY-MM') AS month_key,
            ROUND(SUM(total_amount), 4) AS revenue
        FROM fact_sales
        GROUP BY 1
        ORDER BY 1
    """
    with db_cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [{"month_key": row[0], "revenue": float(row[1])} for row in rows]


def analyze_trend(monthly_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if len(monthly_rows) < 2:
        return {
            "status": "insufficient_data",
            "reason": "Need at least two months to compute trend",
            "points": len(monthly_rows),
        }

    y = np.array([row["revenue"] for row in monthly_rows], dtype=float)
    x = np.arange(len(y), dtype=float)

    slope, intercept = np.polyfit(x, y, deg=1)
    y_pred = slope * x + intercept
    ss_res = float(np.sum((y - y_pred) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 0.0 if ss_tot == 0 else 1.0 - (ss_res / ss_tot)

    direction = "flat"
    if slope > 1e-9:
        direction = "upward"
    elif slope < -1e-9:
        direction = "downward"

    return {
        "status": "ok",
        "points": len(monthly_rows),
        "slope_per_month": round(float(slope), 4),
        "intercept": round(float(intercept), 4),
        "r2": round(float(r2), 4),
        "direction": direction,
        "start_month": monthly_rows[0]["month_key"],
        "end_month": monthly_rows[-1]["month_key"],
        "start_revenue": round(float(monthly_rows[0]["revenue"]), 4),
        "end_revenue": round(float(monthly_rows[-1]["revenue"]), 4),
    }


def run() -> Dict[str, Any]:
    monthly = fetch_monthly_revenue()
    trend = analyze_trend(monthly)
    return {"monthly_revenue": monthly, "trend": trend}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run monthly revenue trend analysis.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()

    result = run()
    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result))

