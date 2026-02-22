from typing import Any, Dict


def build_quality_report(ingest_result: Dict[str, Any]) -> Dict[str, Any]:
    row_in = int(ingest_result.get("row_count_input", 0))
    row_out = int(ingest_result.get("row_count_inserted", 0))
    coerced_nulls = int(ingest_result.get("coerced_nulls", 0))
    columns = ingest_result.get("columns", [])

    completeness = 0.0
    if row_in > 0:
        completeness = row_out / row_in

    return {
        "status": "ok" if row_out > 0 else "warning",
        "row_count_input": row_in,
        "row_count_loaded": row_out,
        "load_completeness": round(completeness, 4),
        "coerced_nulls": coerced_nulls,
        "column_count": len(columns),
        "column_types": [{k: c[k] for k in ("column", "data_type")} for c in columns],
    }

