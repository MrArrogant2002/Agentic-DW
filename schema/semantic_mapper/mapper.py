from typing import Any, Dict, List


NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "decimal",
    "real",
    "double precision",
}
TEXT_TYPES = {"text", "character varying", "character", "varchar", "char"}
TIME_TYPES = {"date", "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone"}


def _score_name(name: str, keywords: List[str], weight: float) -> float:
    lowered = name.lower()
    return weight if any(k in lowered for k in keywords) else 0.0


def build_semantic_map(schema_metadata: Dict[str, Any]) -> Dict[str, Any]:
    entities: List[Dict[str, Any]] = []
    measures: List[Dict[str, Any]] = []
    time_columns: List[Dict[str, Any]] = []
    relationships = schema_metadata.get("relationships", [])
    tables = schema_metadata.get("tables", [])

    for table in tables:
        table_name = str(table.get("table_name"))
        row_count = int(table.get("row_count", 0))
        columns = table.get("columns", [])

        for col in columns:
            column_name = str(col.get("column_name"))
            data_type = str(col.get("data_type"))
            is_pk = bool(col.get("is_primary_key", False))

            if data_type in TEXT_TYPES or data_type in {"integer", "bigint"}:
                score = 0.1
                score += _score_name(column_name, ["country", "customer", "product", "category", "region", "segment", "name", "city"], 0.6)
                if not is_pk:
                    score += 0.2
                if row_count > 0:
                    score += 0.1
                if score >= 0.45:
                    entities.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "data_type": data_type,
                            "row_count": row_count,
                            "score": round(score, 4),
                        }
                    )

            if data_type in NUMERIC_TYPES:
                score = 0.2
                score += _score_name(column_name, ["amount", "revenue", "price", "total", "qty", "quantity", "sales", "value", "score"], 0.6)
                if row_count > 0:
                    score += 0.1
                if score >= 0.45:
                    measures.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "data_type": data_type,
                            "default_agg": "sum",
                            "row_count": row_count,
                            "score": round(score, 4),
                        }
                    )

            if data_type in TIME_TYPES:
                score = 0.3
                score += _score_name(column_name, ["date", "time", "created", "updated", "timestamp"], 0.6)
                if score >= 0.45:
                    time_columns.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "data_type": data_type,
                            "default_grain": "month",
                            "score": round(score, 4),
                        }
                    )

    entities = sorted(entities, key=lambda x: x["score"], reverse=True)
    measures = sorted(measures, key=lambda x: x["score"], reverse=True)
    time_columns = sorted(time_columns, key=lambda x: x["score"], reverse=True)

    return {
        "entities": entities,
        "measures": measures,
        "time_columns": time_columns,
        "relationships": relationships,
    }

