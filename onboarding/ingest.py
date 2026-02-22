import csv
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from schema.introspector.db import connect


def _pick_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            path.read_text(encoding=enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def sanitize_identifier(name: str, fallback: str = "col") -> str:
    lowered = name.strip().lower()
    normalized = re.sub(r"[^a-z0-9_]+", "_", lowered)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = f"c_{normalized}"
    return normalized[:63]


def build_schema_name(dataset_id: str) -> str:
    compact = re.sub(r"[^a-z0-9]", "", dataset_id.lower())[:12]
    return f"raw_{compact}"


def _try_int(v: str) -> bool:
    try:
        int(v)
        return True
    except ValueError:
        return False


def _try_float(v: str) -> bool:
    try:
        float(v)
        return True
    except ValueError:
        return False


def _try_bool(v: str) -> bool:
    return v.strip().lower() in {"true", "false", "yes", "no", "0", "1"}


def _try_date(v: str) -> bool:
    fmts = ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y")
    for fmt in fmts:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


def _try_timestamp(v: str) -> bool:
    fmts = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
    )
    for fmt in fmts:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    try:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def infer_column_type(values: List[str]) -> str:
    cleaned = [v.strip() for v in values if v is not None and str(v).strip() != ""]
    if not cleaned:
        return "TEXT"
    if all(_try_int(v) for v in cleaned):
        return "BIGINT"
    if all(_try_float(v) for v in cleaned):
        return "DOUBLE PRECISION"
    if all(_try_bool(v) for v in cleaned):
        return "BOOLEAN"
    if all(_try_timestamp(v) for v in cleaned):
        return "TIMESTAMP"
    if all(_try_date(v) for v in cleaned):
        return "DATE"
    return "TEXT"


def _parse_value(value: str, pg_type: str) -> Tuple[Any, bool]:
    if value is None:
        return None, False
    raw = str(value).strip()
    if raw == "":
        return None, False

    try:
        if pg_type == "BIGINT":
            return int(raw), False
        if pg_type == "DOUBLE PRECISION":
            return float(raw), False
        if pg_type == "BOOLEAN":
            lowered = raw.lower()
            if lowered in {"true", "yes", "1"}:
                return True, False
            if lowered in {"false", "no", "0"}:
                return False, False
            return None, True
        if pg_type == "DATE":
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
                try:
                    return datetime.strptime(raw, fmt).date(), False
                except ValueError:
                    continue
            return None, True
        if pg_type == "TIMESTAMP":
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y %H:%M:%S",
            ):
                try:
                    return datetime.strptime(raw, fmt), False
                except ValueError:
                    continue
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00")), False
            except ValueError:
                return None, True
        return raw, False
    except Exception:
        return None, True


def _quote(identifier: str) -> str:
    return f'"{identifier}"'


def _read_csv_rows(file_path: str) -> Tuple[List[str], List[Dict[str, str]]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    enc = _pick_encoding(path)
    with path.open("r", encoding=enc, newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)
    return headers, rows


def ingest_csv_to_postgres(file_path: str, schema_name: str, table_name: str = "records") -> Dict[str, Any]:
    headers, rows = _read_csv_rows(file_path)
    if not headers:
        raise ValueError("Input CSV has no headers.")

    original_cols = headers
    normalized_cols: List[str] = []
    used = set()
    for idx, col in enumerate(original_cols):
        base = sanitize_identifier(col, fallback=f"col_{idx+1}")
        candidate = base
        suffix = 1
        while candidate in used:
            suffix += 1
            candidate = f"{base}_{suffix}"
        used.add(candidate)
        normalized_cols.append(candidate)

    values_by_col: Dict[str, List[str]] = {c: [] for c in original_cols}
    for row in rows[:1000]:
        for c in original_cols:
            values_by_col[c].append(row.get(c, ""))

    pg_types: List[str] = [infer_column_type(values_by_col[c]) for c in original_cols]
    mapping = list(zip(original_cols, normalized_cols, pg_types))

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(schema_name)}")
            cur.execute(f"DROP TABLE IF EXISTS {_quote(schema_name)}.{_quote(table_name)}")
            col_defs = ", ".join(f"{_quote(dst)} {pg_t}" for _, dst, pg_t in mapping)
            cur.execute(f"CREATE TABLE {_quote(schema_name)}.{_quote(table_name)} ({col_defs})")

            col_list = ", ".join(_quote(dst) for _, dst, _ in mapping)
            placeholders = ", ".join(["%s"] * len(mapping))
            insert_sql = (
                f"INSERT INTO {_quote(schema_name)}.{_quote(table_name)} "
                f"({col_list}) VALUES ({placeholders})"
            )

            batch: List[Tuple[Any, ...]] = []
            coerced_nulls = 0
            inserted = 0
            for row in rows:
                parsed_row: List[Any] = []
                for src, _, pg_t in mapping:
                    parsed, coerced = _parse_value(row.get(src, ""), pg_t)
                    if coerced:
                        coerced_nulls += 1
                    parsed_row.append(parsed)
                batch.append(tuple(parsed_row))
                if len(batch) >= 1000:
                    cur.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch = []
            if batch:
                cur.executemany(insert_sql, batch)
                inserted += len(batch)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "schema_name": schema_name,
        "table_name": table_name,
        "file_path": file_path,
        "row_count_input": len(rows),
        "row_count_inserted": inserted,
        "coerced_nulls": coerced_nulls,
        "columns": [
            {"source_column": src, "column": dst, "data_type": pg_t}
            for src, dst, pg_t in mapping
        ],
    }

