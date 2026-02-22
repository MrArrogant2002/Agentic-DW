from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from schema.introspector.db import connect
from utils.env_loader import load_environments


def _split_sql_statements(sql_text: str) -> List[str]:
    chunks = [c.strip() for c in sql_text.split(";")]
    return [c for c in chunks if c]


def _extract_execution_time(explain_lines: List[str]) -> float | None:
    for line in explain_lines:
        m = re.search(r"Execution Time:\s*([0-9\.]+)\s*ms", line, flags=re.IGNORECASE)
        if m:
            return float(m.group(1))
    return None


def run_postgres_benchmark(sql_file: Path) -> Dict[str, Any]:
    load_environments()
    sql_text = sql_file.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)

    conn = connect()
    results: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            for idx, stmt in enumerate(statements, start=1):
                try:
                    cur.execute(stmt)
                    rows = cur.fetchall()
                    explain_lines = [str(r[0]) for r in rows]
                    exec_ms = _extract_execution_time(explain_lines)
                    results.append(
                        {
                            "statement_index": idx,
                            "execution_time_ms": exec_ms,
                            "explain_output": explain_lines,
                            "status": "ok",
                        }
                    )
                except Exception as exc:
                    results.append(
                        {
                            "statement_index": idx,
                            "execution_time_ms": None,
                            "explain_output": [],
                            "status": "error",
                            "error": str(exc),
                        }
                    )
                    conn.rollback()
    finally:
        conn.close()

    valid = [r["execution_time_ms"] for r in results if isinstance(r.get("execution_time_ms"), float)]
    return {
        "sql_file": str(sql_file),
        "statement_count": len(results),
        "avg_execution_time_ms": round(sum(valid) / len(valid), 3) if valid else None,
        "max_execution_time_ms": round(max(valid), 3) if valid else None,
        "results": results,
    }


def _write_markdown_report(report: Dict[str, Any], out_path: Path) -> None:
    lines = []
    lines.append("# Benchmark Report")
    lines.append("")
    lines.append(f"- SQL file: `{report['sql_file']}`")
    lines.append(f"- Statements: `{report['statement_count']}`")
    lines.append(f"- Avg execution time (ms): `{report['avg_execution_time_ms']}`")
    lines.append(f"- Max execution time (ms): `{report['max_execution_time_ms']}`")
    lines.append("")
    lines.append("## Statement Details")
    lines.append("")
    for row in report["results"]:
        lines.append(f"### Statement {row['statement_index']}")
        lines.append(f"- Execution time (ms): `{row['execution_time_ms']}`")
        lines.append("```text")
        lines.extend(row["explain_output"])
        lines.append("```")
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run PostgreSQL EXPLAIN ANALYZE benchmark suite.")
    parser.add_argument("--sql-file", default="sql/benchmarks/01_postgres_explain.sql")
    parser.add_argument("--json-out", default="docs/benchmark_report.json")
    parser.add_argument("--md-out", default="docs/benchmark_report.md")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    report = run_postgres_benchmark(Path(args.sql_file))
    Path(args.json_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_markdown_report(report, Path(args.md_out))
    if args.pretty:
        print(json.dumps(report, indent=2))
    else:
        print(json.dumps(report))


if __name__ == "__main__":
    main()
