import sqlite3
from pathlib import Path
from uuid import uuid4

from adapters.sqlite import SQLiteAdapter


def test_sqlite_adapter_introspect_and_execute():
    tmp_dir = Path("data/processed")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_dir / f"test_sqlite_adapter_{uuid4().hex}.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, country TEXT, amount REAL, event_date TEXT)")
        cur.execute("INSERT INTO records(country, amount, event_date) VALUES ('A', 10.0, '2024-01-01')")
        cur.execute("INSERT INTO records(country, amount, event_date) VALUES ('B', 20.0, '2024-01-02')")
        conn.commit()
    finally:
        conn.close()

    adapter = SQLiteAdapter(source_config={"db_path": str(db_path)})
    meta = adapter.introspect_schema()
    assert meta["source"]["db_engine"] == "sqlite"
    assert meta["profile"]["table_count"] == 1
    assert meta["tables"][0]["table_name"] == "records"

    rows = adapter.execute_select("SELECT country, amount FROM records ORDER BY amount DESC", row_limit=1, timeout_ms=1000)
    assert rows == [{"country": "B", "amount": 20.0}]
    db_path.unlink(missing_ok=True)
