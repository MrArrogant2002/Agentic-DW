import argparse
from pathlib import Path

from schema.introspector.db import connect
from utils.env_loader import load_environments


def apply_sql_file(sql_path: Path) -> None:
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply metadata migration SQL after loading .env variables.")
    parser.add_argument(
        "--sql",
        default="migrations/001_agent_metadata.sql",
        help="Path to SQL migration file.",
    )
    args = parser.parse_args()

    load_environments()
    apply_sql_file(Path(args.sql))
    print('{"status":"ok","message":"Metadata migration SQL applied."}')
