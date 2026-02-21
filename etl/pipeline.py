import argparse
import os
from pathlib import Path

from load import load_to_postgres
from transform import transform_csv


def load_env_file(env_path: str = ".env") -> None:
    env_file = Path(env_path)
    if not env_file.exists():
        return

    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def run_pipeline(input_csv: str, processed_dir: str) -> None:
    load_env_file()

    output_dir = Path(processed_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    clean_csv = str(output_dir / "clean_sales.csv")
    rejected_csv = str(output_dir / "rejected_sales.csv")

    stats = transform_csv(
        input_csv=input_csv,
        clean_output_csv=clean_csv,
        rejected_output_csv=rejected_csv,
    )

    print("=== Transform Summary ===")
    print(f"total_rows={stats.total_rows}")
    print(f"kept_rows={stats.kept_rows}")
    print(f"dropped_null_customer={stats.dropped_null_customer}")
    print(f"dropped_nonpositive_quantity={stats.dropped_nonpositive_quantity}")
    print(f"dropped_nonpositive_price={stats.dropped_nonpositive_price}")
    print(f"dropped_bad_datetime={stats.dropped_bad_datetime}")
    print(f"clean_output={clean_csv}")
    print(f"rejected_output={rejected_csv}")

    load_counts = load_to_postgres(clean_csv)
    print("=== Load Summary ===")
    for table_name, count in load_counts.items():
        print(f"{table_name}={count}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run end-to-end ETL pipeline.")
    parser.add_argument("--input", default="data.csv", help="Path to source CSV.")
    parser.add_argument("--processed-dir", default="data/processed", help="Directory for ETL outputs.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(input_csv=args.input, processed_dir=args.processed_dir)
