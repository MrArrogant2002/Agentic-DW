import csv
from pathlib import Path
from typing import Dict, Iterator


def _pick_encoding(path: Path) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            path.read_text(encoding=encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def extract_rows(input_csv: str) -> Iterator[Dict[str, str]]:
    path = Path(input_csv)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    encoding = _pick_encoding(path)
    with path.open("r", encoding=encoding, newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row
