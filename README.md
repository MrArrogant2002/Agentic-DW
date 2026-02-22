# Autonomous SQL Agent Data Warehouse (Baseline)

This repository now includes a baseline implementation for:

- Data warehouse schema (`agentic_ai_db.sql`)
- Reproducible ETL pipeline (`etl/`)
- Validation SQL pack (`sql/validations/`)
- Phase 4 API baseline (`api/`, `agent/`)

## Repository Structure

```text
project/
  data/
    raw/
    processed/
  etl/
    extract.py
    transform.py
    load.py
    pipeline.py
  agent/
    prompts/
  mining/
  api/
  sql/
    validations/
    benchmarks/
  tests/
    unit/
    integration/
  docs/
  agentic_ai_db.sql
  data.csv
  final_plan.md
```

## ETL Cleaning Rules

Rows are accepted only if all rules pass:

- `CustomerID` is present
- `Quantity > 0`
- `UnitPrice > 0`
- `InvoiceDate` parses using format `M/d/yyyy H:mm`

## Python Requirements

- Python 3.10+
- `psycopg` (recommended) or `psycopg2`

Install one PostgreSQL driver:

```bash
pip install psycopg[binary]
```

or

```bash
pip install psycopg2-binary
```

Install API dependencies:

```bash
pip install -r requirements.txt
```

## Database Setup

Run the schema before the first load:

```sql
\i agentic_ai_db.sql
```

## Run ETL

Set DB environment variables in either way:

- `DB_HOST` (required)
- `DB_PORT` (default: `5432`)
- `DB_NAME` (required)
- `DB_USER` (required)
- `DB_PASSWORD` (required)

Option A:
- Add them in `.env` at project root (auto-loaded by `etl/pipeline.py`)

Option B:
- Export them in your shell session

Run pipeline:

```bash
python etl/pipeline.py --input data.csv --processed-dir data/processed
```

Outputs:

- `data/processed/clean_sales.csv`
- `data/processed/rejected_sales.csv`
- Console ETL metrics and load counts

## Validation Queries

Run:

```sql
\i sql/validations/01_row_count_and_revenue.sql
\i sql/validations/02_dimension_counts.sql
\i sql/validations/03_data_quality_assertions.sql
\i sql/validations/04_top_countries_revenue.sql
\i sql/validations/05_monthly_revenue_check.sql
```

Expected values for the current `data.csv` are documented in:

- `sql/validations/EXPECTED_OUTPUTS.md`

## Phase 4 API (Baseline)

Run API:

```bash
uvicorn api.main:app --reload
```

Ollama planner configuration (required):

- `OLLAMA_PLANNER_ENABLED=1`
- `OLLAMA_MODEL=mistral:latest`
- `OLLAMA_BASE_URL=http://localhost:11434`
- `OLLAMA_TIMEOUT_SEC=20`

Planner is Ollama-only in the current implementation. If Ollama is unavailable or disabled, `/analyze` returns an error.

Endpoints:

- `GET /health`
- `POST /analyze`
- `POST /analyze/debug`
- `POST /analyze/report`
- `POST /mining/refresh`

`/analyze` response includes:

- `planner_source` (`ollama`)
- `retries_used` (0 or 1)

Structured report endpoint:

```bash
curl -X POST "http://127.0.0.1:8000/analyze/report" \
  -H "Content-Type: application/json" \
  -d "{\"question\":\"show trend analysis\"}"
```

Optional Ollama-based insight narration for `/analyze/report`:

- `INSIGHT_MODEL_ENABLED=1`
- `INSIGHT_MODEL=mistral:latest` (or reuse `OLLAMA_MODEL`)
- `INSIGHT_MODEL_BASE_URL=http://localhost:11434` (or reuse `OLLAMA_BASE_URL`)
- `INSIGHT_MODEL_TIMEOUT_SEC=20`

When enabled, LLM-generated insights are accepted only if evidence keys map to actual computed values; otherwise API falls back to deterministic insights.

Example request:

```bash
curl -X POST "http://127.0.0.1:8000/analyze" \
  -H "Content-Type: application/json" \
  -d "{\"question\":\"Top 5 countries by revenue\",\"row_limit\":5}"
```

## Phase 5 Mining Modules

Run trend analysis:

```bash
python -m mining.trend --pretty
```

Build RFM features:

```bash
python -m mining.rfm --pretty
```

Run clustering:

```bash
python -m mining.clustering --k 4 --pretty
```

Refresh mining snapshots (precompute/cache):

```bash
python -m mining.snapshots --all --pretty
```

Read one snapshot (auto-refresh if stale):

```bash
python -m mining.snapshots --type trend_analysis --pretty
python -m mining.snapshots --type customer_segmentation --pretty
```

Snapshot cache behavior in API:

- For mining intents (`trend_analysis`, `customer_segmentation`), `/analyze` serves from `mining_snapshots`.
- If snapshot is missing or stale, API recomputes and updates snapshot automatically.
- Staleness TTL can be configured with `MINING_SNAPSHOT_TTL_HOURS` (default: `24`).
- Each snapshot includes `snapshot_version` and `run_id` for traceability.

Refresh snapshots from API:

```bash
curl -X POST "http://127.0.0.1:8000/mining/refresh" -H "Content-Type: application/json" -d "{\"refresh_all\":true}"
curl -X POST "http://127.0.0.1:8000/mining/refresh" -H "Content-Type: application/json" -d "{\"snapshot_type\":\"trend_analysis\",\"refresh_all\":false}"
```
