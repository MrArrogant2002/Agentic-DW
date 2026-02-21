# Final Project Plan
## Design and Implementation of an Autonomous SQL Agent for Data Warehouse Analytics and Pattern Discovery

## 1. Project Goal
Build an end-to-end autonomous analytics system that:
1. Accepts natural language business questions.
2. Translates them into safe analytical SQL over a retail data warehouse.
3. Executes and evaluates results.
4. Runs algorithmic pattern mining (trend analysis and customer segmentation).
5. Produces structured, business-readable insights.

## 2. Target Architecture
User Query -> FastAPI -> Planner Agent -> SQL Generator -> Safe SQL Executor -> Result Evaluator -> Pattern Mining Engine -> Insight Generator -> Structured Report

## 3. Phase-Wise Implementation Plan

### Phase 1: Data Engineering Foundation
Objective:
Create a reliable analytical warehouse based on the Online Retail dataset.

Implementation Description:
1. Profile the source dataset and define data quality rules.
2. Clean invalid records (null IDs, non-positive quantity or price where required, malformed dates).
3. Design star schema with one fact table and supporting dimensions.
4. Implement schema in PostgreSQL with constraints and foreign keys.
5. Create indexes for analytical filters and joins.

Deliverables:
1. Clean dataset definition and cleaning rules.
2. Star schema and ER diagram.
3. Created PostgreSQL tables and indexes.

Exit Criteria:
1. Fact and dimension tables are queryable with valid relationships.
2. Basic aggregate queries run without integrity issues.

Status:
Core schema setup is completed in `agentic_ai_db`.

### Phase 2: Reproducible ETL Pipeline
Objective:
Automate data ingestion and transformation so warehouse population is repeatable.

Implementation Description:
1. Build ETL modules:
   - `etl/extract.py` for raw CSV loading.
   - `etl/transform.py` for cleaning and dimension/fact shaping.
   - `etl/load.py` for PostgreSQL inserts or bulk loads.
   - `etl/pipeline.py` for orchestration.
2. Separate `data/raw` and `data/processed` artifacts.
3. Add idempotency strategy (truncate-and-reload or upsert with keys).
4. Add logging for row counts, rejects, and run duration.
5. Add exception handling and failure-safe rollback behavior.

Deliverables:
1. End-to-end ETL script(s).
2. Re-runnable warehouse loading process.
3. ETL run logs and data quality summary.

Exit Criteria:
1. A single command rebuilds warehouse tables from raw input.
2. Re-running ETL does not produce duplicate facts.

### Phase 3: Analytical SQL Validation Layer
Objective:
Validate warehouse correctness and performance before introducing agents.

Implementation Description:
1. Create a query pack for common analytics:
   - Monthly revenue.
   - Country-wise revenue.
   - Top customers.
   - Top products.
   - Seasonal trend summaries.
2. Run `EXPLAIN ANALYZE` to verify plan quality.
3. Validate index usage on key filters and joins.
4. Benchmark execution times and record baselines.

Deliverables:
1. SQL validation scripts.
2. Performance benchmark report.

Exit Criteria:
1. Query outputs are business-correct.
2. Core analytics execute within acceptable latency targets.

### Phase 4: Autonomous SQL Agent Core
Objective:
Implement safe natural language to SQL reasoning and execution.

Implementation Description:
1. Build FastAPI endpoints for analysis requests and health checks.
2. Implement `planner` module to decompose user intent into sub-tasks.
3. Implement `sql_generator` module with schema-aware prompt templates.
4. Implement `executor` module with hard guardrails:
   - SELECT-only policy.
   - Statement validation and denylist checks.
   - Timeout and row limits.
5. Implement `result_evaluator` module to detect SQL errors, empty results, and invalid granularity.
6. Add retry loop with controlled regeneration when evaluator flags issues.

Deliverables:
1. Working NL-to-SQL flow with multi-step planning.
2. Safe executor and evaluator pipeline.

Exit Criteria:
1. Common business queries run end-to-end from natural language input.
2. Unsafe SQL is blocked consistently.

### Phase 5: Pattern Mining Engine
Objective:
Add algorithmic intelligence beyond direct SQL retrieval.

Implementation Description:
1. Create `mining/trend.py`:
   - Aggregate monthly sales.
   - Fit linear regression.
   - Compute slope and trend direction.
2. Create `mining/rfm.py`:
   - Compute Recency, Frequency, Monetary features per customer.
   - Standardize features for clustering.
3. Create `mining/clustering.py`:
   - Run KMeans.
   - Evaluate cluster quality with silhouette score.
   - Generate interpretable cluster labels.
4. Add optional anomaly detection extension (z-score or residual-based).

Deliverables:
1. Trend analysis outputs with quantitative indicators.
2. Customer segments with quality metrics.

Exit Criteria:
1. Mining modules run deterministically from warehouse data.
2. Cluster quality is measurable and documented.

### Phase 6: Insight Generation and Structured Reporting
Objective:
Convert SQL and mining outputs into decision-ready business insights.

Implementation Description:
1. Define strict JSON schema for final response payload.
2. Build `insight_generator` prompt templates that explain results without fabricating math.
3. Merge:
   - Query context.
   - SQL outputs.
   - Trend and clustering outputs.
4. Produce concise narrative insights, risks, and recommended next actions.
5. Return machine-readable plus human-readable report sections.

Deliverables:
1. Structured report format.
2. Reliable insight narratives tied to computed evidence.

Exit Criteria:
1. Reports are consistent, traceable, and easy to consume.
2. Every narrative claim maps to computed values.

### Phase 7: Evaluation, Observability, and Hardening
Objective:
Demonstrate reliability, quality, and operational readiness.

Implementation Description:
1. Add metrics collection:
   - SQL generation success rate.
   - Execution latency.
   - Retry rate.
   - Mining runtime.
   - Clustering quality scores.
2. Add test suites:
   - Unit tests for ETL and mining logic.
   - Integration tests for FastAPI to executor flow.
   - Negative tests for SQL safety guardrails.
3. Add logging and trace IDs for request-level debugging.
4. Perform error-path validation and recovery tests.

Deliverables:
1. Evaluation report with metrics.
2. Test evidence and quality gates.

Exit Criteria:
1. Core metrics meet predefined thresholds.
2. Failure and recovery paths are validated.

### Phase 8: Documentation and Presentation Readiness
Objective:
Package the system into a complete academic and engineering deliverable.

Implementation Description:
1. Produce system documentation:
   - Architecture diagram.
   - ER diagram.
   - Sequence/flow diagram.
   - Module-level design notes.
2. Document algorithms and evaluation methodology.
3. Prepare reproducibility guide:
   - Setup steps.
   - Environment variables.
   - ETL and API run commands.
4. Compile final results, limitations, and future enhancements.
5. Build demo script and screenshots.

Deliverables:
1. Project report and slide deck artifacts.
2. Reproducible runbook for evaluators.

Exit Criteria:
1. Reviewer can reproduce and understand the full system.
2. Presentation clearly links architecture, implementation, and outcomes.

## 4. Recommended Repository Structure
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
    planner.py
    sql_generator.py
    executor.py
    evaluator.py
    prompts/
  mining/
    trend.py
    rfm.py
    clustering.py
  api/
    main.py
    routes.py
    schemas.py
  sql/
    validations/
    benchmarks/
  tests/
    unit/
    integration/
  docs/
    architecture.md
    evaluation.md
  final_plan.md
```

## 5. Current Position and Immediate Next Step
Current Position:
1. PostgreSQL installed.
2. Database created.
3. Star schema created with indexes.

Immediate Next Step:
Start Phase 2 by implementing the ETL pipeline and making warehouse loading fully reproducible.
