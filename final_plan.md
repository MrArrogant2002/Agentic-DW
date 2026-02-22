# Final Project Plan
## Design and Implementation of an Autonomous SQL Agent for Data Warehouse Analytics and Pattern Discovery

## 1. Project Goal
Build a production-style autonomous analytics agent that can:
1. Understand natural-language analytical questions.
2. Introspect arbitrary SQL database schemas.
3. Plan and generate safe SQL dynamically.
4. Execute queries with strict guardrails.
5. Run mining workflows (trend/segmentation) on scoped data.
6. Generate grounded, evidence-linked insights.
7. Support multiple SQL engines via adapter architecture.

## 2. Target End-State Architecture
User Query -> API -> Planner (LLM) -> Schema-Aware Plan -> SQL Generator (LLM + Schema Context) -> Safe Executor -> Evaluator + Self-Healing -> Mining Engine -> Grounded Insight Generator -> Structured Report

Supporting Control Plane:
Schema Introspector -> Semantic Mapper -> Metadata Store/Cache -> Adapter Layer (Postgres/MySQL/SQLite)

## 3. Phase-Wise Implementation Plan (Updated)

### Phase 1: Data Foundation and Warehouse Reliability
Objective:
Establish a clean, reproducible warehouse foundation.

Implementation:
1. Profile and clean source datasets.
2. Define warehouse schema with constraints/indexes.
3. Build idempotent ETL with logging and rejects.

Deliverables:
1. Warehouse schema and ETL pipeline.
2. Data quality summary.
3. Reproducible load command.

Exit Criteria:
1. Re-runs produce consistent row counts.
2. Core validation SQL matches expected outputs.

### Phase 2: Analytical Baseline and Performance Verification
Objective:
Lock correctness and performance before autonomous behavior.

Implementation:
1. Validation query pack (revenue, top entities, trends).
2. `EXPLAIN ANALYZE` and index verification.
3. Baseline latency measurements.

Deliverables:
1. Validation SQL suite.
2. Benchmark report.

Exit Criteria:
1. Correctness checks pass.
2. Latency is stable and documented.

### Phase 3: Schema Introspector Layer (Per DB)
Objective:
Make the system discover and understand unknown schemas.

Implementation:
1. Implement introspection per adapter:
   - Tables, columns, data types.
   - Primary keys, foreign keys.
   - Row counts per table.
2. Build normalized metadata object:
   - `entities` (candidate grouping dimensions)
   - `measures` (candidate numeric aggregations)
   - `time_columns` (date/timestamp candidates)
   - `relationships` (join graph)

Deliverables:
1. `schema_introspector` modules per DB adapter.
2. Normalized schema metadata JSON.

Exit Criteria:
1. Any supported DB yields normalized metadata.
2. Join graph is machine-usable for SQL generation.

### Phase 4: Semantic Mapping Layer
Objective:
Map raw schema to business-usable analytical roles.

Implementation:
1. Convert schema metadata to semantic roles:
   - Entity candidates (`country`, `customer`, `product`, etc.).
   - Value candidates (`amount`, `revenue`, `price*qty`, etc.).
   - Time candidates (`date`, `created_at`, etc.).
2. Score candidates using heuristics:
   - Naming signals.
   - Data type compatibility.
   - Cardinality/distribution signals.
3. Produce ranked semantic map for planner/builder.

Deliverables:
1. Semantic map output with confidence scores.
2. Heuristic scoring policy.

Exit Criteria:
1. Planner can request entity/value/time from semantic map.
2. Misclassification rate is measurable.

### Phase 5: Structured Planner (LLM-Only)
Objective:
Move from flat intent classification to executable analytical plans.

Implementation:
1. Planner returns structured plan JSON, not just intent.
2. Required plan fields:
   - `task_type`
   - `entity_scope` (e.g., `top_n`, `all`)
   - `entity_dimension`
   - `n`
   - `metric`
   - `time_grain`
   - `compare_against`
3. Validate plan schema before downstream execution.

Deliverables:
1. Planner output contract and validator.
2. Plan trace logs.

Exit Criteria:
1. Planner outputs valid structured plans consistently.
2. Invalid plans are rejected and retried safely.

### Phase 6: Dynamic SQL Builder + Safe Executor
Objective:
Generate SQL from structured plans using schema graph.

Implementation:
1. Build SQL generation pipeline with two modes:
   - LLM SQL generation from schema context.
   - Deterministic SQL templates/AST fallback for constrained tasks.
2. Provide compact schema context to LLM:
   - Allowed tables/columns.
   - Join graph and key paths.
   - Semantic map selections (entity, metric, time).
3. Resolve joins from relationship graph.
4. Apply hard guardrails:
   - SELECT-only
   - Single statement
   - LIMIT policy
   - Timeout policy
   - Allowlist tables/columns from introspection metadata

Deliverables:
1. Plan-to-SQL compiler with schema-aware LLM mode.
2. Safety validator integrated before execution.
3. SQL generation prompt contract and output parser.

Exit Criteria:
1. SQL generated for unseen supported schemas.
2. Unsafe SQL blocked deterministically.
3. LLM-generated SQL uses only allowlisted identifiers.

### Phase 7: Validation + Self-Healing Loop
Objective:
Recover from SQL generation/runtime errors autonomously.

Implementation:
1. Classify failures:
   - Missing column/table.
   - Wrong/ambiguous join.
   - Type mismatch.
   - Empty/invalid granularity output.
2. Regenerate SQL using:
   - DB error details.
   - Schema metadata.
   - Previous failed SQL.
3. Use controlled repair prompt for LLM SQL regeneration.
4. Limit retries (e.g., max 2).

Deliverables:
1. Error taxonomy and retry controller.
2. Regeneration prompts/workflow.

Exit Criteria:
1. Controlled retry behavior with bounded attempts.
2. Improved success rate vs no-healing baseline.

### Phase 8: Domain-Agnostic Mining Interface
Objective:
Decouple mining algorithms from dataset-specific feature logic.

Implementation:
1. Define feature-builder contract:
   - `feature_builder(schema, plan) -> dataframe`
2. Keep mining algorithms generic:
   - Trend analysis.
   - Clustering/segmentation.
3. Add optional domain packs for higher quality mappings.

Deliverables:
1. Generic mining interface.
2. Pluggable feature builders/domain packs.

Exit Criteria:
1. Same mining pipeline runs on different schemas with mapped features.
2. Domain packs improve quality without changing core engine.

### Phase 9: Grounded Insight Generator
Objective:
Ensure narratives are evidence-backed and non-hallucinated.

Implementation:
1. LLM input restricted to:
   - Structured plan.
   - Computed outputs.
   - Evidence keys only.
2. Enforce claim grounding:
   - Every claim maps to an evidence key/path.
   - Reject output if ungrounded.
3. Deterministic fallback report if validation fails.

Deliverables:
1. Insight schema and validation gate.
2. Traceability map for each report claim.

Exit Criteria:
1. No report claim without evidence mapping.
2. Fallback path keeps endpoint reliable.

### Phase 10: Adapter Architecture for Multi-DB
Objective:
Support multiple SQL engines with one orchestration flow.

Implementation:
1. Implement adapter interface:
   - `adapters/base.py`
   - `adapters/postgres.py`
   - `adapters/mysql.py`
   - `adapters/sqlite.py`
2. Each adapter handles:
   - Introspection queries.
   - Dialect-specific SQL rendering.
   - Execution and type normalization.

Deliverables:
1. Adapter framework + concrete adapters.
2. Dialect compatibility tests.

Exit Criteria:
1. Same user query flow works across supported engines.
2. Output parity is acceptable across adapters.

### Phase 11: Metadata Persistence and Caching
Objective:
Improve performance and stability with persistent metadata.

Implementation:
1. Persist schema snapshots by data source.
2. Refresh snapshots only when schema hash changes.
3. Cache common plan-to-SQL patterns.

Deliverables:
1. Metadata store with schema versioning.
2. Cache policies and invalidation strategy.

Exit Criteria:
1. Reduced planning latency on repeated workloads.
2. Correct invalidation on schema changes.

### Phase 12: Evaluation and “Any Dataset” Claim Validation
Objective:
Measure true autonomy and portability.

Implementation:
1. Evaluate on at least 3 different datasets/schemas.
2. Track metrics:
   - Planning accuracy
   - SQL execution success
   - Answer groundedness
   - Latency
   - Retry rate
3. Publish failure analysis and limits.

Deliverables:
1. Evaluation report across datasets.
2. Quantified portability claims.

Exit Criteria:
1. Metrics meet predefined acceptance thresholds.
2. Claims are evidence-backed and reproducible.

## 4. Updated Repository Structure
```text
project/
  adapters/
    base.py
    postgres.py
    mysql.py
    sqlite.py
  metadata/
    schema_cache/
    semantic_maps/
  etl/
  agent/
    planner.py
    sql_generator.py
    executor.py
    evaluator.py
    insight_generator.py
    insight_llm.py
  schema/
    introspector/
    semantic_mapper/
  mining/
    trend.py
    rfm.py
    clustering.py
    snapshots.py
  api/
    main.py
    routes.py
    schemas.py
    report_schema.py
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
1. Core ETL and warehouse baseline are implemented.
2. Safe SQL API flow is implemented.
3. Mining snapshot caching and structured report endpoint are implemented.
4. Ollama-powered planner and optional Ollama-powered insight narration are integrated.

Immediate Next Step:
Start implementing Phase 3 and Phase 4 in code:
1. Build `schema/introspector` for PostgreSQL first.
2. Build semantic mapper with entity/value/time scoring.
3. Add `agent/sql_llm_generator.py` to generate SQL from schema context.
4. Wire guarded execution + self-healing retries for generated SQL.
