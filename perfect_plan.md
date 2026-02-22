# Perfect Plan
## Trustworthy Analytics Assistant: Execution Blueprint

## 1. Vision and Boundaries
Build a production-grade **schema-aware analytics assistant** that answers natural-language questions using SQL, but with explicit boundaries:
- It is an assistant, not an autonomous decision-maker.
- It prioritizes correctness and traceability over broad unsupported coverage.
- It uses a **single local LLM** (Ollama) by default for operational simplicity.

## 2. What Already Exists (Strong Baseline)
From `final_plan.md`, the project already has MVP implementations of:
1. ETL foundation and warehouse validation.
2. Schema introspection and semantic mapping.
3. Structured planning and SQL generation.
4. SQL safety guardrails and execution path.
5. Retry/repair loop and evaluation endpoints.
6. Mining snapshots (trend and segmentation).
7. Adapter architecture (`postgres`, `sqlite`, `mysql`).
8. Metadata persistence, caching, and query traces.

This plan focuses on closing trust and correctness gaps from `flaw-correction.md`.

## 3. Core Problems to Solve
1. Semantic correctness is not guaranteed.
2. Heuristic semantic mapping is too weak for messy schemas.
3. SQL repair improves execution, not necessarily analytical meaning.
4. Mining is useful but currently over-positioned without strong statistical controls.
5. Operational metrics exist, but answer correctness metrics are insufficient.
6. Scale claims are not yet proven with repeatable SLAs.

## 4. Non-Negotiable Product Principles
1. No silent guesses on ambiguous semantics.
2. No high-confidence answer without grounded evidence and checks.
3. No release without benchmarked correctness.
4. No marketing claim without measured proof.

## 5. Target End-State Architecture
User Query  
-> API Orchestrator  
-> Planner (LLM, schema + contract aware)  
-> Plan Validator  
-> SQL Generator (LLM + deterministic fallback)  
-> SQL Safety Validator  
-> Executor  
-> Analytical Guardrails  
-> Evaluator + Trace Logger  
-> Structured Report + Confidence Tier + Traceability

Control Plane:
- Schema Introspector
- Semantic Mapper (bootstrap)
- Semantic Contract Store (authority)
- Metadata/Cache/Trace Store
- Adapter Layer per DB engine
- Evaluation Harness (gold correctness + ops metrics)

## 6. Preferred Delivery Plan (12 Weeks)

## Stage A: Trustworthiness Foundation (Weeks 1-4)
Goal: eliminate confident-but-wrong behavior caused by weak semantics.

### A1. Semantic Contract Authority
Implementation:
1. Add dataset-level semantic contract artifact:
   - entities, measures, time columns, valid grains, join paths, metric definitions.
2. Add contract validation API:
   - `POST /dataset/{dataset_id}/semantic/validate`.
3. Enforce planner and SQL generator to use contract first.
4. Keep semantic mapper as bootstrap assistant only.

Deliverables:
1. `metadata/semantic_contracts/<dataset_id>.json` schema.
2. Contract validator + service integration.
3. Contract-aware planning hooks.

Exit Criteria:
1. No `high` confidence answers without valid contract.
2. Ambiguous requests trigger clarification or controlled refusal.

### A2. Confidence and Transparency Layer
Implementation:
1. Add response fields:
   - `confidence_tier` (`high|medium|low`)
   - `semantic_warnings`
   - `correctness_checks`
   - `blocked_by_guardrail`
2. Add strict fallback policy for low confidence.

Deliverables:
1. Schema updates in `api/schemas.py` and `api/report_schema.py`.
2. Routing and response integration in `api/routes.py`.

Exit Criteria:
1. 100% analyze/report responses include confidence and warnings.

### A3. Positioning and Documentation Hardening
Implementation:
1. Replace overclaims with bounded, evidence-backed statements.
2. Reframe mining as exploratory analytics.

Deliverables:
1. Updated `README.md` and docs limitations section.

Exit Criteria:
1. No "fully autonomous" or unsupported claim language remains.

---

## Stage B: Analytical Correctness (Weeks 5-8)
Goal: ensure SQL results are not just executable, but analytically credible.

### B1. Analytical Guardrails Engine
Implementation:
1. Add post-execution checks:
   - join fan-out inflation,
   - duplicate aggregation risks,
   - grain mismatch,
   - null/key coverage anomalies.
2. Severity model: `block`, `warn`, `info`.

Deliverables:
1. New guardrails module (for example `agent/correctness_guardrails.py`).
2. Guardrail output appended to traces and API payloads.

Exit Criteria:
1. Known fan-out/grain regression cases are blocked or warned 100% of the time.

### B2. Repair Loop Separation
Implementation:
1. Split retries into:
   - `execution_repair` (syntax/object/type/runtime),
   - `semantic_repair` (grain/join/metric meaning).
2. Add hard stop when semantic mismatch remains unresolved.

Deliverables:
1. Enhanced classifier and retry policy.
2. Trace fields: `repair_type`, `repair_reason`, `semantic_repair_attempted`.

Exit Criteria:
1. 100% repaired queries have machine-readable rationale.
2. Execution-only repair cannot silently change metric meaning.

### B3. Gold Correctness Benchmarks
Implementation:
1. Build gold sets for at least 3 datasets.
2. Measure:
   - answer accuracy,
   - semantic intent match,
   - contradiction rate.

Deliverables:
1. `evaluation/gold/` packs.
2. Extended metrics report including correctness section.

Exit Criteria:
1. Accuracy reported in every evaluation run.
2. CI gate fails below threshold.

---

## Stage C: Scale, Reliability, and Governance (Weeks 9-12)
Goal: make support claims operationally verifiable.

### C1. Scale Modes + Freshness SLAs
Implementation:
1. Define modes:
   - `small`: live query path,
   - `medium`: cache + snapshot acceleration,
   - `large`: curated materialized aggregates.
2. Track freshness:
   - source max timestamp,
   - SLA window,
   - staleness indicator.

Deliverables:
1. Scale mode policy doc.
2. Freshness metadata in responses/reports.

Exit Criteria:
1. Published P95 latency and freshness metrics per mode.

### C2. Ops Hardening
Implementation:
1. Add retention/partition policy for trace tables.
2. Add backend guidance:
   - Postgres metadata backend required for multi-user concurrency.
3. Add load test for trace ingestion and API stability.

Deliverables:
1. `docs/ops_runbook.md`.
2. Maintenance scripts for metadata and traces.

Exit Criteria:
1. Stable performance under expected concurrent load.

### C3. Governance Package
Implementation:
1. Publish supported query classes and known failure modes.
2. Publish human escalation policy for low-confidence outputs.

Deliverables:
1. `docs/governance.md`.
2. `docs/limitations.md`.

Exit Criteria:
1. Every major claim in docs points to measured evidence.

## 7. Release Gates (Hard Blockers)
1. Critical SQL safety violations: `0`.
2. Critical guardrail misses: `0`.
3. Gold benchmark answer accuracy: `>= 0.85` initial target.
4. Confidence + traceability coverage: `100%`.
5. Retry transparency coverage: `100%`.
6. Published P95 latency and freshness for all supported modes.

## 8. First 14-Day Action Plan
1. Add semantic contract schema + storage + validator endpoint.
2. Add confidence and guardrail fields to analyze/report models.
3. Implement first guardrail checks (fan-out and grain mismatch).
4. Extend query traces with repair and guardrail metadata.
5. Create first gold benchmark pack and CI threshold check.
6. Update README and mining terminology to match bounded scope.

## 9. Risks and Mitigations
1. Risk: contract authoring overhead.
   Mitigation: assisted contract draft + human approval workflow.
2. Risk: guardrails may over-warn initially.
   Mitigation: severity tuning and per-dataset thresholds.
3. Risk: correctness benchmarks take time to curate.
   Mitigation: start with three canonical datasets, expand iteratively.

## 10. Definition of Done
The project is considered complete when it is defensible as a **trustworthy bounded analytics assistant**:
1. Safe by construction.
2. Explicit about uncertainty.
3. Measurably correct on benchmarked tasks.
4. Transparent in evidence, confidence, and failure reporting.
