# Perfect Plan: Trustworthy Schema-Aware Analytics Assistant

## 1. Mission
Build a production-ready **analytics assistant** that can answer natural-language analytical questions across supported SQL schemas with:
- strict SQL safety,
- explicit semantic governance,
- measurable analytical correctness,
- transparent confidence and traceability.

This plan keeps the strong architecture already built and closes the critical gaps identified in `flaw-correction.md`.

## 2. Product Positioning (Final)
- What this is: a **bounded, schema-aware analytics assistant** with guarded automation.
- What this is not: a fully autonomous, universally correct analytics engine.
- Default operating mode: **single local LLM** (Ollama model from `OLLAMA_MODEL`) for planner + SQL + optional insights.

## 3. Current Baseline (Already Implemented)
From `final_plan.md`, these pillars already exist at MVP level:
- ETL and warehouse validation flow.
- FastAPI orchestration (`/analyze`, onboarding, mining, evaluation APIs).
- Schema introspection + semantic mapping.
- Structured planning and SQL generation with repair loop.
- SQL guardrails (SELECT-only, single statement, timeout, limit).
- Adapter layer (`postgres`, `sqlite`, `mysql`).
- Mining snapshots (trend + segmentation).
- Metadata/cache + query traces.
- Evaluation metrics and failure analytics endpoints.

## 4. Critical Gaps to Close
1. Semantic correctness is not guaranteed.
2. Semantic mapping is too heuristic without governed contracts.
3. Execution repair can hide semantic drift.
4. Mining claims are stronger than current statistical rigor.
5. Scale claims are not yet operationally proven.
6. Metrics emphasize execution health more than answer correctness.

## 5. North-Star Success Criteria
Release is successful only when all are true:
1. **Safety:** 0 critical unsafe SQL escapes in regression suite.
2. **Correctness:** >= 0.85 answer accuracy on gold benchmarks (initial target).
3. **Transparency:** 100% responses include confidence tier + traceability.
4. **Guardrails:** 100% known fan-out/grain regression cases blocked or flagged.
5. **Reliability:** stable P95 latency and freshness metrics published by scale mode.
6. **Claims discipline:** docs contain no unsupported "fully autonomous" language.

## 6. Delivery Strategy (12 Weeks, 3 Stages)

## Stage A (Weeks 1-4): Trustworthiness Foundation
Objective: prevent confident wrong answers and remove ambiguity in system claims.

### Workstream A1: Semantic Contract Layer
- Add dataset-scoped semantic contract artifact:
  - entities, measures, time columns, approved grains, join paths, metric definitions.
- Add validator endpoint: `/dataset/{dataset_id}/semantic/validate`.
- Planner and SQL generator must consult contract first; heuristics become bootstrap fallback.

Deliverables:
- `metadata/semantic_contracts/<dataset_id>.json` + schema validator.
- Contract-aware hooks in planning and SQL generation.

Exit Criteria:
- No high-confidence response without valid semantic contract.

### Workstream A2: Confidence and Clarification Policy
- Add response fields:
  - `confidence_tier` (`high|medium|low`)
  - `semantic_warnings`
  - `blocked_by_guardrail`
- Enforce low-confidence behavior:
  - ask for clarification, or
  - return insufficient grounding response.

Deliverables:
- Updates in `api/schemas.py`, `api/report_schema.py`, `api/routes.py`.

Exit Criteria:
- 100% `/analyze` and `/analyze/report` responses include confidence metadata.

### Workstream A3: Claims and Terminology Hardening
- Align README/docs to "analytics assistant" terminology.
- Reframe mining as "exploratory analytics modules."

Deliverables:
- Updated `README.md` and docs wording.

Exit Criteria:
- No conflicting/autonomy-overclaim text remains.

---

## Stage B (Weeks 5-8): Analytical Correctness and Evidence Quality
Objective: move from execution success to answer correctness assurance.

### Workstream B1: Analytical Guardrails Engine
- Implement post-SQL correctness checks:
  - join fan-out inflation,
  - duplicate aggregation risk,
  - grain mismatch,
  - null/key coverage anomalies.
- Severity model: `block`, `warn`, `info`.

Deliverables:
- New guardrail module (for example `agent/correctness_guardrails.py`).
- Guardrail outcomes appended to traces and API responses.

Exit Criteria:
- Known fan-out test cases are always blocked or warned.

### Workstream B2: Repair Loop Separation
- Split retry paths into:
  - `execution_repair`,
  - `semantic_repair`.
- Prevent silent metric/grain changes during execution-only repair.

Deliverables:
- Extended classifier in `agent/sql_llm_generator.py` + route policy in `api/routes.py`.
- Query trace fields: `repair_type`, `repair_reason`, `semantic_repair_attempted`.

Exit Criteria:
- 100% repaired queries emit machine-readable repair rationale.

### Workstream B3: Gold Benchmark Evaluation
- Create benchmark packs under `evaluation/gold/` for at least 3 datasets.
- Track:
  - answer accuracy,
  - semantic intent match,
  - contradiction rate,
  - operational metrics (existing).

Deliverables:
- Benchmark runner extension and reports in `docs/`.
- Threshold gates integrated in CI.

Exit Criteria:
- Accuracy and intent metrics published in every evaluation run.

---

## Stage C (Weeks 9-12): Scale, Operations, and Governance
Objective: make supported scale claims operational and auditable.

### Workstream C1: Scale Modes + Freshness SLAs
- Define supported modes:
  - `small` (live query),
  - `medium` (cache + snapshot acceleration),
  - `large` (curated aggregates/materialized views).
- Add freshness metadata and SLA tracking in responses/reports.

Deliverables:
- Scale-mode policy doc.
- Freshness fields in snapshot/report payloads.

Exit Criteria:
- Published P95 latency + freshness by mode.

### Workstream C2: Metadata and Trace Ops Hardening
- Add retention/partition strategy for trace and metadata tables.
- Validate concurrency behavior for file fallback mode or strongly recommend Postgres backend for multi-user use.

Deliverables:
- Operational runbook in `docs/`.
- Migration/maintenance scripts.

Exit Criteria:
- High-volume trace ingestion remains stable under load test.

### Workstream C3: Final Governance Package
- Publish:
  - supported query classes,
  - known limitations,
  - escalation policy for low-confidence answers.

Deliverables:
- `docs/governance.md`, `docs/limitations.md`, updated evaluation summary.

Exit Criteria:
- All product claims are evidence-backed.

## 7. Cross-Cutting Technical Decisions
1. **Single-LLM mode stays default** for now (local operational simplicity).
2. Deterministic fallbacks remain mandatory when LLM outputs fail validation.
3. Human-in-the-loop is required for semantic contract approval and low-confidence ambiguity.
4. Mining output must carry signal quality and caveats.

## 8. Release Gates (Blocking)
1. Guardrail critical-miss rate: `0`.
2. Benchmark answer accuracy: `>= 0.85` initial target.
3. Confidence + traceability presence: `100%`.
4. Retry transparency: `100%` of retries categorized.
5. Performance/freshness metrics published for each supported scale mode.

## 9. Immediate 2-Week Execution Backlog
1. Add semantic contract schema, storage, and validator endpoint.
2. Add confidence and semantic warning fields to analyze/report schemas.
3. Implement first version of analytical guardrails (fan-out + grain checks).
4. Extend query traces with repair and guardrail metadata.
5. Create first gold benchmark pack for existing retail dataset.
6. Rewrite README positioning and mining terminology.

## 10. Definition of Done
Project is "done" when it is credibly defensible as a **trustworthy bounded analytics assistant**:
- safe by construction,
- explicit about uncertainty,
- measurable on correctness,
- and transparent about limits and evidence.
