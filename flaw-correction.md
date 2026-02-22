# Flaw Correction Plan: From Prototype to Trustworthy Analytics System

## Goal
Transform this project from a strong prototype into a bounded, reliable analytics system that is explicit about limits, measurable in correctness, and safer against silent analytical errors.

## Guiding Principles
- Do not claim universal autonomy.
- Prefer constrained correctness over broad but fragile coverage.
- Separate execution safety from analytical correctness.
- Add human-governed semantics where inference is weak.
- Track answer quality, not just runtime success.

## Critique-to-Action Mapping

### 1) "Autonomous is an illusion"
**Plan**
- Reposition product language to: "Schema-aware analytics assistant with guarded automation."
- Introduce confidence tiers:
  - `high`: fully mapped semantic contract + passed checks
  - `medium`: partial semantic coverage + no critical check failures
  - `low`: inferred-only semantics or unresolved ambiguity
- Add mandatory fallback behavior for low confidence:
  - ask clarification question, or
  - return "insufficient semantic grounding" instead of executing.

**Deliverables**
- Updated README and API docs with scoped claims.
- `confidence_tier` and `semantic_warnings` in `/analyze` and `/analyze/report`.

**Acceptance Criteria**
- 100% of responses include confidence tier.
- No "high" confidence if semantic contract is missing.

---

### 2) "Semantic mapping breaks on real schemas"
**Plan**
- Add a dataset-level **Semantic Contract** artifact (manual + assisted):
  - canonical entities, measures, time columns, valid grains
  - approved join paths
  - metric definitions and exclusions
- Keep auto-inference as bootstrap only; never final authority for high-confidence responses.
- Build `/dataset/{id}/semantic/validate` endpoint to detect missing required semantics.

**Deliverables**
- `metadata/semantic_contracts/<dataset_id>.json` schema + validator.
- Contract-aware planner and SQL generator.

**Acceptance Criteria**
- For benchmark datasets, >= 95% of benchmark questions map to defined contract fields.
- Unmapped critical terms force clarification instead of guessed SQL.

---

### 3) "Self-healing SQL is fragile"
**Plan**
- Split repair loops into:
  - `execution_repair` (syntax/object existence)
  - `semantic_repair` (grain, join path, metric mismatch)
- Add failure taxonomy and hard-stop policy:
  - if semantic mismatch suspected, stop and request disambiguation.
- Log repair reason and category in query traces.

**Deliverables**
- Extended error classifier and repair policy engine.
- Trace fields: `repair_type`, `repair_reason`, `semantic_repair_attempted`.

**Acceptance Criteria**
- Execution-only repair cannot silently change metric definition or grain.
- 100% repaired queries emit a machine-readable repair rationale.

---

### 4) "Mining/discovery is overclaimed"
**Plan**
- Rebrand mining layer as **Exploratory Analytics Modules**.
- Introduce statistical validity checks where possible:
  - minimum sample thresholds
  - stability checks across windows
  - confidence warnings on weak signals
- Remove "discovery" claims unless hypothesis testing is implemented.

**Deliverables**
- Updated mining docs and endpoint descriptions.
- Signal quality metadata in snapshot payloads.

**Acceptance Criteria**
- Every mining output includes `signal_quality` and caveats.
- No user-facing text claims causal or discovery-level inference.

---

### 5) "Scale story is theoretical"
**Plan**
- Add explicit scale modes:
  - `small`: live queries
  - `medium`: cached plan+SQL and snapshot acceleration
  - `large`: curated aggregate tables/materialized views
- Add freshness policy metadata:
  - source timestamp, refresh SLA, staleness budget
- Add workload benchmark suite with row-volume tiers and P95 latency targets.

**Deliverables**
- Aggregation strategy spec + refresh scheduler contract.
- Benchmark report by dataset size and query class.

**Acceptance Criteria**
- Published P95 latency and freshness metrics per scale mode.
- "Large-scale supported" claim only if large mode tests pass.

---

### 6) "Safety != correctness"
**Plan**
- Implement an **Analytical Guardrails Layer** post-SQL/pre-response:
  - join fan-out detection
  - duplicate aggregation detection
  - grain mismatch checks
  - null/key coverage checks
- Add domain invariants per dataset (e.g., totals should not exceed known bounds).

**Deliverables**
- Guardrail engine with severity levels: `block`, `warn`, `info`.
- Response fields: `correctness_checks`, `blocked_by_guardrail`.

**Acceptance Criteria**
- Known fan-out test cases are blocked or flagged 100% of the time.
- No response marked `high` confidence with unresolved critical guardrail failures.

---

### 7) "Evaluation metrics are weak proxies"
**Plan**
- Add **answer correctness benchmarks**:
  - gold question set per dataset
  - expected SQL (or expected result set/invariants)
  - semantic equivalence tests
- Track:
  - answer accuracy
  - semantic intent match
  - decision usefulness (human rating)
  - contradiction rate vs known truths

**Deliverables**
- `evaluation/gold/` benchmark packs.
- Extended `/evaluation/metrics` with correctness section.

**Acceptance Criteria**
- Accuracy metrics reported on every release.
- Release gate fails below minimum accuracy threshold.

---

### 8) "Too complex for one person quickly"
**Plan**
- Prioritize scope into three product maturity stages:
  - Stage A: trustworthy bounded-domain analytics assistant
  - Stage B: semi-automated multi-domain support
  - Stage C: advanced adaptive reasoning
- Freeze feature expansion until Stage A quality gates pass.

**Deliverables**
- Roadmap with explicit "not in scope" list.
- Quality gates tied to CI before new features are merged.

**Acceptance Criteria**
- No net-new major feature merged without passing Stage A gates.

## Execution Roadmap

## Phase 0 (Week 1): Positioning + Contracts Foundation
- Update claims and documentation.
- Define semantic contract schema and validation rules.
- Add confidence tiers in response models.

## Phase 1 (Weeks 2-3): Correctness Guardrails
- Implement analytical guardrails (fan-out, grain, duplicates).
- Introduce block/warn behavior in `/analyze`.
- Add traceability for guardrail outcomes.

## Phase 2 (Weeks 4-5): Evaluation Upgrade
- Build gold benchmark datasets and expected outcomes.
- Add correctness metrics and release thresholds.
- Integrate benchmark run into CI.

## Phase 3 (Weeks 6-7): Repair Policy Hardening
- Separate execution vs semantic repairs.
- Add stop-and-clarify flow for ambiguous requests.
- Tighten planner/sql generator coupling to semantic contract.

## Phase 4 (Weeks 8-9): Scale & Freshness Reliability
- Add scale modes and freshness SLAs.
- Implement benchmark matrix and publish results.
- Document supported operating envelope.

## Phase 5 (Week 10): Messaging and Governance
- Finalize product positioning to bounded autonomy.
- Publish failure modes, confidence behavior, and operator playbook.

## Quality Gates (Release Blocking)
- Analytical correctness accuracy >= target (set per dataset; initial 0.85).
- Critical guardrail miss rate = 0 on regression suite.
- All responses include confidence tier + traceability.
- No unsupported claim language in docs.
- P95 latency and freshness metrics published for supported modes.

## Immediate Backlog (Start Now)
1. Add `semantic_contract` schema + validator + storage path.
2. Extend `api/schemas.py` and `api/report_schema.py` with confidence and guardrail fields.
3. Add analytical guardrails module and integrate in `api/routes.py`.
4. Add gold benchmark harness under `evaluation/`.
5. Rewrite README claims and limitations section.

## Risks and Mitigations
- **Risk:** Contracts are labor-intensive.  
  **Mitigation:** Assisted generation + human review workflow.
- **Risk:** Guardrails increase false positives.  
  **Mitigation:** severity tiers and dataset-specific threshold tuning.
- **Risk:** Accuracy benchmarking is hard across domains.  
  **Mitigation:** start with 3 reference datasets and expand incrementally.

## Definition of "Saved Project"
The project is considered "saved" when it is no longer judged as a general autonomous system, but as a **trustworthy bounded analytics assistant** with:
- explicit limits,
- measurable correctness,
- enforced semantic governance,
- and transparent confidence/failure reporting.
