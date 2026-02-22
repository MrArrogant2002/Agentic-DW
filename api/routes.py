import os

from fastapi import APIRouter, HTTPException

from agent.evaluator import evaluate_result
from agent.executor import UnsafeSQLError, execute_safe_query
from agent.insight_generator import generate_structured_report
from agent.insight_llm import generate_llm_sections
from agent.planner import build_plan
from agent.sql_generator import generate_sql
from api.report_schema import AnalyzeReportResponse
from mining.snapshots import SNAPSHOT_TYPES, get_snapshot, refresh_all, refresh_snapshot
from api.schemas import (
    AnalyzeDebugResponse,
    AnalyzeRequest,
    AnalyzeResponse,
    MiningRefreshRequest,
    MiningRefreshResponse,
)
from utils.env_loader import load_environments

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _run_analyze(request: AnalyzeRequest, debug_mode: bool = False):
    plan = build_plan(request.question)
    retries_used = 0
    sql = generate_sql(plan, strict=False)
    snapshot_meta = None

    try:
        if plan.intent in {"trend_analysis", "customer_segmentation"}:
            snapshot = get_snapshot(plan.intent, refresh_if_stale=True)
            snapshot_meta = {
                "snapshot_type": snapshot["snapshot_type"],
                "generated_at": snapshot["generated_at"],
                "source_max_date": snapshot["source_max_date"],
                "snapshot_version": snapshot.get("snapshot_version"),
                "run_id": snapshot.get("run_id"),
                "refreshed": snapshot["refreshed"],
            }
            sql = "-- mining snapshot retrieval"
            rows = [
                {
                    "snapshot_type": snapshot["snapshot_type"],
                    "generated_at": snapshot["generated_at"],
                    "source_max_date": snapshot["source_max_date"],
                    "snapshot_version": snapshot.get("snapshot_version"),
                    "run_id": snapshot.get("run_id"),
                    "refreshed": snapshot["refreshed"],
                    "data": snapshot["snapshot_json"],
                }
            ]
            evaluation = {"status": "ok", "reason": None}
        else:
            rows = execute_safe_query(sql, row_limit=request.row_limit, timeout_ms=request.timeout_ms)
            evaluation = evaluate_result(rows)
            if evaluation["status"] == "retry":
                retries_used = 1
                sql = generate_sql(plan, strict=True)
                rows = execute_safe_query(sql, row_limit=request.row_limit, timeout_ms=request.timeout_ms)
                evaluation = evaluate_result(rows)
    except UnsafeSQLError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution error: {exc}") from exc

    base_payload = dict(
        question=request.question,
        intent=plan.intent,
        planner_source=plan.planner_source,
        sql=sql,
        evaluator_status=evaluation["status"],
        evaluator_reason=evaluation["reason"],
        retries_used=retries_used,
        rows=rows,
    )

    if not debug_mode:
        return AnalyzeResponse(**base_payload)

    return AnalyzeDebugResponse(
        **base_payload,
        debug={
            "requires_mining": plan.requires_mining,
            "row_count": len(rows),
            "strict_retry_used": retries_used == 1,
            "snapshot": snapshot_meta,
        },
    )


@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    return _run_analyze(request, debug_mode=False)


@router.post("/analyze/debug", response_model=AnalyzeDebugResponse)
def analyze_debug(request: AnalyzeRequest) -> AnalyzeDebugResponse:
    return _run_analyze(request, debug_mode=True)


@router.post("/analyze/report", response_model=AnalyzeReportResponse)
def analyze_report(request: AnalyzeRequest) -> AnalyzeReportResponse:
    load_environments()
    analysis = _run_analyze(request, debug_mode=True)
    report_payload = generate_structured_report(analysis.model_dump())

    insight_enabled = (os.getenv("INSIGHT_MODEL_ENABLED", "0").strip().lower() in {"1", "true", "yes"})
    if insight_enabled:
        try:
            llm_sections = generate_llm_sections(analysis.model_dump())
            report_payload["key_findings"] = llm_sections["key_findings"]
            report_payload["risk_flags"] = llm_sections["risk_flags"]
            report_payload["recommended_actions"] = llm_sections["recommended_actions"]
            report_payload["traceability"] = llm_sections["traceability"]
            report_payload["confidence"] = llm_sections["confidence"]
            report_payload["assumptions"] = llm_sections["assumptions"]
        except Exception as exc:
            report_payload["risk_flags"].append(f"LLM insight generation fallback applied: {exc}")

    return AnalyzeReportResponse(**report_payload)


@router.post("/mining/refresh", response_model=MiningRefreshResponse)
def refresh_mining(request: MiningRefreshRequest) -> MiningRefreshResponse:
    try:
        if request.refresh_all:
            refreshed = refresh_all()
        else:
            snapshot_type = request.snapshot_type
            if snapshot_type not in SNAPSHOT_TYPES:
                raise HTTPException(
                    status_code=400,
                    detail=f"snapshot_type must be one of {sorted(SNAPSHOT_TYPES)} when refresh_all is false",
                )
            refreshed = [refresh_snapshot(snapshot_type)]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Mining refresh error: {exc}") from exc

    return MiningRefreshResponse(refreshed=refreshed)
