from fastapi import APIRouter, HTTPException

from agent.evaluator import evaluate_result
from agent.executor import UnsafeSQLError, execute_safe_query
from agent.planner import build_plan
from agent.sql_generator import generate_sql
from api.schemas import AnalyzeDebugResponse, AnalyzeRequest, AnalyzeResponse

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _run_analyze(request: AnalyzeRequest, debug_mode: bool = False):
    plan = build_plan(request.question)
    retries_used = 0
    sql = generate_sql(plan, strict=False)

    try:
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
        },
    )


@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    return _run_analyze(request, debug_mode=False)


@router.post("/analyze/debug", response_model=AnalyzeDebugResponse)
def analyze_debug(request: AnalyzeRequest) -> AnalyzeDebugResponse:
    return _run_analyze(request, debug_mode=True)
