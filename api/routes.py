from fastapi import APIRouter, HTTPException

from agent.evaluator import evaluate_result
from agent.executor import UnsafeSQLError, execute_safe_query
from agent.planner import build_plan
from agent.sql_generator import generate_sql
from api.schemas import AnalyzeRequest, AnalyzeResponse

router = APIRouter()


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    plan = build_plan(request.question)
    sql = generate_sql(plan)

    try:
        rows = execute_safe_query(sql, row_limit=request.row_limit, timeout_ms=request.timeout_ms)
    except UnsafeSQLError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution error: {exc}") from exc

    evaluation = evaluate_result(rows)

    return AnalyzeResponse(
        question=request.question,
        intent=plan.intent,
        sql=sql,
        evaluator_status=evaluation["status"],
        evaluator_reason=evaluation["reason"],
        rows=rows,
    )

