from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AnalyzeRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=1000)
    row_limit: int = Field(default=100, ge=1, le=2000)
    timeout_ms: int = Field(default=15000, ge=1000, le=120000)


class AnalyzeResponse(BaseModel):
    question: str
    intent: str
    planner_source: str
    sql: str
    evaluator_status: str
    evaluator_reason: Optional[str]
    retries_used: int
    rows: List[Dict[str, Any]]


class AnalyzeDebugResponse(AnalyzeResponse):
    debug: Dict[str, Any]
