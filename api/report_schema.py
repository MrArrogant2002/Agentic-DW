from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QueryContext(BaseModel):
    trace_id: str
    question: str
    intent: str
    planner_source: str
    evaluator_status: str
    evaluator_reason: Optional[str]


class ExecutionEvidence(BaseModel):
    mode: str = Field(..., description="sql_live or mining_snapshot")
    sql: str
    row_count: int
    retries_used: int
    snapshot_meta: Optional[Dict[str, Any]] = None


class KeyFinding(BaseModel):
    finding: str
    value: Any
    unit: Optional[str] = None


class TraceabilityItem(BaseModel):
    claim: str
    source_path: str
    source_value: Any


class AnalyzeReportResponse(BaseModel):
    query_context: QueryContext
    execution_evidence: ExecutionEvidence
    key_findings: List[KeyFinding]
    risk_flags: List[str]
    recommended_actions: List[str]
    traceability: List[TraceabilityItem]
    confidence: float = Field(..., ge=0.0, le=1.0)
    assumptions: List[str]
