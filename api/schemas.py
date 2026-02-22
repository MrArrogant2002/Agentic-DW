from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AnalyzeRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=1000)
    dataset_id: Optional[str] = Field(default=None, description="Registered dataset id for schema-aware planning")
    row_limit: int = Field(default=100, ge=1, le=2000)
    timeout_ms: int = Field(default=15000, ge=1000, le=120000)


class AnalyzeResponse(BaseModel):
    trace_id: str
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


class MiningRefreshRequest(BaseModel):
    snapshot_type: Optional[str] = Field(default=None, description="trend_analysis or customer_segmentation")
    dataset_id: Optional[str] = Field(default=None, description="Optional dataset id for dataset-scoped mining snapshots")
    refresh_all: bool = Field(default=False)


class MiningRefreshResponse(BaseModel):
    refreshed: List[Dict[str, Any]]


class DatasetOnboardRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    db_engine: str = Field(default="postgres", min_length=3, max_length=30)
    schema_name: str = Field(default="public", min_length=1, max_length=120)
    description: Optional[str] = Field(default=None, max_length=500)
    source_config: Optional[Dict[str, Any]] = None


class DatasetOnboardResponse(BaseModel):
    dataset: Dict[str, Any]
    summary: Dict[str, Any]


class DatasetMetadataResponse(BaseModel):
    dataset: Dict[str, Any]
    summary: Dict[str, Any]
    metadata: Dict[str, Any]
    semantic_map: Optional[Dict[str, Any]] = None


class DatasetListResponse(BaseModel):
    datasets: List[Dict[str, Any]]
    count: int


class DatasetUploadRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    file_path: str = Field(..., min_length=3, max_length=500)
    description: Optional[str] = Field(default=None, max_length=500)


class DatasetUploadResponse(BaseModel):
    dataset: Dict[str, Any]


class DatasetIngestResponse(BaseModel):
    dataset: Dict[str, Any]
    ingest_result: Dict[str, Any]
    quality_report: Dict[str, Any]
    metadata_profile: Dict[str, Any]


class DatasetIngestStatusResponse(BaseModel):
    dataset: Dict[str, Any]
    latest_ingestion_run: Optional[Dict[str, Any]]
    quality_report: Optional[Dict[str, Any]]
