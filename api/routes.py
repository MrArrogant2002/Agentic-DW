import os
import json
import time
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from agent.evaluator import evaluate_result
from agent.executor import UnsafeSQLError, execute_safe_query
from agent.insight_generator import generate_structured_report
from agent.insight_llm import generate_llm_sections
from agent.planner import build_plan
from agent.sql_llm_generator import classify_sql_error, generate_sql_from_plan
from agent.sql_generator import generate_sql
from api.report_schema import AnalyzeReportResponse
from evaluation.failure_analytics import build_failure_analytics
from evaluation.metrics import build_metrics
from metadata.store import append_query_trace, get_cached_sql, get_dataset, load_query_traces, load_schema_metadata, set_cached_sql
from mining.snapshots import SNAPSHOT_TYPES, get_snapshot, refresh_all, refresh_snapshot
from api.schemas import (
    AnalyzeDebugResponse,
    AnalyzeRequest,
    AnalyzeResponse,
    DatasetListResponse,
    DatasetMetadataResponse,
    DatasetOnboardRequest,
    DatasetOnboardResponse,
    DatasetUploadRequest,
    DatasetUploadResponse,
    DatasetIngestResponse,
    DatasetIngestStatusResponse,
    MiningRefreshRequest,
    MiningRefreshResponse,
)
from onboarding.service import (
    get_ingestion_status,
    get_dataset_metadata,
    list_registered_datasets,
    onboard_dataset,
    register_uploaded_dataset,
    run_ingestion,
    refresh_dataset_metadata,
)
from utils.env_loader import load_environments

router = APIRouter()


def _build_plan_cache_key(plan) -> str:
    payload = {
        "intent": plan.intent,
        "task_type": plan.task_type,
        "entity_scope": plan.entity_scope,
        "entity_dimension": plan.entity_dimension,
        "n": plan.n,
        "metric": plan.metric,
        "time_grain": plan.time_grain,
        "compare_against": plan.compare_against,
    }
    return json.dumps(payload, sort_keys=True)


@router.get("/health")
def health() -> dict:
    return {"status": "ok"}


def _run_analyze(request: AnalyzeRequest, debug_mode: bool = False):
    load_environments()
    trace_id = str(uuid4())
    planner_prompt_version = os.getenv("PLANNER_PROMPT_VERSION", "v1")
    sql_prompt_version = os.getenv("SQL_PROMPT_VERSION", "v1")
    started_at = time.perf_counter()
    planner_ms = 0.0
    sql_generation_ms = 0.0
    execution_ms = 0.0
    cache_hit = False
    plan = None

    dataset_metadata = load_schema_metadata(request.dataset_id) if request.dataset_id else None
    dataset_record = get_dataset(request.dataset_id) if request.dataset_id else None
    if request.dataset_id and dataset_record is None and dataset_metadata is None:
        try:
            status_payload = get_ingestion_status(request.dataset_id)
            dataset_record = status_payload.get("dataset")
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    if request.dataset_id and dataset_metadata is None:
        try:
            status_payload = get_ingestion_status(request.dataset_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        dataset = status_payload["dataset"]
        if dataset.get("source_type") == "file_upload":
            raise HTTPException(
                status_code=400,
                detail="Dataset metadata not available. Run /dataset/{dataset_id}/ingest first.",
            )
        raise HTTPException(
            status_code=400,
            detail="Dataset metadata missing. Run /dataset/{dataset_id}/refresh.",
        )

    db_engine = str((dataset_record or {}).get("db_engine") or os.getenv("DB_ENGINE", "postgres")).strip().lower()
    db_source_config = (dataset_record or {}).get("source_config") or None
    schema_hash = (dataset_record or {}).get("schema_hash")
    if dataset_record and dataset_record.get("source_type") != "db_connection":
        db_source_config = None

    planner_started = time.perf_counter()
    try:
        plan = build_plan(
            request.question,
            dataset_metadata=dataset_metadata,
            trace_id=trace_id,
            prompt_version=planner_prompt_version,
        )
    except TypeError:
        plan = build_plan(request.question, dataset_metadata=dataset_metadata)
    planner_ms = (time.perf_counter() - planner_started) * 1000.0

    retries_used = 0
    sql = ""
    snapshot_meta = None
    rows = []
    evaluation = {"status": "error", "reason": None}
    plan_cache_key = _build_plan_cache_key(plan)
    analysis_error = None

    try:
        use_snapshot = (
            plan.intent in {"trend_analysis", "customer_segmentation"}
            and (dataset_metadata is not None or (plan.entity_scope == "all" and not plan.entity_dimension))
        )
        if use_snapshot:
            try:
                snapshot = get_snapshot(
                    plan.intent,
                    refresh_if_stale=True,
                    dataset_id=request.dataset_id,
                    plan=plan,
                    dataset_metadata=dataset_metadata,
                    db_engine=db_engine,
                    source_config=db_source_config,
                )
            except TypeError:
                snapshot = get_snapshot(plan.intent, refresh_if_stale=True)
            snapshot_meta = {
                "snapshot_type": snapshot["snapshot_type"],
                "dataset_id": snapshot.get("dataset_id"),
                "scope_key": snapshot.get("scope_key"),
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
                    "dataset_id": snapshot.get("dataset_id"),
                    "scope_key": snapshot.get("scope_key"),
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
            sql_llm_enabled = os.getenv("SQL_LLM_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
            max_repairs = int(os.getenv("SQL_REPAIR_MAX_RETRIES", "2"))

            if not sql_llm_enabled:
                sql = generate_sql(plan, strict=False, dataset_metadata=dataset_metadata)
                exec_started = time.perf_counter()
                try:
                    rows = execute_safe_query(
                        sql,
                        row_limit=request.row_limit,
                        timeout_ms=request.timeout_ms,
                        db_engine=db_engine,
                        source_config=db_source_config,
                    )
                except TypeError:
                    rows = execute_safe_query(sql, row_limit=request.row_limit, timeout_ms=request.timeout_ms)
                execution_ms += (time.perf_counter() - exec_started) * 1000.0
                evaluation = evaluate_result(rows)
                if evaluation["status"] == "retry":
                    retries_used = 1
                    sql = generate_sql(plan, strict=True, dataset_metadata=dataset_metadata)
                    exec_started = time.perf_counter()
                    try:
                        rows = execute_safe_query(
                            sql,
                            row_limit=request.row_limit,
                            timeout_ms=request.timeout_ms,
                            db_engine=db_engine,
                            source_config=db_source_config,
                        )
                    except TypeError:
                        rows = execute_safe_query(sql, row_limit=request.row_limit, timeout_ms=request.timeout_ms)
                    execution_ms += (time.perf_counter() - exec_started) * 1000.0
                    evaluation = evaluate_result(rows)
            else:
                cached_sql = get_cached_sql(request.dataset_id, plan_cache_key, schema_hash=schema_hash)
                if cached_sql:
                    sql = cached_sql
                    cache_hit = True
                else:
                    sql_started = time.perf_counter()
                    try:
                        sql = generate_sql_from_plan(
                            question=request.question,
                            plan=plan,
                            dataset_metadata=dataset_metadata,
                            trace_id=trace_id,
                            prompt_version=sql_prompt_version,
                        )
                    except TypeError:
                        sql = generate_sql_from_plan(
                            question=request.question,
                            plan=plan,
                            dataset_metadata=dataset_metadata,
                        )
                    sql_generation_ms += (time.perf_counter() - sql_started) * 1000.0
                repair_attempt = 0
                while True:
                    try:
                        exec_started = time.perf_counter()
                        try:
                            rows = execute_safe_query(
                                sql,
                                row_limit=request.row_limit,
                                timeout_ms=request.timeout_ms,
                                db_engine=db_engine,
                                source_config=db_source_config,
                            )
                        except TypeError:
                            rows = execute_safe_query(sql, row_limit=request.row_limit, timeout_ms=request.timeout_ms)
                        execution_ms += (time.perf_counter() - exec_started) * 1000.0
                        evaluation = evaluate_result(rows)
                        if evaluation["status"] == "retry" and repair_attempt < max_repairs:
                            repair_attempt += 1
                            retries_used += 1
                            sql_started = time.perf_counter()
                            try:
                                sql = generate_sql_from_plan(
                                    question=request.question,
                                    plan=plan,
                                    dataset_metadata=dataset_metadata,
                                    previous_sql=sql,
                                    error_message="query_returned_no_rows",
                                    trace_id=trace_id,
                                    prompt_version=sql_prompt_version,
                                )
                            except TypeError:
                                sql = generate_sql_from_plan(
                                    question=request.question,
                                    plan=plan,
                                    dataset_metadata=dataset_metadata,
                                    previous_sql=sql,
                                    error_message="query_returned_no_rows",
                                )
                            sql_generation_ms += (time.perf_counter() - sql_started) * 1000.0
                            continue
                        if evaluation["status"] == "ok" and sql and not cache_hit:
                            set_cached_sql(request.dataset_id, plan_cache_key, sql, schema_hash=schema_hash)
                        break
                    except (UnsafeSQLError, Exception) as exc:
                        if repair_attempt >= max_repairs:
                            raise
                        repair_attempt += 1
                        retries_used += 1
                        sql_started = time.perf_counter()
                        try:
                            sql = generate_sql_from_plan(
                                question=request.question,
                                plan=plan,
                                dataset_metadata=dataset_metadata,
                                previous_sql=sql,
                                error_message=f"{classify_sql_error(exc)}::{exc}",
                                trace_id=trace_id,
                                prompt_version=sql_prompt_version,
                            )
                        except TypeError:
                            sql = generate_sql_from_plan(
                                question=request.question,
                                plan=plan,
                                dataset_metadata=dataset_metadata,
                                previous_sql=sql,
                                error_message=f"{classify_sql_error(exc)}::{exc}",
                            )
                        sql_generation_ms += (time.perf_counter() - sql_started) * 1000.0
                        continue
    except UnsafeSQLError as exc:
        analysis_error = f"unsafe_sql::{exc}"
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        analysis_error = str(exc)
        raise HTTPException(status_code=500, detail=f"Execution error: {exc}") from exc
    finally:
        try:
            append_query_trace(
                {
                    "trace_id": trace_id,
                    "question": request.question,
                    "dataset_id": request.dataset_id,
                    "db_engine": db_engine,
                    "schema_hash": schema_hash,
                    "prompt_versions": {
                        "planner": planner_prompt_version,
                        "sql": sql_prompt_version,
                    },
                    "plan": (
                        {
                            "intent": plan.intent,
                            "task_type": plan.task_type,
                            "entity_scope": plan.entity_scope,
                            "entity_dimension": plan.entity_dimension,
                            "n": plan.n,
                            "metric": plan.metric,
                            "time_grain": plan.time_grain,
                            "compare_against": plan.compare_against,
                        }
                        if plan
                        else None
                    ),
                    "sql": sql,
                    "cache_hit": cache_hit,
                    "evaluation_status": evaluation.get("status"),
                    "evaluation_reason": evaluation.get("reason"),
                    "row_count": len(rows),
                    "retries_used": retries_used,
                    "timing_ms": {
                        "planner": round(planner_ms, 3),
                        "sql_generation": round(sql_generation_ms, 3),
                        "execution": round(execution_ms, 3),
                        "total": round((time.perf_counter() - started_at) * 1000.0, 3),
                    },
                    "error": analysis_error,
                }
            )
        except Exception:
            pass

    base_payload = dict(
        trace_id=trace_id,
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
            "dataset_id": request.dataset_id,
            "metadata_loaded": dataset_metadata is not None,
            "db_engine": db_engine,
            "cache_hit": cache_hit,
            "prompt_versions": {
                "planner": planner_prompt_version,
                "sql": sql_prompt_version,
            },
            "plan": {
                "task_type": plan.task_type,
                "entity_scope": plan.entity_scope,
                "entity_dimension": plan.entity_dimension,
                "n": plan.n,
                "metric": plan.metric,
                "time_grain": plan.time_grain,
                "compare_against": plan.compare_against,
            },
        },
    )


@router.post("/dataset/onboard", response_model=DatasetOnboardResponse)
def dataset_onboard(request: DatasetOnboardRequest) -> DatasetOnboardResponse:
    try:
        result = onboard_dataset(
            name=request.name,
            db_engine=request.db_engine,
            schema_name=request.schema_name,
            description=request.description,
            source_config=request.source_config,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Dataset onboarding error: {exc}") from exc
    return DatasetOnboardResponse(**result)


@router.post("/dataset/upload", response_model=DatasetUploadResponse)
def dataset_upload(request: DatasetUploadRequest) -> DatasetUploadResponse:
    try:
        result = register_uploaded_dataset(
            name=request.name,
            file_path=request.file_path,
            description=request.description,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Dataset upload registration error: {exc}") from exc
    return DatasetUploadResponse(**result)


@router.get("/dataset/list", response_model=DatasetListResponse)
def dataset_list() -> DatasetListResponse:
    return DatasetListResponse(**list_registered_datasets())


@router.get("/dataset/{dataset_id}/metadata", response_model=DatasetMetadataResponse)
def dataset_metadata(dataset_id: str) -> DatasetMetadataResponse:
    try:
        result = get_dataset_metadata(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Metadata read error: {exc}") from exc
    return DatasetMetadataResponse(**result)


@router.post("/dataset/{dataset_id}/refresh", response_model=DatasetOnboardResponse)
def dataset_refresh(dataset_id: str) -> DatasetOnboardResponse:
    try:
        result = refresh_dataset_metadata(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Metadata refresh error: {exc}") from exc
    return DatasetOnboardResponse(**result)


@router.post("/dataset/{dataset_id}/ingest", response_model=DatasetIngestResponse)
def dataset_ingest(dataset_id: str) -> DatasetIngestResponse:
    try:
        result = run_ingestion(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {exc}") from exc
    return DatasetIngestResponse(**result)


@router.get("/dataset/{dataset_id}/ingest/status", response_model=DatasetIngestStatusResponse)
def dataset_ingest_status(dataset_id: str) -> DatasetIngestStatusResponse:
    try:
        result = get_ingestion_status(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestion status error: {exc}") from exc
    return DatasetIngestStatusResponse(**result)


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
    report_payload["query_context"]["trace_id"] = analysis.trace_id

    insight_enabled = (os.getenv("INSIGHT_MODEL_ENABLED", "0").strip().lower() in {"1", "true", "yes"})
    insight_prompt_version = os.getenv("INSIGHT_PROMPT_VERSION", "v1")
    if insight_enabled:
        try:
            try:
                llm_sections = generate_llm_sections(
                    analysis.model_dump(),
                    trace_id=analysis.trace_id,
                    prompt_version=insight_prompt_version,
                )
            except TypeError:
                llm_sections = generate_llm_sections(analysis.model_dump())
            report_payload["key_findings"] = llm_sections["key_findings"]
            report_payload["risk_flags"] = llm_sections["risk_flags"]
            report_payload["recommended_actions"] = llm_sections["recommended_actions"]
            report_payload["traceability"] = llm_sections["traceability"]
            report_payload["confidence"] = llm_sections["confidence"]
            report_payload["assumptions"] = llm_sections["assumptions"]
            append_query_trace(
                {
                    "trace_id": analysis.trace_id,
                    "stage": "insight_generation",
                    "enabled": True,
                    "prompt_version": insight_prompt_version,
                    "finding_count": len(llm_sections["key_findings"]),
                    "risk_count": len(llm_sections["risk_flags"]),
                }
            )
        except Exception as exc:
            report_payload["risk_flags"].append(f"LLM insight generation fallback applied: {exc}")
            append_query_trace(
                {
                    "trace_id": analysis.trace_id,
                    "stage": "insight_generation",
                    "enabled": True,
                    "prompt_version": insight_prompt_version,
                    "error": str(exc),
                    "fallback_used": True,
                }
            )
    else:
        append_query_trace(
            {
                "trace_id": analysis.trace_id,
                "stage": "insight_generation",
                "enabled": False,
                "prompt_version": insight_prompt_version,
            }
        )

    return AnalyzeReportResponse(**report_payload)


@router.post("/mining/refresh", response_model=MiningRefreshResponse)
def refresh_mining(request: MiningRefreshRequest) -> MiningRefreshResponse:
    try:
        if request.refresh_all:
            if request.dataset_id:
                dataset_metadata = load_schema_metadata(request.dataset_id)
                dataset_record = get_dataset(request.dataset_id)
                db_engine = str((dataset_record or {}).get("db_engine") or os.getenv("DB_ENGINE", "postgres")).strip().lower()
                source_config = (dataset_record or {}).get("source_config") if dataset_record else None
                refreshed = [
                    (
                        refresh_snapshot(
                            snapshot_type,
                            dataset_id=request.dataset_id,
                            dataset_metadata=dataset_metadata,
                            db_engine=db_engine,
                            source_config=source_config,
                        )
                    )
                    for snapshot_type in sorted(SNAPSHOT_TYPES)
                ]
            else:
                refreshed = refresh_all()
        else:
            snapshot_type = request.snapshot_type
            if snapshot_type not in SNAPSHOT_TYPES:
                raise HTTPException(
                    status_code=400,
                    detail=f"snapshot_type must be one of {sorted(SNAPSHOT_TYPES)} when refresh_all is false",
                )
            dataset_metadata = load_schema_metadata(request.dataset_id) if request.dataset_id else None
            dataset_record = get_dataset(request.dataset_id) if request.dataset_id else None
            db_engine = str((dataset_record or {}).get("db_engine") or os.getenv("DB_ENGINE", "postgres")).strip().lower()
            source_config = (dataset_record or {}).get("source_config") if dataset_record else None
            try:
                refreshed = [
                    refresh_snapshot(
                        snapshot_type,
                        dataset_id=request.dataset_id,
                        dataset_metadata=dataset_metadata,
                        db_engine=db_engine,
                        source_config=source_config,
                    )
                ]
            except TypeError:
                refreshed = [refresh_snapshot(snapshot_type)]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Mining refresh error: {exc}") from exc

    return MiningRefreshResponse(refreshed=refreshed)


@router.get("/evaluation/metrics")
def evaluation_metrics(limit: int = 1000) -> dict:
    traces = load_query_traces(limit=limit)
    return build_metrics(traces)


@router.get("/evaluation/failures")
def evaluation_failures(limit: int = 5000) -> dict:
    traces = load_query_traces(limit=limit)
    return build_failure_analytics(traces)
