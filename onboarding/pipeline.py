from datetime import datetime, timezone
from typing import Any, Dict

from metadata.store import (
    get_dataset,
    save_ingestion_run,
    save_quality_report,
    save_schema_metadata,
    save_semantic_map,
    update_dataset,
)
from onboarding.ingest import ingest_csv_to_postgres
from onboarding.quality import build_quality_report
from schema.introspector.service import introspect_schema
from schema.semantic_mapper.mapper import build_semantic_map


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_file_ingestion_pipeline(dataset_id: str) -> Dict[str, Any]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        raise ValueError(f"Unknown dataset_id: {dataset_id}")
    if dataset.get("source_type") != "file_upload":
        raise ValueError("Ingestion pipeline only applies to file_upload datasets")

    file_path = (dataset.get("source_config") or {}).get("file_path")
    if not file_path:
        raise ValueError("Dataset source_config.file_path is missing")

    schema_name = dataset.get("schema_name")
    if not schema_name:
        raise ValueError("Dataset schema_name is missing")

    update_dataset(dataset_id, {"status": "ingestion_running"})
    run = {"started_at": _now_iso(), "status": "running"}
    save_ingestion_run(dataset_id, run)

    try:
        ingest_result = ingest_csv_to_postgres(file_path=file_path, schema_name=schema_name, table_name="records")
        quality = build_quality_report(ingest_result)
        metadata = introspect_schema(db_engine="postgres", schema_name=schema_name)
        semantic_map = build_semantic_map(metadata)
        metadata["entities"] = semantic_map["entities"]
        metadata["measures"] = semantic_map["measures"]
        metadata["time_columns"] = semantic_map["time_columns"]
        save_schema_metadata(dataset_id, metadata)
        save_semantic_map(dataset_id, semantic_map)
        save_quality_report(dataset_id, quality)

        completed_run = {
            "started_at": run["started_at"],
            "ended_at": _now_iso(),
            "status": "success",
            "ingest_result": ingest_result,
            "quality": quality,
        }
        save_ingestion_run(dataset_id, completed_run)
        update_dataset(
            dataset_id,
            {
                "status": "ready",
                "last_ingested_at": completed_run["ended_at"],
                "row_count": ingest_result.get("row_count_inserted"),
            },
        )

        return {
            "dataset": get_dataset(dataset_id),
            "ingest_result": ingest_result,
            "quality_report": quality,
            "metadata_profile": metadata.get("profile", {}),
            "semantic_map_preview": {
                "entities": semantic_map.get("entities", [])[:3],
                "measures": semantic_map.get("measures", [])[:3],
                "time_columns": semantic_map.get("time_columns", [])[:3],
            },
        }
    except Exception as exc:
        failed_run = {
            "started_at": run["started_at"],
            "ended_at": _now_iso(),
            "status": "failed",
            "error": str(exc),
        }
        save_ingestion_run(dataset_id, failed_run)
        update_dataset(dataset_id, {"status": "ingestion_failed"})
        raise
