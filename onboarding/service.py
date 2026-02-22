from pathlib import Path
from typing import Any, Dict, Optional

from metadata.store import (
    get_dataset,
    list_datasets,
    load_latest_ingestion_run,
    load_quality_report,
    load_schema_metadata,
    load_semantic_map,
    register_dataset,
    save_schema_metadata,
    save_semantic_map,
    update_dataset,
)
from onboarding.ingest import build_schema_name
from onboarding.pipeline import run_file_ingestion_pipeline
from schema.introspector.service import introspect_schema
from schema.semantic_mapper.mapper import build_semantic_map


def _build_summary(metadata: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "table_count": metadata.get("profile", {}).get("table_count", 0),
        "relationship_count": metadata.get("profile", {}).get("relationship_count", 0),
        "top_entities": metadata.get("entities", [])[:5],
        "top_measures": metadata.get("measures", [])[:5],
        "top_time_columns": metadata.get("time_columns", [])[:5],
    }


def onboard_dataset(
    name: str,
    db_engine: str = "postgres",
    schema_name: str = "public",
    description: Optional[str] = None,
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    dataset = register_dataset(
        name=name,
        source_type="db_connection",
        db_engine=db_engine,
        schema_name=schema_name,
        description=description,
        status="introspection_running",
        source_config=source_config or {},
    )
    metadata = introspect_schema(db_engine=db_engine, schema_name=schema_name, source_config=source_config)
    semantic_map = build_semantic_map(metadata)
    metadata["entities"] = semantic_map["entities"]
    metadata["measures"] = semantic_map["measures"]
    metadata["time_columns"] = semantic_map["time_columns"]
    save_schema_metadata(dataset["dataset_id"], metadata)
    save_semantic_map(dataset["dataset_id"], semantic_map)
    dataset = update_dataset(dataset["dataset_id"], {"status": "ready"})
    return {"dataset": dataset, "summary": _build_summary(metadata)}


def onboard_postgres_dataset(name: str, schema_name: str = "public", description: Optional[str] = None) -> Dict[str, Any]:
    return onboard_dataset(
        name=name,
        db_engine="postgres",
        schema_name=schema_name,
        description=description,
    )


def register_uploaded_dataset(name: str, file_path: str, description: Optional[str] = None) -> Dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise ValueError(f"File path does not exist: {file_path}")

    dataset = register_dataset(
        name=name,
        source_type="file_upload",
        db_engine="postgres",
        schema_name="pending_schema",
        description=description,
        status="uploaded",
        source_config={"file_path": str(path)},
    )
    schema_name = build_schema_name(dataset["dataset_id"])
    dataset = update_dataset(dataset["dataset_id"], {"schema_name": schema_name})
    return {"dataset": dataset}


def run_ingestion(dataset_id: str) -> Dict[str, Any]:
    return run_file_ingestion_pipeline(dataset_id)


def refresh_dataset_metadata(dataset_id: str) -> Dict[str, Any]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        raise ValueError(f"Unknown dataset_id: {dataset_id}")

    if dataset.get("source_type") == "file_upload" and dataset.get("status") != "ready":
        raise ValueError("Dataset is not ready. Run ingestion first.")

    schema_name = dataset.get("schema_name", "public")
    db_engine = str(dataset.get("db_engine") or "postgres")
    metadata = introspect_schema(
        db_engine=db_engine,
        schema_name=schema_name,
        source_config=dataset.get("source_config") or None,
    )
    semantic_map = build_semantic_map(metadata)
    metadata["entities"] = semantic_map["entities"]
    metadata["measures"] = semantic_map["measures"]
    metadata["time_columns"] = semantic_map["time_columns"]
    save_schema_metadata(dataset_id, metadata)
    save_semantic_map(dataset_id, semantic_map)
    dataset = update_dataset(dataset_id, {"status": "ready"})
    return {"dataset": dataset, "summary": _build_summary(metadata)}


def get_dataset_metadata(dataset_id: str) -> Dict[str, Any]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        raise ValueError(f"Unknown dataset_id: {dataset_id}")
    metadata = load_schema_metadata(dataset_id)
    if metadata is None:
        raise ValueError(f"No metadata cache found for dataset_id: {dataset_id}")
    semantic_map = load_semantic_map(dataset_id)
    return {"dataset": dataset, "metadata": metadata, "semantic_map": semantic_map, "summary": _build_summary(metadata)}


def list_registered_datasets() -> Dict[str, Any]:
    items = list_datasets()
    return {"datasets": items, "count": len(items)}


def get_ingestion_status(dataset_id: str) -> Dict[str, Any]:
    dataset = get_dataset(dataset_id)
    if not dataset:
        raise ValueError(f"Unknown dataset_id: {dataset_id}")
    latest_run = load_latest_ingestion_run(dataset_id)
    quality = load_quality_report(dataset_id)
    return {
        "dataset": dataset,
        "latest_ingestion_run": latest_run,
        "quality_report": quality,
    }
