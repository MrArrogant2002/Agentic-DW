from typing import Any, Dict, Optional

from onboarding.service import (
    get_dataset_metadata as _get_dataset_metadata,
    list_registered_datasets as _list_registered_datasets,
    onboard_postgres_dataset as _onboard_postgres_dataset,
    refresh_dataset_metadata as _refresh_dataset_metadata,
)


def onboard_postgres_dataset(name: str, schema_name: str = "public", description: Optional[str] = None) -> Dict[str, Any]:
    return _onboard_postgres_dataset(name=name, schema_name=schema_name, description=description)


def refresh_dataset_metadata(dataset_id: str) -> Dict[str, Any]:
    return _refresh_dataset_metadata(dataset_id)


def get_dataset_metadata(dataset_id: str) -> Dict[str, Any]:
    return _get_dataset_metadata(dataset_id)


def list_registered_datasets() -> Dict[str, Any]:
    return _list_registered_datasets()
