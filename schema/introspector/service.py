from __future__ import annotations

from typing import Any, Dict, Optional

from adapters.factory import get_adapter


def introspect_schema(
    db_engine: str = "postgres",
    schema_name: Optional[str] = None,
    source_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    adapter = get_adapter(db_engine=db_engine, source_config=source_config)
    return adapter.introspect_schema(schema_name=schema_name)
