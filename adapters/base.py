from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class AdapterError(RuntimeError):
    pass


class DatabaseAdapter(ABC):
    engine: str = "unknown"

    def __init__(self, source_config: Optional[Dict[str, Any]] = None):
        self.source_config = source_config or {}

    @abstractmethod
    def execute_select(self, sql: str, row_limit: int, timeout_ms: int) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def introspect_schema(self, schema_name: Optional[str] = None) -> Dict[str, Any]:
        raise NotImplementedError
