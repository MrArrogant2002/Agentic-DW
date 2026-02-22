"""Database adapter layer for multi-engine execution and introspection."""

from adapters.factory import get_adapter

__all__ = ["get_adapter"]
