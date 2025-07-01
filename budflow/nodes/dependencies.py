"""Node-related dependencies for dependency injection."""

from functools import lru_cache

from budflow.nodes.registry import NodeRegistry


@lru_cache()
def get_node_registry() -> NodeRegistry:
    """Get the global node registry instance."""
    return NodeRegistry()