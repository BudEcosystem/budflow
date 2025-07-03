"""Enhanced node registry and discovery system for BudFlow.

This module provides a comprehensive node registry system that allows:
- Dynamic node discovery and registration
- Plugin-based node loading
- Version management
- Dependency resolution
- Node categorization and search
- Hot-reloading of nodes
"""

import asyncio
import importlib
import inspect
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Type, Callable, Any

import structlog
from pydantic import BaseModel, Field, ConfigDict

from budflow.workflows.models import NodeType
from budflow.executor.context import NodeExecutionContext
from .base import BaseNode, NodeDefinition
from ..core.redis_client import get_redis_client

logger = structlog.get_logger()


class NodeVersion(BaseModel):
    """Node version information."""
    
    major: int = 1
    minor: int = 0
    patch: int = 0
    
    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def __lt__(self, other: "NodeVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def __eq__(self, other: "NodeVersion") -> bool:
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
    
    @classmethod
    def from_string(cls, version_str: str) -> "NodeVersion":
        """Parse version from string."""
        parts = version_str.split(".")
        return cls(
            major=int(parts[0]) if len(parts) > 0 else 1,
            minor=int(parts[1]) if len(parts) > 1 else 0,
            patch=int(parts[2]) if len(parts) > 2 else 0,
        )
    
    model_config = ConfigDict(from_attributes=True)


class NodeDependency(BaseModel):
    """Node dependency specification."""
    
    name: str
    version_constraint: str = "*"  # Semver constraint
    optional: bool = False
    
    model_config = ConfigDict(from_attributes=True)


class NodeMetadata(BaseModel):
    """Enhanced node metadata."""
    
    # Basic info
    name: str
    display_name: str
    description: str
    version: NodeVersion = Field(default_factory=NodeVersion)
    
    # Categorization
    category: str = "General"
    subcategory: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    
    # Technical details
    node_class: str  # Fully qualified class name
    module_path: str
    package: Optional[str] = None
    
    # Dependencies
    dependencies: List[NodeDependency] = Field(default_factory=list)
    python_requirements: List[str] = Field(default_factory=list)
    
    # Features
    supports_streaming: bool = False
    supports_batching: bool = False
    is_stateful: bool = False
    is_async: bool = True
    
    # Documentation
    documentation_url: Optional[str] = None
    examples: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Registry info
    registration_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "builtin"  # "builtin", "plugin", "community"
    author: Optional[str] = None
    license: Optional[str] = None
    
    # Status
    is_enabled: bool = True
    is_deprecated: bool = False
    deprecation_message: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)


class PluginInfo(BaseModel):
    """Plugin package information."""
    
    name: str
    version: str
    path: str
    entry_points: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    is_loaded: bool = False
    load_time: Optional[datetime] = None
    
    model_config = ConfigDict(from_attributes=True)


class NodeRegistry:
    """Enhanced registry for node types with discovery and plugin support."""
    
    def __init__(self, redis_client=None):
        # Legacy support
        self._nodes: Dict[str, Type[BaseNode]] = {}
        self._definitions: Dict[str, NodeDefinition] = {}
        
        # Enhanced features
        self.redis = redis_client or get_redis_client()
        self.enhanced_nodes: Dict[str, NodeMetadata] = {}
        self.node_classes: Dict[str, Type[BaseNode]] = {}
        self.plugins: Dict[str, PluginInfo] = {}
        
        # Registry state
        self.is_initialized = False
        self.plugin_paths: List[Path] = []
        self.watchers: List[Callable] = []
        
        # Redis keys
        self.nodes_key = "budflow:node_registry:nodes"
        self.plugins_key = "budflow:node_registry:plugins"
        self.discovery_key = "budflow:node_registry:discovery"
        
        self.logger = logger.bind(component="node_registry")
        
        # Register built-in nodes
        self._register_builtin_nodes()
    
    def _register_builtin_nodes(self):
        """Register built-in node types."""
        # Import here to avoid circular imports
        from .triggers import ManualTriggerNode, WebhookTriggerNode, ScheduleTriggerNode
        from .actions import HTTPNode, EmailNode, DatabaseNode, PostgreSQLNode, MySQLNode, MongoDBNode, FileNode
        from .control import IfNode, LoopNode, WaitNode, StopNode
        from .implementations import SubWorkflowNode
        
        # Register trigger nodes
        self._register_node("manual.trigger", ManualTriggerNode)
        self._register_node("webhook.trigger", WebhookTriggerNode)
        self._register_node("schedule.trigger", ScheduleTriggerNode)
        
        # Register action nodes
        self._register_node("http.request", HTTPNode)
        self._register_node("email.send", EmailNode)
        self._register_node("database.query", DatabaseNode)
        self._register_node("postgresql.query", PostgreSQLNode)
        self._register_node("mysql.query", MySQLNode)
        self._register_node("mongodb.query", MongoDBNode)
        self._register_node("file.operation", FileNode)
        
        # Register control nodes
        self._register_node("if", IfNode)
        self._register_node("loop", LoopNode)
        self._register_node("wait", WaitNode)
        self._register_node("stop", StopNode)
        
        # Register workflow composition node
        self._register_node("subworkflow", SubWorkflowNode)
    
    def _register_node(self, type_key: str, node_class: Type[BaseNode]):
        """Internal method to register a node."""
        self._nodes[type_key] = node_class
        self._definitions[type_key] = node_class.get_definition()
        self.logger.info("Registered node type", type_key=type_key, node_class=node_class.__name__)
    
    def register(self, type_key: str, node_type: Optional[NodeType] = None) -> Callable:
        """Decorator to register a node type."""
        def decorator(node_class: Type[BaseNode]) -> Type[BaseNode]:
            self._register_node(type_key, node_class)
            return node_class
        return decorator
    
    def has_node(self, type_key: str) -> bool:
        """Check if a node type is registered."""
        return type_key in self._nodes
    
    def get_node_class(self, type_key: str) -> Optional[Type[BaseNode]]:
        """Get node class by type key."""
        return self._nodes.get(type_key)
    
    def get_node_definition(self, type_key: str) -> Optional[NodeDefinition]:
        """Get node definition by type key."""
        return self._definitions.get(type_key)
    
    def get_all_nodes(self) -> Dict[str, Type[BaseNode]]:
        """Get all registered nodes."""
        return self._nodes.copy()
    
    def get_nodes_by_type(self, node_type: NodeType) -> Dict[str, Type[BaseNode]]:
        """Get all nodes of a specific type."""
        result = {}
        
        # Map generic types to specific types
        trigger_types = {NodeType.MANUAL, NodeType.WEBHOOK, NodeType.SCHEDULE}
        
        for type_key, node_class in self._nodes.items():
            definition = self._definitions.get(type_key)
            if definition:
                # Handle generic trigger type
                if node_type == NodeType.TRIGGER and definition.type in trigger_types:
                    result[type_key] = node_class
                # Handle exact type match
                elif definition.type == node_type:
                    result[type_key] = node_class
        return result
    
    def get_nodes_by_category(self, category: str) -> Dict[str, Type[BaseNode]]:
        """Get all nodes in a specific category."""
        result = {}
        for type_key, node_class in self._nodes.items():
            definition = self._definitions.get(type_key)
            if definition and definition.category == category:
                result[type_key] = node_class
        return result
    
    def search_nodes(self, query: str) -> Dict[str, Type[BaseNode]]:
        """Search nodes by name or description."""
        query_lower = query.lower()
        result = {}
        
        for type_key, node_class in self._nodes.items():
            definition = self._definitions.get(type_key)
            if definition:
                if (query_lower in definition.name.lower() or 
                    query_lower in definition.description.lower()):
                    result[type_key] = node_class
        
        return result
    
    def _register_enhanced_metadata(self, type_key: str, node_class: Type[BaseNode], node_type: Optional[NodeType] = None) -> None:
        """Register enhanced metadata for a node."""
        try:
            # Extract metadata from node class
            metadata = NodeMetadata(
                name=getattr(node_class, 'name', type_key),
                display_name=getattr(node_class, 'display_name', type_key.replace('.', ' ').title()),
                description=getattr(node_class, 'description', ''),
                category=getattr(node_class, 'category', 'General'),
                node_class=f"{node_class.__module__}.{node_class.__qualname__}",
                module_path=node_class.__module__,
                version=NodeVersion.from_string(getattr(node_class, 'version', '1.0.0')),
                tags=getattr(node_class, 'tags', []),
                supports_streaming=getattr(node_class, 'supports_streaming', False),
                supports_batching=getattr(node_class, 'supports_batching', False),
                is_stateful=getattr(node_class, 'is_stateful', False),
                is_async=asyncio.iscoroutinefunction(getattr(node_class, 'execute', None)),
                source="builtin"
            )
            
            self.enhanced_nodes[type_key] = metadata
            self.node_classes[type_key] = node_class
            
        except Exception as e:
            self.logger.warning(f"Failed to register enhanced metadata for {type_key}: {e}")
    
    async def discover_nodes(self, plugin_paths: Optional[List[Path]] = None) -> Dict[str, NodeMetadata]:
        """Discover nodes from plugin directories."""
        discovered = {}
        search_paths = plugin_paths or self.plugin_paths
        
        for plugin_path in search_paths:
            if not plugin_path.exists():
                continue
                
            try:
                # Look for Python files
                for py_file in plugin_path.rglob("*.py"):
                    if py_file.name.startswith("__"):
                        continue
                    
                    module_name = self._get_module_name(py_file, plugin_path)
                    if not module_name:
                        continue
                    
                    try:
                        # Import module dynamically
                        import importlib.util
                        spec = importlib.util.spec_from_file_location(module_name, py_file)
                        if not spec or not spec.loader:
                            continue
                            
                        module = importlib.util.module_from_spec(spec)
                        sys.modules[module_name] = module
                        spec.loader.exec_module(module)
                        
                        # Find node classes
                        for _, obj in inspect.getmembers(module, inspect.isclass):
                            if (issubclass(obj, BaseNode) and 
                                obj != BaseNode and 
                                hasattr(obj, 'name')):
                                
                                node_metadata = self._create_metadata_from_class(obj, "plugin")
                                discovered[obj.name] = node_metadata
                                
                    except Exception as e:
                        self.logger.warning(f"Failed to load module {module_name}: {e}")
                        
            except Exception as e:
                self.logger.error(f"Failed to discover nodes in {plugin_path}: {e}")
        
        return discovered
    
    def _get_module_name(self, file_path: Path, base_path: Path) -> Optional[str]:
        """Get module name from file path."""
        try:
            relative_path = file_path.relative_to(base_path)
            return str(relative_path.with_suffix("")).replace(os.sep, ".")
        except ValueError:
            return None
    
    def _create_metadata_from_class(self, node_class: Type[BaseNode], source: str) -> NodeMetadata:
        """Create metadata from node class."""
        return NodeMetadata(
            name=getattr(node_class, 'name', node_class.__name__),
            display_name=getattr(node_class, 'display_name', node_class.__name__),
            description=getattr(node_class, 'description', ''),
            category=getattr(node_class, 'category', 'General'),
            node_class=f"{node_class.__module__}.{node_class.__qualname__}",
            module_path=node_class.__module__,
            version=NodeVersion.from_string(getattr(node_class, 'version', '1.0.0')),
            tags=getattr(node_class, 'tags', []),
            supports_streaming=getattr(node_class, 'supports_streaming', False),
            supports_batching=getattr(node_class, 'supports_batching', False),
            is_stateful=getattr(node_class, 'is_stateful', False),
            is_async=asyncio.iscoroutinefunction(getattr(node_class, 'execute', None)),
            source=source,
            author=getattr(node_class, 'author', None),
            license=getattr(node_class, 'license', None)
        )
    
    async def load_plugin(self, plugin_path: Path) -> PluginInfo:
        """Load a plugin from path."""
        if not plugin_path.exists():
            raise ValueError(f"Plugin path does not exist: {plugin_path}")
        
        plugin_name = plugin_path.name
        if plugin_path.is_file() and plugin_path.suffix == ".py":
            plugin_name = plugin_path.stem
        
        try:
            # Load plugin metadata if available
            metadata_file = plugin_path / "plugin.json" if plugin_path.is_dir() else None
            metadata = {}
            
            if metadata_file and metadata_file.exists():
                with open(metadata_file) as f:
                    metadata = json.load(f)
            
            # Discover nodes in plugin
            discovered_nodes = await self.discover_nodes([plugin_path])
            
            # Register discovered nodes
            for node_name, node_metadata in discovered_nodes.items():
                self.enhanced_nodes[node_name] = node_metadata
                
                # Try to load the actual class
                try:
                    module_path, class_name = node_metadata.node_class.rsplit('.', 1)
                    module = sys.modules.get(module_path)
                    if module:
                        node_class = getattr(module, class_name, None)
                        if node_class:
                            self.node_classes[node_name] = node_class
                            self._register_node(node_name, node_class)
                except Exception as e:
                    self.logger.warning(f"Failed to register node class {node_name}: {e}")
            
            plugin_info = PluginInfo(
                name=plugin_name,
                version=metadata.get("version", "1.0.0"),
                path=str(plugin_path),
                entry_points=list(discovered_nodes.keys()),
                metadata=metadata,
                is_loaded=True,
                load_time=datetime.now(timezone.utc)
            )
            
            self.plugins[plugin_name] = plugin_info
            return plugin_info
            
        except Exception as e:
            self.logger.error(f"Failed to load plugin {plugin_name}: {e}")
            raise
    
    async def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin."""
        if plugin_name not in self.plugins:
            return False
        
        try:
            plugin_info = self.plugins[plugin_name]
            
            # Remove nodes from registry
            for entry_point in plugin_info.entry_points:
                if entry_point in self.enhanced_nodes:
                    del self.enhanced_nodes[entry_point]
                if entry_point in self.node_classes:
                    del self.node_classes[entry_point]
                if entry_point in self._nodes:
                    del self._nodes[entry_point]
                if entry_point in self._definitions:
                    del self._definitions[entry_point]
            
            # Mark plugin as unloaded
            plugin_info.is_loaded = False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unload plugin {plugin_name}: {e}")
            return False
    
    async def hot_reload_plugin(self, plugin_name: str) -> bool:
        """Hot reload a plugin."""
        if plugin_name not in self.plugins:
            return False
        
        try:
            plugin_info = self.plugins[plugin_name]
            plugin_path = Path(plugin_info.path)
            
            # Unload current plugin
            await self.unload_plugin(plugin_name)
            
            # Reload plugin
            await self.load_plugin(plugin_path)
            
            self.logger.info(f"Hot reloaded plugin: {plugin_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to hot reload plugin {plugin_name}: {e}")
            return False
    
    def get_plugin_info(self, plugin_name: str) -> Optional[PluginInfo]:
        """Get plugin information."""
        return self.plugins.get(plugin_name)
    
    def list_plugins(self) -> List[PluginInfo]:
        """List all loaded plugins."""
        return list(self.plugins.values())
    
    def get_enhanced_metadata(self, type_key: str) -> Optional[NodeMetadata]:
        """Get enhanced metadata for a node."""
        return self.enhanced_nodes.get(type_key)
    
    def list_enhanced_nodes(self) -> Dict[str, NodeMetadata]:
        """List all enhanced node metadata."""
        return self.enhanced_nodes.copy()
    
    async def sync_with_cluster(self) -> None:
        """Sync node registry with cluster via Redis."""
        if not self.redis:
            return
        
        try:
            # Publish current node registry to Redis
            registry_data = {
                "nodes": {
                    key: metadata.model_dump() 
                    for key, metadata in self.enhanced_nodes.items()
                },
                "plugins": {
                    key: plugin.model_dump() 
                    for key, plugin in self.plugins.items()
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            await self.redis.hset(
                self.nodes_key,
                mapping={"registry": json.dumps(registry_data)}
            )
            
            self.logger.debug("Synced node registry with cluster")
            
        except Exception as e:
            self.logger.error(f"Failed to sync with cluster: {e}")
    
    async def load_from_cluster(self) -> None:
        """Load node registry from cluster via Redis."""
        if not self.redis:
            return
        
        try:
            registry_json = await self.redis.hget(self.nodes_key, "registry")
            if not registry_json:
                return
            
            registry_data = json.loads(registry_json)
            
            # Load enhanced nodes
            for key, metadata_dict in registry_data.get("nodes", {}).items():
                try:
                    metadata = NodeMetadata.model_validate(metadata_dict)
                    self.enhanced_nodes[key] = metadata
                except Exception as e:
                    self.logger.warning(f"Failed to load node metadata {key}: {e}")
            
            # Load plugins
            for key, plugin_dict in registry_data.get("plugins", {}).items():
                try:
                    plugin = PluginInfo.model_validate(plugin_dict)
                    self.plugins[key] = plugin
                except Exception as e:
                    self.logger.warning(f"Failed to load plugin info {key}: {e}")
            
            self.logger.debug("Loaded node registry from cluster")
            
        except Exception as e:
            self.logger.error(f"Failed to load from cluster: {e}")


class NodeFactory:
    """Factory for creating node instances."""
    
    def __init__(self, registry: Optional[NodeRegistry] = None):
        self.registry = registry or NodeRegistry()
        self.logger = logger.bind(component="node_factory")
    
    def create_node(self, context: NodeExecutionContext) -> Optional[BaseNode]:
        """Create a node instance from context."""
        # Get node type key
        type_key = context.node.type_version
        
        # Handle legacy node types
        if not type_key:
            # Map basic node types to type keys
            type_mapping = {
                NodeType.MANUAL: "manual.trigger",
                NodeType.WEBHOOK: "webhook.trigger",
                NodeType.SCHEDULE: "schedule.trigger",
                NodeType.SET: "set",
                NodeType.FUNCTION: "function",
                NodeType.MERGE: "merge",
            }
            type_key = type_mapping.get(context.node.type)
        
        if not type_key:
            self.logger.warning(
                "Unknown node type",
                node_type=context.node.type,
                type_version=context.node.type_version
            )
            return None
        
        # Get node class
        node_class = self.registry.get_node_class(type_key)
        if not node_class:
            self.logger.warning("Node type not registered", type_key=type_key)
            return None
        
        # Create instance
        try:
            node = node_class(context)
            self.logger.debug("Created node instance", type_key=type_key, node_id=context.node.id)
            return node
            
        except Exception as e:
            self.logger.error(
                "Failed to create node instance",
                type_key=type_key,
                error=str(e),
                error_type=type(e).__name__
            )
            raise
    
    def get_node_definition(self, type_key: str) -> Optional[NodeDefinition]:
        """Get node definition by type key."""
        return self.registry.get_node_definition(type_key)
    
    def list_available_nodes(self) -> List[NodeDefinition]:
        """List all available node definitions."""
        definitions = []
        for type_key in self.registry.get_all_nodes():
            definition = self.registry.get_node_definition(type_key)
            if definition:
                definitions.append(definition)
        return definitions


# Global registry instance
default_registry = NodeRegistry()

# Global factory instance
default_factory = NodeFactory(default_registry)


class NodeDiscoveryService:
    """Service for discovering and managing nodes across cluster."""
    
    def __init__(self, registry: NodeRegistry, redis_client=None):
        self.registry = registry
        self.redis = redis_client or get_redis_client()
        self.discovery_key = "budflow:node_discovery"
        self.logger = logger.bind(component="node_discovery")
        
        # Discovery state
        self.is_running = False
        self.discovery_task: Optional[asyncio.Task] = None
        self.discovery_interval = 60  # seconds
    
    async def start_discovery(self) -> None:
        """Start node discovery service."""
        if self.is_running:
            return
        
        self.is_running = True
        self.discovery_task = asyncio.create_task(self._discovery_loop())
        self.logger.info("Started node discovery service")
    
    async def stop_discovery(self) -> None:
        """Stop node discovery service."""
        if not self.is_running:
            return
        
        self.is_running = False
        if self.discovery_task:
            self.discovery_task.cancel()
            try:
                await self.discovery_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Stopped node discovery service")
    
    async def _discovery_loop(self) -> None:
        """Main discovery loop."""
        while self.is_running:
            try:
                await self._perform_discovery()
                await asyncio.sleep(self.discovery_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Discovery loop error: {e}")
                await asyncio.sleep(min(self.discovery_interval, 30))
    
    async def _perform_discovery(self) -> None:
        """Perform node discovery."""
        try:
            # Discover local nodes
            local_nodes = await self.registry.discover_nodes()
            
            # Publish discovery info
            discovery_info = {
                "instance_id": os.environ.get("BUDFLOW_INSTANCE_ID", "unknown"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "nodes": {
                    key: metadata.model_dump() 
                    for key, metadata in local_nodes.items()
                },
                "registry_size": len(self.registry.enhanced_nodes)
            }
            
            await self.redis.hset(
                self.discovery_key,
                mapping={
                    discovery_info["instance_id"]: json.dumps(discovery_info)
                }
            )
            
            # Sync with cluster
            await self.registry.sync_with_cluster()
            
        except Exception as e:
            self.logger.error(f"Discovery failed: {e}")
    
    async def get_cluster_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get nodes from all instances in cluster."""
        try:
            discovery_data = await self.redis.hgetall(self.discovery_key)
            
            cluster_info = {}
            for instance_id, info_json in discovery_data.items():
                try:
                    instance_id = instance_id.decode() if isinstance(instance_id, bytes) else instance_id
                    info_json = info_json.decode() if isinstance(info_json, bytes) else info_json
                    info = json.loads(info_json)
                    cluster_info[instance_id] = info
                except Exception as e:
                    self.logger.warning(f"Failed to parse discovery info for {instance_id}: {e}")
            
            return cluster_info
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster nodes: {e}")
            return {}
    
    async def check_node_health(self) -> Dict[str, Any]:
        """Check health of node discovery service."""
        try:
            cluster_nodes = await self.get_cluster_nodes()
            
            return {
                "is_running": self.is_running,
                "local_nodes": len(self.registry.enhanced_nodes),
                "cluster_instances": len(cluster_nodes),
                "last_discovery": datetime.now(timezone.utc).isoformat(),
                "discovery_interval": self.discovery_interval
            }
            
        except Exception as e:
            return {
                "is_running": self.is_running,
                "error": str(e),
                "status": "unhealthy"
            }


# Global discovery service
default_discovery_service = NodeDiscoveryService(default_registry)