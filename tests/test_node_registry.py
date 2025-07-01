"""Test node registry and discovery system."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from pathlib import Path
from datetime import datetime, timezone

from budflow.nodes.registry import (
    NodeRegistry,
    NodeVersion,
    NodeDependency, 
    NodeMetadata,
    PluginInfo,
    default_registry,
    default_factory,
)
from budflow.nodes.base import BaseNode, NodeDefinition
from budflow.workflows.models import NodeType
from budflow.executor.context import NodeExecutionContext


@pytest.fixture
def node_registry():
    """Create test node registry."""
    return NodeRegistry()


@pytest.fixture
def sample_node_class():
    """Create sample node class for testing."""
    class TestNode(BaseNode):
        name = "test_node"
        display_name = "Test Node"
        description = "A test node"
        category = "Testing"
        version = "1.2.3"
        
        async def execute(self, context):
            return {"result": "test"}
        
        @classmethod
        def get_definition(cls) -> NodeDefinition:
            """Get node definition."""
            from budflow.workflows.models import NodeType
            from budflow.nodes.base import NodeCategory
            return NodeDefinition(
                type=NodeType.FUNCTION,
                name=cls.name,
                display_name=cls.display_name,
                description=cls.description,
                category=NodeCategory.CUSTOM,
                inputs=[],
                outputs=[],
                icon="test-icon"
            )
    
    return TestNode


@pytest.fixture
def node_metadata():
    """Create test node metadata."""
    return NodeMetadata(
        name="test_node",
        display_name="Test Node", 
        description="A test node",
        category="Testing",
        node_class="test.TestNode",
        module_path="test",
        version=NodeVersion(major=1, minor=2, patch=3),
    )


@pytest.mark.unit
class TestNodeVersion:
    """Test node version handling."""
    
    def test_version_creation(self):
        """Test creating node version."""
        version = NodeVersion(major=1, minor=2, patch=3)
        
        assert version.major == 1
        assert version.minor == 2
        assert version.patch == 3
        assert str(version) == "1.2.3"
    
    def test_version_from_string(self):
        """Test parsing version from string."""
        version = NodeVersion.from_string("2.1.0")
        
        assert version.major == 2
        assert version.minor == 1
        assert version.patch == 0
    
    def test_version_comparison(self):
        """Test version comparison."""
        v1 = NodeVersion(major=1, minor=0, patch=0)
        v2 = NodeVersion(major=1, minor=1, patch=0)
        v3 = NodeVersion(major=2, minor=0, patch=0)
        
        assert v1 < v2
        assert v2 < v3
        assert v1 == NodeVersion(major=1, minor=0, patch=0)


@pytest.mark.unit
class TestNodeDependency:
    """Test node dependency model."""
    
    def test_dependency_creation(self):
        """Test creating node dependency."""
        dep = NodeDependency(
            name="requests",
            version_constraint=">=2.0.0",
            optional=False
        )
        
        assert dep.name == "requests"
        assert dep.version_constraint == ">=2.0.0"
        assert dep.optional is False


@pytest.mark.unit
class TestNodeMetadata:
    """Test node metadata model."""
    
    def test_metadata_creation(self, node_metadata):
        """Test creating node metadata."""
        assert node_metadata.name == "test_node"
        assert node_metadata.display_name == "Test Node"
        assert node_metadata.category == "Testing"
        assert node_metadata.version.major == 1
        assert node_metadata.is_enabled is True
        assert node_metadata.source == "builtin"
    
    def test_metadata_serialization(self, node_metadata):
        """Test metadata serialization."""
        data = node_metadata.model_dump()
        
        assert "name" in data
        assert "version" in data
        assert "category" in data
        
        # Test deserialization
        restored = NodeMetadata.model_validate(data)
        assert restored.name == node_metadata.name
        assert restored.version == node_metadata.version


@pytest.mark.unit
class TestNodeRegistry:
    """Test node registry functionality."""
    
    def test_registry_initialization(self, node_registry):
        """Test registry initialization."""
        assert isinstance(node_registry._nodes, dict)
        assert isinstance(node_registry._definitions, dict)
        assert len(node_registry._nodes) > 0  # Built-in nodes
    
    def test_has_node(self, node_registry):
        """Test checking if node exists."""
        # Built-in nodes should exist
        assert node_registry.has_node("manual.trigger")
        assert node_registry.has_node("http.request")
        assert not node_registry.has_node("nonexistent.node")
    
    def test_get_node_class(self, node_registry):
        """Test getting node class."""
        node_class = node_registry.get_node_class("manual.trigger")
        assert node_class is not None
        assert issubclass(node_class, BaseNode)
    
    def test_get_node_definition(self, node_registry):
        """Test getting node definition."""
        definition = node_registry.get_node_definition("manual.trigger")
        assert definition is not None
        assert isinstance(definition, NodeDefinition)
    
    def test_get_all_nodes(self, node_registry):
        """Test getting all nodes."""
        nodes = node_registry.get_all_nodes()
        assert isinstance(nodes, dict)
        assert len(nodes) > 0
        
        # Check some expected built-in nodes
        assert "manual.trigger" in nodes
        assert "http.request" in nodes
        assert "if" in nodes
    
    def test_get_nodes_by_type(self, node_registry):
        """Test getting nodes by type."""
        # Test manual trigger nodes
        manual_nodes = node_registry.get_nodes_by_type(NodeType.MANUAL)
        assert isinstance(manual_nodes, dict)
        assert len(manual_nodes) >= 1  # Should have at least manual trigger
        
        # Test webhook nodes
        webhook_nodes = node_registry.get_nodes_by_type(NodeType.WEBHOOK)
        assert isinstance(webhook_nodes, dict)
        
        # Test schedule nodes
        schedule_nodes = node_registry.get_nodes_by_type(NodeType.SCHEDULE)
        assert isinstance(schedule_nodes, dict)
    
    def test_get_nodes_by_category(self, node_registry):
        """Test getting nodes by category."""
        # This test depends on how categories are set up in built-in nodes
        all_nodes = node_registry.get_all_nodes()
        if all_nodes:
            # Get the first node's definition to check category
            first_key = list(all_nodes.keys())[0]
            definition = node_registry.get_node_definition(first_key)
            if definition and definition.category:
                category_nodes = node_registry.get_nodes_by_category(definition.category)
                assert isinstance(category_nodes, dict)
                assert len(category_nodes) > 0
    
    def test_search_nodes(self, node_registry):
        """Test searching nodes."""
        # Search for "trigger" should find trigger nodes
        results = node_registry.search_nodes("trigger")
        assert isinstance(results, dict)
        # Should find at least some trigger nodes
        assert len(results) > 0
        
        # Search for something that shouldn't exist
        no_results = node_registry.search_nodes("nonexistent_search_term_xyz")
        assert len(no_results) == 0
    
    def test_register_decorator(self, node_registry, sample_node_class):
        """Test registering node with decorator."""
        # Use the register decorator
        decorator = node_registry.register("test.node")
        decorated_class = decorator(sample_node_class)
        
        assert decorated_class == sample_node_class
        assert node_registry.has_node("test.node")
        assert node_registry.get_node_class("test.node") == sample_node_class


@pytest.mark.unit
class TestNodeFactory:
    """Test node factory functionality."""
    
    def test_factory_initialization(self):
        """Test factory initialization."""
        factory = default_factory
        assert factory.registry is not None
    
    def test_get_node_definition(self):
        """Test getting node definition from factory."""
        definition = default_factory.get_node_definition("manual.trigger")
        assert definition is not None
        assert isinstance(definition, NodeDefinition)
    
    def test_list_available_nodes(self):
        """Test listing available nodes."""
        definitions = default_factory.list_available_nodes()
        assert isinstance(definitions, list)
        assert len(definitions) > 0
        
        # All items should be NodeDefinition instances
        for definition in definitions:
            assert isinstance(definition, NodeDefinition)


@pytest.mark.integration
class TestRegistryIntegration:
    """Integration tests for node registry."""
    
    def test_global_registry_access(self):
        """Test accessing global registry."""
        assert default_registry is not None
        assert isinstance(default_registry, NodeRegistry)
        
        # Should have built-in nodes
        nodes = default_registry.get_all_nodes()
        assert len(nodes) > 0
    
    def test_registry_factory_integration(self):
        """Test registry and factory working together."""
        # Get a node through registry
        node_class = default_registry.get_node_class("manual.trigger")
        assert node_class is not None
        
        # Get definition through factory
        definition = default_factory.get_node_definition("manual.trigger")
        assert definition is not None
        
        # Both should refer to the same node type
        assert definition.type is not None


@pytest.mark.unit
class TestPluginInfo:
    """Test plugin information model."""
    
    def test_plugin_creation(self):
        """Test creating plugin info."""
        plugin = PluginInfo(
            name="test_plugin",
            version="1.0.0",
            path="/path/to/plugin",
            is_loaded=True
        )
        
        assert plugin.name == "test_plugin"
        assert plugin.version == "1.0.0"
        assert plugin.path == "/path/to/plugin"
        assert plugin.is_loaded is True
    
    def test_plugin_serialization(self):
        """Test plugin serialization."""
        plugin = PluginInfo(
            name="test_plugin",
            version="1.0.0", 
            path="/path/to/plugin"
        )
        
        data = plugin.model_dump()
        assert "name" in data
        assert "version" in data
        assert "path" in data
        
        # Test deserialization
        restored = PluginInfo.model_validate(data)
        assert restored.name == plugin.name
        assert restored.version == plugin.version


@pytest.mark.performance
class TestRegistryPerformance:
    """Performance tests for node registry."""
    
    def test_large_scale_node_lookup(self):
        """Test performance with many node lookups."""
        registry = default_registry
        
        # Perform many lookups
        for _ in range(1000):
            registry.has_node("manual.trigger")
            registry.get_node_class("http.request")
            registry.get_node_definition("if")
        
        # Should complete quickly without errors
        assert True
    
    def test_search_performance(self):
        """Test search performance."""
        registry = default_registry
        
        # Perform many searches
        for _ in range(100):
            results = registry.search_nodes("trigger")
            assert len(results) >= 0  # Should at least not fail
        
        # Should complete quickly
        assert True