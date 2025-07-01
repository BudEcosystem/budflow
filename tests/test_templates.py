"""Test Template System with versioning."""

import pytest
import asyncio
import json
import tempfile
from unittest.mock import AsyncMock, Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set
from uuid import UUID, uuid4
from pathlib import Path

from budflow.templates.manager import (
    TemplateManager,
    WorkflowTemplate,
    TemplateVersion,
    TemplateMetadata,
    TemplateCategory,
    TemplateRegistry,
    TemplateValidator,
    TemplateInstantiator,
    ParameterDefinition,
    ParameterType,
    TemplateParameter,
    TemplateVariable,
    TemplateCondition,
    ConditionalBlock,
    TemplateLoop,
    TemplateInclude,
    TemplateInheritance,
    TemplateEngine,
    TemplateContext,
    TemplateError,
    ValidationResult,
    InstantiationResult,
    TemplateSearchQuery,
    TemplateSearchResult,
    TemplateCollection,
    TemplateLibrary,
    TemplateImporter,
    TemplateExporter,
    TemplateFormat,
    TemplateStatus,
    VisibilityLevel,
    LicenseType,
    CompatibilityInfo,
    TemplateReview,
    TemplateRating,
    TemplateUsageStats,
    TemplateAnalytics,
    TemplateDependency,
    DependencyType,
    VersionConstraint,
    SemanticVersion,
    VersionRange,
    TemplateManifest,
    NodeTemplate,
    ConnectionTemplate,
    WorkflowStructure,
    TemplateNode,
    TemplateConnection,
    PlaceholderValue,
    VariableResolver,
    ExpressionEvaluator,
    TemplateRenderer,
    RenderContext,
    RenderOptions,
    OutputFormat,
)


@pytest.fixture
def template_manager():
    """Create TemplateManager for testing."""
    return TemplateManager()


@pytest.fixture
def sample_workflow_template():
    """Create sample workflow template."""
    return WorkflowTemplate(
        template_id=uuid4(),
        name="http-data-processor",
        display_name="HTTP Data Processor",
        description="Template for processing data from HTTP endpoints",
        category=TemplateCategory.DATA_PROCESSING,
        author="BudFlow Team",
        version="1.0.0",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=TemplateStatus.PUBLISHED,
        visibility=VisibilityLevel.PUBLIC,
        license=LicenseType.MIT,
        tags=["http", "data", "processing"],
        parameters=[],
        workflow_structure={},
        compatibility=CompatibilityInfo(
            min_version="1.0.0",
            max_version="2.0.0",
            supported_features=["webhooks", "expressions"]
        ),
        usage_count=150,
        rating=4.5,
        review_count=25
    )


@pytest.fixture
def sample_parameter_definition():
    """Create sample parameter definition."""
    return ParameterDefinition(
        name="api_endpoint",
        display_name="API Endpoint",
        description="The HTTP endpoint to fetch data from",
        parameter_type=ParameterType.STRING,
        required=True,
        default_value="https://api.example.com/data",
        validation_pattern=r"^https?://.*",
        placeholder="Enter API endpoint URL",
        help_text="Must be a valid HTTP/HTTPS URL",
        constraints={
            "min_length": 10,
            "max_length": 500
        },
        options=None,
        depends_on=None,
        group="Connection Settings"
    )


@pytest.fixture
def sample_template_version():
    """Create sample template version."""
    return TemplateVersion(
        version_id=uuid4(),
        template_id=uuid4(),
        version_number="1.2.0",
        changelog="Added support for authentication headers",
        created_at=datetime.now(timezone.utc),
        created_by=uuid4(),
        is_stable=True,
        is_deprecated=False,
        compatibility=CompatibilityInfo(
            min_version="1.0.0",
            max_version="2.0.0"
        ),
        download_count=75,
        metadata={}
    )


@pytest.fixture
def sample_template_context():
    """Create sample template context."""
    return TemplateContext(
        variables={
            "api_endpoint": "https://api.example.com/users",
            "timeout": 30,
            "retry_count": 3,
            "output_format": "json"
        },
        constants={
            "max_retries": 5,
            "default_timeout": 60
        },
        functions={
            "format_url": lambda base, path: f"{base.rstrip('/')}/{path.lstrip('/')}",
            "get_timestamp": lambda: datetime.now().isoformat()
        },
        metadata={
            "user_id": str(uuid4()),
            "session_id": str(uuid4())
        }
    )


@pytest.fixture
def sample_node_template():
    """Create sample node template."""
    return NodeTemplate(
        node_id="{{ node_prefix }}_http_request",
        node_type="action.http",
        display_name="{{ endpoint_name }} Request",
        description="HTTP request to {{ api_endpoint }}",
        position={"x": "{{ position_x }}", "y": "{{ position_y }}"},
        parameters={
            "url": "{{ api_endpoint }}",
            "method": "{{ http_method | default('GET') }}",
            "timeout": "{{ timeout | default(30) }}",
            "headers": {
                "Content-Type": "application/json",
                "User-Agent": "{{ user_agent | default('BudFlow/1.0') }}"
            }
        },
        conditions=[
            TemplateCondition(
                expression="{{ enable_auth }}",
                then_block={"parameters": {"auth": {"type": "bearer", "token": "{{ auth_token }}"}}},
                else_block=None
            )
        ]
    )


@pytest.mark.unit
class TestWorkflowTemplate:
    """Test WorkflowTemplate model."""
    
    def test_template_creation(self, sample_workflow_template):
        """Test creating workflow template."""
        assert sample_workflow_template.name == "http-data-processor"
        assert sample_workflow_template.display_name == "HTTP Data Processor"
        assert sample_workflow_template.category == TemplateCategory.DATA_PROCESSING
        assert sample_workflow_template.status == TemplateStatus.PUBLISHED
        assert sample_workflow_template.visibility == VisibilityLevel.PUBLIC
        assert "http" in sample_workflow_template.tags
    
    def test_template_serialization(self, sample_workflow_template):
        """Test template serialization."""
        data = sample_workflow_template.model_dump()
        
        assert "template_id" in data
        assert "name" in data
        assert "version" in data
        assert "category" in data
        
        # Test deserialization
        restored = WorkflowTemplate.model_validate(data)
        assert restored.name == sample_workflow_template.name
        assert restored.version == sample_workflow_template.version
    
    def test_template_version_validation(self):
        """Test template version validation."""
        # Valid semantic versions
        valid_versions = ["1.0.0", "2.1.3", "0.0.1", "1.0.0-alpha.1"]
        for version in valid_versions:
            template = WorkflowTemplate(
                name="test-template",
                display_name="Test Template",
                description="Test template",
                category=TemplateCategory.GENERAL,
                author="Test Author",
                version=version,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                workflow_structure={}
            )
            assert template.version == version
        
        # Invalid versions should raise ValueError
        invalid_versions = ["1.0", "1", "v1.0.0", ""]
        for version in invalid_versions:
            with pytest.raises(ValueError):
                WorkflowTemplate(
                    name="test-template",
                    display_name="Test Template",
                    description="Test template",
                    category=TemplateCategory.GENERAL,
                    author="Test Author",
                    version=version,
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                    workflow_structure={}
                )
    
    def test_template_parameter_management(self, sample_workflow_template, sample_parameter_definition):
        """Test template parameter management."""
        # Add parameter
        sample_workflow_template.add_parameter(sample_parameter_definition)
        assert len(sample_workflow_template.parameters) == 1
        assert sample_workflow_template.parameters[0].name == "api_endpoint"
        
        # Get parameter by name
        param = sample_workflow_template.get_parameter("api_endpoint")
        assert param is not None
        assert param.display_name == "API Endpoint"
        
        # Remove parameter
        sample_workflow_template.remove_parameter("api_endpoint")
        assert len(sample_workflow_template.parameters) == 0
    
    def test_template_compatibility_check(self, sample_workflow_template):
        """Test template compatibility checking."""
        # Compatible version
        assert sample_workflow_template.is_compatible_with("1.5.0")
        
        # Incompatible versions
        assert not sample_workflow_template.is_compatible_with("0.9.0")
        assert not sample_workflow_template.is_compatible_with("3.0.0")


@pytest.mark.unit
class TestParameterDefinition:
    """Test ParameterDefinition model."""
    
    def test_parameter_creation(self, sample_parameter_definition):
        """Test creating parameter definition."""
        assert sample_parameter_definition.name == "api_endpoint"
        assert sample_parameter_definition.parameter_type == ParameterType.STRING
        assert sample_parameter_definition.required is True
        assert sample_parameter_definition.default_value == "https://api.example.com/data"
    
    def test_parameter_validation(self, sample_parameter_definition):
        """Test parameter value validation."""
        # Valid value
        assert sample_parameter_definition.validate_value("https://api.example.com/users") is True
        
        # Invalid value (doesn't match pattern)
        assert sample_parameter_definition.validate_value("not-a-url") is False
        
        # Empty value for required parameter
        assert sample_parameter_definition.validate_value("") is False
    
    def test_parameter_type_coercion(self):
        """Test parameter type coercion."""
        # Integer parameter
        int_param = ParameterDefinition(
            name="timeout",
            parameter_type=ParameterType.INTEGER,
            required=True
        )
        
        # Should coerce string to int
        assert int_param.coerce_value("30") == 30
        assert int_param.coerce_value(30) == 30
        
        # Boolean parameter
        bool_param = ParameterDefinition(
            name="enabled",
            parameter_type=ParameterType.BOOLEAN,
            required=True
        )
        
        # Should coerce string to bool
        assert bool_param.coerce_value("true") is True
        assert bool_param.coerce_value("false") is False
        assert bool_param.coerce_value(True) is True
    
    def test_parameter_options_validation(self):
        """Test parameter with predefined options."""
        param = ParameterDefinition(
            name="method",
            parameter_type=ParameterType.STRING,
            required=True,
            options=["GET", "POST", "PUT", "DELETE"]
        )
        
        # Valid option
        assert param.validate_value("GET") is True
        
        # Invalid option
        assert param.validate_value("PATCH") is False


@pytest.mark.unit
class TestTemplateVersion:
    """Test TemplateVersion model."""
    
    def test_version_creation(self, sample_template_version):
        """Test creating template version."""
        assert sample_template_version.version_number == "1.2.0"
        assert sample_template_version.is_stable is True
        assert sample_template_version.is_deprecated is False
        assert sample_template_version.download_count == 75
    
    def test_version_comparison(self, sample_template_version):
        """Test version comparison."""
        newer_version = TemplateVersion(
            template_id=sample_template_version.template_id,
            version_number="1.3.0",
            created_at=datetime.now(timezone.utc),
            created_by=uuid4()
        )
        
        assert sample_template_version.is_newer_than("1.1.0")
        assert not sample_template_version.is_newer_than("1.3.0")
        assert newer_version.is_newer_than(sample_template_version.version_number)
    
    def test_version_stability(self):
        """Test version stability checking."""
        stable_version = TemplateVersion(
            template_id=uuid4(),
            version_number="2.0.0",
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            is_stable=True
        )
        
        beta_version = TemplateVersion(
            template_id=uuid4(),
            version_number="2.1.0-beta.1",
            created_at=datetime.now(timezone.utc),
            created_by=uuid4(),
            is_stable=False
        )
        
        assert stable_version.is_stable is True
        assert beta_version.is_stable is False


@pytest.mark.unit
class TestTemplateContext:
    """Test TemplateContext model."""
    
    def test_context_creation(self, sample_template_context):
        """Test creating template context."""
        assert sample_template_context.variables["api_endpoint"] == "https://api.example.com/users"
        assert sample_template_context.constants["max_retries"] == 5
        assert "format_url" in sample_template_context.functions
        assert "user_id" in sample_template_context.metadata
    
    def test_context_variable_resolution(self, sample_template_context):
        """Test variable resolution."""
        # Get variable value
        assert sample_template_context.get_variable("api_endpoint") == "https://api.example.com/users"
        assert sample_template_context.get_variable("nonexistent") is None
        assert sample_template_context.get_variable("nonexistent", "default") == "default"
        
        # Set variable value
        sample_template_context.set_variable("new_var", "new_value")
        assert sample_template_context.get_variable("new_var") == "new_value"
    
    def test_context_function_execution(self, sample_template_context):
        """Test function execution."""
        format_url = sample_template_context.functions["format_url"]
        result = format_url("https://api.example.com/", "/users")
        assert result == "https://api.example.com/users"
        
        get_timestamp = sample_template_context.functions["get_timestamp"]
        timestamp = get_timestamp()
        assert isinstance(timestamp, str)
        assert "T" in timestamp  # ISO format
    
    def test_context_merging(self, sample_template_context):
        """Test context merging."""
        other_context = TemplateContext(
            variables={"new_var": "new_value", "timeout": 60},  # timeout should override
            constants={"new_constant": "value"}
        )
        
        merged = sample_template_context.merge(other_context)
        
        # Variables should be merged with override
        assert merged.get_variable("api_endpoint") == "https://api.example.com/users"
        assert merged.get_variable("new_var") == "new_value"
        assert merged.get_variable("timeout") == 60  # Overridden
        
        # Constants should be merged
        assert merged.constants["max_retries"] == 5
        assert merged.constants["new_constant"] == "value"


@pytest.mark.unit
class TestTemplateManager:
    """Test TemplateManager."""
    
    def test_manager_initialization(self, template_manager):
        """Test template manager initialization."""
        assert template_manager is not None
        assert hasattr(template_manager, 'create_template')
        assert hasattr(template_manager, 'get_template')
        assert hasattr(template_manager, 'instantiate_template')
    
    @pytest.mark.asyncio
    async def test_create_template(self, template_manager, sample_workflow_template):
        """Test creating template."""
        with patch.object(template_manager, '_validate_template') as mock_validate:
            with patch.object(template_manager, '_save_template') as mock_save:
                mock_validate.return_value = ValidationResult(is_valid=True, errors=[], warnings=[])
                mock_save.return_value = sample_workflow_template.template_id
                
                result = await template_manager.create_template(sample_workflow_template)
                
                assert result.success is True
                assert result.template_id == sample_workflow_template.template_id
                mock_validate.assert_called_once_with(sample_workflow_template)
                mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_template(self, template_manager, sample_workflow_template):
        """Test getting template."""
        with patch.object(template_manager, '_load_template') as mock_load:
            mock_load.return_value = sample_workflow_template
            
            template = await template_manager.get_template(sample_workflow_template.template_id)
            
            assert template == sample_workflow_template
            mock_load.assert_called_once_with(sample_workflow_template.template_id, None)
    
    @pytest.mark.asyncio
    async def test_instantiate_template(self, template_manager, sample_workflow_template, sample_template_context):
        """Test instantiating template."""
        with patch.object(template_manager, '_load_template') as mock_load:
            with patch.object(template_manager, 'template_engine') as mock_engine:
                mock_load.return_value = sample_workflow_template
                mock_workflow = {
                    "name": "Generated Workflow",
                    "nodes": [{"id": "node1", "type": "http"}],
                    "connections": []
                }
                mock_engine.render_workflow = AsyncMock(return_value=mock_workflow)
                
                result = await template_manager.instantiate_template(
                    template_id=sample_workflow_template.template_id,
                    context=sample_template_context,
                    workflow_name="Test Workflow"
                )
                
                assert result.success is True
                assert result.workflow_data is not None
                assert result.workflow_data["name"] == "Generated Workflow"
    
    @pytest.mark.asyncio
    async def test_search_templates(self, template_manager):
        """Test searching templates."""
        search_query = TemplateSearchQuery(
            query="http",
            category=TemplateCategory.DATA_PROCESSING,
            tags=["api"],
            author="BudFlow Team",
            min_rating=4.0,
            limit=10
        )
        
        mock_results = [
            TemplateSearchResult(
                template_id=uuid4(),
                name="http-processor",
                display_name="HTTP Processor",
                description="Process HTTP data",
                version="1.0.0",
                author="BudFlow Team",
                category=TemplateCategory.DATA_PROCESSING,
                tags=["http", "api"],
                rating=4.5,
                usage_count=100,
                updated_at=datetime.now(timezone.utc)
            )
        ]
        
        with patch.object(template_manager, '_search_templates') as mock_search:
            mock_search.return_value = mock_results
            
            results = await template_manager.search_templates(search_query)
            
            assert len(results) == 1
            assert results[0].name == "http-processor"
            mock_search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_list_templates_by_category(self, template_manager):
        """Test listing templates by category."""
        mock_templates = [
            WorkflowTemplate(
                name="template1",
                display_name="Template 1",
                description="First template",
                category=TemplateCategory.DATA_PROCESSING,
                author="Author 1",
                version="1.0.0",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                workflow_structure={}
            ),
            WorkflowTemplate(
                name="template2", 
                display_name="Template 2",
                description="Second template",
                category=TemplateCategory.DATA_PROCESSING,
                author="Author 2",
                version="1.0.0",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                workflow_structure={}
            )
        ]
        
        with patch.object(template_manager, '_load_templates_by_category') as mock_load:
            mock_load.return_value = mock_templates
            
            templates = await template_manager.list_templates_by_category(TemplateCategory.DATA_PROCESSING)
            
            assert len(templates) == 2
            assert all(t.category == TemplateCategory.DATA_PROCESSING for t in templates)


@pytest.mark.unit
class TestTemplateValidator:
    """Test TemplateValidator."""
    
    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = TemplateValidator()
        assert validator is not None
        assert hasattr(validator, 'validate_template')
    
    @pytest.mark.asyncio
    async def test_validate_template_structure(self, sample_workflow_template):
        """Test validating template structure."""
        validator = TemplateValidator()
        
        result = await validator.validate_template(sample_workflow_template)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    @pytest.mark.asyncio
    async def test_validate_template_with_errors(self):
        """Test validating template with errors."""
        validator = TemplateValidator()
        
        # Create template with missing required fields
        invalid_template = WorkflowTemplate(
            name="",  # Empty name should fail
            display_name="Test Template",
            description="",  # Empty description should fail
            category=TemplateCategory.GENERAL,
            author="Test Author",
            version="invalid-version",  # Invalid version
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            workflow_structure={}
        )
        
        result = await validator.validate_template(invalid_template)
        
        assert result.is_valid is False
        assert len(result.errors) > 0
        assert any("name" in error.lower() for error in result.errors)
        assert any("description" in error.lower() for error in result.errors)
        assert any("version" in error.lower() for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_validate_template_parameters(self, sample_parameter_definition):
        """Test validating template parameters."""
        validator = TemplateValidator()
        
        # Valid parameter
        result = await validator.validate_parameter(sample_parameter_definition)
        assert result.is_valid is True
        
        # Invalid parameter (missing required fields)
        invalid_param = ParameterDefinition(
            name="",  # Empty name
            parameter_type=ParameterType.STRING,
            required=True
        )
        
        result = await validator.validate_parameter(invalid_param)
        assert result.is_valid is False
        assert len(result.errors) > 0


@pytest.mark.unit
class TestTemplateEngine:
    """Test TemplateEngine."""
    
    def test_engine_initialization(self):
        """Test engine initialization."""
        engine = TemplateEngine()
        assert engine is not None
        assert hasattr(engine, 'render_workflow')
        assert hasattr(engine, 'render_node')
    
    @pytest.mark.asyncio
    async def test_render_node_template(self, sample_node_template, sample_template_context):
        """Test rendering node template."""
        engine = TemplateEngine()
        
        # Add required variables to context
        sample_template_context.set_variable("node_prefix", "api")
        sample_template_context.set_variable("endpoint_name", "Users")
        sample_template_context.set_variable("position_x", 100)
        sample_template_context.set_variable("position_y", 200)
        sample_template_context.set_variable("http_method", "GET")
        sample_template_context.set_variable("enable_auth", True)
        sample_template_context.set_variable("auth_token", "secret123")
        
        rendered_node = await engine.render_node(sample_node_template, sample_template_context)
        
        assert rendered_node["node_id"] == "api_http_request"
        assert rendered_node["display_name"] == "Users Request"
        assert rendered_node["parameters"]["url"] == "https://api.example.com/users"
        assert rendered_node["parameters"]["method"] == "GET"
        assert rendered_node["parameters"]["timeout"] == 30
    
    @pytest.mark.asyncio
    async def test_render_conditional_blocks(self, sample_template_context):
        """Test rendering conditional blocks."""
        engine = TemplateEngine()
        
        condition = TemplateCondition(
            expression="{{ enable_feature }}",
            then_block={"feature_enabled": True, "value": "{{ feature_value }}"},
            else_block={"feature_enabled": False, "value": "default"}
        )
        
        # Test with condition true
        sample_template_context.set_variable("enable_feature", True)
        sample_template_context.set_variable("feature_value", "custom")
        
        result = await engine.render_condition(condition, sample_template_context)
        assert result["feature_enabled"] is True
        assert result["value"] == "custom"
        
        # Test with condition false
        sample_template_context.set_variable("enable_feature", False)
        
        result = await engine.render_condition(condition, sample_template_context)
        assert result["feature_enabled"] is False
        assert result["value"] == "default"
    
    @pytest.mark.asyncio
    async def test_render_workflow_template(self, sample_template_context):
        """Test rendering complete workflow template."""
        engine = TemplateEngine()
        
        workflow_structure = {
            "name": "{{ workflow_name }}",
            "description": "{{ workflow_description }}",
            "nodes": [
                {
                    "id": "{{ node_prefix }}_start",
                    "type": "trigger.manual",
                    "parameters": {}
                },
                {
                    "id": "{{ node_prefix }}_http",
                    "type": "action.http",
                    "parameters": {
                        "url": "{{ api_endpoint }}",
                        "timeout": "{{ timeout }}"
                    }
                }
            ],
            "connections": [
                {
                    "source": "{{ node_prefix }}_start",
                    "target": "{{ node_prefix }}_http"
                }
            ]
        }
        
        # Set required variables
        sample_template_context.set_variable("workflow_name", "My API Workflow")
        sample_template_context.set_variable("workflow_description", "Fetches data from API")
        sample_template_context.set_variable("node_prefix", "api")
        
        rendered_workflow = await engine.render_workflow(workflow_structure, sample_template_context)
        
        assert rendered_workflow["name"] == "My API Workflow"
        assert rendered_workflow["description"] == "Fetches data from API"
        assert len(rendered_workflow["nodes"]) == 2
        assert rendered_workflow["nodes"][0]["id"] == "api_start"
        assert rendered_workflow["nodes"][1]["parameters"]["url"] == "https://api.example.com/users"
        assert rendered_workflow["connections"][0]["source"] == "api_start"


@pytest.mark.unit
class TestTemplateInstantiator:
    """Test TemplateInstantiator."""
    
    def test_instantiator_initialization(self):
        """Test instantiator initialization."""
        instantiator = TemplateInstantiator()
        assert instantiator is not None
        assert hasattr(instantiator, 'instantiate')
    
    @pytest.mark.asyncio
    async def test_instantiate_template(self, sample_workflow_template, sample_template_context):
        """Test instantiating template with context."""
        instantiator = TemplateInstantiator()
        
        # Set up template with simple structure
        sample_workflow_template.workflow_structure = {
            "name": "{{ workflow_name }}",
            "nodes": [],
            "connections": []
        }
        
        sample_template_context.set_variable("workflow_name", "Generated Workflow")
        
        result = await instantiator.instantiate(sample_workflow_template, sample_template_context)
        
        assert result.success is True
        assert result.workflow_data["name"] == "Generated Workflow"
    
    @pytest.mark.asyncio
    async def test_instantiate_with_validation_errors(self, sample_workflow_template, sample_template_context):
        """Test instantiation with validation errors."""
        instantiator = TemplateInstantiator()
        
        # Create template with required parameter but missing from context
        param = ParameterDefinition(
            name="required_param",
            parameter_type=ParameterType.STRING,
            required=True
        )
        sample_workflow_template.add_parameter(param)
        
        result = await instantiator.instantiate(sample_workflow_template, sample_template_context)
        
        # Should fail due to missing required parameter
        assert result.success is False
        assert len(result.errors) > 0
        assert any("required_param" in error for error in result.errors)


@pytest.mark.integration
class TestTemplateSystemIntegration:
    """Integration tests for template system."""
    
    @pytest.mark.asyncio
    async def test_full_template_lifecycle(self, template_manager):
        """Test complete template lifecycle."""
        # 1. Create template
        template = WorkflowTemplate(
            name="integration-test-template",
            display_name="Integration Test Template",
            description="Template for integration testing",
            category=TemplateCategory.TESTING,
            author="Test Suite",
            version="1.0.0",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            workflow_structure={
                "name": "{{ workflow_name }}",
                "nodes": [
                    {
                        "id": "start",
                        "type": "trigger.manual",
                        "parameters": {}
                    }
                ],
                "connections": []
            },
            parameters=[
                ParameterDefinition(
                    name="workflow_name",
                    parameter_type=ParameterType.STRING,
                    required=True,
                    default_value="My Workflow"
                )
            ]
        )
        
        with patch.object(template_manager, '_validate_template') as mock_validate:
            with patch.object(template_manager, '_save_template') as mock_save:
                with patch.object(template_manager, '_load_template') as mock_load:
                    
                    # Mock validation success
                    mock_validate.return_value = ValidationResult(is_valid=True, errors=[], warnings=[])
                    mock_save.return_value = template.template_id
                    mock_load.return_value = template
                    
                    # Create template
                    create_result = await template_manager.create_template(template)
                    assert create_result.success is True
                    
                    # Get template
                    retrieved_template = await template_manager.get_template(template.template_id)
                    assert retrieved_template.name == template.name
                    
                    # Instantiate template
                    context = TemplateContext(variables={"workflow_name": "Test Workflow"})
                    
                    with patch.object(template_manager, 'template_engine') as mock_engine:
                        mock_workflow = {
                            "name": "Test Workflow",
                            "nodes": [{"id": "start", "type": "trigger.manual", "parameters": {}}],
                            "connections": []
                        }
                        mock_engine.render_workflow = AsyncMock(return_value=mock_workflow)
                        
                        instantiate_result = await template_manager.instantiate_template(
                            template_id=template.template_id,
                            context=context
                        )
                        
                        assert instantiate_result.success is True
                        assert instantiate_result.workflow_data["name"] == "Test Workflow"
    
    @pytest.mark.asyncio
    async def test_template_versioning_workflow(self, template_manager):
        """Test template versioning workflow."""
        template_id = uuid4()
        
        # Create multiple versions
        versions = [
            TemplateVersion(
                template_id=template_id,
                version_number="1.0.0",
                created_at=datetime.now(timezone.utc) - timedelta(days=10),
                created_by=uuid4(),
                is_stable=True
            ),
            TemplateVersion(
                template_id=template_id,
                version_number="1.1.0",
                created_at=datetime.now(timezone.utc) - timedelta(days=5),
                created_by=uuid4(),
                is_stable=True
            ),
            TemplateVersion(
                template_id=template_id,
                version_number="1.2.0-beta.1",
                created_at=datetime.now(timezone.utc),
                created_by=uuid4(),
                is_stable=False
            )
        ]
        
        with patch.object(template_manager, '_load_template_versions') as mock_load_versions:
            mock_load_versions.return_value = versions
            
            # Get all versions
            all_versions = await template_manager.get_template_versions(template_id)
            assert len(all_versions) == 3
            
            # Get stable versions only
            stable_versions = await template_manager.get_template_versions(template_id, stable_only=True)
            assert len(stable_versions) == 2
            assert all(v.is_stable for v in stable_versions)
            
            # Get latest stable version
            latest_stable = await template_manager.get_latest_version(template_id, stable_only=True)
            assert latest_stable.version_number == "1.1.0"
    
    @pytest.mark.asyncio
    async def test_template_collection_management(self, template_manager):
        """Test template collection management."""
        collection = TemplateCollection(
            collection_id=uuid4(),
            name="Data Processing Templates",
            description="Collection of templates for data processing workflows",
            owner_id=uuid4(),
            created_at=datetime.now(timezone.utc),
            template_ids=[],
            tags=["data", "processing"],
            is_public=True
        )
        
        template_ids = [uuid4(), uuid4(), uuid4()]
        
        with patch.object(template_manager, '_save_collection') as mock_save:
            with patch.object(template_manager, '_load_collection') as mock_load:
                mock_save.return_value = collection.collection_id
                mock_load.return_value = collection
                
                # Create collection
                create_result = await template_manager.create_collection(collection)
                assert create_result.success is True
                
                # Add templates to collection
                for template_id in template_ids:
                    add_result = await template_manager.add_template_to_collection(
                        collection.collection_id,
                        template_id
                    )
                    assert add_result is True
                
                # Get collection with templates
                updated_collection = await template_manager.get_collection(collection.collection_id)
                assert len(updated_collection.template_ids) == len(template_ids)


@pytest.mark.performance
class TestTemplateSystemPerformance:
    """Performance tests for template system."""
    
    @pytest.mark.asyncio
    async def test_large_template_instantiation(self):
        """Test instantiating large template with many nodes."""
        engine = TemplateEngine()
        
        # Create large workflow structure (1000 nodes)
        nodes = []
        connections = []
        
        for i in range(1000):
            nodes.append({
                "id": f"node_{i}",
                "type": "action.http",
                "parameters": {
                    "url": f"https://api{i}.example.com",
                    "timeout": "{{ timeout }}"
                }
            })
            
            if i > 0:
                connections.append({
                    "source": f"node_{i-1}",
                    "target": f"node_{i}"
                })
        
        workflow_structure = {
            "name": "{{ workflow_name }}",
            "nodes": nodes,
            "connections": connections
        }
        
        context = TemplateContext(variables={
            "workflow_name": "Large Workflow",
            "timeout": 30
        })
        
        import time
        start_time = time.time()
        
        rendered_workflow = await engine.render_workflow(workflow_structure, context)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should render 1000 nodes within reasonable time (< 5 seconds)
        assert duration < 5.0
        assert len(rendered_workflow["nodes"]) == 1000
        assert len(rendered_workflow["connections"]) == 999
    
    @pytest.mark.asyncio
    async def test_template_search_performance(self, template_manager):
        """Test template search performance with large dataset."""
        # Mock large search result set
        large_result_set = [
            TemplateSearchResult(
                template_id=uuid4(),
                name=f"template-{i}",
                display_name=f"Template {i}",
                description=f"Description for template {i}",
                version="1.0.0",
                author="Test Author",
                category=TemplateCategory.GENERAL,
                tags=["test"],
                rating=4.0 + (i % 10) / 10,
                usage_count=i * 10,
                updated_at=datetime.now(timezone.utc)
            )
            for i in range(5000)
        ]
        
        with patch.object(template_manager, '_search_templates') as mock_search:
            mock_search.return_value = large_result_set
            
            search_query = TemplateSearchQuery(query="template", limit=5000)
            
            import time
            start_time = time.time()
            
            results = await template_manager.search_templates(search_query)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Should search through 5000 templates quickly (< 2 seconds)
            assert duration < 2.0
            assert len(results) == 5000
    
    @pytest.mark.asyncio
    async def test_concurrent_template_instantiation(self, template_manager):
        """Test concurrent template instantiation."""
        # Create template
        template = WorkflowTemplate(
            name="concurrent-test",
            display_name="Concurrent Test",
            description="Template for concurrency testing",
            category=TemplateCategory.TESTING,
            author="Test Suite",
            version="1.0.0",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            workflow_structure={
                "name": "{{ workflow_name }}",
                "nodes": [{"id": "start", "type": "trigger.manual"}],
                "connections": []
            }
        )
        
        async def instantiate_template(instance_id):
            """Instantiate template with unique context."""
            context = TemplateContext(variables={
                "workflow_name": f"Workflow {instance_id}"
            })
            
            with patch.object(template_manager, '_load_template') as mock_load:
                with patch.object(template_manager, 'template_engine') as mock_engine:
                    mock_load.return_value = template
                    mock_engine.render_workflow = AsyncMock(return_value={
                        "name": f"Workflow {instance_id}",
                        "nodes": [{"id": "start", "type": "trigger.manual"}],
                        "connections": []
                    })
                    
                    return await template_manager.instantiate_template(
                        template_id=template.template_id,
                        context=context
                    )
        
        import time
        start_time = time.time()
        
        # Run 50 concurrent instantiations
        results = await asyncio.gather(*[
            instantiate_template(i) for i in range(50)
        ])
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should handle 50 concurrent instantiations within reasonable time (< 3 seconds)
        assert duration < 3.0
        assert len(results) == 50
        assert all(result.success for result in results)