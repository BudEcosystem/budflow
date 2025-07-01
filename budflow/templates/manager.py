"""Template System with versioning for BudFlow.

This module provides comprehensive template management capabilities including
template creation, versioning, validation, instantiation, and rendering.
"""

import asyncio
import json
import re
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Set, Tuple, Callable
from uuid import UUID, uuid4

import structlog
from pydantic import BaseModel, Field, ConfigDict, field_validator

logger = structlog.get_logger()


class TemplateCategory(str, Enum):
    """Template categories."""
    GENERAL = "general"
    DATA_PROCESSING = "data_processing"
    INTEGRATION = "integration"
    AUTOMATION = "automation"
    MONITORING = "monitoring"
    NOTIFICATION = "notification"
    TRANSFORMATION = "transformation"
    WEBHOOK = "webhook"
    API = "api"
    DATABASE = "database"
    TESTING = "testing"


class TemplateStatus(str, Enum):
    """Template status."""
    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class VisibilityLevel(str, Enum):
    """Template visibility levels."""
    PRIVATE = "private"
    ORGANIZATION = "organization"
    PUBLIC = "public"


class LicenseType(str, Enum):
    """License types."""
    MIT = "mit"
    APACHE = "apache"
    GPL = "gpl"
    BSD = "bsd"
    PROPRIETARY = "proprietary"
    CUSTOM = "custom"


class ParameterType(str, Enum):
    """Parameter types."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    FILE = "file"
    SECRET = "secret"


class TemplateFormat(str, Enum):
    """Template formats."""
    JSON = "json"
    YAML = "yaml"
    XML = "xml"


class OutputFormat(str, Enum):
    """Output formats."""
    JSON = "json"
    YAML = "yaml"
    WORKFLOW = "workflow"


class DependencyType(str, Enum):
    """Dependency types."""
    TEMPLATE = "template"
    NODE = "node"
    CREDENTIAL = "credential"
    WEBHOOK = "webhook"


class TemplateError(Exception):
    """Base exception for template operations."""
    pass


class CompatibilityInfo(BaseModel):
    """Compatibility information."""
    
    min_version: str = Field(..., description="Minimum compatible version")
    max_version: Optional[str] = Field(None, description="Maximum compatible version")
    supported_features: List[str] = Field(default_factory=list, description="Supported features")
    
    model_config = ConfigDict(from_attributes=True)


class ParameterDefinition(BaseModel):
    """Parameter definition model."""
    
    name: str = Field(..., description="Parameter name")
    display_name: str = Field(default="", description="Display name")
    description: str = Field(default="", description="Parameter description")
    parameter_type: ParameterType = Field(..., description="Parameter type")
    required: bool = Field(default=False, description="Whether parameter is required")
    default_value: Optional[Any] = Field(None, description="Default value")
    validation_pattern: Optional[str] = Field(None, description="Validation regex pattern")
    placeholder: Optional[str] = Field(None, description="Placeholder text")
    help_text: Optional[str] = Field(None, description="Help text")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Value constraints")
    options: Optional[List[str]] = Field(None, description="Predefined options")
    depends_on: Optional[str] = Field(None, description="Parameter dependency")
    group: Optional[str] = Field(None, description="Parameter group")
    
    model_config = ConfigDict(from_attributes=True)
    
    def validate_value(self, value: Any) -> bool:
        """Validate parameter value."""
        # Check if required
        if self.required and (value is None or value == ""):
            return False
        
        # Check pattern if provided
        if self.validation_pattern and value:
            if not re.match(self.validation_pattern, str(value)):
                return False
        
        # Check options if provided
        if self.options and value not in self.options:
            return False
        
        # Check constraints
        if self.constraints:
            if "min_length" in self.constraints and len(str(value)) < self.constraints["min_length"]:
                return False
            if "max_length" in self.constraints and len(str(value)) > self.constraints["max_length"]:
                return False
        
        return True
    
    def coerce_value(self, value: Any) -> Any:
        """Coerce value to parameter type."""
        if self.parameter_type == ParameterType.INTEGER:
            return int(value) if value is not None else None
        elif self.parameter_type == ParameterType.FLOAT:
            return float(value) if value is not None else None
        elif self.parameter_type == ParameterType.BOOLEAN:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)
        elif self.parameter_type == ParameterType.STRING:
            return str(value) if value is not None else None
        return value


class TemplateVersion(BaseModel):
    """Template version model."""
    
    version_id: UUID = Field(default_factory=uuid4, description="Version ID")
    template_id: UUID = Field(..., description="Template ID")
    version_number: str = Field(..., description="Version number (semver)")
    changelog: Optional[str] = Field(None, description="Version changelog")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: UUID = Field(..., description="Creator user ID")
    is_stable: bool = Field(default=True, description="Whether version is stable")
    is_deprecated: bool = Field(default=False, description="Whether version is deprecated")
    compatibility: Optional[CompatibilityInfo] = Field(None, description="Compatibility info")
    download_count: int = Field(default=0, description="Download count")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(from_attributes=True)
    
    def is_newer_than(self, other_version: str) -> bool:
        """Check if this version is newer than another."""
        # Simplified version comparison - in production use semver library
        self_parts = [int(x) for x in self.version_number.split("-")[0].split(".")]
        other_parts = [int(x) for x in other_version.split("-")[0].split(".")]
        
        for i in range(max(len(self_parts), len(other_parts))):
            self_part = self_parts[i] if i < len(self_parts) else 0
            other_part = other_parts[i] if i < len(other_parts) else 0
            
            if self_part > other_part:
                return True
            elif self_part < other_part:
                return False
        
        return False


class WorkflowTemplate(BaseModel):
    """Workflow template model."""
    
    template_id: UUID = Field(default_factory=uuid4, description="Template ID")
    name: str = Field(..., description="Template name")
    display_name: str = Field(..., description="Display name")
    description: str = Field(..., description="Template description")
    category: TemplateCategory = Field(..., description="Template category")
    author: str = Field(..., description="Template author")
    version: str = Field(..., description="Current version")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: TemplateStatus = Field(default=TemplateStatus.DRAFT, description="Template status")
    visibility: VisibilityLevel = Field(default=VisibilityLevel.PRIVATE, description="Visibility level")
    license: LicenseType = Field(default=LicenseType.MIT, description="License type")
    tags: List[str] = Field(default_factory=list, description="Template tags")
    parameters: List[ParameterDefinition] = Field(default_factory=list, description="Template parameters")
    workflow_structure: Dict[str, Any] = Field(..., description="Workflow structure")
    compatibility: Optional[CompatibilityInfo] = Field(None, description="Compatibility info")
    usage_count: int = Field(default=0, description="Usage count")
    rating: float = Field(default=0.0, description="Average rating")
    review_count: int = Field(default=0, description="Review count")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(from_attributes=True)
    
    @field_validator('version')
    @classmethod
    def validate_version(cls, v):
        """Validate semantic version."""
        pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9\-]+(?:\.[a-zA-Z0-9\-]+)*))?$'
        if not re.match(pattern, v):
            raise ValueError(f"Invalid semantic version: {v}")
        return v
    
    def add_parameter(self, parameter: ParameterDefinition) -> None:
        """Add parameter to template."""
        self.parameters.append(parameter)
    
    def get_parameter(self, name: str) -> Optional[ParameterDefinition]:
        """Get parameter by name."""
        for param in self.parameters:
            if param.name == name:
                return param
        return None
    
    def remove_parameter(self, name: str) -> bool:
        """Remove parameter by name."""
        for i, param in enumerate(self.parameters):
            if param.name == name:
                del self.parameters[i]
                return True
        return False
    
    def is_compatible_with(self, version: str) -> bool:
        """Check if template is compatible with given version."""
        if not self.compatibility:
            return True
        
        # Simplified version comparison
        if version < self.compatibility.min_version:
            return False
        
        if self.compatibility.max_version and version >= self.compatibility.max_version:
            return False
        
        return True


class TemplateCondition(BaseModel):
    """Template condition for conditional rendering."""
    
    expression: str = Field(..., description="Condition expression")
    then_block: Optional[Dict[str, Any]] = Field(None, description="Block to render if true")
    else_block: Optional[Dict[str, Any]] = Field(None, description="Block to render if false")
    
    model_config = ConfigDict(from_attributes=True)


class NodeTemplate(BaseModel):
    """Node template definition."""
    
    node_id: str = Field(..., description="Node ID template")
    node_type: str = Field(..., description="Node type")
    display_name: str = Field(..., description="Display name template")
    description: str = Field(..., description="Description template")
    position: Dict[str, Any] = Field(default_factory=dict, description="Position template")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Parameter templates")
    conditions: List[TemplateCondition] = Field(default_factory=list, description="Conditional blocks")
    
    model_config = ConfigDict(from_attributes=True)


class ConnectionTemplate(BaseModel):
    """Connection template definition."""
    
    source_node: str = Field(..., description="Source node ID template")
    target_node: str = Field(..., description="Target node ID template")
    source_output: str = Field(default="main", description="Source output")
    target_input: str = Field(default="main", description="Target input")
    conditions: List[TemplateCondition] = Field(default_factory=list, description="Conditional blocks")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateContext(BaseModel):
    """Template rendering context."""
    
    variables: Dict[str, Any] = Field(default_factory=dict, description="Template variables")
    constants: Dict[str, Any] = Field(default_factory=dict, description="Template constants")
    functions: Dict[str, Callable] = Field(default_factory=dict, description="Template functions")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Context metadata")
    
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)
    
    def get_variable(self, name: str, default: Any = None) -> Any:
        """Get variable value."""
        return self.variables.get(name, default)
    
    def set_variable(self, name: str, value: Any) -> None:
        """Set variable value."""
        self.variables[name] = value
    
    def merge(self, other: 'TemplateContext') -> 'TemplateContext':
        """Merge with another context."""
        merged_variables = {**self.variables, **other.variables}
        merged_constants = {**self.constants, **other.constants}
        merged_functions = {**self.functions, **other.functions}
        merged_metadata = {**self.metadata, **other.metadata}
        
        return TemplateContext(
            variables=merged_variables,
            constants=merged_constants,
            functions=merged_functions,
            metadata=merged_metadata
        )


class ValidationResult(BaseModel):
    """Template validation result."""
    
    is_valid: bool = Field(..., description="Whether template is valid")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    
    model_config = ConfigDict(from_attributes=True)


class InstantiationResult(BaseModel):
    """Template instantiation result."""
    
    success: bool = Field(..., description="Whether instantiation succeeded")
    workflow: Optional[Dict[str, Any]] = Field(None, description="Generated workflow")
    workflow_data: Optional[Dict[str, Any]] = Field(None, description="Generated workflow data (alias)")
    template_id: Optional[UUID] = Field(None, description="Template ID")
    instance_id: UUID = Field(default_factory=uuid4, description="Instance ID")
    errors: List[str] = Field(default_factory=list, description="Instantiation errors")
    warnings: List[str] = Field(default_factory=list, description="Instantiation warnings")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Instance metadata")
    
    model_config = ConfigDict(from_attributes=True)
    
    def __init__(self, **data):
        super().__init__(**data)
        # Sync workflow and workflow_data
        if self.workflow and not self.workflow_data:
            self.workflow_data = self.workflow
        elif self.workflow_data and not self.workflow:
            self.workflow = self.workflow_data


class TemplateSearchQuery(BaseModel):
    """Template search query."""
    
    query: Optional[str] = Field(None, description="Search query")
    category: Optional[TemplateCategory] = Field(None, description="Filter by category")
    tags: List[str] = Field(default_factory=list, description="Filter by tags")
    author: Optional[str] = Field(None, description="Filter by author")
    min_rating: Optional[float] = Field(None, description="Minimum rating")
    status: Optional[TemplateStatus] = Field(None, description="Filter by status")
    visibility: Optional[VisibilityLevel] = Field(None, description="Filter by visibility")
    limit: int = Field(default=20, description="Result limit")
    offset: int = Field(default=0, description="Result offset")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateSearchResult(BaseModel):
    """Template search result."""
    
    template_id: UUID = Field(..., description="Template ID")
    name: str = Field(..., description="Template name")
    display_name: str = Field(..., description="Display name")
    description: str = Field(..., description="Description")
    version: str = Field(..., description="Version")
    author: str = Field(..., description="Author")
    category: TemplateCategory = Field(..., description="Category")
    tags: List[str] = Field(default_factory=list, description="Tags")
    rating: float = Field(default=0.0, description="Rating")
    usage_count: int = Field(default=0, description="Usage count")
    updated_at: datetime = Field(..., description="Last updated")
    
    model_config = ConfigDict(from_attributes=True)


# Additional models for comprehensive system
class TemplateMetadata(BaseModel):
    """Template metadata."""
    
    created_by: UUID = Field(..., description="Creator ID")
    organization_id: Optional[UUID] = Field(None, description="Organization ID")
    project_id: Optional[UUID] = Field(None, description="Project ID")
    keywords: List[str] = Field(default_factory=list, description="Keywords")
    documentation_url: Optional[str] = Field(None, description="Documentation URL")
    source_url: Optional[str] = Field(None, description="Source code URL")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateRegistry(BaseModel):
    """Template registry."""
    
    registry_id: UUID = Field(default_factory=uuid4, description="Registry ID")
    name: str = Field(..., description="Registry name")
    url: str = Field(..., description="Registry URL")
    is_public: bool = Field(default=True, description="Whether registry is public")
    authentication: Optional[Dict[str, str]] = Field(None, description="Authentication config")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateValidator:
    """Template validator."""
    
    def validate(self, template: WorkflowTemplate) -> ValidationResult:
        """Validate template."""
        errors = []
        warnings = []
        
        # Basic validation
        if not template.name:
            errors.append("Template name is required")
        
        if not template.workflow_structure:
            errors.append("Workflow structure is required")
        
        # Parameter validation
        for param in template.parameters:
            if not param.name:
                errors.append("Parameter name is required")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def validate_template(self, template: WorkflowTemplate) -> ValidationResult:
        """Validate template (alias for validate)."""
        return self.validate(template)
    
    def validate_parameter(self, parameter: ParameterDefinition) -> ValidationResult:
        """Validate parameter definition."""
        errors = []
        warnings = []
        
        if not parameter.name:
            errors.append("Parameter name is required")
        
        if not parameter.display_name:
            errors.append("Parameter display name is required")
        
        if not parameter.description:
            errors.append("Parameter description is required")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )


class TemplateInstantiator:
    """Template instantiator."""
    
    def __init__(self):
        self.template_engine = TemplateEngine()
    
    async def instantiate(
        self,
        template: WorkflowTemplate,
        context: TemplateContext
    ) -> InstantiationResult:
        """Instantiate template with context."""
        try:
            # Render template
            rendered = await self.template_engine.render(template, context)
            
            return InstantiationResult(
                success=True,
                workflow=rendered,
                workflow_data=rendered,
                template_id=template.template_id
            )
        except Exception as e:
            return InstantiationResult(
                success=False,
                errors=[str(e)]
            )


class TemplateEngine:
    """Template rendering engine."""
    
    def __init__(self):
        self.renderer = TemplateRenderer()
    
    async def render(
        self,
        template: WorkflowTemplate,
        context: TemplateContext,
        options: Optional['RenderOptions'] = None
    ) -> Dict[str, Any]:
        """Render template with context."""
        render_context = RenderContext(
            template=template,
            context=context,
            options=options or RenderOptions()
        )
        
        return await self.renderer.render(render_context)
    
    async def render_workflow(self, template: WorkflowTemplate, context: TemplateContext) -> Dict[str, Any]:
        """Render workflow template."""
        return await self.render(template, context)
    
    async def render_node(self, node_template: NodeTemplate, context: TemplateContext) -> Dict[str, Any]:
        """Render node template."""
        # Simplified node rendering
        return {
            "id": self.renderer._substitute_variables(node_template.node_id, context),
            "type": node_template.node_type,
            "display_name": self.renderer._substitute_variables(node_template.display_name, context),
            "description": self.renderer._substitute_variables(node_template.description, context),
            "parameters": node_template.parameters
        }
    
    async def render_condition(self, condition: TemplateCondition, context: TemplateContext) -> bool:
        """Render conditional expression."""
        # Simplified condition evaluation
        expression = condition.expression
        # Basic variable substitution
        for var, value in context.variables.items():
            expression = expression.replace(f"{{{{{var}}}}}", str(value))
        
        # Basic boolean evaluation
        if expression.lower() in ("true", "1", "yes"):
            return True
        elif expression.lower() in ("false", "0", "no"):
            return False
        
        # Check if it's a variable that evaluates to truthy
        try:
            return bool(eval(expression, {}, context.variables))
        except:
            return False


class RenderOptions(BaseModel):
    """Rendering options."""
    
    output_format: OutputFormat = Field(default=OutputFormat.WORKFLOW, description="Output format")
    include_metadata: bool = Field(default=True, description="Include metadata")
    validate_output: bool = Field(default=True, description="Validate output")
    
    model_config = ConfigDict(from_attributes=True)


class RenderContext(BaseModel):
    """Rendering context."""
    
    template: WorkflowTemplate = Field(..., description="Template to render")
    context: TemplateContext = Field(..., description="Rendering context")
    options: RenderOptions = Field(..., description="Rendering options")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateRenderer:
    """Template renderer."""
    
    async def render(self, render_context: RenderContext) -> Dict[str, Any]:
        """Render template."""
        # Simplified rendering - in production use proper template engine like Jinja2
        template = render_context.template
        context = render_context.context
        
        # Basic variable substitution
        workflow = {
            "name": self._substitute_variables(template.display_name, context),
            "description": self._substitute_variables(template.description, context),
            "nodes": [],
            "connections": []
        }
        
        return workflow
    
    def _substitute_variables(self, text: str, context: TemplateContext) -> str:
        """Simple variable substitution."""
        # This is a simplified implementation
        # In production, use proper template engine
        import re
        
        def replace_var(match):
            var_name = match.group(1)
            return str(context.get_variable(var_name, f"{{{{ {var_name} }}}}"))
        
        return re.sub(r'\{\{\s*(\w+)\s*\}\}', replace_var, text)


class TemplateManager:
    """Main template manager."""
    
    def __init__(self):
        self.validator = TemplateValidator()
        self.instantiator = TemplateInstantiator()
        self.template_engine = TemplateEngine()
        self._templates: Dict[UUID, WorkflowTemplate] = {}
    
    async def create_template(self, template: WorkflowTemplate) -> InstantiationResult:
        """Create new template."""
        # Validate template
        validation = self._validate_template(template)
        if not validation.is_valid:
            return InstantiationResult(
                success=False,
                errors=validation.errors
            )
        
        # Save template
        template_id = await self._save_template(template)
        
        return InstantiationResult(
            success=True,
            template_id=template_id
        )
    
    async def get_template(
        self,
        template_id: UUID,
        version: Optional[str] = None
    ) -> Optional[WorkflowTemplate]:
        """Get template by ID."""
        return await self._load_template(template_id, version)
    
    async def update_template(
        self,
        template_id: UUID,
        updates: Dict[str, Any]
    ) -> InstantiationResult:
        """Update template."""
        template = await self.get_template(template_id)
        if not template:
            return InstantiationResult(
                success=False,
                errors=["Template not found"]
            )
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(template, key):
                setattr(template, key, value)
        
        # Save updated template
        await self._save_template(template)
        
        return InstantiationResult(
            success=True,
            template_id=template_id
        )
    
    async def delete_template(self, template_id: UUID) -> bool:
        """Delete template."""
        # Implementation would remove from storage
        return await self._delete_template(template_id)
    
    async def search_templates(self, query: TemplateSearchQuery) -> List[TemplateSearchResult]:
        """Search templates."""
        # Implementation would search storage
        templates = await self._search_templates(query)
        
        # Convert to search results
        results = []
        for template in templates:
            result = TemplateSearchResult(
                template_id=template.template_id,
                name=template.name,
                display_name=template.display_name,
                description=template.description,
                version=template.version,
                author=template.author,
                category=template.category,
                tags=template.tags,
                rating=template.rating,
                usage_count=template.usage_count,
                updated_at=template.updated_at
            )
            results.append(result)
        
        return results
    
    async def instantiate_template(
        self,
        template_id: UUID,
        context: TemplateContext,
        version: Optional[str] = None,
        workflow_name: Optional[str] = None
    ) -> InstantiationResult:
        """Instantiate template."""
        template = await self.get_template(template_id, version)
        if not template:
            return InstantiationResult(
                success=False,
                errors=["Template not found"]
            )
        
        # Use template engine directly for workflow rendering
        rendered = await self.template_engine.render_workflow(template, context)
        
        return InstantiationResult(
            success=True,
            workflow=rendered,
            workflow_data=rendered,
            template_id=template.template_id
        )
    
    def _validate_template(self, template: WorkflowTemplate) -> ValidationResult:
        """Validate template."""
        return self.validator.validate(template)
    
    async def _save_template(self, template: WorkflowTemplate) -> UUID:
        """Save template to storage."""
        self._templates[template.template_id] = template
        return template.template_id
    
    async def _load_template(
        self,
        template_id: UUID,
        version: Optional[str] = None
    ) -> Optional[WorkflowTemplate]:
        """Load template from storage."""
        return self._templates.get(template_id)
    
    async def _delete_template(self, template_id: UUID) -> bool:
        """Delete template from storage."""
        if template_id in self._templates:
            del self._templates[template_id]
            return True
        return False
    
    async def _search_templates(self, query: TemplateSearchQuery) -> List[WorkflowTemplate]:
        """Search templates in storage."""
        # Simplified search implementation
        results = []
        for template in self._templates.values():
            if self._matches_query(template, query):
                results.append(template)
        
        return results[query.offset:query.offset + query.limit]
    
    async def list_templates_by_category(self, category: TemplateCategory) -> List[WorkflowTemplate]:
        """List templates by category."""
        return await self._load_templates_by_category(category)
    
    async def _load_templates_by_category(self, category: TemplateCategory) -> List[WorkflowTemplate]:
        """Load templates by category."""
        results = []
        for template in self._templates.values():
            if template.category == category:
                results.append(template)
        return results
    
    async def get_template_versions(self, template_id: UUID) -> List[TemplateVersion]:
        """Get all versions of a template."""
        return await self._load_template_versions(template_id)
    
    async def _load_template_versions(self, template_id: UUID) -> List[TemplateVersion]:
        """Load template versions."""
        # Simplified implementation - return single version
        template = self._templates.get(template_id)
        if template:
            version = TemplateVersion(
                template_id=template_id,
                version_number=template.version,
                created_at=template.created_at,
                created_by=uuid4()  # Mock creator
            )
            return [version]
        return []
    
    async def create_collection(self, collection: 'TemplateCollection') -> UUID:
        """Create template collection."""
        return await self._save_collection(collection)
    
    async def _save_collection(self, collection: 'TemplateCollection') -> UUID:
        """Save collection to storage."""
        # Mock implementation
        return collection.collection_id
    
    def _matches_query(self, template: WorkflowTemplate, query: TemplateSearchQuery) -> bool:
        """Check if template matches search query."""
        if query.category and template.category != query.category:
            return False
        
        if query.author and template.author != query.author:
            return False
        
        if query.status and template.status != query.status:
            return False
        
        if query.visibility and template.visibility != query.visibility:
            return False
        
        if query.min_rating and template.rating < query.min_rating:
            return False
        
        if query.tags:
            if not any(tag in template.tags for tag in query.tags):
                return False
        
        if query.query:
            search_text = f"{template.name} {template.description} {' '.join(template.tags)}".lower()
            if query.query.lower() not in search_text:
                return False
        
        return True


# Additional supporting classes
class TemplateParameter(BaseModel):
    """Template parameter value."""
    
    name: str = Field(..., description="Parameter name")
    value: Any = Field(..., description="Parameter value")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateVariable(BaseModel):
    """Template variable."""
    
    name: str = Field(..., description="Variable name")
    value: Any = Field(..., description="Variable value")
    type: str = Field(..., description="Variable type")
    
    model_config = ConfigDict(from_attributes=True)


class ConditionalBlock(BaseModel):
    """Conditional template block."""
    
    condition: str = Field(..., description="Condition expression")
    content: Dict[str, Any] = Field(..., description="Block content")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateLoop(BaseModel):
    """Template loop block."""
    
    variable: str = Field(..., description="Loop variable")
    iterable: str = Field(..., description="Iterable expression")
    content: Dict[str, Any] = Field(..., description="Loop content")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateInclude(BaseModel):
    """Template include block."""
    
    template_path: str = Field(..., description="Template path to include")
    context: Dict[str, Any] = Field(default_factory=dict, description="Include context")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateInheritance(BaseModel):
    """Template inheritance."""
    
    parent_template: str = Field(..., description="Parent template path")
    blocks: Dict[str, Any] = Field(default_factory=dict, description="Override blocks")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateCollection(BaseModel):
    """Collection of templates."""
    
    collection_id: UUID = Field(default_factory=uuid4, description="Collection ID")
    name: str = Field(..., description="Collection name")
    description: str = Field(..., description="Collection description")
    templates: List[UUID] = Field(default_factory=list, description="Template IDs")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateLibrary(BaseModel):
    """Template library."""
    
    library_id: UUID = Field(default_factory=uuid4, description="Library ID")
    name: str = Field(..., description="Library name")
    collections: List[TemplateCollection] = Field(default_factory=list, description="Collections")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateImporter:
    """Template importer."""
    
    async def import_template(self, source: str, format: TemplateFormat) -> WorkflowTemplate:
        """Import template from source."""
        # Implementation would parse source and create template
        raise NotImplementedError
    
    async def import_from_file(self, file_path: Path) -> WorkflowTemplate:
        """Import template from file."""
        # Implementation would read file and import
        raise NotImplementedError


class TemplateExporter:
    """Template exporter."""
    
    async def export_template(
        self,
        template: WorkflowTemplate,
        format: TemplateFormat
    ) -> str:
        """Export template to string."""
        # Implementation would serialize template
        if format == TemplateFormat.JSON:
            return json.dumps(template.model_dump(), indent=2)
        raise NotImplementedError
    
    async def export_to_file(
        self,
        template: WorkflowTemplate,
        file_path: Path,
        format: TemplateFormat
    ) -> None:
        """Export template to file."""
        content = await self.export_template(template, format)
        file_path.write_text(content)


class TemplateReview(BaseModel):
    """Template review."""
    
    review_id: UUID = Field(default_factory=uuid4, description="Review ID")
    template_id: UUID = Field(..., description="Template ID")
    reviewer_id: UUID = Field(..., description="Reviewer ID")
    rating: int = Field(..., description="Rating (1-5)")
    review_text: Optional[str] = Field(None, description="Review text")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    model_config = ConfigDict(from_attributes=True)


class TemplateRating(BaseModel):
    """Template rating summary."""
    
    template_id: UUID = Field(..., description="Template ID")
    average_rating: float = Field(..., description="Average rating")
    total_ratings: int = Field(..., description="Total rating count")
    rating_distribution: Dict[int, int] = Field(..., description="Rating distribution")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateUsageStats(BaseModel):
    """Template usage statistics."""
    
    template_id: UUID = Field(..., description="Template ID")
    total_uses: int = Field(default=0, description="Total usage count")
    unique_users: int = Field(default=0, description="Unique user count")
    last_used: Optional[datetime] = Field(None, description="Last usage timestamp")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateAnalytics(BaseModel):
    """Template analytics."""
    
    template_id: UUID = Field(..., description="Template ID")
    views: int = Field(default=0, description="View count")
    downloads: int = Field(default=0, description="Download count")
    forks: int = Field(default=0, description="Fork count")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateDependency(BaseModel):
    """Template dependency."""
    
    name: str = Field(..., description="Dependency name")
    type: DependencyType = Field(..., description="Dependency type")
    version_constraint: str = Field(..., description="Version constraint")
    is_optional: bool = Field(default=False, description="Whether dependency is optional")
    
    model_config = ConfigDict(from_attributes=True)


class VersionConstraint(BaseModel):
    """Version constraint."""
    
    constraint: str = Field(..., description="Constraint expression")
    
    model_config = ConfigDict(from_attributes=True)
    
    def matches(self, version: str) -> bool:
        """Check if version matches constraint."""
        # Simplified implementation
        return True


class SemanticVersion(BaseModel):
    """Semantic version."""
    
    major: int = Field(..., description="Major version")
    minor: int = Field(..., description="Minor version")
    patch: int = Field(..., description="Patch version")
    prerelease: Optional[str] = Field(None, description="Prerelease identifier")
    
    model_config = ConfigDict(from_attributes=True)
    
    def __str__(self) -> str:
        """String representation."""
        version = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            version += f"-{self.prerelease}"
        return version


class VersionRange(BaseModel):
    """Version range."""
    
    min_version: Optional[str] = Field(None, description="Minimum version")
    max_version: Optional[str] = Field(None, description="Maximum version")
    exclude_prerelease: bool = Field(default=True, description="Exclude prerelease versions")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateManifest(BaseModel):
    """Template manifest."""
    
    name: str = Field(..., description="Template name")
    version: str = Field(..., description="Template version")
    description: str = Field(..., description="Template description")
    dependencies: List[TemplateDependency] = Field(default_factory=list, description="Dependencies")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata")
    
    model_config = ConfigDict(from_attributes=True)


class WorkflowStructure(BaseModel):
    """Workflow structure."""
    
    nodes: List[NodeTemplate] = Field(default_factory=list, description="Node templates")
    connections: List[ConnectionTemplate] = Field(default_factory=list, description="Connection templates")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Structure metadata")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateNode(BaseModel):
    """Template node."""
    
    id: str = Field(..., description="Node ID")
    type: str = Field(..., description="Node type")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Node parameters")
    
    model_config = ConfigDict(from_attributes=True)


class TemplateConnection(BaseModel):
    """Template connection."""
    
    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    output: str = Field(default="main", description="Output name")
    input: str = Field(default="main", description="Input name")
    
    model_config = ConfigDict(from_attributes=True)


class PlaceholderValue(BaseModel):
    """Placeholder value for templates."""
    
    key: str = Field(..., description="Placeholder key")
    default_value: Any = Field(None, description="Default value")
    required: bool = Field(default=False, description="Whether placeholder is required")
    
    model_config = ConfigDict(from_attributes=True)


class VariableResolver:
    """Resolves template variables."""
    
    def resolve(self, expression: str, context: TemplateContext) -> Any:
        """Resolve variable expression."""
        # Simplified implementation
        if expression in context.variables:
            return context.variables[expression]
        return expression


class ExpressionEvaluator:
    """Evaluates template expressions."""
    
    def evaluate(self, expression: str, context: TemplateContext) -> Any:
        """Evaluate expression."""
        # Simplified implementation - in production use safe eval
        try:
            # Basic variable substitution
            for var, value in context.variables.items():
                expression = expression.replace(f"{{{{{var}}}}}", str(value))
            return expression
        except Exception:
            return expression