"""Node API schemas."""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class NodeParameterResponse(BaseModel):
    """Node parameter response schema."""
    name: str
    display_name: str
    type: str
    required: bool = False
    default: Any = None
    description: Optional[str] = None
    placeholder: Optional[str] = None
    options: Optional[List[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    multiline: bool = False


class NodeTypeResponse(BaseModel):
    """Node type response schema."""
    type_key: str = Field(..., description="Unique node type identifier")
    name: str = Field(..., description="Node display name")
    description: str = Field(..., description="Node description")
    category: str = Field(..., description="Node category")
    version: str = Field(default="1.0.0", description="Node version")
    icon: Optional[str] = Field(None, description="Node icon")
    color: Optional[str] = Field(None, description="Node color")
    inputs: List[str] = Field(default_factory=list, description="Input ports")
    outputs: List[str] = Field(default_factory=list, description="Output ports")
    tags: List[str] = Field(default_factory=list, description="Node tags")
    deprecated: bool = Field(default=False, description="Is node deprecated")


class NodeCategoryResponse(BaseModel):
    """Node category response schema."""
    name: str = Field(..., description="Category name")
    display_name: str = Field(..., description="Category display name")
    description: str = Field(..., description="Category description")
    node_count: int = Field(..., description="Number of nodes in category")


class NodeDefinitionResponse(BaseModel):
    """Detailed node definition response schema."""
    type_key: str
    name: str
    description: str
    category: str
    version: str
    icon: Optional[str] = None
    color: Optional[str] = None
    inputs: List[str]
    outputs: List[str]
    parameters: List[Dict[str, Any]]
    tags: List[str] = Field(default_factory=list)
    deprecated: bool = False
    documentation_url: Optional[str] = None
    examples: List[Dict[str, Any]] = Field(default_factory=list)


class NodeSearchResult(BaseModel):
    """Node search result schema."""
    type_key: str
    name: str
    description: str
    category: str
    relevance: float = Field(..., ge=0, le=1, description="Search relevance score")


class NodeSearchResponse(BaseModel):
    """Node search response schema."""
    query: str
    results: List[NodeSearchResult]
    total: int