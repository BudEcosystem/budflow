"""Node API routes."""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Query

from budflow.nodes.registry import default_registry
from budflow.nodes.schemas import (
    NodeTypeResponse,
    NodeCategoryResponse,
    NodeDefinitionResponse,
    NodeSearchResponse
)

router = APIRouter(prefix="/api/v1/nodes", tags=["Nodes"])


@router.get("/", response_model=List[NodeTypeResponse])
async def list_nodes(
    category: Optional[str] = Query(None, description="Filter by category"),
    search: Optional[str] = Query(None, description="Search nodes by name or description")
) -> List[NodeTypeResponse]:
    """List all available node types."""
    try:
        # Get all nodes from registry
        all_nodes = default_registry.get_all_nodes()
        nodes = []
        
        for type_key, node_class in all_nodes.items():
            definition = default_registry.get_node_definition(type_key)
            if definition:
                # Apply filters
                if category and definition.category.value != category:
                    continue
                    
                if search:
                    search_lower = search.lower()
                    if (search_lower not in definition.name.lower() and 
                        search_lower not in definition.description.lower()):
                        continue
                
                # Get enhanced metadata if available
                metadata = default_registry.get_enhanced_metadata(type_key)
                
                nodes.append(NodeTypeResponse(
                    type_key=type_key,
                    name=definition.name,
                    description=definition.description,
                    category=definition.category.value,
                    version=metadata.version.to_string() if metadata else definition.version,
                    icon=definition.icon,
                    color=definition.color,
                    inputs=definition.inputs,
                    outputs=definition.outputs,
                    tags=metadata.tags if metadata else [],
                    deprecated=definition.deprecated
                ))
        
        return nodes
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list nodes: {str(e)}")


@router.get("/categories", response_model=List[NodeCategoryResponse])
async def list_categories() -> List[NodeCategoryResponse]:
    """List all node categories."""
    try:
        from budflow.nodes.base import NodeCategory
        
        categories = []
        for category in NodeCategory:
            # Count nodes in this category
            nodes_in_category = default_registry.get_nodes_by_category(category.value)
            
            categories.append(NodeCategoryResponse(
                name=category.value,
                display_name=category.value.replace("_", " ").title(),
                description=f"Nodes for {category.value}",
                node_count=len(nodes_in_category)
            ))
        
        return categories
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list categories: {str(e)}")


@router.get("/search", response_model=NodeSearchResponse)
async def search_nodes(
    q: str = Query(..., description="Search query"),
    limit: int = Query(10, ge=1, le=100, description="Maximum results")
) -> NodeSearchResponse:
    """Search nodes by name, description, or tags."""
    try:
        # Search nodes
        matching_nodes = default_registry.search_nodes(q)
        
        results = []
        for type_key, node_class in list(matching_nodes.items())[:limit]:
            definition = default_registry.get_node_definition(type_key)
            if definition:
                metadata = default_registry.get_enhanced_metadata(type_key)
                
                results.append({
                    "type_key": type_key,
                    "name": definition.name,
                    "description": definition.description,
                    "category": definition.category.value,
                    "relevance": 1.0  # Simple relevance for now
                })
        
        return NodeSearchResponse(
            query=q,
            results=results,
            total=len(matching_nodes)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to search nodes: {str(e)}")


@router.get("/by-type/{node_type}", response_model=List[NodeTypeResponse])
async def list_nodes_by_type(node_type: str) -> List[NodeTypeResponse]:
    """List all nodes of a specific type (trigger, action, etc)."""
    try:
        from budflow.workflows.models import NodeType
        
        # Validate node type
        try:
            node_type_enum = NodeType(node_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid node type: {node_type}")
        
        # Get nodes by type
        nodes_of_type = default_registry.get_nodes_by_type(node_type_enum)
        
        nodes = []
        for type_key, node_class in nodes_of_type.items():
            definition = default_registry.get_node_definition(type_key)
            if definition:
                metadata = default_registry.get_enhanced_metadata(type_key)
                
                nodes.append(NodeTypeResponse(
                    type_key=type_key,
                    name=definition.name,
                    description=definition.description,
                    category=definition.category.value,
                    version=metadata.version.to_string() if metadata else definition.version,
                    icon=definition.icon,
                    color=definition.color,
                    inputs=definition.inputs,
                    outputs=definition.outputs,
                    tags=metadata.tags if metadata else [],
                    deprecated=definition.deprecated
                ))
        
        return nodes
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list nodes by type: {str(e)}")


@router.post("/reload")
async def reload_nodes() -> Dict[str, Any]:
    """Reload node registry (admin only)."""
    try:
        # In production, add admin authentication check here
        
        # Reload registry
        default_registry._register_builtin_nodes()
        
        # Get stats
        all_nodes = default_registry.get_all_nodes()
        
        return {
            "success": True,
            "message": "Node registry reloaded",
            "node_count": len(all_nodes),
            "nodes": list(all_nodes.keys())
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload nodes: {str(e)}")


@router.get("/{type_key}", response_model=NodeDefinitionResponse)
async def get_node_definition(type_key: str) -> NodeDefinitionResponse:
    """Get detailed definition for a specific node type."""
    try:
        definition = default_registry.get_node_definition(type_key)
        if not definition:
            raise HTTPException(status_code=404, detail=f"Node type '{type_key}' not found")
        
        # Get enhanced metadata
        metadata = default_registry.get_enhanced_metadata(type_key)
        
        return NodeDefinitionResponse(
            type_key=type_key,
            name=definition.name,
            description=definition.description,
            category=definition.category.value,
            version=metadata.version.to_string() if metadata else definition.version,
            icon=definition.icon,
            color=definition.color,
            inputs=definition.inputs,
            outputs=definition.outputs,
            parameters=[
                {
                    "name": param.name,
                    "display_name": param.display_name if param.display_name else param.name,
                    "type": param.type.value,
                    "required": param.required,
                    "default": param.default,
                    "description": param.description,
                    "placeholder": param.placeholder,
                    "options": param.options,
                    "min_value": param.min_value,
                    "max_value": param.max_value,
                    "multiline": param.multiline
                }
                for param in definition.parameters
            ],
            tags=metadata.tags if metadata else [],
            deprecated=definition.deprecated,
            documentation_url=metadata.documentation_url if metadata else None,
            examples=metadata.examples if metadata and hasattr(metadata, 'examples') else []
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get node definition: {str(e)}")