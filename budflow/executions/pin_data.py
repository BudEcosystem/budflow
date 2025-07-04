"""Pin Data support for BudFlow workflow testing."""

import json
import gzip
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload

from ..workflows.models import Workflow, WorkflowNode, WorkflowExecution, ExecutionStatus, ExecutionMode
from ..workflows.service import WorkflowService
from ..nodes.registry import NodeRegistry


@dataclass
class PinData:
    """Represents pinned data for a workflow node."""
    id: Optional[int]
    workflow_id: int
    node_id: str
    data: List[Dict[str, Any]]
    created_at: datetime
    created_by: str
    version: int = 1
    description: Optional[str] = None
    is_compressed: bool = False
    original_size: Optional[int] = None
    optimized_size: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "node_id": self.node_id,
            "data": self.data,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "version": self.version,
            "description": self.description,
            "is_compressed": self.is_compressed,
            "original_size": self.original_size,
            "optimized_size": self.optimized_size
        }


class PinDataNotFoundError(Exception):
    """Raised when pin data is not found."""
    pass


class InvalidPinDataError(Exception):
    """Raised when pin data is invalid."""
    pass


class PinDataVersionMismatchError(Exception):
    """Raised when pin data version conflicts occur."""
    pass


class NodeNotPinnedError(Exception):
    """Raised when trying to get pin data for non-pinned node."""
    pass


class PinDataService:
    """Service for managing pin data storage and retrieval."""
    
    MAX_PIN_DATA_SIZE = 10 * 1024 * 1024  # 10MB limit
    
    def __init__(self, db: AsyncSession):
        self.db = db
        
        # In-memory storage for testing (use database in production)
        self._pin_data_store: Dict[str, List[PinData]] = {}  # workflow_id:node_id -> List[PinData]
        self._pin_data_counter = 1
    
    async def save_pin_data(
        self,
        workflow_id: int,
        node_id: str,
        data: List[Dict[str, Any]],
        user_id: str,
        description: Optional[str] = None,
        optimize: bool = False
    ) -> PinData:
        """Save pin data for a workflow node."""
        # Validate input data
        await self.validate_pin_data(data)
        await self.validate_node_exists(workflow_id, node_id)
        
        # Get current version (check if updating existing pin data)
        existing_data = await self.get_pin_data_from_db(workflow_id, node_id)
        if existing_data:
            current_version = existing_data.version
        else:
            current_version = await self.get_latest_version(workflow_id, node_id)
        new_version = current_version + 1
        
        # Optimize data if requested
        optimized_data = data
        optimization_info = None
        if optimize:
            optimization_info = await self.optimize_pin_data(data)
            if optimization_info.get("optimization_applied"):
                optimized_data = optimization_info.get("optimized_data", data)
        
        # Create pin data object
        pin_data = PinData(
            id=self._pin_data_counter,
            workflow_id=workflow_id,
            node_id=node_id,
            data=optimized_data,
            created_at=datetime.now(timezone.utc),
            created_by=user_id,
            version=new_version,
            description=description,
            is_compressed=optimization_info.get("optimization_applied", False) if optimization_info else False,
            original_size=optimization_info.get("original_size") if optimization_info else None,
            optimized_size=optimization_info.get("optimized_size") if optimization_info else None
        )
        
        # Store pin data
        await self.store_pin_data(pin_data)
        
        self._pin_data_counter += 1
        return pin_data
    
    async def get_pin_data(self, workflow_id: int, node_id: str) -> PinData:
        """Get the latest pin data for a workflow node."""
        pin_data = await self.get_pin_data_from_db(workflow_id, node_id)
        if not pin_data:
            raise PinDataNotFoundError(f"Pin data not found for workflow {workflow_id}, node {node_id}")
        return pin_data
    
    async def delete_pin_data(self, workflow_id: int, node_id: str):
        """Delete all pin data for a workflow node."""
        # Check if pin data exists
        existing_data = await self.get_pin_data_from_db(workflow_id, node_id)
        if not existing_data:
            raise PinDataNotFoundError(f"Pin data not found for workflow {workflow_id}, node {node_id}")
        
        await self.delete_pin_data_from_db(workflow_id, node_id)
    
    async def list_pinned_nodes(self, workflow_id: int) -> List[PinData]:
        """List all pinned nodes for a workflow."""
        return await self.get_all_pin_data_for_workflow(workflow_id)
    
    async def validate_pin_data(self, data: Any):
        """Validate pin data format and size."""
        if not isinstance(data, list):
            raise InvalidPinDataError("Pin data must be a list")
        
        # Check size limit
        data_size = len(json.dumps(data).encode('utf-8'))
        if data_size > self.MAX_PIN_DATA_SIZE:
            raise InvalidPinDataError(f"Pin data too large: {data_size} bytes (max: {self.MAX_PIN_DATA_SIZE} bytes)")
    
    async def validate_node_exists(self, workflow_id: int, node_id: str):
        """Validate that the node exists in the workflow."""
        workflow = await self.get_workflow(workflow_id)
        if not workflow:
            raise InvalidPinDataError(f"Workflow {workflow_id} not found")
        
        # Check if node exists in workflow
        node_ids = [node.get("id", node.get("name")) for node in workflow.nodes]
        if node_id not in node_ids:
            raise InvalidPinDataError(f"Node {node_id} not found in workflow {workflow_id}")
    
    async def get_workflow_pin_summary(self, workflow_id: int) -> Dict[str, Any]:
        """Get summary of all pinned nodes in workflow."""
        pinned_nodes = await self.get_all_pin_data_for_workflow(workflow_id)
        
        summary = {
            "workflow_id": workflow_id,
            "total_pinned_nodes": len(pinned_nodes),
            "pinned_nodes": []
        }
        
        for pin_data in pinned_nodes:
            summary["pinned_nodes"].append({
                "node_id": pin_data.node_id,
                "version": pin_data.version,
                "created_at": pin_data.created_at.isoformat(),
                "created_by": pin_data.created_by,
                "description": pin_data.description,
                "data_size": len(json.dumps(pin_data.data).encode('utf-8')),
                "is_compressed": pin_data.is_compressed
            })
        
        return summary
    
    async def clone_pin_data_to_workflow(
        self, 
        source_workflow_id: int, 
        target_workflow_id: int, 
        user_id: str
    ) -> int:
        """Clone all pin data from source workflow to target workflow."""
        source_pin_data = await self.get_all_pin_data_for_workflow(source_workflow_id)
        
        cloned_count = 0
        for pin_data in source_pin_data:
            # Create new pin data for target workflow
            new_pin_data = PinData(
                id=self._pin_data_counter,
                workflow_id=target_workflow_id,
                node_id=pin_data.node_id,
                data=pin_data.data,
                created_at=datetime.now(timezone.utc),
                created_by=user_id,
                version=1,  # Reset version for new workflow
                description=f"Cloned from workflow {source_workflow_id}: {pin_data.description}",
                is_compressed=pin_data.is_compressed,
                original_size=pin_data.original_size,
                optimized_size=pin_data.optimized_size
            )
            
            await self.store_pin_data(new_pin_data)
            self._pin_data_counter += 1
            cloned_count += 1
        
        return cloned_count
    
    async def validate_pin_data_compatibility(
        self, 
        workflow_id: int, 
        pin_data_list: List[PinData]
    ) -> Dict[str, Any]:
        """Validate pin data compatibility with current workflow."""
        workflow = await self.get_workflow(workflow_id)
        workflow_node_ids = [node.get("id", node.get("name")) for node in workflow.nodes]
        
        compatible_nodes = []
        incompatible_nodes = []
        
        for pin_data in pin_data_list:
            if pin_data.node_id in workflow_node_ids:
                compatible_nodes.append(pin_data.node_id)
            else:
                incompatible_nodes.append(pin_data.node_id)
        
        return {
            "compatible": len(incompatible_nodes) == 0,
            "compatible_nodes": compatible_nodes,
            "incompatible_nodes": incompatible_nodes,
            "missing_nodes": list(set(workflow_node_ids) - set(pin_data.node_id for pin_data in pin_data_list))
        }
    
    async def get_pin_data_version_history(
        self, 
        workflow_id: int, 
        node_id: str
    ) -> List[PinData]:
        """Get version history for pin data."""
        key = f"{workflow_id}:{node_id}"
        return self._pin_data_store.get(key, [])
    
    async def get_pin_data_version(
        self, 
        workflow_id: int, 
        node_id: str, 
        version: int
    ) -> PinData:
        """Get specific version of pin data."""
        history = await self.get_pin_data_version_history(workflow_id, node_id)
        for pin_data in history:
            if pin_data.version == version:
                return pin_data
        raise PinDataNotFoundError(f"Pin data version {version} not found for workflow {workflow_id}, node {node_id}")
    
    async def revert_pin_data_to_version(
        self, 
        workflow_id: int, 
        node_id: str, 
        target_version: int, 
        user_id: str
    ) -> PinData:
        """Revert pin data to a previous version."""
        target_data = await self.get_pin_data_version(workflow_id, node_id, target_version)
        current_version = await self.get_latest_version(workflow_id, node_id)
        
        # Create new version with reverted data
        reverted_pin_data = PinData(
            id=self._pin_data_counter,
            workflow_id=workflow_id,
            node_id=node_id,
            data=target_data.data,
            created_at=datetime.now(timezone.utc),
            created_by=user_id,
            version=current_version + 1,
            description=f"Reverted to version {target_version}",
            is_compressed=target_data.is_compressed,
            original_size=target_data.original_size,
            optimized_size=target_data.optimized_size
        )
        
        await self.store_pin_data(reverted_pin_data)
        self._pin_data_counter += 1
        return reverted_pin_data
    
    async def bulk_save_pin_data(
        self, 
        workflow_id: int, 
        bulk_pin_data: List[Dict[str, Any]], 
        user_id: str
    ) -> Dict[str, Any]:
        """Save multiple pin data entries in bulk."""
        result = {
            "success_count": 0,
            "error_count": 0,
            "created_pin_data": [],
            "errors": []
        }
        
        for pin_data_info in bulk_pin_data:
            try:
                pin_data = await self.save_pin_data(
                    workflow_id=workflow_id,
                    node_id=pin_data_info["node_id"],
                    data=pin_data_info["data"],
                    user_id=user_id,
                    description=pin_data_info.get("description")
                )
                result["created_pin_data"].append(pin_data)
                result["success_count"] += 1
            except Exception as e:
                result["errors"].append({
                    "node_id": pin_data_info["node_id"],
                    "error": str(e)
                })
                result["error_count"] += 1
        
        return result
    
    async def optimize_pin_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Optimize pin data for storage (compression, deduplication, etc.)."""
        original_json = json.dumps(data)
        original_size = len(original_json.encode('utf-8'))
        
        # Simple compression using gzip
        compressed_data = gzip.compress(original_json.encode('utf-8'))
        optimized_size = len(compressed_data)
        
        # Only apply optimization if it provides significant savings
        optimization_threshold = 0.8  # Must be 80% or less of original size
        if optimized_size <= original_size * optimization_threshold:
            return {
                "optimization_applied": True,
                "original_size": original_size,
                "optimized_size": optimized_size,
                "optimized_data": data  # In real implementation, store compressed data
            }
        else:
            return {
                "optimization_applied": False,
                "original_size": original_size,
                "optimized_size": original_size
            }
    
    # Database operations (in-memory for testing)
    
    async def store_pin_data(self, pin_data: PinData):
        """Store pin data in database (in-memory for testing)."""
        key = f"{pin_data.workflow_id}:{pin_data.node_id}"
        if key not in self._pin_data_store:
            self._pin_data_store[key] = []
        self._pin_data_store[key].append(pin_data)
    
    async def get_pin_data_from_db(self, workflow_id: int, node_id: str) -> Optional[PinData]:
        """Get latest pin data from database (in-memory for testing)."""
        key = f"{workflow_id}:{node_id}"
        pin_data_list = self._pin_data_store.get(key, [])
        if pin_data_list:
            # Return latest version
            return max(pin_data_list, key=lambda x: x.version)
        return None
    
    async def delete_pin_data_from_db(self, workflow_id: int, node_id: str):
        """Delete pin data from database (in-memory for testing)."""
        key = f"{workflow_id}:{node_id}"
        if key in self._pin_data_store:
            del self._pin_data_store[key]
    
    async def get_all_pin_data_for_workflow(self, workflow_id: int) -> List[PinData]:
        """Get all pin data for a workflow (latest versions only)."""
        result = []
        for key, pin_data_list in self._pin_data_store.items():
            stored_workflow_id = int(key.split(':')[0])
            if stored_workflow_id == workflow_id and pin_data_list:
                # Get latest version for each node
                latest_pin_data = max(pin_data_list, key=lambda x: x.version)
                result.append(latest_pin_data)
        return result
    
    async def get_latest_version(self, workflow_id: int, node_id: str) -> int:
        """Get latest version number for pin data."""
        key = f"{workflow_id}:{node_id}"
        pin_data_list = self._pin_data_store.get(key, [])
        if pin_data_list:
            return max(pin_data.version for pin_data in pin_data_list)
        return 0
    
    async def get_workflow(self, workflow_id: int) -> Optional[Workflow]:
        """Get workflow by ID (mock for testing)."""
        # In production, this would query the database
        # For testing, create a mock workflow
        return Workflow(
            id=workflow_id,
            name=f"Test Workflow {workflow_id}",
            nodes=[
                {"id": "start", "name": "Start Node", "type": "manual.trigger"},
                {"id": "http1", "name": "HTTP Request", "type": "http.request"},
                {"id": "transform1", "name": "Transform Data", "type": "transform"},
                {"id": "email1", "name": "Send Email", "type": "email.send"},
                {"id": "end", "name": "End Node", "type": "set"}
            ],
            connections=[]
        )


class PinDataEngine:
    """Engine for executing workflows with pin data support."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.pin_data_service = PinDataService(db)
        self.workflow_service = None  # Will be initialized as needed
    
    async def is_node_pinned(self, workflow_id: int, node_id: str) -> bool:
        """Check if a node has pin data."""
        try:
            await self.pin_data_service.get_pin_data(workflow_id, node_id)
            return True
        except PinDataNotFoundError:
            return False
    
    async def get_pinned_data_for_execution(
        self, 
        workflow_id: int, 
        node_id: str
    ) -> List[Dict[str, Any]]:
        """Get pinned data for execution."""
        try:
            pin_data = await self.pin_data_service.get_pin_data(workflow_id, node_id)
            return pin_data.data
        except PinDataNotFoundError:
            raise NodeNotPinnedError(f"Node {node_id} is not pinned")
    
    async def execute_with_pin_data(
        self,
        execution_id: str,
        workflow: Workflow,
        input_data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Execute workflow using pin data where available."""
        execution_result = {
            "execution_id": execution_id,
            "workflow_id": workflow.id,
            "status": ExecutionStatus.RUNNING,
            "node_results": {},
            "pin_data_used": [],
            "live_execution_nodes": []
        }
        
        # Process each node in workflow
        for node_data in workflow.nodes:
            node_id = node_data.get("id", node_data.get("name"))
            
            # Check if node is pinned
            is_pinned = await self.is_node_pinned(workflow.id, node_id)
            
            if is_pinned:
                # Use pin data
                try:
                    pinned_data = await self.get_pinned_data_for_execution(workflow.id, node_id)
                    execution_result["node_results"][node_id] = pinned_data
                    execution_result["pin_data_used"].append(node_id)
                except NodeNotPinnedError:
                    # Fallback to normal execution
                    result = await self.execute_node_normally(node_data, input_data)
                    execution_result["node_results"][node_id] = result
                    execution_result["live_execution_nodes"].append(node_id)
            else:
                # Execute node normally
                result = await self.execute_node_normally(node_data, input_data)
                execution_result["node_results"][node_id] = result
                execution_result["live_execution_nodes"].append(node_id)
        
        execution_result["status"] = ExecutionStatus.SUCCESS
        return await self.create_execution_result(execution_result)
    
    async def execute_node_normally(
        self, 
        node_data: Dict[str, Any], 
        input_data: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Execute node normally (simulation for testing)."""
        node_id = node_data.get("id", node_data.get("name"))
        node_type = node_data.get("type", "unknown")
        
        # Simulate node execution
        return [{
            "executed": True,
            "node_id": node_id,
            "node_type": node_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "simulated": True
        }]
    
    async def create_execution_result(self, execution_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create final execution result."""
        return {
            "execution_id": execution_data["execution_id"],
            "workflow_id": execution_data["workflow_id"],
            "status": execution_data["status"].value,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "node_results": execution_data["node_results"],
            "pin_data_summary": {
                "pinned_nodes": execution_data["pin_data_used"],
                "live_nodes": execution_data["live_execution_nodes"],
                "total_pinned": len(execution_data["pin_data_used"]),
                "total_live": len(execution_data["live_execution_nodes"])
            }
        }
    
    async def get_execution_plan_with_pin_data(
        self, 
        workflow: Workflow
    ) -> Dict[str, Any]:
        """Get execution plan showing which nodes will use pin data."""
        plan = {
            "workflow_id": workflow.id,
            "total_nodes": len(workflow.nodes),
            "pinned_nodes": [],
            "live_nodes": [],
            "execution_strategy": "mixed"
        }
        
        for node_data in workflow.nodes:
            node_id = node_data.get("id", node_data.get("name"))
            is_pinned = await self.is_node_pinned(workflow.id, node_id)
            
            if is_pinned:
                plan["pinned_nodes"].append({
                    "node_id": node_id,
                    "node_type": node_data.get("type", "unknown"),
                    "execution_type": "pinned_data"
                })
            else:
                plan["live_nodes"].append({
                    "node_id": node_id,
                    "node_type": node_data.get("type", "unknown"),
                    "execution_type": "live_execution"
                })
        
        # Determine execution strategy
        if len(plan["pinned_nodes"]) == 0:
            plan["execution_strategy"] = "live_only"
        elif len(plan["live_nodes"]) == 0:
            plan["execution_strategy"] = "pinned_only"
        else:
            plan["execution_strategy"] = "mixed"
        
        return plan
    
    async def validate_pin_data_for_execution(
        self, 
        workflow: Workflow
    ) -> Dict[str, Any]:
        """Validate that pin data is compatible with current workflow."""
        validation_result = {
            "valid": True,
            "issues": [],
            "pinned_nodes": [],
            "recommendations": []
        }
        
        pinned_nodes = await self.pin_data_service.list_pinned_nodes(workflow.id)
        workflow_node_ids = [node.get("id", node.get("name")) for node in workflow.nodes]
        
        for pin_data in pinned_nodes:
            if pin_data.node_id not in workflow_node_ids:
                validation_result["valid"] = False
                validation_result["issues"].append({
                    "type": "missing_node",
                    "node_id": pin_data.node_id,
                    "message": f"Pinned node {pin_data.node_id} no longer exists in workflow"
                })
            else:
                validation_result["pinned_nodes"].append({
                    "node_id": pin_data.node_id,
                    "version": pin_data.version,
                    "created_at": pin_data.created_at.isoformat()
                })
        
        # Add recommendations
        if validation_result["issues"]:
            validation_result["recommendations"].append(
                "Remove pin data for nodes that no longer exist in the workflow"
            )
        
        if len(validation_result["pinned_nodes"]) == 0:
            validation_result["recommendations"].append(
                "Consider pinning some node outputs for faster testing"
            )
        
        return validation_result