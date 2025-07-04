"""Execution data handling classes."""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, ConfigDict


class NodeOutputData(BaseModel):
    """Data output from a node execution."""
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    output_name: str = Field(default="main", description="Output port name")
    items: List[Dict[str, Any]] = Field(default_factory=list, description="Output data items")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Output metadata")
    
    def add_item(self, item: Dict[str, Any]) -> None:
        """Add an item to the output."""
        self.items.append(item)
    
    def extend_items(self, items: List[Dict[str, Any]]) -> None:
        """Extend output with multiple items."""
        self.items.extend(items)
    
    def get_json(self) -> List[Dict[str, Any]]:
        """Get output data as JSON-serializable list."""
        return self.items
    
    def is_empty(self) -> bool:
        """Check if output has no data."""
        return len(self.items) == 0


class ExecutionData(BaseModel):
    """Container for all execution data flowing through workflow."""
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    # Node outputs indexed by node ID and output name
    node_outputs: Dict[str, Dict[str, NodeOutputData]] = Field(
        default_factory=dict,
        description="Node outputs indexed by node_id and output_name"
    )
    
    # Global execution context data
    context_data: Dict[str, Any] = Field(
        default_factory=dict,
        description="Global context data available to all nodes"
    )
    
    # Workflow input data
    input_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Initial workflow input data"
    )
    
    # Execution metadata
    execution_id: Optional[str] = Field(None, description="Execution ID")
    started_at: Optional[datetime] = Field(None, description="Execution start time")
    
    def set_node_output(
        self,
        node_id: str,
        output_data: Union[NodeOutputData, List[Dict[str, Any]], Dict[str, Any]],
        output_name: str = "main"
    ) -> None:
        """Set output data for a node."""
        if node_id not in self.node_outputs:
            self.node_outputs[node_id] = {}
        
        if isinstance(output_data, NodeOutputData):
            self.node_outputs[node_id][output_name] = output_data
        elif isinstance(output_data, list):
            node_output = NodeOutputData(output_name=output_name, items=output_data)
            self.node_outputs[node_id][output_name] = node_output
        elif isinstance(output_data, dict):
            # Single item output
            node_output = NodeOutputData(output_name=output_name, items=[output_data])
            self.node_outputs[node_id][output_name] = node_output
        else:
            raise ValueError(f"Invalid output data type: {type(output_data)}")
    
    def get_node_output(
        self,
        node_id: str,
        output_name: str = "main"
    ) -> Optional[NodeOutputData]:
        """Get output data from a node."""
        if node_id in self.node_outputs:
            return self.node_outputs[node_id].get(output_name)
        return None
    
    def get_node_output_items(
        self,
        node_id: str,
        output_name: str = "main"
    ) -> List[Dict[str, Any]]:
        """Get output items from a node."""
        output = self.get_node_output(node_id, output_name)
        return output.items if output else []
    
    def get_input_data_for_node(
        self,
        node_id: str,
        input_connections: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Get aggregated input data for a node from its connections."""
        all_input_items = []
        
        for connection in input_connections:
            source_node_id = connection.get("source_node_id")
            source_output = connection.get("source_output", "main")
            
            if source_node_id:
                items = self.get_node_output_items(source_node_id, source_output)
                all_input_items.extend(items)
        
        # If no connections, use workflow input data
        if not input_connections and self.input_data:
            all_input_items.append(self.input_data)
        
        return all_input_items
    
    def set_context(self, key: str, value: Any) -> None:
        """Set a context value."""
        self.context_data[key] = value
    
    def get_context(self, key: str, default: Any = None) -> Any:
        """Get a context value."""
        return self.context_data.get(key, default)
    
    def has_node_output(self, node_id: str, output_name: str = "main") -> bool:
        """Check if a node has output data."""
        return node_id in self.node_outputs and output_name in self.node_outputs[node_id]
    
    def to_json(self) -> str:
        """Convert execution data to JSON string."""
        data = {
            "node_outputs": {
                node_id: {
                    output_name: output.get_json()
                    for output_name, output in outputs.items()
                }
                for node_id, outputs in self.node_outputs.items()
            },
            "context_data": self.context_data,
            "input_data": self.input_data,
            "execution_id": self.execution_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
        }
        return json.dumps(data, default=str)
    
    @classmethod
    def from_json(cls, json_str: str) -> "ExecutionData":
        """Create ExecutionData from JSON string."""
        data = json.loads(json_str)
        
        # Reconstruct node outputs
        node_outputs = {}
        for node_id, outputs in data.get("node_outputs", {}).items():
            node_outputs[node_id] = {}
            for output_name, items in outputs.items():
                node_outputs[node_id][output_name] = NodeOutputData(
                    output_name=output_name,
                    items=items
                )
        
        return cls(
            node_outputs=node_outputs,
            context_data=data.get("context_data", {}),
            input_data=data.get("input_data"),
            execution_id=data.get("execution_id"),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert execution data to dictionary."""
        return {
            "node_outputs": {
                node_id: {
                    output_name: output.get_json()
                    for output_name, output in outputs.items()
                }
                for node_id, outputs in self.node_outputs.items()
            },
            "context_data": self.context_data,
            "input_data": self.input_data,
            "execution_id": self.execution_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
        }


class ExecutionMetrics(BaseModel):
    """Metrics collected during execution."""
    
    total_nodes_executed: int = 0
    successful_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0
    total_items_processed: int = 0
    execution_time_ms: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    
    def increment_executed(self) -> None:
        """Increment executed nodes count."""
        self.total_nodes_executed += 1
    
    def increment_successful(self) -> None:
        """Increment successful nodes count."""
        self.successful_nodes += 1
    
    def increment_failed(self) -> None:
        """Increment failed nodes count."""
        self.failed_nodes += 1
    
    def increment_skipped(self) -> None:
        """Increment skipped nodes count."""
        self.skipped_nodes += 1
    
    def add_items_processed(self, count: int) -> None:
        """Add to processed items count."""
        self.total_items_processed += count
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_nodes_executed == 0:
            return 0.0
        return (self.successful_nodes / self.total_nodes_executed) * 100