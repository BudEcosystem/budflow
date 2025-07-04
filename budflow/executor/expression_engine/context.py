"""Expression evaluation context."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from budflow.workflows.models import Workflow, WorkflowExecution, NodeExecution


class ExpressionContext:
    """
    Context for expression evaluation containing workflow data and execution state.
    
    This class provides access to N8N-compatible variables and execution context
    for expression evaluation.
    """
    
    def __init__(
        self,
        workflow: Optional[Workflow] = None,
        execution: Optional[WorkflowExecution] = None,
        current_node_id: Optional[Union[int, str]] = None,
        current_item_index: int = 0,
        node_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ):
        self.workflow = workflow
        self.execution = execution
        self.current_node_id = current_node_id
        self.current_item_index = current_item_index
        self.node_data = node_data or {}
        self.additional_context = additional_context or {}
        
        # Create datetime helper
        self._now = DateTimeHelper()
        
        # Create input helper
        self._input = InputHelper(self)
        
        # Create node helper
        self._node = NodeHelper(self)
        
        # Create workflow helper
        self._workflow = WorkflowHelper(self.workflow) if self.workflow else None
        
        # Create execution helper
        self._execution = ExecutionHelper(self.execution) if self.execution else None
    
    def add_node_data(self, node_name: str, data: List[Dict[str, Any]]) -> None:
        """Add data for a specific node."""
        self.node_data[node_name] = data
    
    def get_node_data(self, node_name: str) -> List[Dict[str, Any]]:
        """Get data for a specific node."""
        return self.node_data.get(node_name, [])
    
    def get_current_item(self) -> Dict[str, Any]:
        """Get current item being processed."""
        if self.current_node_id and str(self.current_node_id) in self.node_data:
            data = self.node_data[str(self.current_node_id)]
            if data and self.current_item_index < len(data):
                return data[self.current_item_index]
        return {}
    
    def to_jinja_context(self) -> Dict[str, Any]:
        """Convert to Jinja template context."""
        context = {
            # N8N-compatible variables
            "$json": self.get_current_item(),
            "$node": self._node,
            "$input": self._input,
            "$now": self._now,
            "$workflow": self._workflow,
            "$execution": self._execution,
            
            # Direct access to data
            "json": self.get_current_item(),
            "node": self._node,
            "input": self._input,
            "now": self._now,
            "workflow": self._workflow,
            "execution": self._execution,
            
            # Node data
            "node_data": self.node_data,
            
            # Additional context
            **self.additional_context
        }
        
        return context


class DateTimeHelper:
    """Helper for date/time operations compatible with N8N."""
    
    def __init__(self):
        self._current_time = datetime.now(timezone.utc)
    
    def __str__(self) -> str:
        return self._current_time.isoformat()
    
    def __repr__(self) -> str:
        return f"DateTimeHelper({self._current_time})"
    
    def format(self, format_str: str) -> str:
        """Format current datetime with N8N-compatible format strings."""
        # Convert N8N format to Python format
        python_format = self._convert_n8n_format(format_str)
        return self._current_time.strftime(python_format)
    
    def plus(self, amount: int, unit: str) -> "DateTimeHelper":
        """Add time to current datetime."""
        from datetime import timedelta
        
        if unit in ('day', 'days'):
            new_time = self._current_time + timedelta(days=amount)
        elif unit in ('hour', 'hours'):
            new_time = self._current_time + timedelta(hours=amount)
        elif unit in ('minute', 'minutes'):
            new_time = self._current_time + timedelta(minutes=amount)
        elif unit in ('second', 'seconds'):
            new_time = self._current_time + timedelta(seconds=amount)
        else:
            raise ValueError(f"Unsupported time unit: {unit}")
        
        result = DateTimeHelper()
        result._current_time = new_time
        return result
    
    def minus(self, amount: int, unit: str) -> "DateTimeHelper":
        """Subtract time from current datetime."""
        return self.plus(-amount, unit)
    
    def _convert_n8n_format(self, format_str: str) -> str:
        """Convert N8N date format to Python strftime format."""
        # Common N8N to Python format conversions
        conversions = {
            'YYYY': '%Y',
            'YY': '%y',
            'MM': '%m',
            'DD': '%d',
            'HH': '%H',
            'mm': '%M',
            'ss': '%S',
            'SSS': '%f',
        }
        
        result = format_str
        for n8n_format, python_format in conversions.items():
            result = result.replace(n8n_format, python_format)
        
        return result
    
    def __getattr__(self, name: str) -> Any:
        """Delegate to underlying datetime object."""
        return getattr(self._current_time, name)


class InputHelper:
    """Helper for accessing input data compatible with N8N."""
    
    def __init__(self, context: ExpressionContext):
        self.context = context
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all input items."""
        if self.context.current_node_id:
            return self.context.get_node_data(str(self.context.current_node_id))
        return []
    
    def first(self) -> Dict[str, Any]:
        """Get first input item."""
        items = self.all()
        return items[0] if items else {}
    
    def last(self) -> Dict[str, Any]:
        """Get last input item."""
        items = self.all()
        return items[-1] if items else {}
    
    def item(self, index: int) -> Dict[str, Any]:
        """Get specific input item by index."""
        items = self.all()
        return items[index] if 0 <= index < len(items) else {}
    
    def length(self) -> int:
        """Get number of input items."""
        return len(self.all())
    
    def __len__(self) -> int:
        """Support len() function."""
        return self.length()
    
    def __getitem__(self, index: int) -> Dict[str, Any]:
        """Support indexing."""
        return self.item(index)
    
    def __iter__(self):
        """Support iteration."""
        return iter(self.all())


class NodeHelper:
    """Helper for accessing node data compatible with N8N."""
    
    def __init__(self, context: ExpressionContext):
        self.context = context
    
    def __getattr__(self, node_name: str) -> "NodeDataHelper":
        """Get data helper for specific node."""
        return NodeDataHelper(self.context, node_name)
    
    def __call__(self, node_name: str) -> "NodeDataHelper":
        """Alternative syntax for accessing node data."""
        return NodeDataHelper(self.context, node_name)


class NodeDataHelper:
    """Helper for accessing specific node's data."""
    
    def __init__(self, context: ExpressionContext, node_name: str):
        self.context = context
        self.node_name = node_name
    
    @property
    def json(self) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get JSON data from the node."""
        data = self.context.get_node_data(self.node_name)
        # If there's only one item, return it directly for convenience
        if len(data) == 1:
            return data[0]
        return data
    
    def __getitem__(self, index: int) -> Dict[str, Any]:
        """Support indexing to get specific item."""
        data = self.context.get_node_data(self.node_name)
        return data[index] if 0 <= index < len(data) else {}


class WorkflowHelper:
    """Helper for accessing workflow data."""
    
    def __init__(self, workflow: Optional[Workflow]):
        self._workflow = workflow
    
    @property
    def id(self) -> Optional[int]:
        """Get workflow ID."""
        return self._workflow.id if self._workflow else None
    
    @property
    def name(self) -> Optional[str]:
        """Get workflow name."""
        return self._workflow.name if self._workflow else None
    
    @property
    def uuid(self) -> Optional[str]:
        """Get workflow UUID."""
        return getattr(self._workflow, 'uuid', None) if self._workflow else None
    
    def __getattr__(self, name: str) -> Any:
        """Delegate to workflow object."""
        if self._workflow:
            return getattr(self._workflow, name, None)
        return None


class ExecutionHelper:
    """Helper for accessing execution data."""
    
    def __init__(self, execution: Optional[WorkflowExecution]):
        self._execution = execution
    
    @property
    def id(self) -> Optional[int]:
        """Get execution ID."""
        return self._execution.id if self._execution else None
    
    @property
    def uuid(self) -> Optional[str]:
        """Get execution UUID."""
        return self._execution.uuid if self._execution else None
    
    @property
    def mode(self) -> Optional[str]:
        """Get execution mode."""
        if self._execution and self._execution.mode:
            return self._execution.mode.value
        return None
    
    @property
    def status(self) -> Optional[str]:
        """Get execution status."""
        if self._execution and self._execution.status:
            return self._execution.status.value
        return None
    
    @property
    def data(self) -> Optional[Dict[str, Any]]:
        """Get execution data."""
        return self._execution.data if self._execution else None
    
    def __getattr__(self, name: str) -> Any:
        """Delegate to execution object."""
        if self._execution:
            return getattr(self._execution, name, None)
        return None