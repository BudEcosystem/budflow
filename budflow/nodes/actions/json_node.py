"""
JSON/Set node implementation for BudFlow.
Port of N8N's JSON node with full feature parity.
"""

import copy
import json
from typing import Dict, Any, List, Optional, Union

from budflow.nodes.base import (
    ActionNode, NodeDefinition, NodeCategory, NodeParameter, ParameterType
)
from budflow.workflows.models import NodeType


class JsonNode(ActionNode):
    """
    JSON/Set node for adding, removing, selecting and updating JSON fields.
    
    This node provides comprehensive JSON manipulation capabilities:
    - SET: Add or update fields
    - UNSET: Remove fields  
    - SELECT: Keep only specified fields
    - APPEND/PUSH: Add items to arrays
    - PREPEND: Add items to beginning of arrays
    - SHIFT: Remove first element from arrays
    """
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get JSON node definition matching N8N specification."""
        return NodeDefinition(
            name="Edit Fields",
            type=NodeType.ACTION,
            category=NodeCategory.TRANSFORM,
            description="Add, remove, select and update JSON fields",
            icon="fa:edit",
            color="#772244",
            version="1.0",
            parameters=[
                NodeParameter(
                    name="operation",
                    display_name="Operation",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="set",
                    description="The operation to perform",
                    options=["set", "unset", "select", "append", "prepend", "push", "shift"]
                ),
                NodeParameter(
                    name="fields",
                    display_name="Fields to Set",
                    type=ParameterType.ARRAY,
                    required=False,
                    description="Fields to set with their values",
                    default=[]
                ),
                NodeParameter(
                    name="keep_only_set",
                    display_name="Keep Only Set",
                    type=ParameterType.BOOLEAN,
                    required=False,
                    default=False,
                    description="If true, removes all other fields except those being set"
                ),
                NodeParameter(
                    name="fields_to_remove",
                    display_name="Fields to Remove",
                    type=ParameterType.ARRAY,
                    required=False,
                    description="Field names to remove from the data",
                    default=[]
                ),
                NodeParameter(
                    name="fields_to_select",
                    display_name="Fields to Select",
                    type=ParameterType.ARRAY,
                    required=False,
                    description="Field names to keep (all others will be removed)",
                    default=[]
                ),
                NodeParameter(
                    name="fields_to_shift",
                    display_name="Fields to Shift",
                    type=ParameterType.ARRAY,
                    required=False,
                    description="Array field names to shift (remove first element)",
                    default=[]
                )
            ],
            inputs=["main"],
            outputs=["main"]
        )
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute JSON operations on input data."""
        operation = self.get_parameter("operation", "set")
        
        if not self.context.input_data:
            return []
        
        result = []
        
        for item in self.context.input_data:
            # Ensure we have a dictionary to work with
            if not isinstance(item, dict):
                item = {"_original": item}
            
            # Deep copy to avoid modifying original data
            processed_item = copy.deepcopy(item)
            
            try:
                if operation == "set":
                    processed_item = await self._handle_set_operation(processed_item)
                elif operation == "unset":
                    processed_item = await self._handle_unset_operation(processed_item)
                elif operation == "select":
                    processed_item = await self._handle_select_operation(processed_item)
                elif operation in ["append", "push"]:
                    processed_item = await self._handle_append_operation(processed_item)
                elif operation == "prepend":
                    processed_item = await self._handle_prepend_operation(processed_item)
                elif operation == "shift":
                    processed_item = await self._handle_shift_operation(processed_item)
                else:
                    raise ValueError(f"Unsupported operation: {operation}")
                
                result.append(processed_item)
                
            except Exception as e:
                self.logger.error(
                    f"Error processing item with {operation} operation",
                    item=item,
                    error=str(e),
                    exc_info=True
                )
                # Re-raise for now, but could be configurable to continue
                raise
        
        return result
    
    async def _handle_set_operation(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Handle SET operation - add or update fields."""
        fields = self.get_parameter("fields", [])
        keep_only_set = self.get_parameter("keep_only_set", False)
        
        # If keep_only_set, start with empty dict
        if keep_only_set:
            result = {}
        else:
            result = item.copy()
        
        # Process each field to set
        for field_config in fields:
            if not isinstance(field_config, dict):
                continue
                
            field_name = field_config.get("name", "")
            field_value = field_config.get("value", "")
            
            if not field_name:
                continue
            
            # Evaluate expressions in field value
            try:
                if isinstance(field_value, str):
                    if field_value.startswith("={{") and field_value.endswith("}}"):
                        # Pure expression
                        expr = field_value[3:-2]  # Remove ={{ and }}
                        evaluated_value = self.evaluate_expression(f"{{{{{expr}}}}}", item)
                    elif "{{" in field_value and "}}" in field_value:
                        # Template with expressions
                        evaluated_value = self.evaluate_expression(field_value, item)
                    else:
                        # Static value
                        evaluated_value = field_value
                else:
                    # Non-string value (number, boolean, object, etc.)
                    evaluated_value = field_value
            except Exception as e:
                self.logger.warning(
                    f"Expression evaluation failed for field {field_name}",
                    expression=field_value,
                    error=str(e)
                )
                evaluated_value = field_value
            
            # Set the field (handles nested paths)
            self._set_nested_field(result, field_name, evaluated_value)
        
        return result
    
    async def _handle_unset_operation(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Handle UNSET operation - remove fields."""
        fields_to_remove = self.get_parameter("fields_to_remove", [])
        result = item.copy()
        
        for field_name in fields_to_remove:
            if not isinstance(field_name, str):
                continue
            self._unset_nested_field(result, field_name)
        
        return result
    
    async def _handle_select_operation(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Handle SELECT operation - keep only specified fields."""
        fields_to_select = self.get_parameter("fields_to_select", [])
        result = {}
        
        for field_name in fields_to_select:
            if not isinstance(field_name, str):
                continue
            
            # Get the field value if it exists
            value = self._get_nested_field(item, field_name)
            if value is not None:
                self._set_nested_field(result, field_name, value)
        
        return result
    
    async def _handle_append_operation(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Handle APPEND/PUSH operation - add items to end of arrays."""
        fields = self.get_parameter("fields", [])
        result = item.copy()
        
        for field_config in fields:
            if not isinstance(field_config, dict):
                continue
                
            field_name = field_config.get("name", "")
            field_value = field_config.get("value")
            
            if not field_name:
                continue
            
            # Get current array or create new one
            current_value = self._get_nested_field(result, field_name)
            
            if current_value is None:
                # Field doesn't exist, create new array
                new_array = [field_value]
            elif isinstance(current_value, list):
                # Field is already an array, append to it
                new_array = current_value + [field_value]
            else:
                # Field exists but is not an array, convert to array
                new_array = [current_value, field_value]
            
            self._set_nested_field(result, field_name, new_array)
        
        return result
    
    async def _handle_prepend_operation(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Handle PREPEND operation - add items to beginning of arrays."""
        fields = self.get_parameter("fields", [])
        result = item.copy()
        
        for field_config in fields:
            if not isinstance(field_config, dict):
                continue
                
            field_name = field_config.get("name", "")
            field_value = field_config.get("value")
            
            if not field_name:
                continue
            
            # Get current array or create new one
            current_value = self._get_nested_field(result, field_name)
            
            if current_value is None:
                # Field doesn't exist, create new array
                new_array = [field_value]
            elif isinstance(current_value, list):
                # Field is already an array, prepend to it
                new_array = [field_value] + current_value
            else:
                # Field exists but is not an array, convert to array
                new_array = [field_value, current_value]
            
            self._set_nested_field(result, field_name, new_array)
        
        return result
    
    async def _handle_shift_operation(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Handle SHIFT operation - remove first element from arrays."""
        fields_to_shift = self.get_parameter("fields_to_shift", [])
        result = item.copy()
        
        for field_name in fields_to_shift:
            if not isinstance(field_name, str):
                continue
            
            current_value = self._get_nested_field(result, field_name)
            
            if isinstance(current_value, list) and len(current_value) > 0:
                # Remove first element
                new_array = current_value[1:]
                self._set_nested_field(result, field_name, new_array)
        
        return result
    
    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get a nested field value using dot notation."""
        if "." not in field_path:
            return data.get(field_path)
        
        parts = field_path.split(".")
        current = data
        
        for part in parts:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        
        return current
    
    def _set_nested_field(self, data: Dict[str, Any], field_path: str, value: Any) -> None:
        """Set a nested field value using dot notation."""
        if "." not in field_path:
            data[field_path] = value
            return
        
        parts = field_path.split(".")
        current = data
        
        # Navigate to the parent of the target field
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                # Overwrite non-dict values with dict
                current[part] = {}
            current = current[part]
        
        # Set the final field
        current[parts[-1]] = value
    
    def _unset_nested_field(self, data: Dict[str, Any], field_path: str) -> None:
        """Remove a nested field using dot notation."""
        if "." not in field_path:
            data.pop(field_path, None)
            return
        
        parts = field_path.split(".")
        current = data
        
        # Navigate to the parent of the target field
        for part in parts[:-1]:
            if not isinstance(current, dict) or part not in current:
                return  # Path doesn't exist
            current = current[part]
        
        # Remove the final field if it exists
        if isinstance(current, dict):
            current.pop(parts[-1], None)