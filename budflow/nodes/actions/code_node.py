"""
Code node implementation for BudFlow.
Port of N8N's Code node with full feature parity.
"""

import ast
import sys
import time
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
import threading
import subprocess
import tempfile
import os

from budflow.nodes.base import (
    ActionNode, NodeDefinition, NodeCategory, NodeParameter, ParameterType
)
from budflow.workflows.models import NodeType


class CodeExecutionError(Exception):
    """Exception raised when code execution fails."""
    pass


class SecurityViolationError(Exception):
    """Exception raised when code violates security constraints."""
    pass


class InputHelper:
    """Helper class for accessing input data in code."""
    
    def __init__(self, input_data: List[Dict[str, Any]]):
        self._data = input_data or []
    
    def all(self) -> List[Dict[str, Any]]:
        """Get all input items."""
        return self._data
    
    def first(self) -> Optional[Dict[str, Any]]:
        """Get first input item."""
        return self._data[0] if self._data else None
    
    def last(self) -> Optional[Dict[str, Any]]:
        """Get last input item."""
        return self._data[-1] if self._data else None
    
    def item(self, index: int) -> Optional[Dict[str, Any]]:
        """Get item by index."""
        try:
            return self._data[index]
        except IndexError:
            return None
    
    def __len__(self) -> int:
        """Get number of items."""
        return len(self._data)


class WorkflowHelper:
    """Helper class for accessing workflow context."""
    
    def __init__(self, workflow_id: str = None):
        self.id = workflow_id or "unknown"


class ExecutionHelper:
    """Helper class for accessing execution context."""
    
    def __init__(self, execution_id: str = None):
        self.id = execution_id or "unknown"


class NodeHelper:
    """Helper class for accessing node context."""
    
    def __init__(self, node_name: str = None):
        self.name = node_name or "unknown"


class PythonSandbox:
    """Secure Python code execution sandbox."""
    
    # Allowed built-in functions
    ALLOWED_BUILTINS = {
        'abs', 'all', 'any', 'bin', 'bool', 'chr', 'dict', 'dir', 'divmod',
        'enumerate', 'filter', 'float', 'format', 'frozenset', 'getattr',
        'hasattr', 'hash', 'hex', 'id', 'int', 'isinstance', 'issubclass',
        'iter', 'len', 'list', 'map', 'max', 'min', 'oct', 'ord', 'pow',
        'range', 'repr', 'reversed', 'round', 'set', 'slice', 'sorted',
        'str', 'sum', 'tuple', 'type', 'zip', 'print'
    }
    
    # Allowed modules
    ALLOWED_MODULES = {
        'json', 'datetime', 'collections', 'itertools', 'functools',
        'operator', 'math', 'statistics', 're', 'uuid', 'base64',
        'hashlib', 'hmac', 'urllib.parse', 'html', 'xml.etree.ElementTree'
    }
    
    # Restricted AST nodes
    RESTRICTED_NODES = {
        ast.Import, ast.ImportFrom, ast.Global, ast.Nonlocal
    }
    
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=1)
    
    def validate_code(self, code: str) -> None:
        """Validate code for security violations."""
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            raise CodeExecutionError(f"Syntax error in code: {e}")
        
        for node in ast.walk(tree):
            # Check for restricted nodes (but allow Import and ImportFrom, we validate them separately)
            restricted_nodes = {ast.Global, ast.Nonlocal}
            if type(node) in restricted_nodes:
                raise SecurityViolationError(f"Restricted operation: {type(node).__name__}")
            
            # Check imports
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                self._validate_import(node)
    
    def _validate_import(self, node: Union[ast.Import, ast.ImportFrom]) -> None:
        """Validate import statements."""
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in self.ALLOWED_MODULES:
                    raise SecurityViolationError(f"Import not allowed: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module not in self.ALLOWED_MODULES:
                raise SecurityViolationError(f"Import not allowed: {node.module}")
    
    def create_safe_globals(self, context_vars: Dict[str, Any]) -> Dict[str, Any]:
        """Create safe global namespace for code execution."""
        # Get builtins correctly for different Python versions
        if isinstance(__builtins__, dict):
            builtins_dict = __builtins__
        else:
            builtins_dict = __builtins__.__dict__
            
        safe_builtins = {name: builtins_dict[name] 
                        for name in self.ALLOWED_BUILTINS 
                        if name in builtins_dict}
        
        # Add custom __import__ that only allows safe modules
        def safe_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name not in self.ALLOWED_MODULES:
                raise SecurityViolationError(f"Import not allowed: {name}")
            return __import__(name, globals, locals, fromlist, level)
        
        safe_globals = {
            '__builtins__': {**safe_builtins, '__import__': safe_import},
            **context_vars
        }
        
        return safe_globals
    
    def execute(self, code: str, context_vars: Dict[str, Any], timeout: int = 30) -> Any:
        """Execute Python code in sandbox."""
        # Process code to replace special variables
        processed_code = self._process_python_code(code)
        self.validate_code(processed_code)
        
        def run_code():
            safe_globals = self.create_safe_globals(context_vars)
            
            # Wrap code in a function to handle return statements
            wrapped_code = f"""
def __user_function__():
{self._indent_code(processed_code)}

__result__ = __user_function__()
"""
            
            local_vars = {}
            
            try:
                exec(wrapped_code, safe_globals, local_vars)
                return local_vars.get('__result__')
            except Exception as e:
                raise CodeExecutionError(f"Code execution failed: {str(e)}")
        
        try:
            future = self.executor.submit(run_code)
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            raise CodeExecutionError(f"Code execution timed out after {timeout} seconds")
        except Exception as e:
            raise CodeExecutionError(f"Code execution failed: {str(e)}")
    
    def _process_python_code(self, code: str) -> str:
        """Process Python code to replace special variables with valid Python syntax."""
        # Replace $ variables with their Python equivalents
        replacements = {
            '$input': '__input__',
            '$workflow': '__workflow__',
            '$execution': '__execution__',
            '$node': '__node__',
            '$now': '__now__'
        }
        
        processed = code
        for old, new in replacements.items():
            processed = processed.replace(old, new)
        
        return processed
    
    def _indent_code(self, code: str) -> str:
        """Indent code for wrapping in function."""
        lines = code.split('\n')
        indented_lines = []
        for line in lines:
            if line.strip():  # Only indent non-empty lines
                indented_lines.append('    ' + line)
            else:
                indented_lines.append(line)
        return '\n'.join(indented_lines)


class JavaScriptSandbox:
    """Secure JavaScript code execution sandbox using Node.js."""
    
    def __init__(self):
        self.node_executable = self._find_node_executable()
    
    def _find_node_executable(self) -> Optional[str]:
        """Find Node.js executable."""
        try:
            result = subprocess.run(['which', 'node'], capture_output=True, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
        except:
            pass
        
        # Try common paths
        for path in ['/usr/bin/node', '/usr/local/bin/node', '/opt/homebrew/bin/node']:
            if os.path.exists(path):
                return path
        
        return None
    
    def execute(self, code: str, context_vars: Dict[str, Any], timeout: int = 30) -> Any:
        """Execute JavaScript code in sandbox."""
        if not self.node_executable:
            raise CodeExecutionError("Node.js not found. JavaScript execution requires Node.js to be installed.")
        
        # Create wrapper script
        wrapper_script = self._create_wrapper_script(code, context_vars)
        
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
                f.write(wrapper_script)
                script_path = f.name
            
            # Execute with timeout
            result = subprocess.run(
                [self.node_executable, script_path],
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode != 0:
                raise CodeExecutionError(f"JavaScript execution failed: {result.stderr}")
            
            # Parse JSON result
            import json
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                raise CodeExecutionError("JavaScript code must return a valid JSON array")
                
        except subprocess.TimeoutExpired:
            raise CodeExecutionError(f"JavaScript execution timed out after {timeout} seconds")
        finally:
            # Clean up temporary file
            try:
                os.unlink(script_path)
            except:
                pass
    
    def _create_wrapper_script(self, user_code: str, context_vars: Dict[str, Any]) -> str:
        """Create wrapper script for safe JavaScript execution."""
        import json
        
        # Serialize context variables
        context_json = json.dumps(context_vars, default=str)
        
        wrapper = f"""
// Security: Remove dangerous globals
delete global.process;
delete global.require;
delete global.Buffer;
delete global.setImmediate;
delete global.clearImmediate;

// Setup context
const context = {context_json};
const $input = context.$input;
const $workflow = context.$workflow;
const $execution = context.$execution;
const $node = context.$node;
const $now = context.$now;

// User code wrapper
function executeUserCode() {{
    {user_code}
}}

try {{
    const result = executeUserCode();
    console.log(JSON.stringify(result));
}} catch (error) {{
    console.error(JSON.stringify({{error: error.message, stack: error.stack}}));
    process.exit(1);
}}
"""
        return wrapper


class CodeNode(ActionNode):
    """
    Code node for executing JavaScript or Python code.
    
    This node provides secure code execution capabilities:
    - Python and JavaScript language support
    - Sandboxed execution environment
    - Access to input data and workflow context
    - Security restrictions on imports and globals
    - Timeout protection against infinite loops
    """
    
    @classmethod
    def get_definition(cls) -> NodeDefinition:
        """Get Code node definition matching N8N specification."""
        return NodeDefinition(
            name="Code",
            type=NodeType.ACTION,
            category=NodeCategory.UTILITY,
            description="Execute JavaScript or Python code",
            icon="fa:code",
            color="#FF6B6B",
            version="1.0",
            parameters=[
                NodeParameter(
                    name="language",
                    display_name="Language",
                    type=ParameterType.OPTIONS,
                    required=True,
                    default="python",
                    description="Programming language to execute",
                    options=["python", "javascript"]
                ),
                NodeParameter(
                    name="code",
                    display_name="Code",
                    type=ParameterType.STRING,
                    required=True,
                    description="Code to execute",
                    multiline=True,
                    placeholder="# Python code\nreturn [{'result': 'Hello World'}]"
                ),
                NodeParameter(
                    name="mode",
                    display_name="Mode",
                    type=ParameterType.OPTIONS,
                    required=False,
                    default="runOnceForAllItems",
                    description="Execution mode",
                    options=["runOnceForAllItems", "runOnceForEachItem"]
                ),
                NodeParameter(
                    name="continue_on_fail",
                    display_name="Continue on Fail",
                    type=ParameterType.BOOLEAN,
                    required=False,
                    default=False,
                    description="Continue execution even if code fails for some items"
                ),
                NodeParameter(
                    name="timeout",
                    display_name="Timeout (seconds)",
                    type=ParameterType.NUMBER,
                    required=False,
                    default=30,
                    min_value=1,
                    max_value=300,
                    description="Maximum execution time in seconds"
                )
            ],
            inputs=["main"],
            outputs=["main"]
        )
    
    async def execute(self) -> List[Dict[str, Any]]:
        """Execute code with the specified language and mode."""
        language = self.get_parameter("language", "python")
        code = self.get_parameter("code", "")
        mode = self.get_parameter("mode", "runOnceForAllItems")
        continue_on_fail = self.get_parameter("continue_on_fail", False)
        timeout = self.get_parameter("timeout", 30)
        
        if not code.strip():
            raise CodeExecutionError("Code parameter is required")
        
        if not self.context.input_data:
            return []
        
        # Initialize sandbox
        if language == "python":
            sandbox = PythonSandbox()
        elif language == "javascript":
            sandbox = JavaScriptSandbox()
        else:
            raise CodeExecutionError(f"Unsupported language: {language}")
        
        if mode == "runOnceForAllItems":
            return await self._execute_once_for_all(sandbox, code, timeout, continue_on_fail)
        elif mode == "runOnceForEachItem":
            return await self._execute_once_per_item(sandbox, code, timeout, continue_on_fail)
        else:
            raise CodeExecutionError(f"Unsupported mode: {mode}")
    
    async def _execute_once_for_all(
        self, 
        sandbox, 
        code: str, 
        timeout: int,
        continue_on_fail: bool
    ) -> List[Dict[str, Any]]:
        """Execute code once for all items."""
        context_vars = self._create_context_vars(self.context.input_data)
        
        try:
            result = sandbox.execute(code, context_vars, timeout)
            
            # Ensure result is a list
            if result is None:
                return []
            elif isinstance(result, list):
                return result
            else:
                return [result] if isinstance(result, dict) else [{"result": result}]
                
        except Exception as e:
            if continue_on_fail:
                self.logger.error(f"Code execution failed: {e}")
                return [{"error": str(e), "_execution_failed": True}]
            else:
                raise
    
    async def _execute_once_per_item(
        self, 
        sandbox, 
        code: str, 
        timeout: int,
        continue_on_fail: bool
    ) -> List[Dict[str, Any]]:
        """Execute code once for each item."""
        results = []
        
        for i, item in enumerate(self.context.input_data):
            context_vars = self._create_context_vars([item])
            
            try:
                result = sandbox.execute(code, context_vars, timeout)
                
                # Ensure result is a list and take first item
                if result is None:
                    continue
                elif isinstance(result, list):
                    if result:
                        results.extend(result)
                else:
                    results.append(result if isinstance(result, dict) else {"result": result})
                    
            except Exception as e:
                if continue_on_fail:
                    self.logger.error(f"Code execution failed for item {i}: {e}")
                    results.append({"error": str(e), "_execution_failed": True, "_item_index": i})
                else:
                    raise
        
        return results
    
    def _create_context_vars(self, input_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create context variables for code execution."""
        # Create helper objects
        input_helper = InputHelper(input_data)
        workflow_helper = WorkflowHelper(getattr(self.context, 'workflow_id', None))
        execution_helper = ExecutionHelper(getattr(self.context, 'execution_id', None))
        node_helper = NodeHelper(getattr(self.context, 'node_name', None))
        
        return {
            # Primary variables (used after processing)
            '__input__': input_helper,
            '__workflow__': workflow_helper,
            '__execution__': execution_helper,
            '__node__': node_helper,
            '__now__': datetime.now().isoformat(),
            # For backward compatibility and JavaScript
            '$input': input_helper,
            '$workflow': workflow_helper,
            '$execution': execution_helper,
            '$node': node_helper,
            '$now': datetime.now().isoformat(),
            'input': input_helper,
            'workflow': workflow_helper,
            'execution': execution_helper,
            'node': node_helper,
            'now': datetime.now().isoformat()
        }