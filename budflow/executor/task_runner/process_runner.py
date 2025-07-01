"""Process-based task runner implementation."""

import json
import asyncio
import tempfile
import time
import os
import sys
import resource
import signal
from typing import Dict, Any, Optional
from pathlib import Path
import ast

import structlog

from . import (
    TaskRunner,
    TaskResult,
    TaskTimeout,
    TaskMemoryExceeded,
    TaskExecutionError,
    ProcessConfig,
)

logger = structlog.get_logger()


class ProcessTaskRunner(TaskRunner):
    """Process-based task runner for secure code execution."""
    
    def __init__(self, config: ProcessConfig):
        """Initialize process task runner."""
        self.config = config
        self.blocked_modules = [
            'os', 'subprocess', 'socket', 'sys', '__builtins__',
            'eval', 'exec', 'compile', 'open', 'file', 'input',
            'importlib', 'pickle', 'shelve', 'marshal'
        ]
    
    async def run(
        self,
        code: str,
        input_data: Dict[str, Any],
        timeout: Optional[int] = None
    ) -> TaskResult:
        """Run code in subprocess."""
        start_time = time.time()
        
        try:
            # Validate code safety
            self._validate_code(code)
            
            # Prepare code and input
            wrapped_code = self._wrap_code(code, input_data)
            
            # Calculate timeout
            timeout_seconds = timeout or self.config.resource_limits.timeout_seconds
            
            # Execute in subprocess
            result = await self._execute_in_process(wrapped_code, timeout_seconds)
            
            # Calculate execution time
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            return TaskResult(
                success=result['success'],
                output=result['output'],
                error=result.get('error'),
                exit_code=result['exit_code'],
                execution_time_ms=execution_time_ms,
                logs=result.get('logs', [])
            )
            
        except TaskTimeout:
            raise
        except Exception as e:
            logger.error(f"Process execution failed: {str(e)}")
            raise TaskExecutionError(f"Process execution failed: {str(e)}")
    
    def _validate_code(self, code: str):
        """Validate code for security issues."""
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            # Allow syntax errors to be caught during execution
            return
        
        # Check for blocked imports
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if any(alias.name.startswith(blocked) for blocked in self.blocked_modules):
                        raise TaskExecutionError(f"Import of blocked module '{alias.name}' is not allowed")
            
            elif isinstance(node, ast.ImportFrom):
                if node.module and any(node.module.startswith(blocked) for blocked in self.blocked_modules):
                    raise TaskExecutionError(f"Import from blocked module '{node.module}' is not allowed")
            
            # Check for dangerous function calls
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ['eval', 'exec', 'compile', '__import__']:
                        raise TaskExecutionError(f"Use of '{node.func.id}' is not allowed")
    
    def _wrap_code(self, code: str, input_data: Dict[str, Any]) -> str:
        """Wrap user code with input handling and output capture."""
        # Create wrapper that captures output and handles errors
        wrapper = f'''
import json
import sys
import traceback

# Restricted imports
def restricted_import(name, *args, **kwargs):
    blocked = {self.blocked_modules}
    if any(name.startswith(b) for b in blocked):
        raise ImportError(f"Import of '{{name}}' is blocked")
    return __import__(name, *args, **kwargs)

# Override import
__builtins__['__import__'] = restricted_import

# Set input data from environment
try:
    INPUT_DATA = os.environ.get('INPUT_DATA', '{{}}')
except:
    INPUT_DATA = '{json.dumps(json.dumps(input_data))}'

# Capture output
output_parts = []
original_print = print

def captured_print(*args, **kwargs):
    output = ' '.join(str(arg) for arg in args)
    output_parts.append(output)

print = captured_print

# Execute user code
try:
{self._indent_code(code)}
    
    # Process output
    output_text = '\\n'.join(output_parts).strip()
    if output_text:
        try:
            output = json.loads(output_text)
        except:
            output = output_text
    else:
        output = None
    
    result = {{
        "success": True,
        "output": output,
        "exit_code": 0
    }}
    
except Exception as e:
    result = {{
        "success": False,
        "output": None,
        "error": str(e) + "\\n" + traceback.format_exc(),
        "exit_code": 1
    }}

# Print result
print = original_print
print(json.dumps(result))
'''
        return wrapper
    
    def _indent_code(self, code: str) -> str:
        """Indent code for insertion into wrapper."""
        lines = code.strip().split('\n')
        return '\n'.join(f'    {line}' for line in lines)
    
    async def _execute_in_process(self, code: str, timeout: float) -> Dict[str, Any]:
        """Execute code in subprocess with resource limits."""
        # Write code to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            code_file = f.name
        
        try:
            # Prepare environment
            env = os.environ.copy()
            if self.config.environment:
                env.update(self.config.environment)
            
            # Add input data to environment
            if 'INPUT_DATA' not in env:
                env['INPUT_DATA'] = json.dumps({})
            
            # Create subprocess
            process = await asyncio.create_subprocess_exec(
                self.config.python_path,
                code_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=self.config.working_dir,
                preexec_fn=self._setup_resource_limits if self.config.use_sandbox else None
            )
            
            # Wait for completion with timeout
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise TaskTimeout(f"Execution timed out after {timeout} seconds")
            
            # Check output size
            output_size = len(stdout) + len(stderr)
            if output_size > self.config.resource_limits.max_output_size:
                raise TaskExecutionError(
                    f"Output size {output_size} exceeded limit of {self.config.resource_limits.max_output_size}"
                )
            
            # Parse result
            return self._parse_output(stdout, stderr, process.returncode)
            
        finally:
            # Clean up temp file
            try:
                Path(code_file).unlink()
            except:
                pass
    
    def _setup_resource_limits(self):
        """Set resource limits for subprocess (Unix only)."""
        if not hasattr(resource, 'setrlimit'):
            return
        
        limits = self.config.resource_limits
        
        # Memory limit
        if limits.memory_mb:
            memory_bytes = limits.memory_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
        
        # CPU time limit
        if limits.timeout_seconds:
            resource.setrlimit(resource.RLIMIT_CPU, (limits.timeout_seconds, limits.timeout_seconds))
        
        # Disable core dumps
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        
        # Limit file descriptors
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    
    def _parse_output(self, stdout: bytes, stderr: bytes, returncode: int) -> Dict[str, Any]:
        """Parse subprocess output."""
        try:
            output_text = stdout.decode('utf-8').strip()
            error_text = stderr.decode('utf-8').strip()
            
            if output_text:
                # Try to find JSON result in output
                lines = output_text.split('\n')
                for line in reversed(lines):
                    if line.strip():
                        try:
                            result = json.loads(line)
                            if isinstance(result, dict) and 'success' in result:
                                result['logs'] = lines[:-1] + ([error_text] if error_text else [])
                                return result
                        except:
                            continue
            
            # No valid result found
            return {
                'success': returncode == 0,
                'output': output_text if output_text else None,
                'error': error_text if error_text or returncode != 0 else None,
                'exit_code': returncode,
                'logs': output_text.split('\n') + ([error_text] if error_text else [])
            }
            
        except Exception as e:
            return {
                'success': False,
                'output': None,
                'error': f"Failed to parse output: {str(e)}",
                'exit_code': returncode,
                'logs': []
            }
    
    async def cleanup(self) -> None:
        """Clean up process resources."""
        # Nothing specific to clean up for process runner
        pass