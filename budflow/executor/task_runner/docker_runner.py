"""Docker-based task runner implementation."""

import json
import asyncio
import tempfile
import time
from typing import Dict, Any, Optional
from pathlib import Path

import docker
from docker.errors import DockerException, ContainerError, APIError
import structlog

from . import (
    TaskRunner,
    TaskResult,
    TaskTimeout,
    TaskMemoryExceeded,
    TaskExecutionError,
    DockerConfig,
)

logger = structlog.get_logger()


class DockerTaskRunner(TaskRunner):
    """Docker-based task runner for secure code execution."""
    
    def __init__(self, config: DockerConfig):
        """Initialize Docker task runner."""
        self.config = config
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Docker client."""
        try:
            self.client = docker.from_env()
            # Test connection
            self.client.ping()
            logger.info("Docker client initialized successfully")
        except DockerException as e:
            logger.error(f"Failed to initialize Docker client: {str(e)}")
            raise TaskExecutionError(f"Docker initialization failed: {str(e)}")
    
    async def run(
        self,
        code: str,
        input_data: Dict[str, Any],
        timeout: Optional[int] = None
    ) -> TaskResult:
        """Run code in Docker container."""
        start_time = time.time()
        container = None
        
        try:
            # Prepare code and input
            wrapped_code = self._wrap_code(code, input_data)
            
            # Calculate resource limits
            timeout_seconds = timeout or self.config.resource_limits.timeout_seconds
            
            # Create and run container
            container = await self._create_container(wrapped_code)
            
            # Wait for completion with timeout
            try:
                result = await asyncio.wait_for(
                    self._wait_for_container(container),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                logger.warning(f"Container execution timed out after {timeout_seconds}s")
                container.kill()
                raise TaskTimeout(f"Execution timed out after {timeout_seconds} seconds")
            
            # Process result
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
            logger.error(f"Docker execution failed: {str(e)}")
            raise TaskExecutionError(f"Docker execution failed: {str(e)}")
        finally:
            # Cleanup
            if container and self.config.remove_container:
                try:
                    container.remove(force=True)
                except:
                    pass
    
    def _wrap_code(self, code: str, input_data: Dict[str, Any]) -> str:
        """Wrap user code with input handling and output capture."""
        wrapper = f'''
import json
import sys
import traceback

# Set input data
INPUT_DATA = {json.dumps(json.dumps(input_data))}

# Capture stdout
original_stdout = sys.stdout
output_parts = []

class OutputCapture:
    def write(self, text):
        output_parts.append(text)
    
    def flush(self):
        pass

sys.stdout = OutputCapture()

# Execute user code
try:
{self._indent_code(code)}
    
    # Try to parse output as JSON
    output_text = ''.join(output_parts).strip()
    if output_text:
        try:
            output = json.loads(output_text)
        except:
            output = output_text
    else:
        output = None
    
    # Return success result
    result = {{
        "success": True,
        "output": output,
        "exit_code": 0
    }}
    
except Exception as e:
    # Return error result
    result = {{
        "success": False,
        "output": None,
        "error": str(e) + "\\n" + traceback.format_exc(),
        "exit_code": 1
    }}

# Restore stdout and print result
sys.stdout = original_stdout
print(json.dumps(result))
'''
        return wrapper
    
    def _indent_code(self, code: str) -> str:
        """Indent code for insertion into wrapper."""
        lines = code.strip().split('\n')
        return '\n'.join(f'    {line}' for line in lines)
    
    async def _create_container(self, code: str) -> docker.models.containers.Container:
        """Create and start Docker container."""
        # Write code to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            code_file = f.name
        
        try:
            # Prepare container configuration
            container_config = {
                'image': self.config.image,
                'command': ['python', '/code/script.py'],
                'detach': True,
                'network_mode': self.config.network_mode,
                'user': self.config.user,
                'working_dir': self.config.working_dir,
                'volumes': {
                    code_file: {'bind': '/code/script.py', 'mode': 'ro'}
                },
                'environment': self.config.environment or {},
                'remove': False,  # We'll remove manually
            }
            
            # Add resource limits
            if self.config.resource_limits:
                container_config.update({
                    'mem_limit': f"{self.config.resource_limits.memory_mb}m",
                    'memswap_limit': f"{self.config.resource_limits.memory_mb}m",
                    'cpu_period': 100000,
                    'cpu_quota': int(100000 * self.config.resource_limits.cpu_cores),
                })
            
            # Create and start container
            container = self.client.containers.run(**container_config)
            
            return container
            
        finally:
            # Clean up temp file
            try:
                Path(code_file).unlink()
            except:
                pass
    
    async def _wait_for_container(self, container) -> Dict[str, Any]:
        """Wait for container to complete and get results."""
        # Wait for container to finish
        result = container.wait()
        
        # Check if killed by OOM
        container.reload()
        if container.attrs['State'].get('OOMKilled', False):
            raise TaskMemoryExceeded("Container killed due to memory limit")
        
        # Get logs
        logs = container.logs(stdout=True, stderr=True).decode('utf-8')
        
        # Check output size
        if len(logs) > self.config.resource_limits.max_output_size:
            raise TaskExecutionError(
                f"Output size exceeded limit of {self.config.resource_limits.max_output_size} bytes"
            )
        
        # Parse result
        try:
            # Find the last JSON line (our result)
            lines = logs.strip().split('\n')
            for line in reversed(lines):
                if line.strip():
                    try:
                        result_data = json.loads(line)
                        if isinstance(result_data, dict) and 'success' in result_data:
                            result_data['logs'] = lines[:-1]  # All lines except result
                            return result_data
                    except:
                        continue
            
            # No valid result found
            return {
                'success': False,
                'output': None,
                'error': logs or 'No output produced',
                'exit_code': result['StatusCode'],
                'logs': lines
            }
            
        except Exception as e:
            return {
                'success': False,
                'output': None,
                'error': f"Failed to parse output: {str(e)}",
                'exit_code': result['StatusCode'],
                'logs': logs.split('\n')
            }
    
    async def cleanup(self) -> None:
        """Clean up Docker resources."""
        if self.client:
            try:
                # Clean up any remaining containers
                containers = self.client.containers.list(
                    filters={'label': 'budflow-task-runner=true'}
                )
                for container in containers:
                    try:
                        container.remove(force=True)
                    except:
                        pass
                
                self.client.close()
            except:
                pass