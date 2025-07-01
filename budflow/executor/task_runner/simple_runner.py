"""Simple task runner implementation for testing."""

import json
import asyncio
import tempfile
import time
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger()


@dataclass
class TaskResult:
    """Result of task execution."""
    success: bool
    output: Any
    error: Optional[str] = None
    exit_code: int = 0
    execution_time_ms: int = 0
    logs: list = field(default_factory=list)


class SimpleTaskRunner:
    """Simple subprocess-based task runner."""
    
    def __init__(self, timeout: int = 30):
        """Initialize simple task runner."""
        self.timeout = timeout
    
    async def run(
        self,
        code: str,
        input_data: Dict[str, Any] = None
    ) -> TaskResult:
        """Run code in subprocess."""
        start_time = time.time()
        input_data = input_data or {}
        
        # Wrap code
        wrapped_code = f'''
import json
import sys

# Input data
INPUT_DATA = {json.dumps(json.dumps(input_data))}

# Capture output
output_parts = []
_print = print

def print(*args, **kwargs):
    output = ' '.join(str(arg) for arg in args)
    output_parts.append(output)

# Execute code
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
    import traceback
    result = {{
        "success": False,
        "output": None,
        "error": str(e) + "\\n" + traceback.format_exc(),
        "exit_code": 1
    }}

# Print result
_print(json.dumps(result))
'''
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(wrapped_code)
            code_file = f.name
        
        try:
            # Execute
            process = await asyncio.create_subprocess_exec(
                'python3',
                code_file,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Wait with timeout
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=self.timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                return TaskResult(
                    success=False,
                    output=None,
                    error=f"Timeout after {self.timeout} seconds",
                    exit_code=-1
                )
            
            # Parse output
            output_text = stdout.decode('utf-8').strip()
            if output_text:
                try:
                    result = json.loads(output_text.split('\n')[-1])
                    return TaskResult(
                        success=result.get('success', False),
                        output=result.get('output'),
                        error=result.get('error'),
                        exit_code=result.get('exit_code', 0),
                        execution_time_ms=int((time.time() - start_time) * 1000)
                    )
                except:
                    pass
            
            # Fallback
            return TaskResult(
                success=process.returncode == 0,
                output=output_text if output_text else None,
                error=stderr.decode('utf-8') if stderr else None,
                exit_code=process.returncode,
                execution_time_ms=int((time.time() - start_time) * 1000)
            )
            
        finally:
            try:
                Path(code_file).unlink()
            except:
                pass
    
    def _indent_code(self, code: str) -> str:
        """Indent code."""
        lines = code.strip().split('\n')
        return '\n'.join(f'    {line}' for line in lines)