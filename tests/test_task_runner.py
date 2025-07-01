"""Tests for task runner implementation."""

import pytest
import asyncio
import json
import os
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timezone

from budflow.executor.task_runner import (
    TaskRunner,
    DockerTaskRunner,
    ProcessTaskRunner,
    TaskRunnerConfig,
    TaskRunnerMode,
    TaskResult,
    TaskError,
    TaskTimeout,
    TaskMemoryExceeded,
    TaskExecutionError,
    ResourceLimits,
    DockerConfig,
    ProcessConfig,
)


@pytest.fixture
def resource_limits():
    """Create resource limits."""
    return ResourceLimits(
        memory_mb=512,
        cpu_cores=1.0,
        disk_mb=1024,
        timeout_seconds=30,
        max_output_size=1024 * 1024  # 1MB
    )


@pytest.fixture
def docker_config(resource_limits):
    """Create Docker configuration."""
    return DockerConfig(
        image="python:3.11-slim",
        network_mode="none",
        remove_container=True,
        environment={},
        volumes={},
        working_dir="/app",
        user="nobody",
        resource_limits=resource_limits
    )


@pytest.fixture
def process_config(resource_limits):
    """Create process configuration."""
    return ProcessConfig(
        python_path="/usr/bin/python3",
        working_dir="/tmp",
        environment={},
        resource_limits=resource_limits,
        use_sandbox=True
    )


@pytest.fixture
def task_runner_config():
    """Create task runner configuration."""
    return TaskRunnerConfig(
        mode=TaskRunnerMode.DOCKER,
        docker_socket="/var/run/docker.sock",
        enable_network=False,
        allowed_modules=["json", "datetime", "math"],
        blocked_modules=["os", "subprocess", "socket"],
        log_output=True
    )


class TestDockerTaskRunner:
    """Test Docker-based task runner."""
    
    @pytest.mark.asyncio
    async def test_run_simple_code(self, docker_config):
        """Test running simple Python code."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container
            mock_container = Mock()
            mock_container.logs.return_value = b'{"result": 42}'
            mock_container.wait.return_value = {"StatusCode": 0}
            mock_container.attrs = {"State": {"ExitCode": 0}}
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code
            code = """
result = 21 * 2
print(json.dumps({"result": result}))
"""
            result = await runner.run(code, {})
            
            assert result.success is True
            assert result.output == {"result": 42}
            assert result.error is None
            assert result.exit_code == 0
    
    @pytest.mark.asyncio
    async def test_run_with_input_data(self, docker_config):
        """Test running code with input data."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container
            mock_container = Mock()
            mock_container.logs.return_value = b'{"sum": 15}'
            mock_container.wait.return_value = {"StatusCode": 0}
            mock_container.attrs = {"State": {"ExitCode": 0}}
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code with input
            code = """
import json
input_data = json.loads(INPUT_DATA)
result = sum(input_data["numbers"])
print(json.dumps({"sum": result}))
"""
            input_data = {"numbers": [1, 2, 3, 4, 5]}
            result = await runner.run(code, input_data)
            
            assert result.success is True
            assert result.output == {"sum": 15}
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self, docker_config):
        """Test timeout handling."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container that times out
            mock_container = Mock()
            mock_container.logs.return_value = b''
            mock_container.wait.side_effect = asyncio.TimeoutError()
            mock_container.kill = Mock()
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner with short timeout
            docker_config.resource_limits.timeout_seconds = 1
            runner = DockerTaskRunner(docker_config)
            
            # Run code that times out
            code = """
import time
time.sleep(10)
"""
            with pytest.raises(TaskTimeout):
                await runner.run(code, {})
            
            # Verify container was killed
            mock_container.kill.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_memory_limit(self, docker_config):
        """Test memory limit enforcement."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container that exceeds memory
            mock_container = Mock()
            mock_container.logs.return_value = b'Killed'
            mock_container.wait.return_value = {"StatusCode": 137}  # OOM kill
            mock_container.attrs = {"State": {"OOMKilled": True}}
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code that uses too much memory
            code = """
# Allocate lots of memory
data = [0] * (1024 * 1024 * 1024)  # 1GB
"""
            with pytest.raises(TaskMemoryExceeded):
                await runner.run(code, {})
    
    @pytest.mark.asyncio
    async def test_syntax_error_handling(self, docker_config):
        """Test syntax error handling."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container with syntax error
            error_output = b'SyntaxError: invalid syntax'
            mock_container = Mock()
            mock_container.logs.return_value = error_output
            mock_container.wait.return_value = {"StatusCode": 1}
            mock_container.attrs = {"State": {"ExitCode": 1}}
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code with syntax error
            code = """
def broken(
    print("missing closing paren")
"""
            result = await runner.run(code, {})
            
            assert result.success is False
            assert result.exit_code == 1
            assert "SyntaxError" in str(result.error)
    
    @pytest.mark.asyncio
    async def test_output_size_limit(self, docker_config):
        """Test output size limit."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container with large output
            large_output = b'x' * (2 * 1024 * 1024)  # 2MB
            mock_container = Mock()
            mock_container.logs.return_value = large_output
            mock_container.wait.return_value = {"StatusCode": 0}
            mock_container.attrs = {"State": {"ExitCode": 0}}
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code
            code = 'print("x" * (2 * 1024 * 1024))'
            
            with pytest.raises(TaskExecutionError) as exc_info:
                await runner.run(code, {})
            
            assert "Output size exceeded" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_cleanup_on_error(self, docker_config):
        """Test container cleanup on error."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock container that fails
            mock_container = Mock()
            mock_container.logs.side_effect = Exception("Docker error")
            mock_container.remove = Mock()
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code that fails
            try:
                await runner.run("print('test')", {})
            except:
                pass
            
            # Verify cleanup
            mock_container.remove.assert_called_once()


class TestProcessTaskRunner:
    """Test process-based task runner."""
    
    @pytest.mark.asyncio
    async def test_run_simple_code(self, process_config):
        """Test running simple Python code."""
        # Create runner
        runner = ProcessTaskRunner(process_config)
        
        # Run code
        code = """
import json
result = 21 * 2
print(json.dumps({"result": result}))
"""
        result = await runner.run(code, {})
        
        assert result.success is True
        assert result.output == {"result": 42}
        assert result.error is None
        assert result.exit_code == 0
    
    @pytest.mark.asyncio
    async def test_run_with_input_data(self, process_config):
        """Test running code with input data."""
        # Create runner
        runner = ProcessTaskRunner(process_config)
        
        # Run code with input
        code = """
import json
import os
input_data = json.loads(os.environ.get('INPUT_DATA', '{}'))
result = sum(input_data["numbers"])
print(json.dumps({"sum": result}))
"""
        input_data = {"numbers": [1, 2, 3, 4, 5]}
        result = await runner.run(code, input_data)
        
        assert result.success is True
        assert result.output == {"sum": 15}
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self, process_config):
        """Test timeout handling."""
        # Create runner with short timeout
        process_config.resource_limits.timeout_seconds = 0.1
        runner = ProcessTaskRunner(process_config)
        
        # Run code that times out
        code = """
import time
time.sleep(10)
"""
        with pytest.raises(TaskTimeout):
            await runner.run(code, {})
    
    @pytest.mark.asyncio
    async def test_blocked_module_detection(self, process_config):
        """Test blocked module detection."""
        # Create runner
        runner = ProcessTaskRunner(process_config)
        
        # Try to import blocked module
        code = """
import os
print(os.environ)
"""
        result = await runner.run(code, {})
        
        assert result.success is False
        assert "blocked module" in str(result.error).lower()
    
    @pytest.mark.asyncio
    async def test_sandbox_restrictions(self, process_config):
        """Test sandbox restrictions."""
        # Create runner with sandbox
        runner = ProcessTaskRunner(process_config)
        
        # Try to access filesystem
        code = """
with open('/etc/passwd', 'r') as f:
    print(f.read())
"""
        result = await runner.run(code, {})
        
        assert result.success is False
        assert result.exit_code != 0


class TestTaskRunnerFactory:
    """Test task runner factory."""
    
    def test_create_docker_runner(self, task_runner_config):
        """Test creating Docker runner."""
        task_runner_config.mode = TaskRunnerMode.DOCKER
        
        runner = TaskRunner.create(task_runner_config)
        
        assert isinstance(runner, DockerTaskRunner)
    
    def test_create_process_runner(self, task_runner_config):
        """Test creating process runner."""
        task_runner_config.mode = TaskRunnerMode.PROCESS
        
        runner = TaskRunner.create(task_runner_config)
        
        assert isinstance(runner, ProcessTaskRunner)
    
    def test_invalid_mode(self, task_runner_config):
        """Test invalid runner mode."""
        task_runner_config.mode = "invalid"
        
        with pytest.raises(ValueError):
            TaskRunner.create(task_runner_config)


class TestResourceEnforcement:
    """Test resource limit enforcement."""
    
    @pytest.mark.asyncio
    async def test_cpu_limit_docker(self, docker_config):
        """Test CPU limit in Docker."""
        with patch('docker.from_env') as mock_docker:
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            mock_container = Mock()
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Check Docker run arguments
            await runner.run("print('test')", {})
            
            # Verify CPU limit was set
            call_args = mock_client.containers.run.call_args
            assert 'cpu_period' in call_args[1]
            assert 'cpu_quota' in call_args[1]
    
    @pytest.mark.asyncio
    async def test_memory_limit_process(self, process_config):
        """Test memory limit in process runner."""
        if not hasattr(os, 'setrlimit'):
            pytest.skip("Resource limits not supported on this platform")
        
        # Create runner
        runner = ProcessTaskRunner(process_config)
        
        # Run code that tries to allocate too much memory
        code = """
try:
    data = [0] * (1024 * 1024 * 1024)  # 1GB
    print("Should not reach here")
except MemoryError:
    print('{"error": "memory_exceeded"}')
"""
        result = await runner.run(code, {})
        
        # Should handle memory error gracefully
        assert result.output.get("error") == "memory_exceeded"


class TestSecurityFeatures:
    """Test security features."""
    
    @pytest.mark.asyncio
    async def test_network_isolation_docker(self, docker_config):
        """Test network isolation in Docker."""
        docker_config.network_mode = "none"
        
        with patch('docker.from_env') as mock_docker:
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            mock_container = Mock()
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code
            await runner.run("print('test')", {})
            
            # Verify network mode
            call_args = mock_client.containers.run.call_args
            assert call_args[1]['network_mode'] == 'none'
    
    @pytest.mark.asyncio
    async def test_user_isolation_docker(self, docker_config):
        """Test user isolation in Docker."""
        docker_config.user = "nobody"
        
        with patch('docker.from_env') as mock_docker:
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            mock_container = Mock()
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Run code
            await runner.run("print('test')", {})
            
            # Verify user
            call_args = mock_client.containers.run.call_args
            assert call_args[1]['user'] == 'nobody'
    
    @pytest.mark.asyncio
    async def test_code_injection_prevention(self, process_config):
        """Test code injection prevention."""
        # Create runner
        runner = ProcessTaskRunner(process_config)
        
        # Try code injection
        malicious_input = {
            "data": '"; import os; os.system("rm -rf /"); "'
        }
        
        code = """
import json
import os
data = json.loads(os.environ.get('INPUT_DATA', '{}'))
print(json.dumps({"received": data}))
"""
        result = await runner.run(code, malicious_input)
        
        # Should safely handle malicious input
        assert result.success is True
        assert result.output["received"]["data"] == malicious_input["data"]


class TestIntegration:
    """Integration tests for task runner."""
    
    @pytest.mark.asyncio
    async def test_workflow_code_execution(self, docker_config):
        """Test executing code from workflow context."""
        with patch('docker.from_env') as mock_docker:
            # Mock Docker client
            mock_client = Mock()
            mock_docker.return_value = mock_client
            
            # Mock successful execution
            mock_container = Mock()
            mock_container.logs.return_value = b'{"processed": true, "count": 5}'
            mock_container.wait.return_value = {"StatusCode": 0}
            mock_container.attrs = {"State": {"ExitCode": 0}}
            
            mock_client.containers.run.return_value = mock_container
            
            # Create runner
            runner = DockerTaskRunner(docker_config)
            
            # Workflow code that processes data
            code = """
import json

# Get input data
input_data = json.loads(INPUT_DATA)

# Process items
processed_items = []
for item in input_data.get("items", []):
    processed_items.append({
        "id": item["id"],
        "processed": True
    })

# Return result
result = {
    "processed": True,
    "count": len(processed_items)
}
print(json.dumps(result))
"""
            
            # Input from previous node
            input_data = {
                "items": [
                    {"id": 1, "name": "Item 1"},
                    {"id": 2, "name": "Item 2"},
                    {"id": 3, "name": "Item 3"},
                    {"id": 4, "name": "Item 4"},
                    {"id": 5, "name": "Item 5"},
                ]
            }
            
            result = await runner.run(code, input_data)
            
            assert result.success is True
            assert result.output["processed"] is True
            assert result.output["count"] == 5
    
    @pytest.mark.asyncio
    async def test_error_propagation(self, process_config):
        """Test error propagation to workflow."""
        # Create runner
        runner = ProcessTaskRunner(process_config)
        
        # Code with business logic error
        code = """
import json

def process_order(order_data):
    if order_data["amount"] < 0:
        raise ValueError("Order amount cannot be negative")
    return {"status": "processed"}

try:
    input_data = {"amount": -100}
    result = process_order(input_data)
    print(json.dumps(result))
except Exception as e:
    error_result = {
        "error": str(e),
        "type": type(e).__name__
    }
    print(json.dumps(error_result))
"""
        
        result = await runner.run(code, {})
        
        assert result.success is True  # Code ran successfully
        assert "error" in result.output
        assert result.output["error"] == "Order amount cannot be negative"
        assert result.output["type"] == "ValueError"