"""Task runner for secure code execution."""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import structlog

logger = structlog.get_logger()


class TaskRunnerMode(Enum):
    """Task runner execution modes."""
    DOCKER = "docker"
    PROCESS = "process"
    VM = "vm"  # Future: Virtual machine


@dataclass
class ResourceLimits:
    """Resource limits for task execution."""
    memory_mb: int = 512
    cpu_cores: float = 1.0
    disk_mb: int = 1024
    timeout_seconds: int = 300
    max_output_size: int = 10 * 1024 * 1024  # 10MB


@dataclass
class TaskResult:
    """Result of task execution."""
    success: bool
    output: Any
    error: Optional[str] = None
    exit_code: int = 0
    execution_time_ms: int = 0
    memory_used_mb: int = 0
    logs: List[str] = field(default_factory=list)


@dataclass
class DockerConfig:
    """Docker-specific configuration."""
    image: str = "python:3.11-slim"
    network_mode: str = "none"
    remove_container: bool = True
    environment: Dict[str, str] = field(default_factory=dict)
    volumes: Dict[str, str] = field(default_factory=dict)
    working_dir: str = "/app"
    user: str = "nobody"
    resource_limits: ResourceLimits = field(default_factory=ResourceLimits)


@dataclass
class ProcessConfig:
    """Process-specific configuration."""
    python_path: str = "python3"
    working_dir: str = "/tmp"
    environment: Dict[str, str] = field(default_factory=dict)
    resource_limits: ResourceLimits = field(default_factory=ResourceLimits)
    use_sandbox: bool = True


@dataclass
class TaskRunnerConfig:
    """Task runner configuration."""
    mode: TaskRunnerMode = TaskRunnerMode.DOCKER
    docker_socket: str = "/var/run/docker.sock"
    enable_network: bool = False
    allowed_modules: List[str] = field(default_factory=list)
    blocked_modules: List[str] = field(default_factory=list)
    log_output: bool = True


class TaskError(Exception):
    """Base exception for task execution errors."""
    pass


class TaskTimeout(TaskError):
    """Task execution timed out."""
    pass


class TaskMemoryExceeded(TaskError):
    """Task exceeded memory limit."""
    pass


class TaskExecutionError(TaskError):
    """Task execution failed."""
    pass


class TaskRunner(ABC):
    """Abstract base class for task runners."""
    
    @abstractmethod
    async def run(
        self,
        code: str,
        input_data: Dict[str, Any],
        timeout: Optional[int] = None
    ) -> TaskResult:
        """Run code with given input data."""
        pass
    
    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up resources."""
        pass
    
    @staticmethod
    def create(config: TaskRunnerConfig) -> 'TaskRunner':
        """Create task runner based on configuration."""
        if config.mode == TaskRunnerMode.DOCKER:
            from .docker_runner import DockerTaskRunner
            return DockerTaskRunner(
                DockerConfig(
                    resource_limits=ResourceLimits()
                )
            )
        elif config.mode == TaskRunnerMode.PROCESS:
            from .process_runner import ProcessTaskRunner
            return ProcessTaskRunner(
                ProcessConfig(
                    resource_limits=ResourceLimits()
                )
            )
        else:
            raise ValueError(f"Unknown task runner mode: {config.mode}")


__all__ = [
    'TaskRunner',
    'DockerTaskRunner', 
    'ProcessTaskRunner',
    'TaskRunnerConfig',
    'TaskRunnerMode',
    'TaskResult',
    'TaskError',
    'TaskTimeout',
    'TaskMemoryExceeded',
    'TaskExecutionError',
    'ResourceLimits',
    'DockerConfig',
    'ProcessConfig',
]