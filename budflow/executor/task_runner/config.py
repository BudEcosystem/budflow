"""Task runner configuration."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any


class TaskRunnerMode(str, Enum):
    """Task runner execution mode."""
    
    INTERNAL = "internal"  # Child process
    EXTERNAL = "external"  # Separate process/container
    DOCKER = "docker"      # Docker container


@dataclass
class ResourceLimits:
    """Resource limits for task execution."""
    
    max_memory_mb: int = 512  # Maximum memory in MB
    max_cpu_percent: float = 50.0  # Maximum CPU percentage
    max_execution_time_seconds: int = 300  # Maximum execution time
    max_payload_size_mb: int = 1024  # Maximum payload size (1GB default)
    max_file_descriptors: int = 256  # Maximum open file descriptors
    
    def validate(self) -> None:
        """Validate resource limits."""
        if self.max_memory_mb <= 0:
            raise ValueError("max_memory_mb must be positive")
        if not 0 < self.max_cpu_percent <= 100:
            raise ValueError("max_cpu_percent must be between 0 and 100")
        if self.max_execution_time_seconds <= 0:
            raise ValueError("max_execution_time_seconds must be positive")
        if self.max_payload_size_mb <= 0:
            raise ValueError("max_payload_size_mb must be positive")


@dataclass
class SecurityConfig:
    """Security configuration for task execution."""
    
    enable_sandboxing: bool = True
    allowed_builtin_modules: List[str] = field(default_factory=lambda: [
        "json", "datetime", "math", "re", "collections", "itertools",
        "functools", "operator", "string", "decimal", "fractions",
        "random", "statistics", "base64", "hashlib", "hmac",
        "urllib.parse", "html", "xml.etree.ElementTree",
    ])
    allowed_external_modules: List[str] = field(default_factory=list)
    enable_prototype_pollution_prevention: bool = True
    enable_code_analysis: bool = True
    disallow_code_generation: bool = True
    disallow_exec_eval: bool = True
    disallow_import: bool = False  # Allow controlled imports
    disallow_attribute_access: bool = False
    disallow_item_access: bool = False
    max_ast_depth: int = 100  # Maximum AST depth to prevent deeply nested code
    
    def is_module_allowed(self, module_name: str) -> bool:
        """Check if a module is allowed."""
        if module_name in self.allowed_builtin_modules:
            return True
        if module_name in self.allowed_external_modules:
            return True
        # Check for submodules
        for allowed in self.allowed_builtin_modules + self.allowed_external_modules:
            if module_name.startswith(f"{allowed}."):
                return True
        return False


@dataclass
class TaskRunnerConfig:
    """Task runner configuration."""
    
    # Execution mode
    mode: TaskRunnerMode = TaskRunnerMode.INTERNAL
    
    # Concurrency settings
    max_concurrency: int = 10  # Maximum concurrent tasks per runner
    max_runners: int = 4  # Maximum number of runner processes
    
    # Timeouts and intervals
    task_timeout: int = 300  # Default task timeout in seconds
    heartbeat_interval: int = 30  # Heartbeat interval in seconds
    offer_validity_seconds: int = 60  # How long task offers are valid
    shutdown_timeout: int = 30  # Graceful shutdown timeout
    
    # Resource limits
    max_memory_mb: int = 512
    max_cpu_percent: float = 50.0
    max_payload_size_mb: int = 1024
    resource_check_interval: float = 1.0  # Resource check interval in seconds
    
    # Security settings
    enable_sandboxing: bool = True
    allowed_modules: List[str] = field(default_factory=lambda: [
        "json", "datetime", "math", "re", "collections",
    ])
    enable_prototype_pollution_prevention: bool = True
    
    # Communication settings
    ipc_method: str = "queue"  # "queue", "pipe", "zeromq", "redis"
    ipc_timeout: float = 5.0  # IPC operation timeout
    
    # Logging and monitoring
    enable_task_logging: bool = True
    enable_metrics: bool = True
    log_execution_details: bool = False  # Log code and results (security risk)
    
    # Advanced settings
    restart_on_crash: bool = True
    max_restart_attempts: int = 3
    restart_delay_seconds: int = 5
    chunk_size: int = 100  # For processing large datasets
    
    # Environment settings
    env_vars_passthrough: List[str] = field(default_factory=list)  # Environment variables to pass
    working_directory: Optional[str] = None  # Working directory for tasks
    
    def get_resource_limits(self) -> ResourceLimits:
        """Get resource limits configuration."""
        return ResourceLimits(
            max_memory_mb=self.max_memory_mb,
            max_cpu_percent=self.max_cpu_percent,
            max_execution_time_seconds=self.task_timeout,
            max_payload_size_mb=self.max_payload_size_mb,
        )
    
    def get_security_config(self) -> SecurityConfig:
        """Get security configuration."""
        return SecurityConfig(
            enable_sandboxing=self.enable_sandboxing,
            allowed_builtin_modules=self.allowed_modules,
            allowed_external_modules=[],
            enable_prototype_pollution_prevention=self.enable_prototype_pollution_prevention,
        )
    
    def validate(self) -> None:
        """Validate configuration."""
        if self.max_concurrency <= 0:
            raise ValueError("max_concurrency must be positive")
        if self.max_runners <= 0:
            raise ValueError("max_runners must be positive")
        if self.task_timeout <= 0:
            raise ValueError("task_timeout must be positive")
        if self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        
        # Validate resource limits
        self.get_resource_limits().validate()