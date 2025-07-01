"""Resource limiting for task execution."""

import asyncio
import logging
import os
import resource
import signal
import sys
import time
from typing import Optional, Dict, Any, Callable, Tuple

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("psutil not installed, resource monitoring limited")

from .config import ResourceLimits
from .exceptions import ResourceLimitError, TaskTimeoutError

logger = logging.getLogger(__name__)


class ResourceLimitEnforcer:
    """Enforces resource limits during task execution."""
    
    def __init__(self, limits: ResourceLimits, process_id: Optional[int] = None):
        self.limits = limits
        self.process_id = process_id or os.getpid()
        self.start_time: Optional[float] = None
        self._process: Optional[Any] = None
        self._original_limits: Dict[int, Tuple[int, int]] = {}
        
        if PSUTIL_AVAILABLE:
            try:
                self._process = psutil.Process(self.process_id)
            except psutil.NoSuchProcess:
                self._process = None
    
    def start_execution(self) -> None:
        """Start tracking execution time."""
        self.start_time = time.time()
    
    def apply_system_limits(self) -> None:
        """Apply system resource limits using resource module."""
        if sys.platform == "win32":
            # Windows doesn't support resource module limits
            logger.warning("System resource limits not supported on Windows")
            return
        
        try:
            # Set memory limit (RSS)
            if hasattr(resource, "RLIMIT_RSS"):
                limit_bytes = self.limits.max_memory_mb * 1024 * 1024
                old_limit = resource.getrlimit(resource.RLIMIT_RSS)
                self._original_limits[resource.RLIMIT_RSS] = old_limit
                resource.setrlimit(resource.RLIMIT_RSS, (limit_bytes, limit_bytes))
            
            # Set CPU time limit
            if hasattr(resource, "RLIMIT_CPU"):
                old_limit = resource.getrlimit(resource.RLIMIT_CPU)
                self._original_limits[resource.RLIMIT_CPU] = old_limit
                resource.setrlimit(
                    resource.RLIMIT_CPU,
                    (self.limits.max_execution_time_seconds, self.limits.max_execution_time_seconds)
                )
            
            # Set file descriptor limit
            if hasattr(resource, "RLIMIT_NOFILE"):
                old_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
                self._original_limits[resource.RLIMIT_NOFILE] = old_limit
                resource.setrlimit(
                    resource.RLIMIT_NOFILE,
                    (self.limits.max_file_descriptors, self.limits.max_file_descriptors)
                )
            
        except Exception as e:
            logger.error(f"Failed to set system resource limits: {e}")
    
    def restore_system_limits(self) -> None:
        """Restore original system resource limits."""
        if sys.platform == "win32":
            return
        
        for limit_type, (soft, hard) in self._original_limits.items():
            try:
                resource.setrlimit(limit_type, (soft, hard))
            except Exception as e:
                logger.error(f"Failed to restore resource limit {limit_type}: {e}")
        
        self._original_limits.clear()
    
    async def check_memory_usage(self) -> None:
        """Check current memory usage against limit."""
        if not PSUTIL_AVAILABLE or not self._process:
            return
        
        try:
            memory_info = self._process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            if memory_mb > self.limits.max_memory_mb:
                raise ResourceLimitError(
                    f"Memory usage {memory_mb:.1f}MB exceeds limit {self.limits.max_memory_mb}MB",
                    resource_type="memory",
                    limit=self.limits.max_memory_mb,
                    usage=memory_mb
                )
        except ResourceLimitError:
            # Re-raise resource limit errors
            raise
        except Exception as e:
            if PSUTIL_AVAILABLE and hasattr(psutil, 'NoSuchProcess') and isinstance(e, psutil.NoSuchProcess):
                # Process no longer exists
                pass
            elif hasattr(e, '__class__') and e.__class__.__name__ == 'NoSuchProcess':
                # Process no longer exists
                pass
            else:
                logger.error(f"Error checking memory usage: {e}")
    
    async def check_cpu_usage(self) -> None:
        """Check current CPU usage against limit."""
        if not PSUTIL_AVAILABLE or not self._process:
            return
        
        try:
            # Get CPU percentage over a short interval
            cpu_percent = self._process.cpu_percent(interval=0.1)
            
            if cpu_percent > self.limits.max_cpu_percent:
                # Give it a grace period - CPU can spike temporarily
                await asyncio.sleep(1.0)
                cpu_percent = self._process.cpu_percent(interval=0.1)
                
                if cpu_percent > self.limits.max_cpu_percent:
                    raise ResourceLimitError(
                        f"CPU usage {cpu_percent:.1f}% exceeds limit {self.limits.max_cpu_percent}%",
                        resource_type="cpu",
                        limit=self.limits.max_cpu_percent,
                        usage=cpu_percent
                    )
        except ResourceLimitError:
            # Re-raise resource limit errors
            raise
        except Exception as e:
            if PSUTIL_AVAILABLE and hasattr(psutil, 'NoSuchProcess') and isinstance(e, psutil.NoSuchProcess):
                # Process no longer exists
                pass
            elif hasattr(e, '__class__') and e.__class__.__name__ == 'NoSuchProcess':
                # Process no longer exists
                pass
            else:
                logger.error(f"Error checking memory usage: {e}")
        except Exception as e:
            logger.error(f"Error checking CPU usage: {e}")
    
    def check_execution_time(self) -> None:
        """Check if execution time has exceeded limit."""
        if self.start_time is None:
            return
        
        elapsed = time.time() - self.start_time
        if elapsed > self.limits.max_execution_time_seconds:
            raise TaskTimeoutError(
                f"Execution time {elapsed:.1f}s exceeds limit {self.limits.max_execution_time_seconds}s",
                timeout_seconds=self.limits.max_execution_time_seconds
            )
    
    async def monitor_resources(
        self,
        check_interval: float = 1.0,
        stop_event: Optional[asyncio.Event] = None
    ) -> None:
        """Monitor resources continuously."""
        while True:
            try:
                # Check various resources
                await self.check_memory_usage()
                await self.check_cpu_usage()
                self.check_execution_time()
                
                # Check if we should stop
                if stop_event and stop_event.is_set():
                    break
                
                await asyncio.sleep(check_interval)
                
            except (ResourceLimitError, TaskTimeoutError):
                # Re-raise resource limit errors
                raise
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                await asyncio.sleep(check_interval)
    
    def get_current_usage(self) -> Dict[str, Any]:
        """Get current resource usage."""
        usage = {
            "memory_mb": None,
            "cpu_percent": None,
            "execution_time_seconds": None,
        }
        
        if PSUTIL_AVAILABLE and self._process:
            try:
                memory_info = self._process.memory_info()
                usage["memory_mb"] = memory_info.rss / (1024 * 1024)
                usage["cpu_percent"] = self._process.cpu_percent(interval=0.1)
            except Exception:
                pass
        
        if self.start_time:
            usage["execution_time_seconds"] = time.time() - self.start_time
        
        return usage
    
    def create_timeout_handler(self, callback: Callable) -> Optional[Callable]:
        """Create a signal-based timeout handler (Unix only)."""
        if sys.platform == "win32":
            return None
        
        def timeout_handler(signum, frame):
            callback()
        
        return timeout_handler
    
    def set_timeout_signal(self, timeout_seconds: int, handler: Callable) -> None:
        """Set timeout using signal (Unix only)."""
        if sys.platform == "win32":
            return
        
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(timeout_seconds)
    
    def clear_timeout_signal(self) -> None:
        """Clear timeout signal (Unix only)."""
        if sys.platform == "win32":
            return
        
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)


class PayloadSizeChecker:
    """Checks payload size limits."""
    
    @staticmethod
    def check_size(data: Any, max_size_mb: int) -> None:
        """Check if data size exceeds limit."""
        # Simple size estimation using sys.getsizeof
        # In production, might need more sophisticated size calculation
        size_bytes = sys.getsizeof(data)
        
        # For complex objects, try to get a better estimate
        if isinstance(data, (dict, list, tuple, set)):
            try:
                import json
                json_str = json.dumps(data, default=str)
                size_bytes = len(json_str.encode('utf-8'))
            except Exception:
                pass
        
        size_mb = size_bytes / (1024 * 1024)
        
        if size_mb > max_size_mb:
            raise ResourceLimitError(
                f"Payload size {size_mb:.1f}MB exceeds limit {max_size_mb}MB",
                resource_type="payload_size",
                limit=max_size_mb,
                usage=size_mb
            )