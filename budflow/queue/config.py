"""Queue configuration."""

from dataclasses import dataclass
from typing import Optional
from datetime import timedelta


@dataclass
class QueueConfig:
    """Queue system configuration."""
    
    # Redis configuration
    redis_url: str = "redis://localhost:6379/0"
    redis_username: Optional[str] = None
    redis_password: Optional[str] = None
    redis_tls: bool = False
    redis_cluster: bool = False
    redis_db: int = 0
    redis_key_prefix: str = "budflow"
    
    # Queue configuration
    queue_name: str = "jobs"
    job_type: str = "job"
    
    # Worker configuration
    worker_concurrency: int = 10
    worker_id: Optional[str] = None
    worker_server_host: str = "0.0.0.0"
    worker_server_port: int = 5678
    
    # Lock configuration
    lock_duration: int = 30  # seconds
    lock_renew_time: int = 15  # seconds
    
    # Stalled job configuration
    stalled_interval: int = 30  # seconds
    max_stalled_count: int = 1
    
    # Recovery configuration
    recovery_enabled: bool = True
    recovery_interval: int = 60  # seconds
    recovery_batch_size: int = 100
    
    # Metrics configuration
    enable_metrics: bool = True
    metrics_interval: int = 30  # seconds
    prometheus_enabled: bool = False
    prometheus_port: int = 9090
    
    # Job options defaults
    default_priority: int = 1000
    remove_on_complete: bool = True
    remove_on_fail: bool = True
    
    # Timeout configuration
    job_timeout: Optional[int] = None  # seconds
    shutdown_timeout: int = 30  # seconds
    
    def get_redis_url(self) -> str:
        """Get Redis connection URL with authentication."""
        if self.redis_username and self.redis_password:
            # Parse URL and add auth
            parts = self.redis_url.split("://", 1)
            if len(parts) == 2:
                scheme = parts[0]
                rest = parts[1]
                return f"{scheme}://{self.redis_username}:{self.redis_password}@{rest}"
        return self.redis_url
    
    def get_celery_config(self) -> dict:
        """Get Celery configuration."""
        return {
            "broker_url": self.get_redis_url(),
            "result_backend": self.get_redis_url(),
            "task_default_queue": self.queue_name,
            "task_default_priority": self.default_priority,
            "task_acks_late": True,
            "task_reject_on_worker_lost": True,
            "worker_prefetch_multiplier": 1,
            "worker_max_tasks_per_child": 1000,
            "broker_connection_retry_on_startup": True,
            "broker_connection_retry": True,
            "broker_connection_max_retries": 10,
            "redis_backend_health_check_interval": 30,
        }
    
    @classmethod
    def from_env(cls) -> "QueueConfig":
        """Create config from environment variables."""
        import os
        
        return cls(
            redis_url=os.getenv("QUEUE_REDIS_URL", "redis://localhost:6379/0"),
            redis_username=os.getenv("QUEUE_REDIS_USERNAME"),
            redis_password=os.getenv("QUEUE_REDIS_PASSWORD"),
            redis_tls=os.getenv("QUEUE_REDIS_TLS", "false").lower() == "true",
            redis_cluster=os.getenv("QUEUE_REDIS_CLUSTER", "false").lower() == "true",
            redis_db=int(os.getenv("QUEUE_REDIS_DB", "0")),
            redis_key_prefix=os.getenv("QUEUE_REDIS_PREFIX", "budflow"),
            queue_name=os.getenv("QUEUE_NAME", "jobs"),
            worker_concurrency=int(os.getenv("QUEUE_WORKER_CONCURRENCY", "10")),
            worker_server_host=os.getenv("QUEUE_WORKER_HOST", "0.0.0.0"),
            worker_server_port=int(os.getenv("QUEUE_WORKER_PORT", "5678")),
            lock_duration=int(os.getenv("QUEUE_LOCK_DURATION", "30")),
            lock_renew_time=int(os.getenv("QUEUE_LOCK_RENEW_TIME", "15")),
            stalled_interval=int(os.getenv("QUEUE_STALLED_INTERVAL", "30")),
            max_stalled_count=int(os.getenv("QUEUE_MAX_STALLED_COUNT", "1")),
            recovery_enabled=os.getenv("QUEUE_RECOVERY_ENABLED", "true").lower() == "true",
            recovery_interval=int(os.getenv("QUEUE_RECOVERY_INTERVAL", "60")),
            recovery_batch_size=int(os.getenv("QUEUE_RECOVERY_BATCH_SIZE", "100")),
            enable_metrics=os.getenv("QUEUE_METRICS_ENABLED", "true").lower() == "true",
            metrics_interval=int(os.getenv("QUEUE_METRICS_INTERVAL", "30")),
            prometheus_enabled=os.getenv("QUEUE_PROMETHEUS_ENABLED", "false").lower() == "true",
            prometheus_port=int(os.getenv("QUEUE_PROMETHEUS_PORT", "9090")),
            job_timeout=int(os.getenv("QUEUE_JOB_TIMEOUT")) if os.getenv("QUEUE_JOB_TIMEOUT") else None,
            shutdown_timeout=int(os.getenv("QUEUE_SHUTDOWN_TIMEOUT", "30")),
        )