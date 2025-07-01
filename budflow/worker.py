"""Celery worker entry point."""

import sys

import structlog
from celery import Celery
from rich.console import Console

from budflow.config import settings

logger = structlog.get_logger()
console = Console()

# Create Celery app
celery_app = Celery(
    "budflow",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "budflow.workflows.tasks",
        "budflow.integrations.tasks",
        "budflow.queue.tasks",
    ],
)

# Configure Celery
celery_app.conf.update(
    task_serializer=settings.celery_task_serializer,
    accept_content=settings.celery_accept_content,
    result_serializer=settings.celery_result_serializer,
    timezone=settings.celery_timezone,
    enable_utc=True,
    task_track_started=True,
    task_time_limit=settings.max_execution_time,
    task_soft_time_limit=settings.max_execution_time - 60,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
)


@celery_app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery setup."""
    logger.info(f"Request: {self.request!r}")
    return f"Debug task executed successfully"


def main():
    """Main worker entry point."""
    console.print("🔧 Starting BudFlow Celery worker...")
    
    try:
        # Start the worker
        celery_app.start([
            "worker",
            "--loglevel=info",
            "--concurrency=4",
            "--queues=default,workflows,integrations",
        ])
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error("Worker error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()