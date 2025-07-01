"""Celery tasks for queue management and maintenance.

This module contains tasks for managing the queue system itself,
including monitoring, cleanup, and health checks.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from collections import defaultdict

import structlog
from celery import Task, states
from celery.result import AsyncResult
from redis import Redis

from budflow.config import settings
from budflow.worker import celery_app
from budflow.metrics import metrics
from budflow.database import get_db
from budflow.queue.metrics import QueueMetrics

logger = structlog.get_logger()


class QueueManagementTask(Task):
    """Base class for queue management tasks."""
    
    def before_start(self, task_id, args, kwargs):
        """Called before task execution starts."""
        logger.info(
            "Starting queue management task",
            task_id=task_id,
            task_name=self.name
        )
        metrics.increment("queue_management_tasks_started", tags={"task": self.name})
    
    def on_success(self, retval, task_id, args, kwargs):
        """Called on successful task completion."""
        metrics.increment("queue_management_tasks_completed", tags={"task": self.name})
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called on task failure."""
        logger.error(
            "Queue management task failed",
            task_id=task_id,
            task_name=self.name,
            error=str(exc)
        )
        metrics.increment("queue_management_tasks_failed", tags={"task": self.name})


@celery_app.task(
    base=QueueManagementTask,
    name="budflow.monitor_queues",
    queue="maintenance"
)
def monitor_queues_task() -> Dict[str, Any]:
    """Monitor queue health and metrics.
    
    Returns:
        Queue monitoring data
    """
    redis_client = Redis.from_url(settings.celery_broker_url)
    queue_metrics = QueueMetrics()
    
    try:
        # Get queue lengths
        queues = ["workflows", "integrations", "maintenance", "scheduler"]
        queue_lengths = {}
        
        for queue_name in queues:
            queue_key = f"celery:queue:{queue_name}"
            length = redis_client.llen(queue_key)
            queue_lengths[queue_name] = length
            
            # Update metrics
            metrics.gauge(f"queue_length_{queue_name}", length)
        
        # Get active tasks
        active_tasks = celery_app.control.inspect().active()
        active_count = sum(len(tasks) for tasks in (active_tasks or {}).values())
        
        # Get scheduled tasks
        scheduled_tasks = celery_app.control.inspect().scheduled()
        scheduled_count = sum(len(tasks) for tasks in (scheduled_tasks or {}).values())
        
        # Get worker stats
        stats = celery_app.control.inspect().stats()
        worker_count = len(stats) if stats else 0
        
        # Calculate health score
        health_score = 100
        for queue_name, length in queue_lengths.items():
            if length > 1000:
                health_score -= 20
            elif length > 500:
                health_score -= 10
        
        if active_count > 100:
            health_score -= 10
        
        if worker_count == 0:
            health_score = 0
        
        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "queue_lengths": queue_lengths,
            "active_tasks": active_count,
            "scheduled_tasks": scheduled_count,
            "worker_count": worker_count,
            "health_score": max(0, health_score),
            "status": "healthy" if health_score >= 70 else "degraded" if health_score >= 40 else "unhealthy"
        }
        
        # Store metrics
        queue_metrics.record_queue_depth(queue_lengths)
        queue_metrics.record_task_count(active_count, "active")
        queue_metrics.record_task_count(scheduled_count, "scheduled")
        
        return result
        
    finally:
        redis_client.close()


@celery_app.task(
    base=QueueManagementTask,
    name="budflow.cleanup_failed_tasks",
    queue="maintenance"
)
def cleanup_failed_tasks(days: int = 7) -> Dict[str, Any]:
    """Clean up old failed tasks from result backend.
    
    Args:
        days: Number of days to keep failed tasks
    
    Returns:
        Cleanup result
    """
    redis_client = Redis.from_url(settings.celery_result_backend)
    cleaned_count = 0
    
    try:
        # Get all result keys
        cursor = 0
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        while True:
            cursor, keys = redis_client.scan(
                cursor=cursor,
                match="celery-task-meta-*",
                count=100
            )
            
            for key in keys:
                try:
                    # Get task result
                    result_data = redis_client.get(key)
                    if result_data:
                        result = json.loads(result_data)
                        
                        # Check if task is failed and old
                        if result.get("status") in [states.FAILURE, states.REVOKED]:
                            task_time = datetime.fromisoformat(
                                result.get("date_done", "2000-01-01")
                            )
                            if task_time < cutoff_time:
                                redis_client.delete(key)
                                cleaned_count += 1
                                
                except Exception as e:
                    logger.warning(f"Error processing task result {key}: {str(e)}")
            
            if cursor == 0:
                break
        
        logger.info(f"Cleaned up {cleaned_count} failed tasks")
        metrics.gauge("failed_tasks_cleaned", cleaned_count)
        
        return {
            "cleaned_count": cleaned_count,
            "cutoff_days": days,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    finally:
        redis_client.close()


@celery_app.task(
    base=QueueManagementTask,
    name="budflow.requeue_stuck_tasks",
    queue="maintenance"
)
def requeue_stuck_tasks(timeout_minutes: int = 30) -> Dict[str, Any]:
    """Requeue tasks that are stuck in processing.
    
    Args:
        timeout_minutes: Minutes before considering a task stuck
    
    Returns:
        Requeue result
    """
    requeued_count = 0
    stuck_tasks = []
    
    # Inspect active tasks
    active_tasks = celery_app.control.inspect().active()
    if not active_tasks:
        return {
            "requeued_count": 0,
            "stuck_tasks": [],
            "status": "no_active_tasks"
        }
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
    
    for worker, tasks in active_tasks.items():
        for task in tasks:
            # Check if task is stuck
            start_time = datetime.fromisoformat(task.get("time_start", ""))
            if start_time < cutoff_time:
                stuck_tasks.append({
                    "task_id": task["id"],
                    "name": task["name"],
                    "worker": worker,
                    "runtime_minutes": (datetime.now(timezone.utc) - start_time).seconds // 60
                })
                
                # Revoke and requeue
                celery_app.control.revoke(task["id"], terminate=True)
                
                # Requeue based on task type
                if task["name"] == "budflow.execute_workflow":
                    from budflow.workflows.tasks import execute_workflow_task
                    execute_workflow_task.apply_async(
                        kwargs=task.get("kwargs", {}),
                        countdown=60  # Wait 1 minute before retry
                    )
                    requeued_count += 1
    
    logger.info(f"Requeued {requeued_count} stuck tasks")
    metrics.gauge("stuck_tasks_requeued", requeued_count)
    
    return {
        "requeued_count": requeued_count,
        "stuck_tasks": stuck_tasks,
        "timeout_minutes": timeout_minutes,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@celery_app.task(
    base=QueueManagementTask,
    name="budflow.balance_queues",
    queue="maintenance"
)
def balance_queues_task() -> Dict[str, Any]:
    """Balance work across queues by moving tasks.
    
    Returns:
        Balancing result
    """
    redis_client = Redis.from_url(settings.celery_broker_url)
    moved_tasks = 0
    
    try:
        # Define queue priorities and thresholds
        queue_config = {
            "workflows": {"max_length": 1000, "priority": 1},
            "integrations": {"max_length": 500, "priority": 2},
            "scheduler": {"max_length": 200, "priority": 3},
            "maintenance": {"max_length": 100, "priority": 4}
        }
        
        # Get current queue lengths
        queue_lengths = {}
        for queue_name in queue_config:
            queue_key = f"celery:queue:{queue_name}"
            queue_lengths[queue_name] = redis_client.llen(queue_key)
        
        # Find overloaded queues
        overloaded = []
        underloaded = []
        
        for queue_name, config in queue_config.items():
            length = queue_lengths[queue_name]
            if length > config["max_length"]:
                overloaded.append((queue_name, length - config["max_length"]))
            elif length < config["max_length"] * 0.5:
                underloaded.append((queue_name, config["max_length"] - length))
        
        # Balance if needed
        if overloaded and underloaded:
            # Move tasks from overloaded to underloaded queues
            # This is a simplified implementation
            logger.info(
                "Queue balancing needed",
                overloaded=overloaded,
                underloaded=underloaded
            )
        
        return {
            "queue_lengths": queue_lengths,
            "moved_tasks": moved_tasks,
            "balanced": moved_tasks > 0,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    finally:
        redis_client.close()


@celery_app.task(
    base=QueueManagementTask,
    name="budflow.collect_task_metrics",
    queue="maintenance"
)
def collect_task_metrics_task() -> Dict[str, Any]:
    """Collect detailed task execution metrics.
    
    Returns:
        Task metrics
    """
    redis_client = Redis.from_url(settings.celery_result_backend)
    task_stats = defaultdict(lambda: {
        "total": 0,
        "success": 0,
        "failure": 0,
        "retry": 0,
        "avg_runtime": 0
    })
    
    try:
        # Scan for recent task results
        cursor = 0
        total_tasks = 0
        
        while cursor != 0 or total_tasks == 0:
            cursor, keys = redis_client.scan(
                cursor=cursor,
                match="celery-task-meta-*",
                count=100
            )
            
            for key in keys:
                try:
                    result_data = redis_client.get(key)
                    if result_data:
                        result = json.loads(result_data)
                        task_name = result.get("task", "unknown")
                        status = result.get("status", "UNKNOWN")
                        
                        task_stats[task_name]["total"] += 1
                        
                        if status == states.SUCCESS:
                            task_stats[task_name]["success"] += 1
                        elif status == states.FAILURE:
                            task_stats[task_name]["failure"] += 1
                        elif status == states.RETRY:
                            task_stats[task_name]["retry"] += 1
                        
                        # Calculate runtime if available
                        if "date_done" in result and "date_start" in result:
                            try:
                                start = datetime.fromisoformat(result["date_start"])
                                done = datetime.fromisoformat(result["date_done"])
                                runtime = (done - start).total_seconds()
                                
                                current_avg = task_stats[task_name]["avg_runtime"]
                                current_count = task_stats[task_name]["total"] - 1
                                new_avg = (current_avg * current_count + runtime) / task_stats[task_name]["total"]
                                task_stats[task_name]["avg_runtime"] = new_avg
                            except:
                                pass
                        
                        total_tasks += 1
                        
                except Exception as e:
                    logger.warning(f"Error processing task metrics for {key}: {str(e)}")
            
            if total_tasks > 1000:  # Limit to prevent long-running task
                break
        
        # Convert to regular dict and update metrics
        task_stats = dict(task_stats)
        for task_name, stats in task_stats.items():
            metrics.gauge(f"task_total_{task_name}", stats["total"])
            metrics.gauge(f"task_success_{task_name}", stats["success"])
            metrics.gauge(f"task_failure_{task_name}", stats["failure"])
            metrics.gauge(f"task_avg_runtime_{task_name}", stats["avg_runtime"])
        
        return {
            "task_stats": task_stats,
            "total_tasks_analyzed": total_tasks,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    finally:
        redis_client.close()


@celery_app.task(
    base=QueueManagementTask,
    name="budflow.health_check",
    queue="maintenance"
)
def health_check_task() -> Dict[str, Any]:
    """Perform comprehensive health check of the queue system.
    
    Returns:
        Health check result
    """
    health_status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "checks": {},
        "overall_status": "healthy"
    }
    
    # Check Redis connectivity
    try:
        redis_client = Redis.from_url(settings.celery_broker_url)
        redis_client.ping()
        health_status["checks"]["redis"] = {"status": "healthy", "message": "Redis is accessible"}
        redis_client.close()
    except Exception as e:
        health_status["checks"]["redis"] = {"status": "unhealthy", "message": str(e)}
        health_status["overall_status"] = "unhealthy"
    
    # Check worker availability
    try:
        stats = celery_app.control.inspect().stats()
        worker_count = len(stats) if stats else 0
        if worker_count > 0:
            health_status["checks"]["workers"] = {
                "status": "healthy",
                "message": f"{worker_count} workers available"
            }
        else:
            health_status["checks"]["workers"] = {
                "status": "unhealthy",
                "message": "No workers available"
            }
            health_status["overall_status"] = "unhealthy"
    except Exception as e:
        health_status["checks"]["workers"] = {"status": "unhealthy", "message": str(e)}
        health_status["overall_status"] = "unhealthy"
    
    # Check database connectivity
    try:
        async def check_db():
            async for session in get_db():
                await session.execute("SELECT 1")
                return True
        
        asyncio.run(check_db())
        health_status["checks"]["database"] = {"status": "healthy", "message": "Database is accessible"}
    except Exception as e:
        health_status["checks"]["database"] = {"status": "unhealthy", "message": str(e)}
        health_status["overall_status"] = "degraded"
    
    # Check queue depths
    queue_result = monitor_queues_task()
    if queue_result["health_score"] < 70:
        health_status["checks"]["queues"] = {
            "status": "degraded" if queue_result["health_score"] >= 40 else "unhealthy",
            "message": f"Queue health score: {queue_result['health_score']}"
        }
        if health_status["overall_status"] == "healthy":
            health_status["overall_status"] = "degraded"
    else:
        health_status["checks"]["queues"] = {
            "status": "healthy",
            "message": f"Queue health score: {queue_result['health_score']}"
        }
    
    # Update metrics
    metrics.gauge("system_health_score", 
                  100 if health_status["overall_status"] == "healthy" else 
                  50 if health_status["overall_status"] == "degraded" else 0)
    
    return health_status


# Beat schedule for queue management tasks
celery_app.conf.beat_schedule.update({
    'monitor-queues': {
        'task': 'budflow.monitor_queues',
        'schedule': 60.0,  # Every minute
    },
    'cleanup-failed-tasks': {
        'task': 'budflow.cleanup_failed_tasks',
        'schedule': crontab(hour=3, minute=0),  # Daily at 3 AM
        'kwargs': {'days': 7}
    },
    'requeue-stuck-tasks': {
        'task': 'budflow.requeue_stuck_tasks',
        'schedule': 300.0,  # Every 5 minutes
        'kwargs': {'timeout_minutes': 30}
    },
    'collect-task-metrics': {
        'task': 'budflow.collect_task_metrics',
        'schedule': 300.0,  # Every 5 minutes
    },
    'health-check': {
        'task': 'budflow.health_check',
        'schedule': 60.0,  # Every minute
    }
})

# Import crontab for beat schedule
from celery.schedules import crontab

# Task routing
celery_app.conf.task_routes.update({
    'budflow.monitor_queues': {'queue': 'maintenance'},
    'budflow.cleanup_failed_tasks': {'queue': 'maintenance'},
    'budflow.requeue_stuck_tasks': {'queue': 'maintenance'},
    'budflow.balance_queues': {'queue': 'maintenance'},
    'budflow.collect_task_metrics': {'queue': 'maintenance'},
    'budflow.health_check': {'queue': 'maintenance'},
})