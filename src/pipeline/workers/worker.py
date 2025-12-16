"""Worker configuration and health monitoring."""

import logging
import signal
import sys
import time
from typing import Dict, Optional

from celery import states
from celery.signals import task_postrun, task_prerun, worker_ready, worker_shutting_down
from celery.utils.log import get_task_logger

from pipeline.config.settings import settings
from pipeline.workers.celery_app import celery_app

logger = logging.getLogger(__name__)
task_logger = get_task_logger(__name__)


class WorkerMonitor:
    """Monitor worker health and task execution."""

    def __init__(self):
        self.task_count = 0
        self.task_errors = 0
        self.task_successes = 0
        self.start_time = time.time()
        self.last_heartbeat = time.time()

    def task_started(self, task_id: str, task_name: str):
        """Record task start."""
        self.task_count += 1
        self.last_heartbeat = time.time()
        logger.info(f"Task started: {task_name} (task_id={task_id})")

    def task_completed(self, task_id: str, task_name: str, success: bool):
        """Record task completion."""
        self.last_heartbeat = time.time()
        if success:
            self.task_successes += 1
            logger.info(f"Task completed successfully: {task_name} (task_id={task_id})")
        else:
            self.task_errors += 1
            logger.error(f"Task failed: {task_name} (task_id={task_id})")

    def get_stats(self) -> Dict:
        """Get worker statistics."""
        uptime = time.time() - self.start_time
        return {
            "tasks_processed": self.task_count,
            "tasks_succeeded": self.task_successes,
            "tasks_failed": self.task_errors,
            "success_rate": (
                self.task_successes / self.task_count if self.task_count > 0 else 0
            ),
            "uptime_seconds": uptime,
            "last_heartbeat": self.last_heartbeat,
            "is_healthy": (time.time() - self.last_heartbeat) < 300,  # 5 minutes
        }

    def health_check(self) -> bool:
        """Perform health check."""
        stats = self.get_stats()
        return stats["is_healthy"]


# Global monitor instance
monitor = WorkerMonitor()


# Signal handlers for task lifecycle
@task_prerun.connect
def task_prerun_handler(sender=None, task_id=None, task=None, args=None, kwargs=None, **kwds):
    """Handle task pre-run signal."""
    monitor.task_started(task_id, task.name if task else "unknown")


@task_postrun.connect
def task_postrun_handler(
    sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **kwds
):
    """Handle task post-run signal."""
    success = state == states.SUCCESS
    monitor.task_completed(task_id, task.name if task else "unknown", success)


@worker_ready.connect
def worker_ready_handler(sender=None, **kwargs):
    """Handle worker ready signal."""
    logger.info("Worker is ready and accepting tasks")
    logger.info(f"Worker configuration: concurrency={settings.WORKER_CONCURRENCY}")


@worker_shutting_down.connect
def worker_shutting_down_handler(sender=None, **kwargs):
    """Handle worker shutting down signal."""
    logger.info("Worker is shutting down")
    stats = monitor.get_stats()
    logger.info(f"Final stats: {stats}")


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


@celery_app.task(name="pipeline.workers.tasks.health_check_task")
def health_check_task() -> Dict:
    """
    Health check task for monitoring worker status.

    Returns:
        Dictionary with health status
    """
    stats = monitor.get_stats()
    is_healthy = monitor.health_check()

    return {
        "status": "healthy" if is_healthy else "unhealthy",
        "stats": stats,
        "timestamp": time.time(),
    }


def get_worker_info() -> Dict:
    """
    Get worker information and statistics.

    Returns:
        Dictionary with worker info
    """
    stats = monitor.get_stats()
    return {
        "worker_stats": stats,
        "settings": {
            "concurrency": settings.WORKER_CONCURRENCY,
            "max_tasks_per_child": settings.WORKER_MAX_TASKS_PER_CHILD,
            "batch_size": settings.BATCH_SIZE,
        },
    }


if __name__ == "__main__":
    """Run worker directly (alternative to celery worker command)."""
    setup_signal_handlers()

    logger.info("Starting Celery worker...")
    logger.info(f"Broker: {settings.CELERY_BROKER_URL}")
    logger.info(f"Backend: {settings.CELERY_RESULT_BACKEND}")

    # Start worker
    celery_app.worker_main([
        "worker",
        f"--loglevel={settings.LOG_LEVEL.lower()}",
        f"--concurrency={settings.WORKER_CONCURRENCY}",
        f"--max-tasks-per-child={settings.WORKER_MAX_TASKS_PER_CHILD}",
        "--queues=ingestion,transformation,persistence,pipeline",
    ])

