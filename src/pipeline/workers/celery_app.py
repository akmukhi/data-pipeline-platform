"""Celery application initialization."""

import logging
from celery import Celery

from pipeline.config.settings import settings

logger = logging.getLogger(__name__)

# Create Celery app
celery_app = Celery(
    "data_pipeline",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

# Celery configuration
celery_app.conf.update(
    task_serializer=settings.CELERY_TASK_SERIALIZER,
    accept_content=settings.get_celery_accept_content(),
    result_serializer=settings.CELERY_RESULT_SERIALIZER,
    timezone=settings.CELERY_TIMEZONE,
    enable_utc=settings.CELERY_ENABLE_UTC,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=4,
    worker_max_tasks_per_child=settings.WORKER_MAX_TASKS_PER_CHILD,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=3600,  # 1 hour
    task_routes={
        "pipeline.workers.tasks.ingest_task": {"queue": "ingestion"},
        "pipeline.workers.tasks.transform_task": {"queue": "transformation"},
        "pipeline.workers.tasks.persist_task": {"queue": "persistence"},
        "pipeline.workers.tasks.run_pipeline": {"queue": "pipeline"},
    },
)

# Autodiscover tasks
celery_app.autodiscover_tasks(["pipeline.workers"])

logger.info("Celery app initialized")

