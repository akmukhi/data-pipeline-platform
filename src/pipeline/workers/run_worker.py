#!/usr/bin/env python3
"""Script to run Celery worker for the data pipeline platform."""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipeline.config.settings import settings
from pipeline.workers.celery_app import celery_app

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    # Start worker with appropriate configuration
    celery_app.worker_main([
        "worker",
        f"--loglevel={settings.LOG_LEVEL.lower()}",
        f"--concurrency={settings.WORKER_CONCURRENCY}",
        f"--max-tasks-per-child={settings.WORKER_MAX_TASKS_PER_CHILD}",
        "--queues=ingestion,transformation,persistence,pipeline",
        "--hostname=worker@%h",
    ])

