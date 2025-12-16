"""Integration tests for Celery workers."""

import pytest

from pipeline.workers.celery_app import celery_app
from pipeline.workers.tasks import health_check_task, ingest_task


@pytest.mark.integration
@pytest.mark.requires_celery
class TestWorkers:
    """Test Celery workers and tasks."""

    def test_health_check_task(self, celery_app):
        """Test health check task."""
        result = health_check_task.delay()
        task_result = result.get(timeout=10)

        assert "status" in task_result
        assert "stats" in task_result

    def test_ingest_task(self, celery_app, in_memory_db):
        """Test ingest task."""
        # Note: This requires actual database connection
        # In real integration tests, use test database
        pass

    def test_celery_app_config(self, celery_app):
        """Test Celery app configuration."""
        assert celery_app.conf.task_serializer == "json"
        assert celery_app.conf.result_serializer == "json"

    def test_task_registration(self, celery_app):
        """Test that tasks are registered."""
        registered = celery_app.tasks.keys()
        assert "pipeline.workers.tasks.health_check_task" in registered
        assert "pipeline.workers.tasks.ingest_task" in registered
        assert "pipeline.workers.tasks.transform_task" in registered
        assert "pipeline.workers.tasks.persist_task" in registered
        assert "pipeline.workers.tasks.run_pipeline" in registered

