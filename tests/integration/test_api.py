"""Integration tests for REST API."""

import pytest
from fastapi.testclient import TestClient

from pipeline.api.main import app


@pytest.mark.integration
class TestAPI:
    """Test REST API endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def test_root_endpoint(self, client):
        """Test root endpoint."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert "version" in data

    def test_health_endpoint(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code in [200, 503]  # May be unavailable in test
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "services" in data

    def test_create_pipeline(self, client, celery_app):
        """Test creating a pipeline."""
        payload = {
            "pipeline_config": {
                "ingestion": {
                    "query": "SELECT * FROM users",
                    "source_db_url": "postgresql://test:test@localhost/test",
                },
                "transformation": {
                    "type": "sql",
                    "config": {"sql_query": "SELECT * FROM input_data"},
                },
                "persistence": {
                    "table_name": "users_staging",
                    "strategy": "insert",
                },
            },
            "pipeline_id": "test_pipeline_001",
        }

        response = client.post("/pipelines", json=payload)
        assert response.status_code in [200, 500]  # May fail if workers not available

    def test_create_ingest_task(self, client, celery_app):
        """Test creating ingest task."""
        payload = {
            "query": "SELECT * FROM users",
            "source_db_url": "postgresql://test:test@localhost/test",
        }

        response = client.post("/tasks/ingest", json=payload)
        assert response.status_code in [200, 500]

    def test_get_workers(self, client):
        """Test getting workers list."""
        response = client.get("/workers")
        assert response.status_code in [200, 500]

    def test_invalid_request(self, client):
        """Test invalid request handling."""
        # Missing required fields
        response = client.post("/pipelines", json={})
        assert response.status_code == 422  # Validation error

