"""Unit tests for connection manager."""

import pytest
from sqlalchemy import text

from pipeline.ingestion.connection_manager import ConnectionManager


@pytest.mark.unit
class TestConnectionManager:
    """Test ConnectionManager."""

    def test_init_with_url(self):
        """Test initialization with database URL."""
        manager = ConnectionManager("sqlite:///:memory:")
        assert manager.database_url == "sqlite:///:memory:"
        assert manager.pool_size == 5
        assert manager.engine is not None

    def test_init_with_custom_pool_size(self):
        """Test initialization with custom pool size."""
        manager = ConnectionManager(
            "sqlite:///:memory:", pool_size=10, max_overflow=20
        )
        assert manager.pool_size == 10
        assert manager.max_overflow == 20

    def test_get_connection(self, in_memory_db):
        """Test getting a connection."""
        manager = ConnectionManager("sqlite:///:memory:")
        with manager.get_connection() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1

    def test_get_engine(self):
        """Test getting engine."""
        manager = ConnectionManager("sqlite:///:memory:")
        engine = manager.get_engine()
        assert engine is not None

    def test_health_check(self, in_memory_db):
        """Test health check."""
        manager = ConnectionManager("sqlite:///:memory:")
        assert manager.health_check() is True

    def test_get_pool_status(self):
        """Test getting pool status."""
        manager = ConnectionManager("sqlite:///:memory:")
        status = manager.get_pool_status()
        assert "size" in status
        assert "checked_in" in status
        assert "checked_out" in status

    def test_context_manager(self):
        """Test using as context manager."""
        with ConnectionManager("sqlite:///:memory:") as manager:
            assert manager.health_check() is True

    def test_close(self):
        """Test closing connection pool."""
        manager = ConnectionManager("sqlite:///:memory:")
        manager.close()  # Should not raise

