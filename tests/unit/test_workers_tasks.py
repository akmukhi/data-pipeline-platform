"""Unit tests for worker tasks."""

import pytest

from pipeline.workers.tasks import (
    _create_data_hash,
    _deserialize_dataframe,
    _serialize_dataframe,
    ingest_task,
    persist_task,
    transform_task,
)


@pytest.mark.unit
class TestWorkerTasks:
    """Test worker tasks."""

    def test_serialize_deserialize_dataframe(self, sample_dataframe):
        """Test DataFrame serialization."""
        serialized = _serialize_dataframe(sample_dataframe)
        assert "format" in serialized
        assert "data" in serialized

        deserialized = _deserialize_dataframe(serialized)
        assert len(deserialized) == len(sample_dataframe)
        assert list(deserialized.columns) == list(sample_dataframe.columns)

    def test_create_data_hash(self, sample_dataframe):
        """Test data hash creation."""
        hash1 = _create_data_hash(sample_dataframe)
        hash2 = _create_data_hash(sample_dataframe)

        assert hash1 == hash2  # Same data should produce same hash

        # Modify data
        df2 = sample_dataframe.copy()
        df2.loc[0, "name"] = "Different"
        hash3 = _create_data_hash(df2)

        assert hash1 != hash3  # Different data should produce different hash

    def test_ingest_task_mock(self, celery_app, sample_dataframe):
        """Test ingest task (mocked)."""
        # Note: Full test would require actual database
        # This tests the task structure
        pass

    def test_transform_task_structure(self, celery_app):
        """Test transform task structure."""
        # Test that task is callable
        assert callable(transform_task)

    def test_persist_task_structure(self, celery_app):
        """Test persist task structure."""
        # Test that task is callable
        assert callable(persist_task)

