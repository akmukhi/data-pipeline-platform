"""Unit tests for idempotency utility."""

import pytest

from pipeline.utils.idempotency import (
    IdempotencyKeyError,
    IdempotencyManager,
    get_idempotency_manager,
    idempotent,
)


@pytest.mark.unit
class TestIdempotencyManager:
    """Test IdempotencyManager."""

    def test_init_with_mock_redis(self, mock_redis):
        """Test initialization with mock Redis."""
        manager = IdempotencyManager(redis_client=mock_redis)
        assert manager.enabled is True

    def test_init_without_redis(self, monkeypatch):
        """Test initialization without Redis."""
        monkeypatch.setattr("pipeline.utils.idempotency.REDIS_AVAILABLE", False)
        manager = IdempotencyManager()
        assert manager.enabled is False

    def test_generate_key(self, mock_redis):
        """Test key generation."""
        manager = IdempotencyManager(redis_client=mock_redis)

        key1 = manager.generate_key("source_1", "content_1")
        key2 = manager.generate_key("source_1", "content_1")
        key3 = manager.generate_key("source_1", "content_2")

        assert key1 == key2  # Same source and content
        assert key1 != key3  # Different content

    def test_check_key(self, mock_redis):
        """Test checking key."""
        manager = IdempotencyManager(redis_client=mock_redis)

        key = manager.generate_key("test", "data")
        exists = manager.check_key(key)

        assert exists is False  # Key doesn't exist yet

    def test_set_key(self, mock_redis):
        """Test setting key."""
        manager = IdempotencyManager(redis_client=mock_redis)

        key = manager.generate_key("test", "data")
        success = manager.set_key(key, {"result": "test"})

        assert success is True

    def test_check_and_set(self, mock_redis):
        """Test atomic check and set."""
        manager = IdempotencyManager(redis_client=mock_redis)

        is_dup, key = manager.check_and_set("source_1", "content_1")
        assert is_dup is False  # First call

        # Mock that key now exists
        mock_redis.set.return_value = False  # Key already exists
        is_dup, key = manager.check_and_set("source_1", "content_1")
        assert is_dup is True  # Duplicate

    def test_get_key(self, mock_redis):
        """Test getting key value."""
        manager = IdempotencyManager(redis_client=mock_redis)

        key = manager.generate_key("test", "data")
        value = manager.get_key(key)

        assert value is None  # Key doesn't exist

    def test_delete_key(self, mock_redis):
        """Test deleting key."""
        manager = IdempotencyManager(redis_client=mock_redis)

        key = manager.generate_key("test", "data")
        success = manager.delete_key(key)

        assert success is True

    def test_test_connection(self, mock_redis):
        """Test connection test."""
        manager = IdempotencyManager(redis_client=mock_redis)
        assert manager.test_connection() is True


@pytest.mark.unit
class TestIdempotentDecorator:
    """Test idempotent decorator."""

    def test_idempotent_success(self, mock_redis):
        """Test idempotent function execution."""
        manager = IdempotencyManager(redis_client=mock_redis)

        call_count = 0

        @idempotent(source_id_field="task_id", manager=manager)
        def process_task(task_id, data):
            nonlocal call_count
            call_count += 1
            return {"status": "processed", "count": call_count}

        result1 = process_task("task_1", {"action": "process"})
        assert call_count == 1

        # Second call should be detected as duplicate
        with pytest.raises(IdempotencyKeyError):
            process_task("task_1", {"action": "process"})

        assert call_count == 1  # Should not execute again

    def test_idempotent_different_params(self, mock_redis):
        """Test idempotent with different parameters."""
        manager = IdempotencyManager(redis_client=mock_redis)

        call_count = 0

        @idempotent(source_id_field="task_id", content_fields=["data"], manager=manager)
        def process_task(task_id, data):
            nonlocal call_count
            call_count += 1
            return {"status": "processed"}

        # Different data should execute
        result1 = process_task("task_1", {"action": "process"})
        result2 = process_task("task_1", {"action": "different"})

        assert call_count == 2  # Both should execute

