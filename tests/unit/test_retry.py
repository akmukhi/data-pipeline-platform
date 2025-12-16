"""Unit tests for retry utility."""

import pytest
import time

from pipeline.utils.retry import RetryError, RetryableOperation, retry, retry_async


@pytest.mark.unit
class TestRetry:
    """Test retry decorator."""

    def test_retry_success(self):
        """Test retry with successful operation."""
        call_count = 0

        @retry(max_attempts=3)
        def successful_operation():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_operation()
        assert result == "success"
        assert call_count == 1

    def test_retry_with_failure_then_success(self):
        """Test retry that succeeds after failures."""
        call_count = 0

        @retry(max_attempts=3, base_delay=0.1)
        def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "success"

        result = flaky_operation()
        assert result == "success"
        assert call_count == 2

    def test_retry_max_attempts_exceeded(self):
        """Test retry when max attempts exceeded."""
        call_count = 0

        @retry(max_attempts=3, base_delay=0.1)
        def failing_operation():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")

        with pytest.raises(RetryError):
            failing_operation()

        assert call_count == 3

    def test_retry_specific_exception(self):
        """Test retry only on specific exceptions."""
        call_count = 0

        @retry(max_attempts=3, retry_on=ConnectionError, base_delay=0.1)
        def operation():
            nonlocal call_count
            call_count += 1
            raise ValueError("Wrong exception")

        with pytest.raises(ValueError):
            operation()

        assert call_count == 1  # Should not retry

    def test_retry_with_callback(self):
        """Test retry with callback."""
        call_count = 0
        callback_calls = []

        def on_retry(attempt, exc):
            callback_calls.append((attempt, str(exc)))

        @retry(max_attempts=3, base_delay=0.1, on_retry=on_retry)
        def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "success"

        result = flaky_operation()
        assert result == "success"
        assert len(callback_calls) == 1

    def test_retry_exponential_backoff(self):
        """Test exponential backoff."""
        call_times = []

        @retry(max_attempts=3, base_delay=0.1, exponential_base=2.0)
        def flaky_operation():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise ConnectionError("Fail")

        with pytest.raises(RetryError):
            flaky_operation()

        # Check that delays increase exponentially
        if len(call_times) >= 2:
            delay1 = call_times[1] - call_times[0]
            delay2 = call_times[2] - call_times[1]
            # delay2 should be approximately 2x delay1
            assert delay2 > delay1


@pytest.mark.unit
class TestRetryableOperation:
    """Test RetryableOperation context manager."""

    def test_success(self):
        """Test successful operation."""
        with RetryableOperation(max_attempts=3) as op:
            result = "success"
            assert result == "success"

    def test_retry_then_success(self):
        """Test operation that succeeds after retry."""
        call_count = 0

        with RetryableOperation(max_attempts=3, base_delay=0.1) as op:
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Fail")

        assert call_count == 2

    def test_max_attempts_exceeded(self):
        """Test when max attempts exceeded."""
        call_count = 0

        with pytest.raises(ConnectionError):
            with RetryableOperation(max_attempts=3, base_delay=0.1) as op:
                call_count += 1
                raise ConnectionError("Always fails")

        assert call_count == 3


@pytest.mark.unit
@pytest.mark.asyncio
class TestRetryAsync:
    """Test async retry decorator."""

    async def test_async_retry_success(self):
        """Test async retry with success."""
        call_count = 0

        @retry_async(max_attempts=3)
        async def async_operation():
            nonlocal call_count
            call_count += 1
            return "success"

        result = await async_operation()
        assert result == "success"
        assert call_count == 1

    async def test_async_retry_with_failure(self):
        """Test async retry with failure then success."""
        call_count = 0

        @retry_async(max_attempts=3, base_delay=0.1)
        async def flaky_async_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Fail")
            return "success"

        result = await flaky_async_operation()
        assert result == "success"
        assert call_count == 2

