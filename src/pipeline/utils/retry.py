"""Retry logic with exponential backoff."""

import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type, Union

logger = logging.getLogger(__name__)


class RetryError(Exception):
    """Exception raised when max retries are exceeded."""

    def __init__(self, message: str, last_exception: Optional[Exception] = None):
        super().__init__(message)
        self.last_exception = last_exception


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: Optional[float] = None,
    exponential_base: float = 2.0,
    retry_on: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
    reraise: bool = True,
):
    """
    Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds before first retry (default: 1.0)
        max_delay: Maximum delay in seconds between retries (default: None, no limit)
        exponential_base: Base for exponential backoff calculation (default: 2.0)
                         Delay = base_delay * (exponential_base ^ attempt_number)
        retry_on: Exception type(s) to retry on (default: Exception, retry on all)
        on_retry: Optional callback function called on each retry attempt
                  Called with (attempt_number, exception)
        reraise: If True, raise RetryError after max attempts exceeded (default: True)

    Returns:
        Decorated function

    Example:
        @retry(max_attempts=5, base_delay=1.0, exponential_base=2.0)
        def risky_operation():
            # This will retry up to 5 times with delays: 1s, 2s, 4s, 8s, 16s
            return some_operation()

        @retry(
            max_attempts=3,
            retry_on=(ConnectionError, TimeoutError),
            on_retry=lambda attempt, exc: print(f"Retry {attempt}: {exc}")
        )
        def network_operation():
            # Only retries on ConnectionError or TimeoutError
            return network_call()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"{func.__name__} succeeded after {attempt} attempts"
                        )
                    return result

                except retry_on as exc:
                    last_exception = exc

                    if attempt >= max_attempts:
                        error_msg = (
                            f"{func.__name__} failed after {max_attempts} attempts: {exc}"
                        )
                        logger.error(error_msg, exc_info=True)

                        if on_retry:
                            try:
                                on_retry(attempt, exc)
                            except Exception as e:
                                logger.warning(f"Error in on_retry callback: {e}")

                        if reraise:
                            raise RetryError(error_msg, last_exception) from exc
                        raise exc

                    # Calculate delay with exponential backoff
                    delay = base_delay * (exponential_base ** (attempt - 1))

                    # Apply max delay if specified
                    if max_delay is not None:
                        delay = min(delay, max_delay)

                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {exc}. "
                        f"Retrying in {delay:.2f}s..."
                    )

                    if on_retry:
                        try:
                            on_retry(attempt, exc)
                        except Exception as e:
                            logger.warning(f"Error in on_retry callback: {e}")

                    time.sleep(delay)

                except Exception as exc:
                    # Don't retry on exceptions not in retry_on
                    logger.error(
                        f"{func.__name__} raised unexpected exception: {exc}",
                        exc_info=True,
                    )
                    raise

            # This should never be reached, but just in case
            if reraise and last_exception:
                raise RetryError(
                    f"{func.__name__} failed after {max_attempts} attempts",
                    last_exception,
                ) from last_exception

            raise RuntimeError("Unexpected retry loop exit")

        return wrapper

    return decorator


def retry_async(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: Optional[float] = None,
    exponential_base: float = 2.0,
    retry_on: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    on_retry: Optional[Callable[[int, Exception], None]] = None,
    reraise: bool = True,
):
    """
    Async retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds before first retry (default: 1.0)
        max_delay: Maximum delay in seconds between retries (default: None, no limit)
        exponential_base: Base for exponential backoff calculation (default: 2.0)
        retry_on: Exception type(s) to retry on (default: Exception)
        on_retry: Optional callback function called on each retry attempt
        reraise: If True, raise RetryError after max attempts exceeded (default: True)

    Returns:
        Decorated async function

    Example:
        @retry_async(max_attempts=3, base_delay=0.5)
        async def async_operation():
            return await some_async_operation()
    """
    import asyncio

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = await func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"{func.__name__} succeeded after {attempt} attempts"
                        )
                    return result

                except retry_on as exc:
                    last_exception = exc

                    if attempt >= max_attempts:
                        error_msg = (
                            f"{func.__name__} failed after {max_attempts} attempts: {exc}"
                        )
                        logger.error(error_msg, exc_info=True)

                        if on_retry:
                            try:
                                on_retry(attempt, exc)
                            except Exception as e:
                                logger.warning(f"Error in on_retry callback: {e}")

                        if reraise:
                            raise RetryError(error_msg, last_exception) from exc
                        raise exc

                    # Calculate delay with exponential backoff
                    delay = base_delay * (exponential_base ** (attempt - 1))

                    # Apply max delay if specified
                    if max_delay is not None:
                        delay = min(delay, max_delay)

                    logger.warning(
                        f"{func.__name__} failed (attempt {attempt}/{max_attempts}): {exc}. "
                        f"Retrying in {delay:.2f}s..."
                    )

                    if on_retry:
                        try:
                            on_retry(attempt, exc)
                        except Exception as e:
                            logger.warning(f"Error in on_retry callback: {e}")

                    await asyncio.sleep(delay)

                except Exception as exc:
                    # Don't retry on exceptions not in retry_on
                    logger.error(
                        f"{func.__name__} raised unexpected exception: {exc}",
                        exc_info=True,
                    )
                    raise

            # This should never be reached
            if reraise and last_exception:
                raise RetryError(
                    f"{func.__name__} failed after {max_attempts} attempts",
                    last_exception,
                ) from last_exception

            raise RuntimeError("Unexpected retry loop exit")

        return wrapper

    return decorator


class RetryableOperation:
    """
    Context manager for retryable operations with exponential backoff.

    Example:
        with RetryableOperation(max_attempts=3, base_delay=1.0) as op:
            result = risky_operation()
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: Optional[float] = None,
        exponential_base: float = 2.0,
        retry_on: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retry_on = retry_on
        self.on_retry = on_retry
        self.attempt = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return False

        if not isinstance(exc_val, self.retry_on):
            return False  # Don't handle unexpected exceptions

        self.attempt += 1

        if self.attempt >= self.max_attempts:
            return False  # Let exception propagate

        # Calculate delay
        delay = self.base_delay * (self.exponential_base ** (self.attempt - 1))
        if self.max_delay is not None:
            delay = min(delay, self.max_delay)

        logger.warning(
            f"Operation failed (attempt {self.attempt}/{self.max_attempts}): {exc_val}. "
            f"Retrying in {delay:.2f}s..."
        )

        if self.on_retry:
            try:
                self.on_retry(self.attempt, exc_val)
            except Exception as e:
                logger.warning(f"Error in on_retry callback: {e}")

        time.sleep(delay)

        # Return True to suppress the exception and retry
        return True

