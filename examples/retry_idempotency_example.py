"""Example demonstrating retry and idempotency utilities."""

import logging
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.utils.idempotency import IdempotencyManager, idempotent
from pipeline.utils.retry import RetryError, retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Example 1: Using retry decorator
@retry(max_attempts=3, base_delay=1.0, exponential_base=2.0)
def unreliable_operation(should_fail: bool = True):
    """Example function that may fail."""
    if should_fail:
        logger.info("Operation failed, will retry...")
        raise ConnectionError("Connection failed")
    return "Success!"


# Example 2: Using retry with specific exceptions
@retry(
    max_attempts=5,
    base_delay=0.5,
    exponential_base=2.0,
    retry_on=(ConnectionError, TimeoutError),
    on_retry=lambda attempt, exc: logger.warning(f"Retry attempt {attempt}: {exc}"),
)
def network_operation():
    """Example that only retries on specific exceptions."""
    import random

    if random.random() < 0.7:
        raise ConnectionError("Network error")
    return "Network operation succeeded"


# Example 3: Using idempotency decorator
@idempotent(source_id_field="task_id", content_fields=["data"])
def process_task(task_id: str, data: dict):
    """Example function that should only execute once per task_id + data."""
    logger.info(f"Processing task {task_id} with data {data}")
    time.sleep(1)  # Simulate processing
    return {"status": "processed", "task_id": task_id, "result": "done"}


# Example 4: Using IdempotencyManager directly
def example_idempotency_manager():
    """Example using IdempotencyManager directly."""
    manager = IdempotencyManager()

    if not manager.is_enabled():
        logger.warning("Idempotency is disabled (Redis not available)")
        return

    # Generate a key
    source_id = "pipeline_123"
    content = {"query": "SELECT * FROM users", "params": {"limit": 100}}

    key = manager.generate_key(source_id, content)
    logger.info(f"Generated idempotency key: {key}")

    # Check if key exists
    is_duplicate, key = manager.check_and_set(source_id, content)
    logger.info(f"First call - is_duplicate: {is_duplicate}, key: {key}")

    # Try again with same parameters
    is_duplicate, key = manager.check_and_set(source_id, content)
    logger.info(f"Second call - is_duplicate: {is_duplicate}, key: {key}")

    # Get stored value
    value = manager.get_key(key)
    logger.info(f"Stored value: {value}")


def main():
    """Run examples."""
    logger.info("=== Retry Examples ===")

    # Example 1: Retry with failure
    try:
        result = unreliable_operation(should_fail=True)
        logger.info(f"Result: {result}")
    except RetryError as e:
        logger.error(f"Max retries exceeded: {e}")

    # Example 1b: Retry with success
    try:
        result = unreliable_operation(should_fail=False)
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    # Example 2: Network operation with specific retry exceptions
    try:
        result = network_operation()
        logger.info(f"Network result: {result}")
    except Exception as e:
        logger.error(f"Network operation failed: {e}")

    logger.info("\n=== Idempotency Examples ===")

    # Example 3: Idempotent function
    try:
        result1 = process_task("task_001", {"action": "process", "value": 123})
        logger.info(f"First call result: {result1}")

        # Try again with same parameters (should fail as duplicate)
        result2 = process_task("task_001", {"action": "process", "value": 123})
        logger.info(f"Second call result: {result2}")
    except Exception as e:
        logger.error(f"Idempotency error: {e}")

    # Example 4: Direct manager usage
    example_idempotency_manager()


if __name__ == "__main__":
    main()

