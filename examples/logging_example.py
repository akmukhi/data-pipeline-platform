"""Example demonstrating structured logging with correlation IDs."""

import logging
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.utils.logging import (
    PipelineLogger,
    generate_correlation_id,
    get_logger,
    set_correlation_id,
    setup_logging,
)

# Setup logging
setup_logging(level="INFO", format_type="console")


def example_basic_logging():
    """Basic logging example."""
    logger = get_logger(__name__)
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")


def example_correlation_id():
    """Example with correlation ID."""
    correlation_id = generate_correlation_id()
    set_correlation_id(correlation_id)

    logger = get_logger(__name__, correlation_id=correlation_id)
    logger.info("Processing request", request_id="req_123")

    # All subsequent logs will use the same correlation ID
    logger.info("Fetching data from database")
    logger.info("Transforming data")
    logger.info("Saving results")


def example_pipeline_stages():
    """Example with pipeline stages."""
    logger = get_logger(__name__)

    # Use context manager for pipeline stages
    with logger.with_pipeline("pipeline_001"):
        logger.info("Starting pipeline")

        with logger.with_stage("ingestion"):
            logger.info("Ingesting data from source")
            time.sleep(0.1)  # Simulate work
            logger.info("Ingestion complete: 1000 rows")

        with logger.with_stage("transformation"):
            logger.info("Transforming data")
            time.sleep(0.1)  # Simulate work
            logger.info("Transformation complete")

        with logger.with_stage("persistence"):
            logger.info("Persisting data to destination")
            time.sleep(0.1)  # Simulate work
            logger.info("Persistence complete: 1000 rows written")

        logger.info("Pipeline completed successfully")


def example_task_tracking():
    """Example with task tracking."""
    logger = get_logger(__name__)

    with logger.with_pipeline("pipeline_002"):
        with logger.with_task("task_ingest_001"):
            logger.info("Executing ingest task")
            logger.info("Task progress: 50%")
            logger.info("Task progress: 100%")

        with logger.with_task("task_transform_001"):
            logger.info("Executing transform task")
            logger.warning("Warning: Some rows filtered")


def example_error_logging():
    """Example with error context."""
    logger = get_logger(__name__)

    with logger.with_pipeline("pipeline_003"):
        try:
            logger.info("Attempting risky operation")
            raise ValueError("Something went wrong")
        except Exception:
            logger.error(
                "Operation failed",
                operation="risky_operation",
                retry_count=3,
                exc_info=True,
            )


def example_custom_fields():
    """Example with custom log fields."""
    logger = get_logger(__name__)

    logger.info(
        "Processing batch",
        batch_number=1,
        batch_size=1000,
        source_table="users",
        destination_table="users_staging",
    )

    logger.info(
        "Transformation complete",
        rows_input=1000,
        rows_output=950,
        rows_filtered=50,
        duration_ms=1234,
    )


def main():
    """Run all examples."""
    print("=" * 60)
    print("Example 1: Basic Logging")
    print("=" * 60)
    example_basic_logging()

    print("\n" + "=" * 60)
    print("Example 2: Correlation ID")
    print("=" * 60)
    example_correlation_id()

    print("\n" + "=" * 60)
    print("Example 3: Pipeline Stages")
    print("=" * 60)
    example_pipeline_stages()

    print("\n" + "=" * 60)
    print("Example 4: Task Tracking")
    print("=" * 60)
    example_task_tracking()

    print("\n" + "=" * 60)
    print("Example 5: Error Logging")
    print("=" * 60)
    example_error_logging()

    print("\n" + "=" * 60)
    print("Example 6: Custom Fields")
    print("=" * 60)
    example_custom_fields()


if __name__ == "__main__":
    main()

