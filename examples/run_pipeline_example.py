"""Example script demonstrating how to run a pipeline using Celery tasks."""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline.workers.tasks import run_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Run an example pipeline."""
    # Example pipeline configuration
    pipeline_config = {
        "ingestion": {
            "query": "SELECT id, name, email, created_at FROM users WHERE created_at > :start_date",
            "source_db_url": "postgresql://user:password@localhost:5432/source_db",
            "parameters": {"start_date": "2024-01-01"},
            "batch_size": 10000,
        },
        "transformation": {
            "type": "sql",
            "config": {
                "sql_query": "SELECT id, UPPER(name) as name_upper, email, EXTRACT(YEAR FROM created_at) as created_year FROM input_data",
            },
        },
        "persistence": {
            "table_name": "users_staging",
            "schema": "public",
            "strategy": "upsert",
            "upsert_keys": ["id"],
            "dest_db_url": "postgresql://user:password@localhost:5432/dest_db",
        },
    }

    logger.info("Submitting pipeline task...")

    # Submit pipeline task asynchronously
    result = run_pipeline.delay(
        pipeline_config=pipeline_config,
        pipeline_id="example_pipeline_001",
    )

    logger.info(f"Pipeline task submitted with ID: {result.id}")
    logger.info("Waiting for pipeline to complete...")

    try:
        # Wait for result (with timeout)
        pipeline_result = result.get(timeout=3600)  # 1 hour timeout

        if pipeline_result.get("status") == "success":
            logger.info("Pipeline completed successfully!")
            logger.info(f"Ingestion: {pipeline_result['ingestion']['row_count']} rows")
            logger.info(f"Transformation: {pipeline_result['transformation']['row_count']} rows")
            logger.info(f"Persistence: {pipeline_result['persistence']['rows_written']} rows written")
        else:
            logger.error(f"Pipeline failed: {pipeline_result.get('error')}")

    except Exception as e:
        logger.error(f"Error running pipeline: {e}", exc_info=True)


if __name__ == "__main__":
    main()

