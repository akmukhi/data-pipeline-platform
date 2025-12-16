"""Celery task definitions for pipeline stages."""

import hashlib
import json
import logging
import pickle
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from celery import Task

from pipeline.config.settings import settings
from pipeline.ingestion import BatchIngestor
from pipeline.persistence import BatchWriter, WriteStrategy
from pipeline.transformation import (
    CodeTransformer,
    ConfigTransformer,
    SQLTransformer,
)
from pipeline.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


class PipelineTask(Task):
    """Base task class with error handling and logging."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure."""
        logger.error(
            f"Task {self.name} failed: {exc}",
            extra={
                "task_id": task_id,
                "args": args,
                "kwargs": kwargs,
                "exception_info": str(einfo),
            },
            exc_info=True,
        )

    def on_success(self, retval, task_id, args, kwargs):
        """Handle task success."""
        logger.info(
            f"Task {self.name} completed successfully",
            extra={
                "task_id": task_id,
                "result_size": len(retval) if isinstance(retval, (list, dict)) else 1,
            },
        )


@celery_app.task(
    base=PipelineTask,
    name="pipeline.workers.tasks.ingest_task",
    bind=True,
    max_retries=None,
    default_retry_delay=settings.RETRY_DELAY,
)
def ingest_task(
    self,
    query: str,
    source_db_url: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    batch_size: Optional[int] = None,
    task_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ingest data from source database.

    Args:
        query: SQL query to execute
        source_db_url: Source database URL (uses settings if not provided)
        parameters: Optional query parameters
        batch_size: Batch size for ingestion
        task_id: Optional task identifier for tracking
        pipeline_id: Optional pipeline identifier

    Returns:
        Dictionary with ingestion results:
        {
            "status": "success",
            "data_id": "...",  # Identifier for the ingested data
            "row_count": 12345,
            "columns": [...],
            "pipeline_id": "..."
        }
    """
    try:
        db_url = source_db_url or settings.SOURCE_DB_URL
        batch_size = batch_size or settings.BATCH_SIZE

        logger.info(
            f"Ingesting data (task_id={task_id}, pipeline_id={pipeline_id})",
            extra={
                "query_preview": query[:100],
                "batch_size": batch_size,
            },
        )

        # Create ingestor
        ingestor = BatchIngestor(
            database_url=db_url,
            batch_size=batch_size,
            max_retries=settings.MAX_RETRIES,
            retry_delay=settings.RETRY_DELAY,
        )

        # Ingest data
        df = ingestor.ingest(query, parameters=parameters, stream=True)

        if df.empty:
            logger.warning("Ingestion returned empty DataFrame")
            return {
                "status": "success",
                "data_id": None,
                "row_count": 0,
                "columns": [],
                "pipeline_id": pipeline_id,
                "task_id": task_id,
            }

        # Create data identifier (hash of data sample for idempotency)
        data_hash = _create_data_hash(df)
        data_id = f"data_{data_hash}"

        # Store data in Redis or return serialized data
        # For large datasets, we might store reference instead of full data
        data_serialized = _serialize_dataframe(df)

        result = {
            "status": "success",
            "data_id": data_id,
            "row_count": len(df),
            "columns": list(df.columns.tolist()),
            "pipeline_id": pipeline_id,
            "task_id": task_id,
            "data": data_serialized,  # Serialized DataFrame
        }

        logger.info(
            f"Ingestion complete: {len(df)} rows",
            extra={"data_id": data_id, "pipeline_id": pipeline_id},
        )

        return result

    except Exception as exc:
        logger.error(f"Ingestion task failed: {exc}", exc_info=True)
        # Retry with exponential backoff
        raise self.retry(exc=exc, countdown=settings.RETRY_BACKOFF_BASE ** self.request.retries)


@celery_app.task(
    base=PipelineTask,
    name="pipeline.workers.tasks.transform_task",
    bind=True,
    max_retries=None,
    default_retry_delay=settings.RETRY_DELAY,
)
def transform_task(
    self,
    data_id: str,
    transformation_type: str,
    transformation_config: Dict[str, Any],
    input_data: Optional[Dict[str, Any]] = None,
    pipeline_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Transform data using specified transformer.

    Args:
        data_id: Identifier for input data
        transformation_type: Type of transformation ("sql", "code", or "config")
        transformation_config: Configuration for transformation
        input_data: Optional serialized input DataFrame (if passed from previous task)
        pipeline_id: Optional pipeline identifier
        task_id: Optional task identifier

    Returns:
        Dictionary with transformation results:
        {
            "status": "success",
            "data_id": "...",
            "row_count": 12345,
            "columns": [...],
            "pipeline_id": "..."
        }
    """
    try:
        logger.info(
            f"Transforming data (data_id={data_id}, type={transformation_type}, "
            f"pipeline_id={pipeline_id})"
        )

        # Deserialize input data
        if input_data:
            df = _deserialize_dataframe(input_data)
        else:
            # In production, fetch from storage using data_id
            raise ValueError(f"Data not found for data_id: {data_id}")

        # Apply transformation based on type
        if transformation_type == "sql":
            transformer = SQLTransformer()
            sql_query = transformation_config.get("sql_query")
            params = transformation_config.get("parameters")
            df = transformer.transform(
                df,
                sql_query,
                parameters=params,
                transformation_id=pipeline_id,
            )
        elif transformation_type == "code":
            transformer = CodeTransformer()
            func_path = transformation_config.get("function_path")
            func_kwargs = transformation_config.get("kwargs", {})
            df = transformer.transform(df, func_path, **func_kwargs)
        elif transformation_type == "config":
            transformer = ConfigTransformer()
            config = transformation_config.get("config")
            df = transformer.transform(df, config=config)
        else:
            raise ValueError(f"Unknown transformation type: {transformation_type}")

        # Create new data identifier
        data_hash = _create_data_hash(df)
        new_data_id = f"data_{data_hash}"

        result = {
            "status": "success",
            "data_id": new_data_id,
            "row_count": len(df),
            "columns": list(df.columns.tolist()),
            "pipeline_id": pipeline_id,
            "task_id": task_id,
            "data": _serialize_dataframe(df),
        }

        logger.info(
            f"Transformation complete: {len(df)} rows",
            extra={"data_id": new_data_id, "pipeline_id": pipeline_id},
        )

        return result

    except Exception as exc:
        logger.error(f"Transformation task failed: {exc}", exc_info=True)
        raise self.retry(exc=exc, countdown=settings.RETRY_BACKOFF_BASE ** self.request.retries)


@celery_app.task(
    base=PipelineTask,
    name="pipeline.workers.tasks.persist_task",
    bind=True,
    max_retries=None,
    default_retry_delay=settings.RETRY_DELAY,
)
def persist_task(
    self,
    data_id: str,
    table_name: str,
    schema: Optional[str] = None,
    strategy: str = "insert",
    dest_db_url: Optional[str] = None,
    input_data: Optional[Dict[str, Any]] = None,
    upsert_keys: Optional[List[str]] = None,
    pipeline_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Persist data to destination database.

    Args:
        data_id: Identifier for data to persist
        table_name: Target table name
        schema: Optional schema name
        strategy: Write strategy ("insert", "upsert", "replace", "append")
        dest_db_url: Destination database URL (uses settings if not provided)
        input_data: Optional serialized input DataFrame
        upsert_keys: Column names for upsert conflict resolution
        pipeline_id: Optional pipeline identifier
        task_id: Optional task identifier

    Returns:
        Dictionary with persistence results:
        {
            "status": "success",
            "rows_written": 12345,
            "table": "...",
            "pipeline_id": "..."
        }
    """
    try:
        db_url = dest_db_url or settings.DEST_DB_URL

        logger.info(
            f"Persisting data to {schema}.{table_name if schema else table_name} "
            f"(data_id={data_id}, strategy={strategy}, pipeline_id={pipeline_id})"
        )

        # Deserialize input data
        if input_data:
            df = _deserialize_dataframe(input_data)
        else:
            raise ValueError(f"Data not found for data_id: {data_id}")

        # Create writer
        writer = BatchWriter(
            database_url=db_url,
            batch_size=settings.BATCH_SIZE,
            max_retries=settings.MAX_RETRIES,
            retry_delay=settings.RETRY_DELAY,
        )

        # Map strategy string to enum
        strategy_enum = WriteStrategy[strategy.upper()] if hasattr(WriteStrategy, strategy.upper()) else WriteStrategy.INSERT

        # Write data
        rows_written = writer.write(
            df,
            table_name,
            schema=schema,
            strategy=strategy_enum,
            upsert_keys=upsert_keys,
        )

        result = {
            "status": "success",
            "rows_written": rows_written,
            "table": f"{schema}.{table_name}" if schema else table_name,
            "pipeline_id": pipeline_id,
            "task_id": task_id,
        }

        logger.info(
            f"Persistence complete: {rows_written} rows written",
            extra={"table": result["table"], "pipeline_id": pipeline_id},
        )

        return result

    except Exception as exc:
        logger.error(f"Persistence task failed: {exc}", exc_info=True)
        raise self.retry(exc=exc, countdown=settings.RETRY_BACKOFF_BASE ** self.request.retries)


@celery_app.task(
    base=PipelineTask,
    name="pipeline.workers.tasks.run_pipeline",
    bind=True,
)
def run_pipeline(
    self,
    pipeline_config: Dict[str, Any],
    pipeline_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run a complete pipeline: ingest → transform → persist.

    This task chains together ingest, transform, and persist tasks.

    Args:
        pipeline_config: Complete pipeline configuration:
            {
                "ingestion": {
                    "query": "SELECT * FROM ...",
                    "source_db_url": "...",
                    "parameters": {...}
                },
                "transformation": {
                    "type": "sql|code|config",
                    "config": {...}
                },
                "persistence": {
                    "table_name": "...",
                    "schema": "...",
                    "strategy": "insert|upsert|replace|append",
                    "upsert_keys": [...]
                }
            }
        pipeline_id: Optional pipeline identifier

    Returns:
        Dictionary with pipeline execution results:
        {
            "status": "success",
            "pipeline_id": "...",
            "ingestion": {...},
            "transformation": {...},
            "persistence": {...}
        }
    """
    try:
        pipeline_id = pipeline_id or f"pipeline_{self.request.id}"

        logger.info(f"Starting pipeline execution (pipeline_id={pipeline_id})")

        # Extract configurations
        ingestion_config = pipeline_config.get("ingestion", {})
        transformation_config = pipeline_config.get("transformation", {})
        persistence_config = pipeline_config.get("persistence", {})

        # Create task chain: ingest → transform → persist
        # Note: In Celery, we need to pass data between tasks
        # For production, consider using result backend or storage

        # Start ingestion
        ingest_result = ingest_task.apply_async(
            kwargs={
                "query": ingestion_config.get("query"),
                "source_db_url": ingestion_config.get("source_db_url"),
                "parameters": ingestion_config.get("parameters"),
                "batch_size": ingestion_config.get("batch_size"),
                "pipeline_id": pipeline_id,
            }
        )

        # Wait for ingestion and chain transformation
        ingestion_data = ingest_result.get()
        if ingestion_data.get("status") != "success":
            raise ValueError(f"Ingestion failed: {ingestion_data}")

        transform_result = transform_task.apply_async(
            kwargs={
                "data_id": ingestion_data.get("data_id"),
                "transformation_type": transformation_config.get("type"),
                "transformation_config": transformation_config.get("config", {}),
                "input_data": ingestion_data.get("data"),  # Pass data directly
                "pipeline_id": pipeline_id,
            }
        )

        # Wait for transformation and chain persistence
        transformation_data = transform_result.get()
        if transformation_data.get("status") != "success":
            raise ValueError(f"Transformation failed: {transformation_data}")

        persist_result = persist_task.apply_async(
            kwargs={
                "data_id": transformation_data.get("data_id"),
                "table_name": persistence_config.get("table_name"),
                "schema": persistence_config.get("schema"),
                "strategy": persistence_config.get("strategy", "insert"),
                "dest_db_url": persistence_config.get("dest_db_url"),
                "input_data": transformation_data.get("data"),  # Pass data directly
                "upsert_keys": persistence_config.get("upsert_keys"),
                "pipeline_id": pipeline_id,
            }
        )

        persistence_data = persist_result.get()
        if persistence_data.get("status") != "success":
            raise ValueError(f"Persistence failed: {persistence_data}")

        result = {
            "status": "success",
            "pipeline_id": pipeline_id,
            "ingestion": ingestion_data,
            "transformation": transformation_data,
            "persistence": persistence_data,
        }

        logger.info(f"Pipeline execution complete (pipeline_id={pipeline_id})")

        return result

    except Exception as exc:
        logger.error(f"Pipeline execution failed: {exc}", exc_info=True)
        return {
            "status": "error",
            "pipeline_id": pipeline_id,
            "error": str(exc),
        }


def _serialize_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """Serialize DataFrame to JSON-serializable format."""
    # Use pickle for now; in production, consider parquet or other efficient formats
    import base64

    pickled = pickle.dumps(df)
    encoded = base64.b64encode(pickled).decode("utf-8")
    return {
        "format": "pickle",
        "data": encoded,
        "shape": df.shape,
        "columns": df.columns.tolist(),
    }


def _deserialize_dataframe(data: Dict[str, Any]) -> pd.DataFrame:
    """Deserialize DataFrame from JSON-serializable format."""
    import base64

    if data.get("format") == "pickle":
        encoded = data["data"]
        pickled = base64.b64decode(encoded.encode("utf-8"))
        return pickle.loads(pickled)
    else:
        raise ValueError(f"Unknown data format: {data.get('format')}")


def _create_data_hash(df: pd.DataFrame) -> str:
    """Create a hash identifier for DataFrame."""
    # Create hash from column names and sample of data
    sample = df.head(100) if len(df) > 100 else df
    data_str = json.dumps(
        {
            "columns": df.columns.tolist(),
            "dtypes": {str(k): str(v) for k, v in df.dtypes.items()},
            "sample": sample.to_dict("records"),
        },
        default=str,
    )
    return hashlib.sha256(data_str.encode()).hexdigest()[:16]

