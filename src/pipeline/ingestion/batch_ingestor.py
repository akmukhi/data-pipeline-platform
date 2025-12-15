"""Batch data ingestion from databases."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Result

from pipeline.config.settings import DatabaseSettings, PipelineSettings
from pipeline.ingestion.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class BatchIngestor:
    """Batch ingestor for reading data from databases in chunks."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        batch_size: int = 10000,
        max_batch_size: Optional[int] = None,
    ):
        """
        Initialize batch ingestor.

        Args:
            connection_manager: ConnectionManager instance for database access
            batch_size: Number of rows to fetch per batch
            max_batch_size: Maximum allowed batch size (for validation)
        """
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        self.max_batch_size = max_batch_size

    @classmethod
    def from_settings(
        cls,
        database_url: str,
        db_settings: Optional[DatabaseSettings] = None,
        pipeline_settings: Optional[PipelineSettings] = None,
        batch_size: Optional[int] = None,
    ) -> "BatchIngestor":
        """
        Create BatchIngestor from settings.

        Args:
            database_url: Database connection URL
            db_settings: Database settings object
            pipeline_settings: Pipeline settings object
            batch_size: Override default batch size

        Returns:
            BatchIngestor instance
        """
        if db_settings is None or pipeline_settings is None:
            from pipeline.config.settings import settings as app_settings

            db_settings = db_settings or app_settings.database
            pipeline_settings = pipeline_settings or app_settings.pipeline

        connection_manager = ConnectionManager.from_settings(database_url, db_settings)
        batch_size = batch_size or pipeline_settings.default_batch_size

        return cls(
            connection_manager=connection_manager,
            batch_size=batch_size,
            max_batch_size=pipeline_settings.max_batch_size,
        )

    def ingest_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        chunk_size: Optional[int] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Execute a query and yield results in batches.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters (for parameterized queries)
            chunk_size: Override default batch size for this operation

        Yields:
            DataFrame chunks containing query results

        Example:
            ingestor = BatchIngestor(connection_manager)
            for batch in ingestor.ingest_query("SELECT * FROM users WHERE status = :status", {"status": "active"}):
                process_batch(batch)
        """
        effective_batch_size = chunk_size or self.batch_size

        if self.max_batch_size and effective_batch_size > self.max_batch_size:
            raise ValueError(
                f"Batch size {effective_batch_size} exceeds maximum allowed {self.max_batch_size}"
            )

        logger.info(
            f"Starting batch ingestion with query: {query[:100]}... (batch_size={effective_batch_size})"
        )

        with self.connection_manager.get_connection() as conn:
            try:
                # Execute query with parameters
                if parameters:
                    result = conn.execute(text(query), parameters)
                else:
                    result = conn.execute(text(query))

                # Fetch results in batches
                total_rows = 0
                batch_num = 0

                while True:
                    rows = result.fetchmany(effective_batch_size)
                    if not rows:
                        break

                    # Convert rows to DataFrame
                    df = pd.DataFrame(rows)
                    total_rows += len(df)
                    batch_num += 1

                    logger.debug(f"Fetched batch {batch_num} with {len(df)} rows")

                    yield df

                logger.info(f"Completed batch ingestion: {total_rows} total rows in {batch_num} batches")

            except Exception as e:
                logger.error(f"Error during batch ingestion: {e}", exc_info=True)
                raise

    def ingest_table(
        self,
        table_name: str,
        schema: Optional[str] = None,
        where_clause: Optional[str] = None,
        order_by: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Ingest data from a table in batches.

        Args:
            table_name: Name of the table to read from
            schema: Optional schema name
            where_clause: Optional WHERE clause (without the WHERE keyword)
            order_by: Optional ORDER BY clause (without the ORDER BY keywords)
            chunk_size: Override default batch size for this operation

        Yields:
            DataFrame chunks containing table data

        Example:
            ingestor = BatchIngestor(connection_manager)
            for batch in ingestor.ingest_table("users", where_clause="status = 'active'", order_by="id"):
                process_batch(batch)
        """
        # Build query
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name

        query = f"SELECT * FROM {full_table_name}"

        if where_clause:
            query += f" WHERE {where_clause}"

        if order_by:
            query += f" ORDER BY {order_by}"

        return self.ingest_query(query, chunk_size=chunk_size)

    def ingest_with_offset(
        self,
        query: str,
        offset_column: str = "id",
        initial_offset: Optional[Any] = None,
        parameters: Optional[Dict[str, Any]] = None,
        chunk_size: Optional[int] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Ingest data using offset-based pagination (useful for large tables).

        This method expects the query to be a template that can accept :offset and :limit parameters.
        The query should include ORDER BY clause and can optionally include WHERE clause.

        Args:
            query: SQL query template with :offset and :limit placeholders.
                   Example: "SELECT * FROM users WHERE status = :status AND id > :offset ORDER BY id LIMIT :limit"
            offset_column: Column name to use for extracting the next offset value from results
            initial_offset: Starting offset value (None means start from beginning)
            parameters: Optional query parameters (will be merged with offset/limit params)
            chunk_size: Override default batch size for this operation

        Yields:
            DataFrame chunks containing query results

        Example:
            query = "SELECT * FROM users WHERE status = :status AND id > :offset ORDER BY id LIMIT :limit"
            params = {"status": "active"}
            for batch in ingestor.ingest_with_offset(query, offset_column="id", initial_offset=0, parameters=params):
                process_batch(batch)
        """
        effective_batch_size = chunk_size or self.batch_size
        current_offset = initial_offset

        logger.info(
            f"Starting offset-based ingestion (offset_column={offset_column}, "
            f"initial_offset={current_offset}, batch_size={effective_batch_size})"
        )

        with self.connection_manager.get_connection() as conn:
            while True:
                # Build parameters for this batch
                batch_params = parameters.copy() if parameters else {}
                batch_params["offset"] = current_offset
                batch_params["limit"] = effective_batch_size

                try:
                    # Execute query with current offset
                    result = conn.execute(text(query), batch_params)
                    rows = result.fetchall()

                    if not rows:
                        break

                    # Convert to DataFrame
                    batch = pd.DataFrame(rows)
                    rows_fetched = len(batch)

                    if rows_fetched == 0:
                        break

                    logger.debug(f"Fetched batch with {rows_fetched} rows (offset={current_offset})")

                    # Update offset to last value in batch
                    if offset_column in batch.columns:
                        current_offset = batch[offset_column].max()
                    else:
                        logger.warning(
                            f"Offset column '{offset_column}' not found in results. "
                            "Cannot continue pagination."
                        )
                        yield batch
                        break

                    yield batch

                    # If we got fewer rows than requested, we've reached the end
                    if rows_fetched < effective_batch_size:
                        break

                except Exception as e:
                    logger.error(f"Error during offset-based ingestion at offset {current_offset}: {e}", exc_info=True)
                    raise

        logger.info("Completed offset-based ingestion")

    def get_row_count(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Get the total row count for a query (without fetching all data).

        Args:
            query: SQL query
            parameters: Optional query parameters

        Returns:
            Total number of rows
        """
        count_query = f"SELECT COUNT(*) as count FROM ({query}) as subquery"

        with self.connection_manager.get_connection() as conn:
            try:
                if parameters:
                    result = conn.execute(text(count_query), parameters)
                else:
                    result = conn.execute(text(count_query))

                row = result.fetchone()
                return row[0] if row else 0

            except Exception as e:
                logger.error(f"Error getting row count: {e}", exc_info=True)
                raise

    def test_connection(self) -> bool:
        """
        Test the database connection.

        Returns:
            True if connection is successful, False otherwise
        """
        return self.connection_manager.health_check()

