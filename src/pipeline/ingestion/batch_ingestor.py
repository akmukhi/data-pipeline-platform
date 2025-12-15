"""Batch data ingestion from databases with chunked reading and error handling."""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from pipeline.ingestion.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)


class BatchIngestor:
    """Batch data ingestion from databases with connection pooling and chunked reading."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        connection_manager: Optional[ConnectionManager] = None,
        engine: Optional[Engine] = None,
        batch_size: int = 10000,
        chunk_size: int = 10000,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize batch ingestor.

        Args:
            database_url: Database connection URL (used if connection_manager is not provided)
            connection_manager: ConnectionManager instance (takes precedence over database_url)
            engine: SQLAlchemy engine (takes precedence over connection_manager)
            batch_size: Number of rows to fetch per batch
            chunk_size: Number of rows to process in memory at once
            max_retries: Maximum number of retry attempts on failure
            retry_delay: Delay in seconds between retries
        """
        if engine is not None:
            self.engine = engine
            self.connection_manager = None
            self._owns_connection = False
        elif connection_manager is not None:
            self.connection_manager = connection_manager
            self.engine = connection_manager.get_engine()
            self._owns_connection = False
        elif database_url is not None:
            self.connection_manager = ConnectionManager(database_url)
            self.engine = self.connection_manager.get_engine()
            self._owns_connection = True
        else:
            raise ValueError(
                "Either database_url, connection_manager, or engine must be provided"
            )

        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        logger.info(
            f"Initialized BatchIngestor with batch_size={batch_size}, chunk_size={chunk_size}"
        )

    def ingest(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        stream: bool = True,
        index_col: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        Ingest data from database query.

        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            stream: If True, use streaming/chunked reading (recommended for large datasets)
            index_col: Column(s) to use as index

        Returns:
            DataFrame containing the query results

        Example:
            ingestor = BatchIngestor(database_url="postgresql://...")
            df = ingestor.ingest(
                "SELECT * FROM users WHERE created_at > :start_date",
                parameters={"start_date": "2024-01-01"}
            )
        """
        logger.info(f"Starting data ingestion with query: {query[:100]}...")

        if stream and self.batch_size > 0:
            # Use chunked reading for large datasets
            return self._ingest_streaming(query, parameters, index_col)
        else:
            # Load all data at once
            return self._ingest_all(query, parameters, index_col)

    def ingest_batches(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        index_col: Optional[Union[str, List[str]]] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Ingest data in batches as an iterator (generator).

        Args:
            query: SQL query to execute
            parameters: Optional query parameters
            index_col: Column(s) to use as index

        Yields:
            DataFrame batches

        Example:
            ingestor = BatchIngestor(database_url="postgresql://...")
            for batch in ingestor.ingest_batches("SELECT * FROM large_table"):
                # Process each batch
                transformed = transform(batch)
                writer.write(transformed, "output_table")
        """
        logger.info(f"Starting batch ingestion with query: {query[:100]}...")

        offset = 0
        total_rows = 0

        while True:
            # Add LIMIT and OFFSET to query for pagination
            # Note: This assumes the query doesn't already have LIMIT/OFFSET
            paginated_query = self._add_pagination(query, offset, self.batch_size)

            try:
                batch_df = self._execute_query(paginated_query, parameters, index_col)

                if batch_df.empty:
                    logger.debug(f"No more rows to fetch (offset={offset})")
                    break

                rows_in_batch = len(batch_df)
                total_rows += rows_in_batch
                logger.debug(
                    f"Fetched batch: {rows_in_batch} rows (total: {total_rows}, offset: {offset})"
                )

                yield batch_df

                # If we got fewer rows than batch_size, we've reached the end
                if rows_in_batch < self.batch_size:
                    logger.debug("Reached end of data (last batch smaller than batch_size)")
                    break

                offset += self.batch_size

            except Exception as e:
                logger.error(f"Error fetching batch at offset {offset}: {e}", exc_info=True)
                raise

        logger.info(f"Batch ingestion complete: {total_rows} total rows")

    def _ingest_streaming(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]],
        index_col: Optional[Union[str, List[str]]],
    ) -> pd.DataFrame:
        """Ingest data using streaming/chunked reading."""
        all_chunks = []

        try:
            if self.connection_manager:
                with self.connection_manager.get_connection() as conn:
                    result = self._execute_query_with_connection(
                        conn, query, parameters, index_col, stream=True
                    )
                    for chunk in result:
                        all_chunks.append(chunk)
            else:
                with self.engine.connect() as conn:
                    result = self._execute_query_with_connection(
                        conn, query, parameters, index_col, stream=True
                    )
                    for chunk in result:
                        all_chunks.append(chunk)

            if all_chunks:
                df = pd.concat(all_chunks, ignore_index=True)
                logger.info(f"Ingested {len(df)} rows using streaming")
                return df
            else:
                logger.warning("No data returned from query")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error during streaming ingestion: {e}", exc_info=True)
            raise

    def _ingest_all(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]],
        index_col: Optional[Union[str, List[str]]],
    ) -> pd.DataFrame:
        """Ingest all data at once."""
        try:
            if self.connection_manager:
                with self.connection_manager.get_connection() as conn:
                    df = self._execute_query_with_connection(
                        conn, query, parameters, index_col, stream=False
                    )
            else:
                with self.engine.connect() as conn:
                    df = self._execute_query_with_connection(
                        conn, query, parameters, index_col, stream=False
                    )

            logger.info(f"Ingested {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Error during ingestion: {e}", exc_info=True)
            raise

    def _execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]],
        index_col: Optional[Union[str, List[str]]],
    ) -> pd.DataFrame:
        """Execute query with retry logic."""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                if self.connection_manager:
                    with self.connection_manager.get_connection() as conn:
                        return self._execute_query_with_connection(
                            conn, query, parameters, index_col, stream=False
                        )
                else:
                    with self.engine.connect() as conn:
                        return self._execute_query_with_connection(
                            conn, query, parameters, index_col, stream=False
                        )
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"Query execution failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Query execution failed after {self.max_retries} attempts: {e}")
                    raise

        if last_exception:
            raise last_exception

    def _execute_query_with_connection(
        self,
        conn,
        query: str,
        parameters: Optional[Dict[str, Any]],
        index_col: Optional[Union[str, List[str]]],
        stream: bool = False,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Execute query using a connection."""
        if stream:
            # Return iterator of chunks
            return self._stream_query_results(conn, query, parameters, index_col)
        else:
            # Return single DataFrame
            if parameters:
                result = conn.execute(text(query), parameters)
            else:
                result = conn.execute(text(query))

            # Convert to DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            if index_col and index_col in df.columns:
                df.set_index(index_col, inplace=True)

            return df

    def _stream_query_results(
        self,
        conn,
        query: str,
        parameters: Optional[Dict[str, Any]],
        index_col: Optional[Union[str, List[str]]],
    ) -> Iterator[pd.DataFrame]:
        """Stream query results in chunks."""
        if parameters:
            result = conn.execute(text(query), parameters)
        else:
            result = conn.execute(text(query))

        chunk = []
        columns = result.keys()

        for row in result:
            chunk.append(row)
            if len(chunk) >= self.chunk_size:
                df = pd.DataFrame(chunk, columns=columns)
                if index_col and index_col in df.columns:
                    df.set_index(index_col, inplace=True)
                yield df
                chunk = []

        # Yield remaining rows
        if chunk:
            df = pd.DataFrame(chunk, columns=columns)
            if index_col and index_col in df.columns:
                df.set_index(index_col, inplace=True)
            yield df

    def _add_pagination(self, query: str, offset: int, limit: int) -> str:
        """
        Add LIMIT and OFFSET to query for pagination.

        This is a simple implementation. For production, consider using a proper
        SQL parser to handle complex queries with existing LIMIT/OFFSET clauses.
        """
        query_upper = query.upper().strip()

        # Remove trailing semicolon if present
        query = query.rstrip(";").strip()

        # Check if query already has LIMIT/OFFSET
        has_limit = "LIMIT" in query_upper
        has_offset = "OFFSET" in query_upper

        if has_limit or has_offset:
            # For now, log a warning and don't modify
            # In production, you might want to parse and replace
            logger.warning(
                "Query already contains LIMIT/OFFSET. Pagination may not work as expected."
            )
            return query

        # Add pagination
        # Try to detect if it's a SELECT query
        if query_upper.startswith("SELECT"):
            # Simple approach: append LIMIT and OFFSET
            return f"{query} LIMIT {limit} OFFSET {offset}"
        else:
            # For non-SELECT queries, wrap in a subquery
            return f"SELECT * FROM ({query}) AS subquery LIMIT {limit} OFFSET {offset}"

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if self.connection_manager:
                return self.connection_manager.health_check()
            else:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about a database table.

        Args:
            table_name: Name of the table
            schema: Optional schema name

        Returns:
            Dictionary with table information (columns, row count, etc.)
        """
        try:
            if self.connection_manager:
                with self.connection_manager.get_connection() as conn:
                    return self._get_table_info_with_connection(conn, table_name, schema)
            else:
                with self.engine.connect() as conn:
                    return self._get_table_info_with_connection(conn, table_name, schema)
        except Exception as e:
            logger.error(f"Error getting table info: {e}", exc_info=True)
            raise

    def _get_table_info_with_connection(
        self, conn, table_name: str, schema: Optional[str]
    ) -> Dict[str, Any]:
        """Get table info using a connection."""
        # Get column information
        if schema:
            query = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table_name
                ORDER BY ordinal_position
            """)
            params = {"schema": schema, "table_name": table_name}
        else:
            query = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = :table_name
                ORDER BY ordinal_position
            """)
            params = {"table_name": table_name}

        result = conn.execute(query, params)
        columns = [dict(row._mapping) for row in result]

        # Get row count
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        count_query = text(f"SELECT COUNT(*) as count FROM {full_table_name}")
        count_result = conn.execute(count_query)
        row_count = count_result.scalar()

        return {
            "table_name": table_name,
            "schema": schema,
            "columns": columns,
            "row_count": row_count,
        }

    def close(self) -> None:
        """Close connections if we own them."""
        if self._owns_connection and self.connection_manager:
            self.connection_manager.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

