"""Efficient PostgreSQL batch writer with transaction management."""

import io
import logging
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Optional import for schema integration
try:
    from pipeline.transformation.schema_validator import SchemaValidator, SchemaDefinition
except ImportError:
    SchemaValidator = None  # type: ignore
    SchemaDefinition = None  # type: ignore


class WriteStrategy(Enum):
    """Write strategies for batch operations."""

    INSERT = "insert"  # Simple insert (may fail on duplicates)
    UPSERT = "upsert"  # Insert or update on conflict
    REPLACE = "replace"  # Replace entire table
    APPEND = "append"  # Append to existing table


class BatchWriter:
    """Efficient PostgreSQL batch writer with transaction management."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        engine: Optional[Engine] = None,
        batch_size: int = 10000,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        schema_validator: Optional["SchemaValidator"] = None,
        use_copy: bool = True,
    ):
        """
        Initialize batch writer.

        Args:
            database_url: PostgreSQL connection URL (postgresql://user:pass@host/db)
            engine: SQLAlchemy engine (takes precedence over database_url)
            batch_size: Number of rows to write per batch
            max_retries: Maximum number of retry attempts on failure
            retry_delay: Delay in seconds between retries
            schema_validator: Optional SchemaValidator for schema validation
            use_copy: If True, use PostgreSQL COPY for faster writes (default: True)
        """
        if engine is not None:
            self.engine = engine
        elif database_url is not None:
            # Optimize engine for batch writes
            self.engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                future=True,
            )
        else:
            raise ValueError("Either database_url or engine must be provided")

        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.schema_validator = schema_validator
        self.use_copy = use_copy
        self._write_stats: Dict[str, Any] = {}

    def write(
        self,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        strategy: WriteStrategy = WriteStrategy.INSERT,
        if_exists: str = "append",
        index: bool = False,
        chunksize: Optional[int] = None,
        upsert_keys: Optional[List[str]] = None,
        validate_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
    ) -> int:
        """
        Write DataFrame to PostgreSQL table with transaction management.

        Args:
            data: DataFrame to write
            table_name: Target table name
            schema: Optional database schema name
            strategy: Write strategy (INSERT, UPSERT, REPLACE, APPEND)
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index
            chunksize: Chunk size for writing (uses batch_size if None)
            upsert_keys: Column names for UPSERT conflict resolution
            validate_schema: Optional schema to validate data against

        Returns:
            Number of rows written

        Raises:
            ValueError: If validation fails or invalid parameters
            SQLAlchemyError: If database operation fails
        """
        if data.empty:
            logger.warning("DataFrame is empty, nothing to write")
            return 0

        # Validate schema if provided
        if validate_schema and self.schema_validator:
            logger.debug("Validating data against schema")
            data = self.schema_validator.validate(
                data, validate_schema, strict=False, allow_extra_columns=True
            )

        chunksize = chunksize or self.batch_size
        total_rows = len(data)
        rows_written = 0

        logger.info(
            f"Writing {total_rows} rows to {schema}.{table_name if schema else table_name} "
            f"using {strategy.value} strategy"
        )

        start_time = time.time()

        try:
            with self._transaction() as conn:
                # Create table if needed
                if strategy == WriteStrategy.REPLACE or if_exists == "replace":
                    self._create_table_if_needed(
                        conn, data, table_name, schema, if_exists=if_exists
                    )

                # Write data based on strategy
                if strategy == WriteStrategy.UPSERT:
                    rows_written = self._write_upsert(
                        conn, data, table_name, schema, chunksize, upsert_keys
                    )
                elif strategy == WriteStrategy.INSERT and self.use_copy:
                    rows_written = self._write_copy(
                        conn, data, table_name, schema, chunksize
                    )
                elif strategy == WriteStrategy.REPLACE:
                    rows_written = self._write_replace(
                        conn, data, table_name, schema, chunksize, index
                    )
                else:
                    rows_written = self._write_append(
                        conn, data, table_name, schema, chunksize, index, if_exists
                    )

                # Commit transaction
                conn.commit()
                logger.info(f"Successfully wrote {rows_written} rows in {time.time() - start_time:.2f}s")

        except Exception as e:
            logger.error(f"Error writing to {table_name}: {e}", exc_info=True)
            raise

        # Track statistics
        self._write_stats[table_name] = {
            "rows_written": rows_written,
            "total_rows": total_rows,
            "duration": time.time() - start_time,
            "strategy": strategy.value,
            "timestamp": time.time(),
        }

        return rows_written

    def _write_copy(
        self,
        conn: Any,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str],
        chunksize: int,
    ) -> int:
        """Write using PostgreSQL COPY (fastest method)."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        rows_written = 0

        # Get raw connection for COPY
        try:
            # SQLAlchemy 2.0 style
            raw_conn = conn.connection.dbapi_connection
        except AttributeError:
            try:
                # SQLAlchemy 1.x style
                raw_conn = conn.connection
            except AttributeError:
                # Fallback: use pandas to_sql with method='multi'
                logger.warning(
                    "Could not get raw connection for COPY, falling back to bulk insert"
                )
                return self._write_append(conn, data, table_name, schema, chunksize, False, "append")

        # Process in chunks
        for i in range(0, len(data), chunksize):
            chunk = data.iloc[i : i + chunksize]
            buffer = io.StringIO()
            chunk.to_csv(buffer, index=False, header=False, na_rep="\\N")
            buffer.seek(0)

            # Get column names (escape if needed)
            columns = ", ".join([f'"{col}"' for col in chunk.columns])

            # Use COPY FROM
            cursor = raw_conn.cursor()
            try:
                copy_sql = f"COPY {full_table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
                cursor.copy_expert(copy_sql, buffer)
                rows_written += len(chunk)
                logger.debug(f"Wrote chunk: {len(chunk)} rows (total: {rows_written})")
            finally:
                cursor.close()

        return rows_written

    def _write_upsert(
        self,
        conn: Any,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str],
        chunksize: int,
        upsert_keys: Optional[List[str]],
    ) -> int:
        """Write using UPSERT (INSERT ... ON CONFLICT)."""
        if not upsert_keys:
            raise ValueError("upsert_keys required for UPSERT strategy")

        full_table_name = f"{schema}.{table_name}" if schema else table_name
        rows_written = 0

        # Get table metadata
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            schema=schema,
            autoload_with=self.engine,
        )

        # Process in chunks
        for i in range(0, len(data), chunksize):
            chunk = data.iloc[i : i + chunksize]
            records = chunk.to_dict("records")

            # Build upsert statement
            stmt = insert(table).values(records)
            update_dict = {
                col.name: stmt.excluded[col.name]
                for col in table.columns
                if col.name not in upsert_keys
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=upsert_keys, set_=update_dict
            )

            result = conn.execute(stmt)
            rows_written += result.rowcount or len(chunk)
            logger.debug(f"Upserted chunk: {len(chunk)} rows (total: {rows_written})")

        return rows_written

    def _write_replace(
        self,
        conn: Any,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str],
        chunksize: int,
        index: bool,
    ) -> int:
        """Replace table contents."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name

        # Truncate table
        conn.execute(text(f"TRUNCATE TABLE {full_table_name}"))

        # Write using pandas to_sql (handles schema creation)
        rows_written = data.to_sql(
            table_name,
            conn,
            schema=schema,
            if_exists="append",
            index=index,
            method="multi",
            chunksize=chunksize,
        )

        return rows_written

    def _write_append(
        self,
        conn: Any,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str],
        chunksize: int,
        index: bool,
        if_exists: str,
    ) -> int:
        """Append data to table."""
        rows_written = data.to_sql(
            table_name,
            conn,
            schema=schema,
            if_exists=if_exists,
            index=index,
            method="multi",
            chunksize=chunksize,
        )

        return rows_written

    def _create_table_if_needed(
        self,
        conn: Any,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str],
        if_exists: str = "append",
    ) -> None:
        """Create table if it doesn't exist."""
        if schema:
            # Create schema if it doesn't exist
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

        # Check if table exists (using parameterized query for safety)
        if schema:
            check_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_name = :table_name
                )
            """)
            result = conn.execute(check_query, {"schema": schema, "table_name": table_name})
        else:
            check_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = current_schema()
                    AND table_name = :table_name
                )
            """)
            result = conn.execute(check_query, {"table_name": table_name})
        
        table_exists = result.scalar()

        if not table_exists or if_exists == "replace":
            # Create table using pandas
            temp_df = data.head(0)  # Empty DataFrame with same structure
            temp_df.to_sql(
                table_name,
                conn,
                schema=schema,
                if_exists="replace",
                index=False,
            )
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            logger.debug(f"Created table {full_table_name}")

    @contextmanager
    def _transaction(self):
        """Context manager for database transactions with retry logic."""
        conn = None
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                conn = self.engine.connect()
                trans = conn.begin()
                try:
                    yield conn
                    trans.commit()
                    return
                except Exception as e:
                    trans.rollback()
                    raise
            except SQLAlchemyError as e:
                last_exception = e
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None

                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"Transaction failed (attempt {attempt + 1}/{self.max_retries}): {e}. "
                        f"Retrying in {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Transaction failed after {self.max_retries} attempts: {e}"
                    )
                    raise
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                        conn.close()
                    except Exception:
                        pass
                raise

        if last_exception:
            raise last_exception

    @contextmanager
    def savepoint(self, name: str = "sp"):
        """
        Create a savepoint for nested transactions.

        Args:
            name: Savepoint name

        Example:
            with writer.savepoint("checkpoint1"):
                writer.write(data1, "table1")
                with writer.savepoint("checkpoint2"):
                    writer.write(data2, "table2")
                    # Can rollback to checkpoint2 without affecting checkpoint1
        """
        conn = self.engine.connect()
        trans = conn.begin()
        try:
            conn.execute(text(f"SAVEPOINT {name}"))
            yield conn
            conn.execute(text(f"RELEASE SAVEPOINT {name}"))
            trans.commit()
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {name}"))
            trans.rollback()
            raise
        finally:
            conn.close()

    def write_batch(
        self,
        data: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        strategy: WriteStrategy = WriteStrategy.INSERT,
        **kwargs: Any,
    ) -> int:
        """
        Write data in batches with automatic chunking.

        This is a convenience method that handles large datasets by automatically
        chunking them into smaller batches.

        Args:
            data: DataFrame to write
            table_name: Target table name
            schema: Optional database schema name
            strategy: Write strategy
            **kwargs: Additional arguments passed to write()

        Returns:
            Total number of rows written
        """
        if data.empty:
            return 0

        total_rows = len(data)
        chunksize = kwargs.pop("chunksize", self.batch_size)
        total_written = 0

        logger.info(f"Writing {total_rows} rows in batches of {chunksize}")

        for i in range(0, total_rows, chunksize):
            chunk = data.iloc[i : i + chunksize]
            rows_written = self.write(
                chunk,
                table_name,
                schema=schema,
                strategy=strategy,
                chunksize=chunksize,
                **kwargs,
            )
            total_written += rows_written
            logger.debug(
                f"Batch {i // chunksize + 1}: wrote {rows_written} rows "
                f"({total_written}/{total_rows} total)"
            )

        return total_written

    def get_write_stats(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get write statistics.

        Args:
            table_name: Optional table name to get stats for specific table

        Returns:
            Dictionary with write statistics
        """
        if table_name:
            return self._write_stats.get(table_name, {})
        return self._write_stats.copy()

    def clear_stats(self) -> None:
        """Clear write statistics."""
        self._write_stats.clear()

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

