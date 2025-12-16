"""Unit tests for batch ingestor."""

import pytest
from sqlalchemy import create_engine, text

from pipeline.ingestion.batch_ingestor import BatchIngestor


@pytest.mark.unit
class TestBatchIngestor:
    """Test BatchIngestor."""

    def test_init_with_url(self):
        """Test initialization with database URL."""
        ingestor = BatchIngestor(database_url="sqlite:///:memory:")
        assert ingestor.batch_size == 10000
        assert ingestor.engine is not None

    def test_init_with_connection_manager(self):
        """Test initialization with connection manager."""
        from pipeline.ingestion.connection_manager import ConnectionManager

        cm = ConnectionManager("sqlite:///:memory:")
        ingestor = BatchIngestor(connection_manager=cm)
        assert ingestor.connection_manager == cm

    def test_ingest_from_query(self, in_memory_db):
        """Test ingesting data from a query."""
        ingestor = BatchIngestor(engine=in_memory_db, batch_size=100)
        df = ingestor.ingest("SELECT * FROM test_users", stream=False)

        assert len(df) == 3
        assert "id" in df.columns
        assert "name" in df.columns
        assert df.iloc[0]["name"] == "Alice"

    def test_ingest_batches(self, in_memory_db):
        """Test ingesting data in batches."""
        ingestor = BatchIngestor(engine=in_memory_db, batch_size=2)

        batches = list(ingestor.ingest_batches("SELECT * FROM test_users"))
        assert len(batches) == 2  # 3 rows / 2 batch_size = 2 batches
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1

    def test_ingest_with_parameters(self, in_memory_db):
        """Test ingesting with query parameters."""
        ingestor = BatchIngestor(engine=in_memory_db)

        # Add a parameterized query
        with in_memory_db.connect() as conn:
            conn.execute(
                text("CREATE TABLE test_filter (id INTEGER, value INTEGER)")
            )
            conn.execute(text("INSERT INTO test_filter VALUES (1, 10), (2, 20), (3, 30)"))
            conn.commit()

        df = ingestor.ingest(
            "SELECT * FROM test_filter WHERE value > :min_value",
            parameters={"min_value": 15},
        )

        assert len(df) == 2
        assert all(df["value"] > 15)

    def test_test_connection(self, in_memory_db):
        """Test connection test."""
        ingestor = BatchIngestor(engine=in_memory_db)
        assert ingestor.test_connection() is True

    def test_get_table_info(self, in_memory_db):
        """Test getting table information."""
        ingestor = BatchIngestor(engine=in_memory_db)
        info = ingestor.get_table_info("test_users")

        assert info["table_name"] == "test_users"
        assert info["row_count"] == 3
        assert len(info["columns"]) > 0

    def test_context_manager(self, in_memory_db):
        """Test using as context manager."""
        with BatchIngestor(engine=in_memory_db) as ingestor:
            assert ingestor.test_connection() is True

