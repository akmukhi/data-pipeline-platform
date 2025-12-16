"""Unit tests for batch writer."""

import pytest
import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.persistence.batch_writer import BatchWriter, WriteStrategy


@pytest.mark.unit
class TestBatchWriter:
    """Test BatchWriter."""

    @pytest.fixture
    def test_db(self):
        """Create test database."""
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE test_table (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        value INTEGER
                    )
                """
                )
            )
            conn.commit()
        return engine

    def test_init_with_url(self):
        """Test initialization with database URL."""
        writer = BatchWriter(database_url="sqlite:///:memory:")
        assert writer.batch_size == 10000
        assert writer.engine is not None

    def test_init_with_engine(self, test_db):
        """Test initialization with engine."""
        writer = BatchWriter(engine=test_db)
        assert writer.engine == test_db

    def test_write_insert(self, test_db, sample_dataframe):
        """Test write with INSERT strategy."""
        writer = BatchWriter(engine=test_db, batch_size=100)

        # Rename columns to match table
        df = sample_dataframe[["id", "name"]].copy()
        df["value"] = df["id"] * 10

        rows_written = writer.write(
            df, "test_table", strategy=WriteStrategy.INSERT
        )

        assert rows_written == len(df)

        # Verify data
        with test_db.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_table"))
            assert result.scalar() == len(df)

    def test_write_append(self, test_db, sample_dataframe):
        """Test write with APPEND strategy."""
        writer = BatchWriter(engine=test_db)

        df = sample_dataframe[["id", "name"]].copy()
        df["value"] = df["id"] * 10

        # Write twice
        writer.write(df, "test_table", strategy=WriteStrategy.APPEND)
        writer.write(df, "test_table", strategy=WriteStrategy.APPEND)

        with test_db.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_table"))
            assert result.scalar() == len(df) * 2

    def test_write_replace(self, test_db, sample_dataframe):
        """Test write with REPLACE strategy."""
        writer = BatchWriter(engine=test_db)

        df = sample_dataframe[["id", "name"]].copy()
        df["value"] = df["id"] * 10

        # Write initial data
        writer.write(df, "test_table", strategy=WriteStrategy.INSERT)

        # Replace with new data
        df2 = df.head(2)
        writer.write(df2, "test_table", strategy=WriteStrategy.REPLACE)

        with test_db.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM test_table"))
            assert result.scalar() == len(df2)

    def test_write_batch(self, test_db, sample_dataframe_large):
        """Test write_batch method."""
        writer = BatchWriter(engine=test_db, batch_size=100)

        # Create table first
        with test_db.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE test_batch (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        value INTEGER
                    )
                """
                )
            )
            conn.commit()

        df = sample_dataframe_large.copy()
        df["value"] = df["id"] * 10

        rows_written = writer.write_batch(df, "test_batch")

        assert rows_written == len(df)

    def test_get_write_stats(self, test_db, sample_dataframe):
        """Test getting write statistics."""
        writer = BatchWriter(engine=test_db)

        df = sample_dataframe[["id", "name"]].copy()
        df["value"] = df["id"] * 10

        writer.write(df, "test_table", strategy=WriteStrategy.INSERT)

        stats = writer.get_write_stats("test_table")
        assert "rows_written" in stats
        assert stats["rows_written"] == len(df)

    def test_test_connection(self, test_db):
        """Test connection test."""
        writer = BatchWriter(engine=test_db)
        assert writer.test_connection() is True

    def test_empty_dataframe(self, test_db):
        """Test writing empty DataFrame."""
        writer = BatchWriter(engine=test_db)

        rows_written = writer.write(
            pd.DataFrame(), "test_table", strategy=WriteStrategy.INSERT
        )

        assert rows_written == 0

