"""Integration tests for complete pipeline flows."""

import pytest
import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.ingestion import BatchIngestor
from pipeline.persistence import BatchWriter, WriteStrategy
from pipeline.transformation import SQLTransformer


@pytest.mark.integration
@pytest.mark.requires_db
class TestPipelineFlow:
    """Test complete pipeline flows."""

    @pytest.fixture
    def test_db(self):
        """Create test database with data."""
        engine = create_engine("sqlite:///:memory:")

        with engine.connect() as conn:
            # Create source table
            conn.execute(
                text(
                    """
                    CREATE TABLE source_users (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        email TEXT,
                        age INTEGER,
                        created_at TIMESTAMP
                    )
                """
                )
            )

            # Insert test data
            conn.execute(
                text(
                    """
                    INSERT INTO source_users (id, name, email, age, created_at)
                    VALUES
                        (1, 'Alice', 'alice@example.com', 25, '2024-01-01'),
                        (2, 'Bob', 'bob@example.com', 30, '2024-01-02'),
                        (3, 'Charlie', 'charlie@example.com', 35, '2024-01-03')
                """
                )
            )

            # Create destination table
            conn.execute(
                text(
                    """
                    CREATE TABLE dest_users (
                        id INTEGER PRIMARY KEY,
                        name_upper TEXT,
                        email TEXT,
                        age_doubled INTEGER
                    )
                """
                )
            )

            conn.commit()

        return engine

    def test_complete_pipeline(self, test_db):
        """Test complete pipeline: ingest → transform → persist."""
        # Step 1: Ingest
        ingestor = BatchIngestor(engine=test_db)
        df = ingestor.ingest("SELECT * FROM source_users", stream=False)

        assert len(df) == 3
        assert "id" in df.columns

        # Step 2: Transform
        transformer = SQLTransformer(database_url="sqlite:///:memory:")
        transformed_df = transformer.transform(
            df,
            "SELECT id, UPPER(name) as name_upper, email, age * 2 as age_doubled FROM input_data",
        )

        assert "name_upper" in transformed_df.columns
        assert "age_doubled" in transformed_df.columns
        assert transformed_df.iloc[0]["name_upper"] == "ALICE"

        # Step 3: Persist
        writer = BatchWriter(engine=test_db)
        rows_written = writer.write(
            transformed_df[["id", "name_upper", "email", "age_doubled"]],
            "dest_users",
            strategy=WriteStrategy.INSERT,
        )

        assert rows_written == 3

        # Verify persisted data
        with test_db.connect() as conn:
            result = conn.execute(text("SELECT * FROM dest_users"))
            rows = result.fetchall()
            assert len(rows) == 3
            assert rows[0][1] == "ALICE"  # name_upper

    def test_pipeline_with_batch_processing(self, test_db):
        """Test pipeline with batch processing."""
        # Create larger dataset
        with test_db.connect() as conn:
            for i in range(1, 101):
                conn.execute(
                    text(
                        f"INSERT INTO source_users (id, name, email, age) VALUES ({i}, 'User_{i}', 'user{i}@example.com', {20 + i})"
                    )
                )
            conn.commit()

        # Ingest in batches
        ingestor = BatchIngestor(engine=test_db, batch_size=20)
        batches = list(ingestor.ingest_batches("SELECT * FROM source_users"))

        total_rows = sum(len(batch) for batch in batches)
        assert total_rows == 100

        # Transform and persist
        transformer = SQLTransformer(database_url="sqlite:///:memory:")
        writer = BatchWriter(engine=test_db, batch_size=20)

        for batch in batches:
            transformed = transformer.transform(
                batch, "SELECT id, UPPER(name) as name_upper FROM input_data"
            )
            writer.write(transformed[["id", "name_upper"]], "dest_users", strategy=WriteStrategy.APPEND)

        # Verify
        with test_db.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM dest_users"))
            assert result.scalar() == 100

