"""Unit tests for SQL transformer."""

import pytest
import pandas as pd

from pipeline.transformation.sql_transformer import SQLTransformer


@pytest.mark.unit
class TestSQLTransformer:
    """Test SQLTransformer."""

    def test_init_with_url(self):
        """Test initialization with database URL."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")
        assert transformer.engine is not None

    def test_transform_basic(self, sample_dataframe):
        """Test basic SQL transformation."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        result = transformer.transform(
            sample_dataframe,
            "SELECT id, UPPER(name) as name_upper, age FROM input_data",
        )

        assert len(result) == len(sample_dataframe)
        assert "name_upper" in result.columns
        assert result.iloc[0]["name_upper"] == "ALICE"

    def test_transform_in_memory(self, sample_dataframe):
        """Test in-memory transformation."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        result = transformer.transform_in_memory(
            sample_dataframe,
            "SELECT id, name, age * 2 as age_doubled FROM input_data",
        )

        assert len(result) == len(sample_dataframe)
        assert "age_doubled" in result.columns
        assert result.iloc[0]["age_doubled"] == 50  # 25 * 2

    def test_transform_with_parameters(self, sample_dataframe):
        """Test transformation with parameters."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        result = transformer.transform(
            sample_dataframe,
            "SELECT * FROM input_data WHERE age > :min_age",
            parameters={"min_age": 30},
        )

        assert len(result) < len(sample_dataframe)
        assert all(result["age"] > 30)

    def test_register_query_version(self):
        """Test registering query version."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        transformer.register_query_version(
            "test_transform", 1, "SELECT * FROM input_data", "Initial version"
        )

        query = transformer.get_query_version("test_transform", 1)
        assert query == "SELECT * FROM input_data"

    def test_transform_with_version(self, sample_dataframe):
        """Test transformation with versioned query."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        transformer.register_query_version(
            "test_transform",
            1,
            "SELECT id, UPPER(name) as name_upper FROM input_data",
        )

        result = transformer.transform_with_version(
            sample_dataframe, "test_transform", version=1
        )

        assert "name_upper" in result.columns

    def test_get_transformation_history(self):
        """Test getting transformation history."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        transformer.register_query_version("test", 1, "SELECT * FROM input")
        transformer.register_query_version("test", 2, "SELECT id FROM input")

        history = transformer.get_transformation_history("test")
        assert len(history) == 2
        assert history[0]["version"] == 1
        assert history[1]["version"] == 2

    def test_empty_dataframe(self):
        """Test transformation with empty DataFrame."""
        transformer = SQLTransformer(database_url="sqlite:///:memory:")

        result = transformer.transform(
            pd.DataFrame(), "SELECT * FROM input_data"
        )

        assert result.empty

