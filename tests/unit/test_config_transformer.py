"""Unit tests for config transformer."""

import pytest
import pandas as pd

from pipeline.transformation.config_transformer import ConfigTransformer


@pytest.mark.unit
class TestConfigTransformer:
    """Test ConfigTransformer."""

    def test_init_with_dict(self):
        """Test initialization with dict config."""
        config = {"select": ["id", "name"]}
        transformer = ConfigTransformer(config=config)
        assert transformer.config == config

    def test_init_with_file(self, temp_config_file):
        """Test initialization with config file."""
        transformer = ConfigTransformer(config=str(temp_config_file))
        assert transformer.config is not None

    def test_load_config_dict(self):
        """Test loading config from dict."""
        transformer = ConfigTransformer()
        config = {"select": ["id", "name"]}
        transformer.load_config(config)
        assert transformer.config == config

    def test_load_config_file(self, temp_config_file):
        """Test loading config from file."""
        transformer = ConfigTransformer()
        transformer.load_config(str(temp_config_file))
        assert transformer.config is not None

    def test_transform_select(self, sample_dataframe):
        """Test select transformation."""
        config = {"select": ["id", "name", "email"]}
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert set(result.columns) == {"id", "name", "email"}
        assert len(result) == len(sample_dataframe)

    def test_transform_rename(self, sample_dataframe):
        """Test rename transformation."""
        config = {"rename": {"name": "full_name", "email": "email_address"}}
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert "full_name" in result.columns
        assert "email_address" in result.columns
        assert "name" not in result.columns

    def test_transform_filter(self, sample_dataframe):
        """Test filter transformation."""
        config = {"filter": {"age": 30}}
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert len(result) == 1
        assert result.iloc[0]["age"] == 30

    def test_transform_filter_operators(self, sample_dataframe):
        """Test filter with operators."""
        config = {"filter": {"age": {">": 30}}}
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert all(result["age"] > 30)

    def test_transform_add_columns(self, sample_dataframe):
        """Test add columns transformation."""
        config = {"add_columns": {"age_plus_10": "age + 10"}}
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert "age_plus_10" in result.columns
        assert result.iloc[0]["age_plus_10"] == 35  # 25 + 10

    def test_transform_drop_columns(self, sample_dataframe):
        """Test drop columns transformation."""
        config = {"drop_columns": ["email", "created_at"]}
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert "email" not in result.columns
        assert "created_at" not in result.columns

    def test_transform_aggregations(self, sample_dataframe):
        """Test aggregations transformation."""
        config = {
            "aggregations": {
                "group_by": ["age"],
                "aggregate": {"count": "COUNT(*)"},
            }
        }
        transformer = ConfigTransformer(config=config)

        result = transformer.transform(sample_dataframe)

        assert "age" in result.columns
        assert len(result) <= len(sample_dataframe)

    def test_register_config_version(self):
        """Test registering config version."""
        transformer = ConfigTransformer()

        config = {"select": ["id"]}
        transformer.register_config_version("test", 1, config)

        stored_config = transformer.get_config_version("test", 1)
        assert stored_config == config

    def test_get_transformation_history(self):
        """Test getting transformation history."""
        transformer = ConfigTransformer()

        transformer.register_config_version("test", 1, {"select": ["id"]})
        transformer.register_config_version("test", 2, {"select": ["name"]})

        history = transformer.get_transformation_history("test")
        assert len(history) == 2

    def test_empty_dataframe(self):
        """Test transformation with empty DataFrame."""
        transformer = ConfigTransformer(config={"select": ["id"]})

        result = transformer.transform(pd.DataFrame())
        assert result.empty

