"""Unit tests for code transformer."""

import pytest
import pandas as pd

from pipeline.transformation.code_transformer import CodeTransformer


@pytest.mark.unit
class TestCodeTransformer:
    """Test CodeTransformer."""

    def test_init(self):
        """Test initialization."""
        transformer = CodeTransformer()
        assert transformer.safe_mode is True

    def test_transform_with_function(self, sample_dataframe):
        """Test transformation with a function."""
        transformer = CodeTransformer()

        def add_column(df):
            df["new_col"] = df["age"] * 2
            return df

        result = transformer.transform(sample_dataframe, add_column)

        assert "new_col" in result.columns
        assert result.iloc[0]["new_col"] == 50  # 25 * 2

    def test_transform_with_lambda(self, sample_dataframe):
        """Test transformation with lambda."""
        transformer = CodeTransformer()

        result = transformer.transform(
            sample_dataframe, lambda df: df[df["age"] > 30]
        )

        assert len(result) < len(sample_dataframe)
        assert all(result["age"] > 30)

    def test_transform_with_class(self, sample_dataframe):
        """Test transformation with class."""
        transformer = CodeTransformer()

        class Multiplier:
            def __init__(self, factor=2):
                self.factor = factor

            def transform(self, df):
                df["multiplied"] = df["age"] * self.factor
                return df

        result = transformer.transform_with_class(
            sample_dataframe, Multiplier, factor=3
        )

        assert "multiplied" in result.columns
        assert result.iloc[0]["multiplied"] == 75  # 25 * 3

    def test_register_function_version(self, sample_dataframe):
        """Test registering function version."""
        transformer = CodeTransformer()

        def transform_v1(df):
            return df[["id", "name"]]

        transformer.register_function_version("test", 1, transform_v1)

        func = transformer.get_function_version("test", 1)
        result = transformer.transform(sample_dataframe, func)

        assert set(result.columns) == {"id", "name"}

    def test_get_transformation_history(self):
        """Test getting transformation history."""
        transformer = CodeTransformer()

        def func1(df):
            return df

        transformer.register_function_version("test", 1, func1)
        transformer.register_function_version("test", 2, func1)

        history = transformer.get_transformation_history("test")
        assert len(history) == 2

    def test_empty_dataframe(self):
        """Test transformation with empty DataFrame."""
        transformer = CodeTransformer()

        def transform(df):
            return df

        result = transformer.transform(pd.DataFrame(), transform)
        assert result.empty

    def test_invalid_return_type(self, sample_dataframe):
        """Test error when function doesn't return DataFrame."""
        transformer = CodeTransformer()

        def invalid_transform(df):
            return "not a dataframe"

        with pytest.raises(ValueError, match="must return a DataFrame"):
            transformer.transform(sample_dataframe, invalid_transform)

