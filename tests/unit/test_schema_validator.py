"""Unit tests for schema validator."""

import pytest
import pandas as pd

from pipeline.transformation.schema_validator import (
    ColumnSchema,
    SchemaDefinition,
    SchemaValidator,
)


@pytest.mark.unit
class TestColumnSchema:
    """Test ColumnSchema."""

    def test_create_column_schema(self):
        """Test creating column schema."""
        col = ColumnSchema(
            name="id", dtype="int64", nullable=False, description="User ID"
        )
        assert col.name == "id"
        assert col.dtype == "int64"
        assert col.nullable is False

    def test_validate_dtype_mapping(self):
        """Test dtype validation and mapping."""
        col = ColumnSchema(name="name", dtype="string")
        assert col.dtype == "object"  # string maps to object

        col = ColumnSchema(name="age", dtype="int")
        assert col.dtype == "int64"


@pytest.mark.unit
class TestSchemaDefinition:
    """Test SchemaDefinition."""

    def test_create_schema_definition(self):
        """Test creating schema definition."""
        columns = [
            ColumnSchema(name="id", dtype="int64"),
            ColumnSchema(name="name", dtype="object"),
        ]
        schema = SchemaDefinition(version=1, columns=columns)

        assert schema.version == 1
        assert len(schema.columns) == 2

    def test_get_column_names(self):
        """Test getting column names."""
        columns = [
            ColumnSchema(name="id", dtype="int64"),
            ColumnSchema(name="name", dtype="object"),
        ]
        schema = SchemaDefinition(columns=columns)

        names = schema.get_column_names()
        assert names == {"id", "name"}

    def test_get_column(self):
        """Test getting column by name."""
        columns = [
            ColumnSchema(name="id", dtype="int64"),
            ColumnSchema(name="name", dtype="object"),
        ]
        schema = SchemaDefinition(columns=columns)

        col = schema.get_column("id")
        assert col is not None
        assert col.name == "id"

        col = schema.get_column("nonexistent")
        assert col is None


@pytest.mark.unit
class TestSchemaValidator:
    """Test SchemaValidator."""

    def test_init(self):
        """Test initialization."""
        validator = SchemaValidator()
        assert validator.schema_registry_table == "schema_versions"

    def test_validate_columns(self, sample_dataframe):
        """Test column validation."""
        validator = SchemaValidator()

        schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
                ColumnSchema(name="name", dtype="object"),
            ]
        )

        result = validator.validate(sample_dataframe, schema, strict=False)

        assert "id" in result.columns
        assert "name" in result.columns

    def test_validate_types(self, sample_dataframe):
        """Test type validation and conversion."""
        validator = SchemaValidator()

        # Convert age to string
        schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="age", dtype="object"),
            ]
        )

        result = validator.validate(sample_dataframe, schema, strict=False)
        assert result["age"].dtype == "object"

    def test_validate_missing_columns(self, sample_dataframe):
        """Test validation with missing columns."""
        validator = SchemaValidator()

        schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
                ColumnSchema(name="missing_col", dtype="object", default="default_value"),
            ]
        )

        result = validator.validate(sample_dataframe, schema, strict=False)
        assert "missing_col" in result.columns

    def test_validate_extra_columns(self, sample_dataframe):
        """Test validation with extra columns."""
        validator = SchemaValidator()

        schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
            ]
        )

        result = validator.validate(
            sample_dataframe, schema, strict=False, allow_extra_columns=True
        )
        assert len(result.columns) > 1

        result = validator.validate(
            sample_dataframe, schema, strict=False, allow_extra_columns=False
        )
        assert len(result.columns) == 1

    def test_apply_defaults(self, sample_dataframe):
        """Test applying default values."""
        validator = SchemaValidator()

        # Add a column with nulls
        df = sample_dataframe.copy()
        df["status"] = None

        schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="status", dtype="object", default="active"),
            ]
        )

        result = validator.validate(df, schema, strict=False)
        assert result["status"].notna().all()

    def test_evolve_schema(self, sample_dataframe):
        """Test schema evolution."""
        validator = SchemaValidator()

        old_schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
                ColumnSchema(name="name", dtype="object"),
            ]
        )

        new_schema = SchemaDefinition(
            version=2,
            columns=[
                ColumnSchema(name="id", dtype="int64"),
                ColumnSchema(name="name", dtype="object"),
                ColumnSchema(name="new_col", dtype="object", default="default"),
            ],
        )

        result = validator.evolve_schema(old_schema, new_schema, sample_dataframe)

        assert "new_col" in result.columns
        assert len(result.columns) >= 3

    def test_register_schema(self):
        """Test registering schema."""
        validator = SchemaValidator()

        schema = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
            ]
        )

        # Should not raise (uses in-memory if no connection manager)
        validator.register_schema("test_schema", schema)

    def test_compare_schemas(self):
        """Test comparing schemas."""
        validator = SchemaValidator()

        schema1 = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
                ColumnSchema(name="name", dtype="object"),
            ]
        )

        schema2 = SchemaDefinition(
            columns=[
                ColumnSchema(name="id", dtype="int64"),
                ColumnSchema(name="email", dtype="object"),
            ]
        )

        comparison = validator.compare_schemas(schema1, schema2)

        assert "added_columns" in comparison
        assert "removed_columns" in comparison
        assert "email" in comparison["added_columns"]
        assert "name" in comparison["removed_columns"]

