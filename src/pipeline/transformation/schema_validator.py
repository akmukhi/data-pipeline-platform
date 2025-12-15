"""Schema validation and evolution support."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator

# Optional imports for connection management
try:
    from pipeline.ingestion.connection_manager import ConnectionManager
except ImportError:
    ConnectionManager = None  # type: ignore

logger = logging.getLogger(__name__)


class ColumnSchema(BaseModel):
    """Schema definition for a single column."""

    name: str
    dtype: str = Field(..., description="Pandas dtype or Python type name")
    nullable: bool = True
    default: Optional[Any] = None
    description: Optional[str] = None

    @field_validator("dtype")
    @classmethod
    def validate_dtype(cls, v: str) -> str:
        """Validate and normalize dtype."""
        # Map common type names to pandas dtypes
        type_mapping = {
            "string": "object",
            "str": "object",
            "int": "int64",
            "integer": "int64",
            "float": "float64",
            "double": "float64",
            "bool": "bool",
            "boolean": "bool",
            "datetime": "datetime64[ns]",
            "date": "datetime64[ns]",
            "timestamp": "datetime64[ns]",
        }
        return type_mapping.get(v.lower(), v)


class SchemaDefinition(BaseModel):
    """Complete schema definition for a dataset."""

    version: int = Field(default=1, description="Schema version number")
    columns: List[ColumnSchema]
    created_at: Optional[datetime] = None
    description: Optional[str] = None

    def get_column_names(self) -> Set[str]:
        """Get set of column names in schema."""
        return {col.name for col in self.columns}

    def get_column(self, name: str) -> Optional[ColumnSchema]:
        """Get column schema by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None


class SchemaValidator:
    """Validates data against schemas and handles schema evolution."""

    def __init__(
        self,
        connection_manager: Optional[ConnectionManager] = None,
        database_url: Optional[str] = None,
        schema_registry_table: str = "schema_versions",
    ):
        """
        Initialize schema validator.

        Args:
            connection_manager: ConnectionManager for schema registry (optional)
            database_url: Database URL for schema registry (optional)
            schema_registry_table: Name of table to store schema versions
        """
        self.connection_manager = connection_manager
        if connection_manager is None and database_url is not None:
            if ConnectionManager is None:
                logger.warning(
                    "ConnectionManager not available. Schema registry will use in-memory storage only."
                )
            else:
                self.connection_manager = ConnectionManager(database_url)
        self.schema_registry_table = schema_registry_table
        self._schema_cache: Dict[str, SchemaDefinition] = {}
        self._schema_history: Dict[str, List[SchemaDefinition]] = {}  # Track schema versions

    def validate(
        self,
        data: pd.DataFrame,
        schema: Union[SchemaDefinition, Dict, str],
        strict: bool = False,
        allow_extra_columns: bool = True,
    ) -> pd.DataFrame:
        """
        Validate data against a schema.

        Args:
            data: DataFrame to validate
            schema: SchemaDefinition, dict, or schema name (if registered)
            strict: If True, raise error on validation failure; if False, attempt to fix
            allow_extra_columns: If True, allow columns not in schema; if False, drop them

        Returns:
            Validated (and potentially transformed) DataFrame

        Raises:
            ValueError: If validation fails and strict=True
        """
        schema_def = self._get_schema_definition(schema)

        logger.debug(
            f"Validating {len(data)} rows against schema version {schema_def.version}"
        )

        try:
            validated_data = self._validate_columns(
                data, schema_def, strict, allow_extra_columns
            )
            validated_data = self._validate_types(validated_data, schema_def, strict)
            validated_data = self._apply_defaults(validated_data, schema_def)

            logger.debug(f"Validation complete: {len(validated_data)} rows")
            return validated_data

        except Exception as e:
            if strict:
                raise ValueError(f"Schema validation failed: {e}") from e
            logger.warning(f"Schema validation issue (non-strict): {e}")
            return data

    def _get_schema_definition(self, schema: Union[SchemaDefinition, Dict, str]) -> SchemaDefinition:
        """Get SchemaDefinition from various input types."""
        if isinstance(schema, SchemaDefinition):
            return schema
        elif isinstance(schema, dict):
            return SchemaDefinition(**schema)
        elif isinstance(schema, str):
            # Load from registry
            return self.load_schema(schema)
        else:
            raise ValueError(f"Invalid schema type: {type(schema)}")

    def _validate_columns(
        self,
        data: pd.DataFrame,
        schema: SchemaDefinition,
        strict: bool,
        allow_extra: bool,
    ) -> pd.DataFrame:
        """Validate that required columns are present."""
        schema_columns = schema.get_column_names()
        data_columns = set(data.columns)

        missing_columns = schema_columns - data_columns
        extra_columns = data_columns - schema_columns

        if missing_columns:
            if strict:
                raise ValueError(f"Missing required columns: {missing_columns}")
            logger.warning(f"Missing columns (will be added with defaults): {missing_columns}")
            # Add missing columns with None/default values
            for col_name in missing_columns:
                col_schema = schema.get_column(col_name)
                default = col_schema.default if col_schema else None
                data[col_name] = default

        if extra_columns and not allow_extra:
            if strict:
                raise ValueError(f"Extra columns not allowed: {extra_columns}")
            logger.warning(f"Dropping extra columns: {extra_columns}")
            data = data.drop(columns=extra_columns)
        elif extra_columns:
            logger.debug(f"Extra columns allowed: {extra_columns}")

        # Reorder columns to match schema order
        ordered_columns = [col.name for col in schema.columns if col.name in data.columns]
        ordered_columns.extend([col for col in data.columns if col not in ordered_columns])

        return data[ordered_columns]

    def _validate_types(
        self, data: pd.DataFrame, schema: SchemaDefinition, strict: bool
    ) -> pd.DataFrame:
        """Validate and convert column types."""
        result = data.copy()

        for col_schema in schema.columns:
            col_name = col_schema.name
            if col_name not in result.columns:
                continue

            expected_dtype = col_schema.dtype
            actual_dtype = str(result[col_name].dtype)

            # Check if type conversion is needed
            if expected_dtype != actual_dtype:
                try:
                    if expected_dtype == "object":
                        result[col_name] = result[col_name].astype(str)
                    elif expected_dtype in ["int64", "int32", "int"]:
                        result[col_name] = pd.to_numeric(result[col_name], errors="coerce").astype(
                            "int64"
                        )
                    elif expected_dtype in ["float64", "float32", "float"]:
                        result[col_name] = pd.to_numeric(result[col_name], errors="coerce").astype(
                            "float64"
                        )
                    elif expected_dtype == "bool":
                        result[col_name] = result[col_name].astype(bool)
                    elif expected_dtype == "datetime64[ns]":
                        result[col_name] = pd.to_datetime(result[col_name], errors="coerce")
                    else:
                        result[col_name] = result[col_name].astype(expected_dtype)

                    logger.debug(f"Converted {col_name} from {actual_dtype} to {expected_dtype}")

                except Exception as e:
                    if strict:
                        raise ValueError(
                            f"Cannot convert column {col_name} to {expected_dtype}: {e}"
                        ) from e
                    logger.warning(
                        f"Type conversion failed for {col_name} (keeping original type): {e}"
                    )

        return result

    def _apply_defaults(self, data: pd.DataFrame, schema: SchemaDefinition) -> pd.DataFrame:
        """Apply default values for nullable columns with None values."""
        result = data.copy()

        for col_schema in schema.columns:
            col_name = col_schema.name
            if col_name not in result.columns:
                continue

            if col_schema.default is not None:
                # Fill null values with default
                null_mask = result[col_name].isna()
                if null_mask.any():
                    result.loc[null_mask, col_name] = col_schema.default
                    logger.debug(
                        f"Applied default value to {null_mask.sum()} null values in {col_name}"
                    )

        return result

    def register_schema(
        self,
        schema_name: str,
        schema: Union[SchemaDefinition, Dict],
        description: Optional[str] = None,
    ) -> None:
        """
        Register a schema in the schema registry.

        Args:
            schema_name: Name to register schema under
            schema: SchemaDefinition or dict
            description: Optional description
        """
        schema_def = self._get_schema_definition(schema)
        if description:
            schema_def.description = description
        
        if self.connection_manager is None:
            logger.warning("No connection manager, schema will only be cached in memory")
            self._schema_cache[schema_name] = schema_def
            # Track schema history
            if schema_name not in self._schema_history:
                self._schema_history[schema_name] = []
            self._schema_history[schema_name].append(schema_def)
            # Keep only last 10 versions in memory
            if len(self._schema_history[schema_name]) > 10:
                self._schema_history[schema_name] = self._schema_history[schema_name][-10:]
            logger.info(f"Registered schema '{schema_name}' version {schema_def.version} (in-memory only)")
            return

        # Store in database
        with self.connection_manager.get_connection() as conn:
            from sqlalchemy import text

            # Create schema registry table if it doesn't exist
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.schema_registry_table} (
                    schema_name VARCHAR(255) PRIMARY KEY,
                    version INTEGER NOT NULL,
                    schema_json TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            conn.execute(text(create_table_sql))
            conn.commit()

            # Insert or update schema
            import json

            schema_json = json.dumps(schema_def.model_dump(), default=str)

            insert_sql = f"""
                INSERT INTO {self.schema_registry_table}
                (schema_name, version, schema_json, description, updated_at)
                VALUES (:schema_name, :version, :schema_json, :description, CURRENT_TIMESTAMP)
                ON CONFLICT (schema_name) DO UPDATE SET
                    version = EXCLUDED.version,
                    schema_json = EXCLUDED.schema_json,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP
            """
            conn.execute(
                text(insert_sql),
                {
                    "schema_name": schema_name,
                    "version": schema_def.version,
                    "schema_json": schema_json,
                    "description": description,
                },
            )
            conn.commit()

        # Cache in memory
        self._schema_cache[schema_name] = schema_def
        
        # Track schema history
        if schema_name not in self._schema_history:
            self._schema_history[schema_name] = []
        self._schema_history[schema_name].append(schema_def)
        # Keep only last 10 versions in memory
        if len(self._schema_history[schema_name]) > 10:
            self._schema_history[schema_name] = self._schema_history[schema_name][-10:]
        
        logger.info(f"Registered schema '{schema_name}' version {schema_def.version}")

    def load_schema(self, schema_name: str) -> SchemaDefinition:
        """
        Load a schema from the registry.

        Args:
            schema_name: Name of the schema to load

        Returns:
            SchemaDefinition
        """
        # Check cache first
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        if self.connection_manager is None:
            raise ValueError(
                f"Schema '{schema_name}' not found in cache and no connection manager available"
            )

        # Load from database
        with self.connection_manager.get_connection() as conn:
            from sqlalchemy import text

            select_sql = f"""
                SELECT schema_json FROM {self.schema_registry_table}
                WHERE schema_name = :schema_name
                ORDER BY version DESC
                LIMIT 1
            """
            result = conn.execute(text(select_sql), {"schema_name": schema_name})
            row = result.fetchone()

            if row is None:
                raise ValueError(f"Schema '{schema_name}' not found in registry")

            import json

            schema_dict = json.loads(row[0])
            schema_def = SchemaDefinition(**schema_dict)

        # Cache it
        self._schema_cache[schema_name] = schema_def
        return schema_def

    def evolve_schema(
        self,
        old_schema: Union[SchemaDefinition, str],
        new_schema: Union[SchemaDefinition, Dict],
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Evolve data from old schema to new schema.

        Handles:
        - Adding new columns (with defaults)
        - Removing deprecated columns
        - Type conversions
        - Column renames (if specified)

        Args:
            old_schema: Current schema (SchemaDefinition or schema name)
            new_schema: Target schema (SchemaDefinition or dict)
            data: Data to evolve

        Returns:
            Evolved DataFrame
        """
        old_schema_def = self._get_schema_definition(old_schema)
        new_schema_def = self._get_schema_definition(new_schema)

        logger.info(
            f"Evolving schema from version {old_schema_def.version} to {new_schema_def.version}"
        )

        result = data.copy()

        # Get column sets
        old_columns = old_schema_def.get_column_names()
        new_columns = new_schema_def.get_column_names()

        # Add new columns with defaults
        for col_name in new_columns - old_columns:
            col_schema = new_schema_def.get_column(col_name)
            default = col_schema.default if col_schema and col_schema.default is not None else None
            result[col_name] = default
            logger.debug(f"Added new column '{col_name}' with default value")

        # Remove deprecated columns (columns in old but not in new)
        deprecated = old_columns - new_columns
        if deprecated:
            logger.info(f"Removing deprecated columns: {deprecated}")
            result = result.drop(columns=list(deprecated))

        # Validate against new schema
        result = self.validate(result, new_schema_def, strict=False, allow_extra_columns=False)

        logger.info("Schema evolution complete")
        return result

    def get_schema_history(self, schema_name: str) -> List[SchemaDefinition]:
        """
        Get version history for a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List of SchemaDefinitions in version order
        """
        if schema_name in self._schema_history:
            return sorted(self._schema_history[schema_name], key=lambda s: s.version)
        return []

    def get_schema_version(self, schema_name: str, version: int) -> Optional[SchemaDefinition]:
        """
        Get a specific version of a schema.

        Args:
            schema_name: Name of the schema
            version: Version number to retrieve

        Returns:
            SchemaDefinition for the specified version, or None if not found
        """
        history = self.get_schema_history(schema_name)
        for schema in history:
            if schema.version == version:
                return schema
        return None

    def compare_schemas(
        self,
        schema1: Union[SchemaDefinition, str, int],
        schema2: Union[SchemaDefinition, str, int],
        schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare two schemas and return differences.

        Args:
            schema1: First schema (SchemaDefinition, schema name, or version number)
            schema2: Second schema (SchemaDefinition, schema name, or version number)
            schema_name: Required if schema1 or schema2 are version numbers

        Returns:
            Dictionary with comparison results:
            - added_columns: Columns in schema2 but not in schema1
            - removed_columns: Columns in schema1 but not in schema2
            - type_changes: Columns with different types
            - nullable_changes: Columns with different nullable settings
        """
        def get_schema(s: Union[SchemaDefinition, str, int]) -> SchemaDefinition:
            if isinstance(s, SchemaDefinition):
                return s
            elif isinstance(s, str):
                return self.load_schema(s)
            elif isinstance(s, int):
                if schema_name is None:
                    raise ValueError("schema_name required when using version numbers")
                result = self.get_schema_version(schema_name, s)
                if result is None:
                    raise ValueError(f"Schema version {s} not found for {schema_name}")
                return result
            else:
                raise ValueError(f"Invalid schema type: {type(s)}")

        s1 = get_schema(schema1)
        s2 = get_schema(schema2)

        s1_cols = {col.name: col for col in s1.columns}
        s2_cols = {col.name: col for col in s2.columns}

        added = [col for name, col in s2_cols.items() if name not in s1_cols]
        removed = [col for name, col in s1_cols.items() if name not in s2_cols]

        type_changes = []
        nullable_changes = []

        for name in s1_cols.keys() & s2_cols.keys():
            c1, c2 = s1_cols[name], s2_cols[name]
            if c1.dtype != c2.dtype:
                type_changes.append({
                    "column": name,
                    "old_type": c1.dtype,
                    "new_type": c2.dtype,
                })
            if c1.nullable != c2.nullable:
                nullable_changes.append({
                    "column": name,
                    "old_nullable": c1.nullable,
                    "new_nullable": c2.nullable,
                })

        return {
            "added_columns": [col.name for col in added],
            "removed_columns": [col.name for col in removed],
            "type_changes": type_changes,
            "nullable_changes": nullable_changes,
            "version_from": s1.version,
            "version_to": s2.version,
        }

