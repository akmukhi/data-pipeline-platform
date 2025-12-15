"""SQL-based data transformation with evolution support."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Optional import for schema integration
try:
    from pipeline.transformation.schema_validator import SchemaValidator, SchemaDefinition
except ImportError:
    SchemaValidator = None  # type: ignore
    SchemaDefinition = None  # type: ignore


class SQLTransformer:
    """Transforms data using SQL queries with evolution support."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        engine: Optional[Engine] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ):
        """
        Initialize SQL transformer.

        Args:
            database_url: Database connection URL (used if engine is not provided)
            engine: SQLAlchemy engine (takes precedence over database_url)
            schema_validator: Optional SchemaValidator for evolution support
        """
        if engine is not None:
            self.engine = engine
        elif database_url is not None:
            self.engine = create_engine(database_url, future=True)
        else:
            raise ValueError("Either database_url or engine must be provided")
        
        self.schema_validator = schema_validator
        self._transformation_history: Dict[str, List[Dict[str, Any]]] = {}
        self._query_versions: Dict[str, Dict[int, str]] = {}

    def transform(
        self,
        data: pd.DataFrame,
        sql_query: str,
        table_name: str = "input_data",
        parameters: Optional[Dict[str, Any]] = None,
        transformation_id: Optional[str] = None,
        version: Optional[int] = None,
        input_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        output_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        evolve_schema: bool = False,
    ) -> pd.DataFrame:
        """
        Transform data using a SQL query with evolution support.

        The input DataFrame is temporarily loaded into the database as a table,
        then the SQL query is executed against it.

        Args:
            data: Input DataFrame to transform
            sql_query: SQL query to execute (can reference :table_name)
            table_name: Name of the temporary table to create from input data
            parameters: Optional query parameters
            transformation_id: Optional ID for tracking transformation versions
            version: Optional version number for the transformation
            input_schema: Optional input schema for validation/evolution
            output_schema: Optional output schema for validation
            evolve_schema: If True, evolve data schema before transformation

        Returns:
            Transformed DataFrame

        Example:
            transformer = SQLTransformer(database_url="postgresql://...")
            result = transformer.transform(
                df,
                "SELECT id, UPPER(name) as name_upper, COUNT(*) OVER() as total FROM input_data",
                transformation_id="user_transform",
                version=2
            )
        """
        if data.empty:
            logger.warning("Input DataFrame is empty, returning empty DataFrame")
            return pd.DataFrame()

        # Track transformation
        if transformation_id:
            self._track_transformation(transformation_id, version, sql_query)

        # Handle schema evolution
        if evolve_schema and self.schema_validator and input_schema:
            logger.debug("Evolving input schema before transformation")
            data = self.schema_validator.validate(
                data, input_schema, strict=False, allow_extra_columns=True
            )

        logger.debug(f"Transforming {len(data)} rows using SQL query")

        try:
            # Load DataFrame into temporary table
            with self.engine.connect() as conn:
                # Use pandas to_sql to create temporary table
                data.to_sql(
                    table_name,
                    conn,
                    if_exists="replace",
                    index=False,
                    method="multi",
                )

                # Replace :table_name placeholder in query if present
                query = sql_query.replace(":table_name", table_name)

                # Execute transformation query
                if parameters:
                    result = conn.execute(text(query), parameters)
                else:
                    result = conn.execute(text(query))

                # Fetch results
                rows = result.fetchall()
                column_names = result.keys()

                # Convert to DataFrame
                transformed_df = pd.DataFrame(rows, columns=column_names)

                # Clean up temporary table
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()

                # Validate output schema if provided
                if output_schema and self.schema_validator:
                    logger.debug("Validating output against schema")
                    transformed_df = self.schema_validator.validate(
                        transformed_df, output_schema, strict=False, allow_extra_columns=True
                    )

                logger.debug(f"Transformation complete: {len(transformed_df)} rows output")
                return transformed_df

        except Exception as e:
            logger.error(f"Error during SQL transformation: {e}", exc_info=True)
            # Clean up temporary table on error
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    conn.commit()
            except Exception:
                pass
            raise

    def transform_in_memory(
        self,
        data: pd.DataFrame,
        sql_query: str,
        table_name: str = "input_data",
    ) -> pd.DataFrame:
        """
        Transform data using SQL query with in-memory SQLite database.

        This is faster for small datasets as it doesn't require a persistent database.

        Args:
            data: Input DataFrame to transform
            sql_query: SQL query to execute
            table_name: Name of the temporary table

        Returns:
            Transformed DataFrame
        """
        if data.empty:
            logger.warning("Input DataFrame is empty, returning empty DataFrame")
            return pd.DataFrame()

        logger.debug(f"Transforming {len(data)} rows using in-memory SQL")

        # Use in-memory SQLite
        in_memory_engine = create_engine("sqlite:///:memory:", future=True)

        try:
            with in_memory_engine.connect() as conn:
                # Load DataFrame into temporary table
                data.to_sql(
                    table_name,
                    conn,
                    if_exists="replace",
                    index=False,
                )

                # Replace :table_name placeholder
                query = sql_query.replace(":table_name", table_name)

                # Execute query
                result = conn.execute(text(query))

                # Fetch results
                rows = result.fetchall()
                column_names = result.keys()

                # Convert to DataFrame
                transformed_df = pd.DataFrame(rows, columns=column_names)

                logger.debug(f"In-memory transformation complete: {len(transformed_df)} rows output")
                return transformed_df

        except Exception as e:
            logger.error(f"Error during in-memory SQL transformation: {e}", exc_info=True)
            raise

    def register_query_version(
        self,
        transformation_id: str,
        version: int,
        sql_query: str,
        description: Optional[str] = None,
    ) -> None:
        """
        Register a versioned SQL query for a transformation.

        Args:
            transformation_id: Unique identifier for the transformation
            version: Version number
            sql_query: SQL query for this version
            description: Optional description of changes in this version
        """
        if transformation_id not in self._query_versions:
            self._query_versions[transformation_id] = {}
        
        self._query_versions[transformation_id][version] = sql_query
        
        # Track in history
        if transformation_id not in self._transformation_history:
            self._transformation_history[transformation_id] = []
        
        self._transformation_history[transformation_id].append({
            "version": version,
            "query": sql_query,
            "description": description,
            "timestamp": datetime.now().isoformat(),
        })
        
        logger.info(f"Registered query version {version} for transformation '{transformation_id}'")

    def get_query_version(self, transformation_id: str, version: int) -> Optional[str]:
        """
        Get a specific version of a SQL query.

        Args:
            transformation_id: Transformation identifier
            version: Version number

        Returns:
            SQL query string, or None if not found
        """
        if transformation_id in self._query_versions:
            return self._query_versions[transformation_id].get(version)
        return None

    def get_transformation_history(self, transformation_id: str) -> List[Dict[str, Any]]:
        """
        Get version history for a transformation.

        Args:
            transformation_id: Transformation identifier

        Returns:
            List of transformation records sorted by version
        """
        if transformation_id in self._transformation_history:
            return sorted(
                self._transformation_history[transformation_id],
                key=lambda x: x.get("version", 0)
            )
        return []

    def transform_with_version(
        self,
        data: pd.DataFrame,
        transformation_id: str,
        version: Optional[int] = None,
        table_name: str = "input_data",
        parameters: Optional[Dict[str, Any]] = None,
        input_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        output_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
    ) -> pd.DataFrame:
        """
        Transform data using a registered versioned query.

        Args:
            data: Input DataFrame
            transformation_id: Transformation identifier
            version: Version to use (uses latest if not specified)
            table_name: Temporary table name
            parameters: Optional query parameters
            input_schema: Optional input schema
            output_schema: Optional output schema

        Returns:
            Transformed DataFrame
        """
        if transformation_id not in self._query_versions:
            raise ValueError(f"Transformation '{transformation_id}' not found")

        if version is None:
            # Use latest version
            versions = sorted(self._query_versions[transformation_id].keys(), reverse=True)
            if not versions:
                raise ValueError(f"No versions found for transformation '{transformation_id}'")
            version = versions[0]
            logger.debug(f"Using latest version {version} for '{transformation_id}'")

        query = self.get_query_version(transformation_id, version)
        if query is None:
            raise ValueError(
                f"Version {version} not found for transformation '{transformation_id}'"
            )

        return self.transform(
            data,
            query,
            table_name=table_name,
            parameters=parameters,
            transformation_id=transformation_id,
            version=version,
            input_schema=input_schema,
            output_schema=output_schema,
            evolve_schema=True,
        )

    def _track_transformation(
        self, transformation_id: str, version: Optional[int], sql_query: str
    ) -> None:
        """Track transformation execution."""
        if transformation_id not in self._transformation_history:
            self._transformation_history[transformation_id] = []
        
        self._transformation_history[transformation_id].append({
            "version": version or 1,
            "query": sql_query,
            "timestamp": datetime.now().isoformat(),
            "executed": True,
        })

