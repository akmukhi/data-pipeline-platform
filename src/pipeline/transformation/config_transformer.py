"""Config-driven data transformation using YAML/JSON configuration with evolution support."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Optional import for schema integration
try:
    from pipeline.transformation.schema_validator import SchemaValidator, SchemaDefinition
except ImportError:
    SchemaValidator = None  # type: ignore
    SchemaDefinition = None  # type: ignore


class ConfigTransformer:
    """Transforms data using declarative YAML/JSON configuration with evolution support."""

    def __init__(
        self,
        config: Optional[Union[Dict, str, Path]] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ):
        """
        Initialize config transformer.

        Args:
            config: Configuration dict, or path to YAML/JSON config file
            schema_validator: Optional SchemaValidator for evolution support
        """
        self.schema_validator = schema_validator
        self.config: Optional[Dict[str, Any]] = None
        self._transformation_history: Dict[str, List[Dict[str, Any]]] = {}
        self._config_versions: Dict[str, Dict[int, Dict[str, Any]]] = {}
        if config is not None:
            self.load_config(config)

    def load_config(self, config: Union[Dict, str, Path]) -> None:
        """
        Load configuration from dict or file.

        Args:
            config: Configuration dict, or path to YAML/JSON config file
        """
        if isinstance(config, dict):
            self.config = config
        elif isinstance(config, (str, Path)):
            config_path = Path(config)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")

            with open(config_path, "r") as f:
                if config_path.suffix in [".yaml", ".yml"]:
                    self.config = yaml.safe_load(f)
                elif config_path.suffix == ".json":
                    self.config = json.load(f)
                else:
                    raise ValueError(
                        f"Unsupported config file format: {config_path.suffix}"
                    )
        else:
            raise ValueError(f"Invalid config type: {type(config)}")

        logger.debug(f"Loaded transformation config: {list(self.config.keys()) if self.config else 'None'}")

    def transform(
        self,
        data: pd.DataFrame,
        config: Optional[Union[Dict, str, Path]] = None,
        transformation_id: Optional[str] = None,
        version: Optional[int] = None,
        input_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        output_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        evolve_schema: bool = False,
    ) -> pd.DataFrame:
        """
        Transform data using configuration with evolution support.

        Args:
            data: Input DataFrame to transform
            config: Optional config override (uses instance config if not provided)
            transformation_id: Optional ID for tracking transformation versions
            version: Optional version number for the transformation
            input_schema: Optional input schema for validation/evolution
            output_schema: Optional output schema for validation
            evolve_schema: If True, evolve data schema before transformation

        Returns:
            Transformed DataFrame

        Example config structure:
            {
                "version": 1,
                "select": ["id", "name", "email"],
                "rename": {"name": "full_name", "email": "email_address"},
                "filter": {"status": "active"},
                "add_columns": {
                    "created_year": "EXTRACT(YEAR FROM created_at)"
                },
                "drop_columns": ["internal_id"],
                "aggregations": {
                    "group_by": ["category"],
                    "aggregate": {
                        "total": "SUM(amount)",
                        "count": "COUNT(*)"
                    }
                }
            }
        """
        # Handle schema evolution
        if evolve_schema and self.schema_validator and input_schema:
            logger.debug("Evolving input schema before transformation")
            data = self.schema_validator.validate(
                data, input_schema, strict=False, allow_extra_columns=True
            )

        # Track transformation
        if transformation_id:
            self._track_transformation(transformation_id, version, config)

        if config is not None:
            # Use provided config (temporary)
            original_config = self.config
            self.load_config(config)
            try:
                result = self._transform_with_config(data)
            finally:
                self.config = original_config
        else:
            if self.config is None:
                raise ValueError("No configuration provided. Use load_config() or pass config to transform()")
            result = self._transform_with_config(data)

        # Validate output schema if provided
        if output_schema and self.schema_validator:
            logger.debug("Validating output against schema")
            result = self.schema_validator.validate(
                result, output_schema, strict=False, allow_extra_columns=True
            )

        return result

    def _transform_with_config(self, data: pd.DataFrame) -> pd.DataFrame:
        """Internal method to perform transformation with current config."""
        if data.empty:
            logger.warning("Input DataFrame is empty, returning empty DataFrame")
            return pd.DataFrame()

        logger.debug(f"Transforming {len(data)} rows using config transformation")

        result = data.copy()

        try:
            # Apply transformations in order
            if "select" in self.config:
                result = self._apply_select(result, self.config["select"])

            if "rename" in self.config:
                result = self._apply_rename(result, self.config["rename"])

            if "filter" in self.config:
                result = self._apply_filter(result, self.config["filter"])

            if "add_columns" in self.config:
                result = self._apply_add_columns(result, self.config["add_columns"])

            if "drop_columns" in self.config:
                result = self._apply_drop_columns(result, self.config["drop_columns"])

            if "transformations" in self.config:
                result = self._apply_custom_transformations(
                    result, self.config["transformations"]
                )

            if "aggregations" in self.config:
                result = self._apply_aggregations(result, self.config["aggregations"])

            logger.debug(f"Config transformation complete: {len(result)} rows output")
            return result

        except Exception as e:
            logger.error(f"Error during config transformation: {e}", exc_info=True)
            raise

    def _apply_select(self, data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Select specific columns."""
        missing_cols = [col for col in columns if col not in data.columns]
        if missing_cols:
            logger.warning(f"Columns not found in data: {missing_cols}")
            columns = [col for col in columns if col in data.columns]

        return data[columns]

    def _apply_rename(self, data: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
        """Rename columns."""
        return data.rename(columns=rename_map)

    def _apply_filter(self, data: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """Apply filters to data."""
        mask = pd.Series([True] * len(data), index=data.index)

        for column, value in filters.items():
            if column not in data.columns:
                logger.warning(f"Filter column '{column}' not found in data")
                continue

            if isinstance(value, dict):
                # Handle operators: {">": 100}, {"<=": 50}, {"in": [1, 2, 3]}
                for op, op_value in value.items():
                    if op == ">":
                        mask &= data[column] > op_value
                    elif op == ">=":
                        mask &= data[column] >= op_value
                    elif op == "<":
                        mask &= data[column] < op_value
                    elif op == "<=":
                        mask &= data[column] <= op_value
                    elif op == "==" or op == "equals":
                        mask &= data[column] == op_value
                    elif op == "!=" or op == "not_equals":
                        mask &= data[column] != op_value
                    elif op == "in":
                        mask &= data[column].isin(op_value)
                    elif op == "not_in":
                        mask &= ~data[column].isin(op_value)
                    elif op == "contains":
                        mask &= data[column].str.contains(op_value, na=False)
                    elif op == "is_null":
                        mask &= data[column].isna()
                    elif op == "is_not_null":
                        mask &= data[column].notna()
            else:
                # Simple equality filter
                mask &= data[column] == value

        return data[mask]

    def _apply_add_columns(self, data: pd.DataFrame, columns: Dict[str, Any]) -> pd.DataFrame:
        """Add computed columns."""
        result = data.copy()

        for col_name, expression in columns.items():
            if isinstance(expression, str):
                # Try to evaluate as pandas expression
                try:
                    # Use eval with data namespace
                    result[col_name] = result.eval(expression)
                except Exception:
                    # Fallback: treat as literal value
                    result[col_name] = expression
            else:
                # Literal value
                result[col_name] = expression

        return result

    def _apply_drop_columns(self, data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Drop columns."""
        cols_to_drop = [col for col in columns if col in data.columns]
        return data.drop(columns=cols_to_drop)

    def _apply_custom_transformations(
        self, data: pd.DataFrame, transformations: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """Apply custom transformations defined in config."""
        result = data.copy()

        for transform in transformations:
            transform_type = transform.get("type")
            if transform_type == "map":
                # Column mapping: {"type": "map", "column": "status", "mapping": {"A": "Active", "I": "Inactive"}}
                column = transform.get("column")
                mapping = transform.get("mapping", {})
                if column and column in result.columns:
                    result[column] = result[column].map(mapping)
            elif transform_type == "apply":
                # Apply function: {"type": "apply", "column": "name", "function": "upper"}
                column = transform.get("column")
                func_name = transform.get("function")
                if column and column in result.columns and func_name:
                    if func_name == "upper":
                        result[column] = result[column].str.upper()
                    elif func_name == "lower":
                        result[column] = result[column].str.lower()
                    elif func_name == "strip":
                        result[column] = result[column].str.strip()

        return result

    def _apply_aggregations(
        self, data: pd.DataFrame, aggregations: Dict[str, Any]
    ) -> pd.DataFrame:
        """Apply aggregations (group by and aggregate)."""
        group_by = aggregations.get("group_by", [])
        aggregate = aggregations.get("aggregate", {})

        if not group_by:
            # No grouping, just aggregate entire dataset
            result = pd.DataFrame()
            for col_name, func in aggregate.items():
                if func == "COUNT(*)":
                    result[col_name] = [len(data)]
                else:
                    # Try to parse aggregation like "SUM(column)"
                    # This is simplified - in production, use proper SQL-like parsing
                    logger.warning(
                        "Complex aggregations without group_by are not fully supported. "
                        "Consider using SQL transformer for complex aggregations."
                    )
            return result

        # Group by and aggregate
        grouped = data.groupby(group_by)

        agg_dict = {}
        for col_name, func_expr in aggregate.items():
            # Parse expressions like "SUM(amount)" or "AVG(price)"
            # Simplified parsing - assumes format "FUNC(column)"
            if "(" in func_expr and ")" in func_expr:
                func_name = func_expr.split("(")[0].upper()
                col = func_expr.split("(")[1].split(")")[0]

                if col in data.columns:
                    if func_name == "SUM":
                        agg_dict[col] = "sum"
                    elif func_name == "AVG" or func_name == "AVERAGE":
                        agg_dict[col] = "mean"
                    elif func_name == "COUNT":
                        agg_dict[col] = "count"
                    elif func_name == "MIN":
                        agg_dict[col] = "min"
                    elif func_name == "MAX":
                        agg_dict[col] = "max"
                    else:
                        logger.warning(f"Unsupported aggregation function: {func_name}")

        if agg_dict:
            result = grouped.agg(agg_dict)
            result.columns = [f"{col}_{func}" for col, func in agg_dict.items()]
            result = result.reset_index()
        else:
            result = grouped.size().reset_index(name="count")

        return result

    def register_config_version(
        self,
        transformation_id: str,
        version: int,
        config: Union[Dict, str, Path],
        description: Optional[str] = None,
    ) -> None:
        """
        Register a versioned config for a transformation.

        Args:
            transformation_id: Unique identifier for the transformation
            version: Version number
            config: Configuration dict or path to config file
            description: Optional description of changes in this version
        """
        # Load config if it's a path
        if isinstance(config, (str, Path)):
            config_path = Path(config)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_path}")
            
            with open(config_path, "r") as f:
                if config_path.suffix in [".yaml", ".yml"]:
                    config_dict = yaml.safe_load(f)
                elif config_path.suffix == ".json":
                    config_dict = json.load(f)
                else:
                    raise ValueError(f"Unsupported config file format: {config_path.suffix}")
        else:
            config_dict = config

        if transformation_id not in self._config_versions:
            self._config_versions[transformation_id] = {}
        
        self._config_versions[transformation_id][version] = config_dict
        
        # Track in history
        if transformation_id not in self._transformation_history:
            self._transformation_history[transformation_id] = []
        
        self._transformation_history[transformation_id].append({
            "version": version,
            "config_keys": list(config_dict.keys()) if isinstance(config_dict, dict) else [],
            "description": description,
            "timestamp": datetime.now().isoformat(),
        })
        
        logger.info(f"Registered config version {version} for transformation '{transformation_id}'")

    def get_config_version(
        self, transformation_id: str, version: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific version of a config.

        Args:
            transformation_id: Transformation identifier
            version: Version number

        Returns:
            Config dictionary, or None if not found
        """
        if transformation_id in self._config_versions:
            return self._config_versions[transformation_id].get(version)
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
        input_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        output_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
    ) -> pd.DataFrame:
        """
        Transform data using a registered versioned config.

        Args:
            data: Input DataFrame
            transformation_id: Transformation identifier
            version: Version to use (uses latest if not specified)
            input_schema: Optional input schema
            output_schema: Optional output schema

        Returns:
            Transformed DataFrame
        """
        if transformation_id not in self._config_versions:
            raise ValueError(f"Transformation '{transformation_id}' not found")

        if version is None:
            # Use latest version
            versions = sorted(self._config_versions[transformation_id].keys(), reverse=True)
            if not versions:
                raise ValueError(f"No versions found for transformation '{transformation_id}'")
            version = versions[0]
            logger.debug(f"Using latest version {version} for '{transformation_id}'")

        config = self.get_config_version(transformation_id, version)
        if config is None:
            raise ValueError(
                f"Version {version} not found for transformation '{transformation_id}'"
            )

        return self.transform(
            data,
            config,
            transformation_id=transformation_id,
            version=version,
            input_schema=input_schema,
            output_schema=output_schema,
            evolve_schema=True,
        )

    def migrate_config(
        self,
        old_config: Union[Dict, str, Path],
        new_config: Union[Dict, str, Path],
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Migrate data using config evolution (applies old then new config).

        Args:
            old_config: Previous version of config
            new_config: New version of config
            data: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        logger.info("Migrating data using config evolution")
        
        # First apply old config to ensure compatibility
        old_result = self.transform(data, old_config)
        
        # Then apply new config
        new_result = self.transform(old_result, new_config)
        
        logger.info("Config migration complete")
        return new_result

    def _track_transformation(
        self,
        transformation_id: str,
        version: Optional[int],
        config: Optional[Union[Dict, str, Path]],
    ) -> None:
        """Track transformation execution."""
        if transformation_id not in self._transformation_history:
            self._transformation_history[transformation_id] = []
        
        config_repr = "current_config" if config is None else (
            list(config.keys()) if isinstance(config, dict) else str(config)
        )
        self._transformation_history[transformation_id].append({
            "version": version or 1,
            "config": config_repr,
            "timestamp": datetime.now().isoformat(),
            "executed": True,
        })

