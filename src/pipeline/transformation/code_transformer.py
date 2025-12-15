"""Code-based data transformation using Python functions with evolution support."""

import importlib.util
import logging
import sys
from datetime import datetime
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

# Optional import for schema integration
try:
    from pipeline.transformation.schema_validator import SchemaValidator, SchemaDefinition
except ImportError:
    SchemaValidator = None  # type: ignore
    SchemaDefinition = None  # type: ignore


class CodeTransformer:
    """Transforms data using Python code (functions or classes) with evolution support."""

    def __init__(
        self,
        safe_mode: bool = True,
        schema_validator: Optional["SchemaValidator"] = None,
    ):
        """
        Initialize code transformer.

        Args:
            safe_mode: If True, restricts certain operations for security (default: True)
            schema_validator: Optional SchemaValidator for evolution support
        """
        self.safe_mode = safe_mode
        self.schema_validator = schema_validator
        self._loaded_modules: Dict[str, ModuleType] = {}
        self._transformation_history: Dict[str, List[Dict[str, Any]]] = {}
        self._function_versions: Dict[str, Dict[int, Union[Callable, str]]] = {}

    def transform(
        self,
        data: pd.DataFrame,
        transform_func: Union[Callable[[pd.DataFrame], pd.DataFrame], str],
        transformation_id: Optional[str] = None,
        version: Optional[int] = None,
        input_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        output_schema: Optional[Union[SchemaDefinition, str, Dict]] = None,
        evolve_schema: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Transform data using a Python function with evolution support.

        Args:
            data: Input DataFrame to transform
            transform_func: Function that takes DataFrame and returns DataFrame,
                           or path to Python file containing transform function
            transformation_id: Optional ID for tracking transformation versions
            version: Optional version number for the transformation
            input_schema: Optional input schema for validation/evolution
            output_schema: Optional output schema for validation
            evolve_schema: If True, evolve data schema before transformation
            **kwargs: Additional arguments to pass to transform function

        Returns:
            Transformed DataFrame

        Example:
            # Using a function directly
            def my_transform(df):
                df['new_col'] = df['old_col'] * 2
                return df

            transformer = CodeTransformer()
            result = transformer.transform(
                data, my_transform,
                transformation_id="multiply_transform",
                version=1
            )

            # Using a function from a file
            result = transformer.transform(
                data, "path/to/transform.py:transform_function",
                transformation_id="file_transform"
            )
        """
        if data.empty:
            logger.warning("Input DataFrame is empty, returning empty DataFrame")
            return pd.DataFrame()

        # Handle schema evolution
        if evolve_schema and self.schema_validator and input_schema:
            logger.debug("Evolving input schema before transformation")
            data = self.schema_validator.validate(
                data, input_schema, strict=False, allow_extra_columns=True
            )

        # Track transformation
        if transformation_id:
            self._track_transformation(transformation_id, version, transform_func)

        logger.debug(f"Transforming {len(data)} rows using code transformation")

        try:
            # Get the actual function
            func = self._get_transform_function(transform_func)

            # Execute transformation
            if kwargs:
                result = func(data, **kwargs)
            else:
                result = func(data)

            # Validate result is a DataFrame
            if not isinstance(result, pd.DataFrame):
                raise ValueError(
                    f"Transform function must return a DataFrame, got {type(result)}"
                )

            # Validate output schema if provided
            if output_schema and self.schema_validator:
                logger.debug("Validating output against schema")
                result = self.schema_validator.validate(
                    result, output_schema, strict=False, allow_extra_columns=True
                )

            logger.debug(f"Code transformation complete: {len(result)} rows output")
            return result

        except Exception as e:
            logger.error(f"Error during code transformation: {e}", exc_info=True)
            raise

    def _get_transform_function(
        self, transform_func: Union[Callable, str]
    ) -> Callable[[pd.DataFrame], pd.DataFrame]:
        """
        Get the transform function from various input types.

        Args:
            transform_func: Function, class, or string path to function

        Returns:
            Callable transform function
        """
        if callable(transform_func):
            return transform_func

        if isinstance(transform_func, str):
            # Handle string paths like "module:function" or "path/to/file.py:function"
            if ":" in transform_func:
                module_path, func_name = transform_func.rsplit(":", 1)
                return self._load_function_from_path(module_path, func_name)
            else:
                # Assume it's a module path
                return self._load_function_from_module(transform_func)

        raise ValueError(f"Invalid transform_func type: {type(transform_func)}")

    def _load_function_from_path(self, file_path: str, function_name: str) -> Callable:
        """
        Load a function from a Python file.

        Args:
            file_path: Path to Python file
            function_name: Name of the function to load

        Returns:
            The loaded function
        """
        file_path_obj = Path(file_path)

        if not file_path_obj.exists():
            raise FileNotFoundError(f"Transform file not found: {file_path}")

        # Check if already loaded
        cache_key = f"{file_path}:{function_name}"
        if cache_key in self._loaded_modules:
            module = self._loaded_modules[cache_key]
            return getattr(module, function_name)

        # Load module
        spec = importlib.util.spec_from_file_location("transform_module", file_path_obj)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load module from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules["transform_module"] = module
        spec.loader.exec_module(module)

        # Cache and return function
        self._loaded_modules[cache_key] = module
        func = getattr(module, function_name, None)

        if func is None:
            raise AttributeError(
                f"Function '{function_name}' not found in {file_path}"
            )

        if not callable(func):
            raise TypeError(f"'{function_name}' in {file_path} is not callable")

        return func

    def _load_function_from_module(self, module_path: str) -> Callable:
        """
        Load a function from a Python module.

        Args:
            module_path: Module path (e.g., "my_package.transform")

        Returns:
            The loaded function (assumes default function name 'transform')
        """
        if module_path in self._loaded_modules:
            module = self._loaded_modules[module_path]
            return getattr(module, "transform", None) or module

        module = importlib.import_module(module_path)
        self._loaded_modules[module_path] = module

        # Try to get 'transform' function, otherwise return the module
        func = getattr(module, "transform", None)
        if func is None:
            # If module itself is callable, use it
            if callable(module):
                return module
            raise AttributeError(f"No 'transform' function found in module {module_path}")

        return func

    def transform_with_class(
        self,
        data: pd.DataFrame,
        transform_class: Union[type, str],
        **init_kwargs: Any,
    ) -> pd.DataFrame:
        """
        Transform data using a Python class.

        The class should have a 'transform' method that takes a DataFrame and returns a DataFrame.

        Args:
            data: Input DataFrame to transform
            transform_class: Class or path to class (e.g., "module:TransformClass")
            **init_kwargs: Arguments to pass to class constructor

        Returns:
            Transformed DataFrame

        Example:
            class MyTransformer:
                def __init__(self, multiplier=2):
                    self.multiplier = multiplier

                def transform(self, df):
                    df['new_col'] = df['old_col'] * self.multiplier
                    return df

            transformer = CodeTransformer()
            result = transformer.transform_with_class(data, MyTransformer, multiplier=3)
        """
        if data.empty:
            logger.warning("Input DataFrame is empty, returning empty DataFrame")
            return pd.DataFrame()

        logger.debug(f"Transforming {len(data)} rows using class-based transformation")

        try:
            # Get the class
            if isinstance(transform_class, type):
                cls = transform_class
            elif isinstance(transform_class, str):
                if ":" in transform_class:
                    module_path, class_name = transform_class.rsplit(":", 1)
                    module = importlib.import_module(module_path)
                    cls = getattr(module, class_name)
                else:
                    raise ValueError(
                        "Class path must be in format 'module:ClassName'"
                    )
            else:
                raise ValueError(f"Invalid transform_class type: {type(transform_class)}")

            # Instantiate and transform
            instance = cls(**init_kwargs)
            if not hasattr(instance, "transform"):
                raise AttributeError(f"Class {cls.__name__} does not have a 'transform' method")

            result = instance.transform(data)

            if not isinstance(result, pd.DataFrame):
                raise ValueError(
                    f"Transform method must return a DataFrame, got {type(result)}"
                )

            logger.debug(f"Class-based transformation complete: {len(result)} rows output")
            return result

        except Exception as e:
            logger.error(f"Error during class-based transformation: {e}", exc_info=True)
            raise

    def register_function_version(
        self,
        transformation_id: str,
        version: int,
        transform_func: Union[Callable, str],
        description: Optional[str] = None,
    ) -> None:
        """
        Register a versioned function for a transformation.

        Args:
            transformation_id: Unique identifier for the transformation
            version: Version number
            transform_func: Function or path to function for this version
            description: Optional description of changes in this version
        """
        if transformation_id not in self._function_versions:
            self._function_versions[transformation_id] = {}
        
        self._function_versions[transformation_id][version] = transform_func
        
        # Track in history
        if transformation_id not in self._transformation_history:
            self._transformation_history[transformation_id] = []
        
        func_repr = (
            transform_func.__name__ if callable(transform_func) else str(transform_func)
        )
        self._transformation_history[transformation_id].append({
            "version": version,
            "function": func_repr,
            "description": description,
            "timestamp": datetime.now().isoformat(),
        })
        
        logger.info(f"Registered function version {version} for transformation '{transformation_id}'")

    def get_function_version(
        self, transformation_id: str, version: int
    ) -> Optional[Union[Callable, str]]:
        """
        Get a specific version of a transform function.

        Args:
            transformation_id: Transformation identifier
            version: Version number

        Returns:
            Function or function path, or None if not found
        """
        if transformation_id in self._function_versions:
            return self._function_versions[transformation_id].get(version)
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
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Transform data using a registered versioned function.

        Args:
            data: Input DataFrame
            transformation_id: Transformation identifier
            version: Version to use (uses latest if not specified)
            input_schema: Optional input schema
            output_schema: Optional output schema
            **kwargs: Additional arguments to pass to function

        Returns:
            Transformed DataFrame
        """
        if transformation_id not in self._function_versions:
            raise ValueError(f"Transformation '{transformation_id}' not found")

        if version is None:
            # Use latest version
            versions = sorted(self._function_versions[transformation_id].keys(), reverse=True)
            if not versions:
                raise ValueError(f"No versions found for transformation '{transformation_id}'")
            version = versions[0]
            logger.debug(f"Using latest version {version} for '{transformation_id}'")

        func = self.get_function_version(transformation_id, version)
        if func is None:
            raise ValueError(
                f"Version {version} not found for transformation '{transformation_id}'"
            )

        return self.transform(
            data,
            func,
            transformation_id=transformation_id,
            version=version,
            input_schema=input_schema,
            output_schema=output_schema,
            evolve_schema=True,
            **kwargs,
        )

    def _track_transformation(
        self,
        transformation_id: str,
        version: Optional[int],
        transform_func: Union[Callable, str],
    ) -> None:
        """Track transformation execution."""
        if transformation_id not in self._transformation_history:
            self._transformation_history[transformation_id] = []
        
        func_repr = (
            transform_func.__name__ if callable(transform_func) else str(transform_func)
        )
        self._transformation_history[transformation_id].append({
            "version": version or 1,
            "function": func_repr,
            "timestamp": datetime.now().isoformat(),
            "executed": True,
        })

