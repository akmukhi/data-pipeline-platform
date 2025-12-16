"""Structured logging with correlation IDs and pipeline stage tracking."""

import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Any, Dict, Optional

try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False
    structlog = None  # type: ignore

from pipeline.config.settings import settings

# Context variables for correlation tracking
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)
pipeline_id_var: ContextVar[Optional[str]] = ContextVar("pipeline_id", default=None)
pipeline_stage_var: ContextVar[Optional[str]] = ContextVar("pipeline_stage", default=None)
task_id_var: ContextVar[Optional[str]] = ContextVar("task_id", default=None)


class CorrelationFilter(logging.Filter):
    """Logging filter that adds correlation IDs to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation context to log record."""
        record.correlation_id = correlation_id_var.get() or "N/A"
        record.pipeline_id = pipeline_id_var.get() or "N/A"
        record.pipeline_stage = pipeline_stage_var.get() or "N/A"
        record.task_id = task_id_var.get() or "N/A"
        return True


class StructuredFormatter(logging.Formatter):
    """Structured formatter that formats logs as JSON or text."""

    def __init__(self, format_type: str = "json"):
        """
        Initialize formatter.

        Args:
            format_type: Format type ("json", "text", or "console")
        """
        super().__init__()
        self.format_type = format_type.lower()

    def format(self, record: logging.LogRecord) -> str:
        """Format log record."""
        if self.format_type == "json":
            return self._format_json(record)
        elif self.format_type == "text":
            return self._format_text(record)
        elif self.format_type == "console":
            return self._format_console(record)
        else:
            return super().format(record)

    def _format_json(self, record: logging.LogRecord) -> str:
        """Format as JSON."""
        import json
        import time

        log_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", "N/A"),
            "pipeline_id": getattr(record, "pipeline_id", "N/A"),
            "pipeline_stage": getattr(record, "pipeline_stage", "N/A"),
            "task_id": getattr(record, "task_id", "N/A"),
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        if hasattr(record, "__dict__"):
            for key, value in record.__dict__.items():
                if key not in [
                    "name",
                    "msg",
                    "args",
                    "created",
                    "filename",
                    "funcName",
                    "levelname",
                    "levelno",
                    "lineno",
                    "module",
                    "msecs",
                    "message",
                    "pathname",
                    "process",
                    "processName",
                    "relativeCreated",
                    "thread",
                    "threadName",
                    "exc_info",
                    "exc_text",
                    "stack_info",
                    "correlation_id",
                    "pipeline_id",
                    "pipeline_stage",
                    "task_id",
                ]:
                    log_data[key] = value

        return json.dumps(log_data, default=str)

    def _format_text(self, record: logging.LogRecord) -> str:
        """Format as structured text."""
        correlation_id = getattr(record, "correlation_id", "N/A")
        pipeline_id = getattr(record, "pipeline_id", "N/A")
        pipeline_stage = getattr(record, "pipeline_stage", "N/A")
        task_id = getattr(record, "task_id", "N/A")

        msg = (
            f"{self.formatTime(record)} | "
            f"{record.levelname:8s} | "
            f"correlation_id={correlation_id} | "
            f"pipeline_id={pipeline_id} | "
            f"stage={pipeline_stage} | "
            f"task_id={task_id} | "
            f"{record.name} | "
            f"{record.getMessage()}"
        )

        if record.exc_info:
            msg += f"\n{self.formatException(record.exc_info)}"

        return msg

    def _format_console(self, record: logging.LogRecord) -> str:
        """Format for console output with colors."""
        try:
            import colorama

            colorama.init()
        except ImportError:
            # colorama not available, use text format
            return self._format_text(record)

        colors = {
            "DEBUG": colorama.Fore.CYAN,
            "INFO": colorama.Fore.GREEN,
            "WARNING": colorama.Fore.YELLOW,
            "ERROR": colorama.Fore.RED,
            "CRITICAL": colorama.Fore.RED + colorama.Style.BRIGHT,
        }

        reset = colorama.Style.RESET_ALL
        color = colors.get(record.levelname, "")

        correlation_id = getattr(record, "correlation_id", "N/A")
        pipeline_id = getattr(record, "pipeline_id", "N/A")
        pipeline_stage = getattr(record, "pipeline_stage", "N/A")

        msg = (
            f"{color}{self.formatTime(record)} | "
            f"{record.levelname:8s}{reset} | "
            f"cid={correlation_id[:8]} | "
            f"pid={pipeline_id[:8] if pipeline_id != 'N/A' else 'N/A'} | "
            f"{pipeline_stage} | "
            f"{record.name} | "
            f"{record.getMessage()}{reset}"
        )

        if record.exc_info:
            msg += f"\n{color}{self.formatException(record.exc_info)}{reset}"

        return msg


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID from context."""
    return correlation_id_var.get()


def set_correlation_id(correlation_id: str) -> None:
    """Set correlation ID in context."""
    correlation_id_var.set(correlation_id)


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


def get_pipeline_id() -> Optional[str]:
    """Get current pipeline ID from context."""
    return pipeline_id_var.get()


def set_pipeline_id(pipeline_id: str) -> None:
    """Set pipeline ID in context."""
    pipeline_id_var.set(pipeline_id)


def get_pipeline_stage() -> Optional[str]:
    """Get current pipeline stage from context."""
    return pipeline_stage_var.get()


def set_pipeline_stage(stage: str) -> None:
    """Set pipeline stage in context."""
    pipeline_stage_var.set(stage)


def get_task_id() -> Optional[str]:
    """Get current task ID from context."""
    return task_id_var.get()


def set_task_id(task_id: str) -> None:
    """Set task ID in context."""
    task_id_var.set(task_id)


class PipelineLogger:
    """Logger wrapper with pipeline context management."""

    def __init__(self, name: str, correlation_id: Optional[str] = None):
        """
        Initialize pipeline logger.

        Args:
            name: Logger name
            correlation_id: Optional correlation ID (generated if not provided)
        """
        self.name = name
        self.logger = logging.getLogger(name)

        if correlation_id:
            set_correlation_id(correlation_id)
        elif not get_correlation_id():
            set_correlation_id(generate_correlation_id())

    def _add_context(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add pipeline context to log extra fields."""
        context = {
            "correlation_id": get_correlation_id(),
            "pipeline_id": get_pipeline_id(),
            "pipeline_stage": get_pipeline_stage(),
            "task_id": get_task_id(),
        }

        if extra:
            context.update(extra)

        return context

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(message, extra=self._add_context(kwargs))

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(message, extra=self._add_context(kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(message, extra=self._add_context(kwargs))

    def error(self, message: str, exc_info: bool = False, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(message, extra=self._add_context(kwargs), exc_info=exc_info)

    def critical(self, message: str, exc_info: bool = False, **kwargs: Any) -> None:
        """Log critical message."""
        self.logger.critical(
            message, extra=self._add_context(kwargs), exc_info=exc_info
        )

    def with_stage(self, stage: str):
        """Create a context manager for pipeline stage."""
        return PipelineStageContext(self, stage)

    def with_pipeline(self, pipeline_id: str):
        """Create a context manager for pipeline."""
        return PipelineContext(self, pipeline_id)

    def with_task(self, task_id: str):
        """Create a context manager for task."""
        return TaskContext(self, task_id)


class PipelineStageContext:
    """Context manager for pipeline stage logging."""

    def __init__(self, logger: PipelineLogger, stage: str):
        self.logger = logger
        self.stage = stage
        self.previous_stage = None

    def __enter__(self):
        self.previous_stage = get_pipeline_stage()
        set_pipeline_stage(self.stage)
        self.logger.info(f"Pipeline stage started: {self.stage}", stage=self.stage)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(
                f"Pipeline stage failed: {self.stage}",
                stage=self.stage,
                exc_info=True,
            )
        else:
            self.logger.info(f"Pipeline stage completed: {self.stage}", stage=self.stage)

        if self.previous_stage:
            set_pipeline_stage(self.previous_stage)
        else:
            pipeline_stage_var.set(None)
        return False


class PipelineContext:
    """Context manager for pipeline logging."""

    def __init__(self, logger: PipelineLogger, pipeline_id: str):
        self.logger = logger
        self.pipeline_id = pipeline_id
        self.previous_pipeline_id = None

    def __enter__(self):
        self.previous_pipeline_id = get_pipeline_id()
        set_pipeline_id(self.pipeline_id)
        self.logger.info(
            f"Pipeline started: {self.pipeline_id}", pipeline_id=self.pipeline_id
        )
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(
                f"Pipeline failed: {self.pipeline_id}",
                pipeline_id=self.pipeline_id,
                exc_info=True,
            )
        else:
            self.logger.info(
                f"Pipeline completed: {self.pipeline_id}", pipeline_id=self.pipeline_id
            )

        if self.previous_pipeline_id:
            set_pipeline_id(self.previous_pipeline_id)
        else:
            pipeline_id_var.set(None)
        return False


class TaskContext:
    """Context manager for task logging."""

    def __init__(self, logger: PipelineLogger, task_id: str):
        self.logger = logger
        self.task_id = task_id
        self.previous_task_id = None

    def __enter__(self):
        self.previous_task_id = get_task_id()
        set_task_id(self.task_id)
        self.logger.info(f"Task started: {self.task_id}", task_id=self.task_id)
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(
                f"Task failed: {self.task_id}", task_id=self.task_id, exc_info=True
            )
        else:
            self.logger.info(f"Task completed: {self.task_id}", task_id=self.task_id)

        if self.previous_task_id:
            set_task_id(self.previous_task_id)
        else:
            task_id_var.set(None)
        return False


def setup_logging(
    level: Optional[str] = None,
    format_type: Optional[str] = None,
    use_structlog: bool = False,
) -> None:
    """
    Set up structured logging for the application.

    Args:
        level: Log level (uses settings if not provided)
        format_type: Log format type (uses settings if not provided)
        use_structlog: If True, use structlog instead of standard logging
    """
    log_level = level or settings.LOG_LEVEL
    log_format = format_type or settings.LOG_FORMAT

    # Validate and set level
    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    numeric_level = valid_levels.get(log_level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)

    # Add correlation filter
    console_handler.addFilter(CorrelationFilter())

    # Set formatter
    if use_structlog and STRUCTLOG_AVAILABLE:
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer() if log_format == "json" else structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
    else:
        # Use standard logging with structured formatter
        formatter = StructuredFormatter(format_type=log_format)
        console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)

    logging.info(
        f"Logging configured: level={log_level}, format={log_format}, structlog={use_structlog}"
    )


def get_logger(name: str, correlation_id: Optional[str] = None) -> PipelineLogger:
    """
    Get a pipeline logger instance.

    Args:
        name: Logger name
        correlation_id: Optional correlation ID

    Returns:
        PipelineLogger instance
    """
    return PipelineLogger(name, correlation_id=correlation_id)


# Initialize logging on import if not already configured
_logging_initialized = False


def ensure_logging_initialized() -> None:
    """Ensure logging is initialized."""
    global _logging_initialized
    if not _logging_initialized:
        setup_logging()
        _logging_initialized = True


# Auto-initialize on import
ensure_logging_initialized()

