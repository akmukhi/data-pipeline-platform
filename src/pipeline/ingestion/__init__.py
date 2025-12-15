"""Data ingestion module."""

from pipeline.ingestion.batch_ingestor import BatchIngestor
from pipeline.ingestion.connection_manager import ConnectionManager

__all__ = ["BatchIngestor", "ConnectionManager"]

