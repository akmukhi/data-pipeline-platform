"""Ingestion layer for data pipeline."""

from pipeline.ingestion.batch_ingestor import BatchIngestor
from pipeline.ingestion.connection_manager import ConnectionManager

__all__ = ["BatchIngestor", "ConnectionManager"]

