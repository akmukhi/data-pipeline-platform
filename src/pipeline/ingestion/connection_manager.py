"""Database connection pool management with health checks and automatic reconnection."""

import logging
import time
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.pool import Pool

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages database connection pools with health checks and automatic reconnection."""

    def __init__(
        self,
        database_url: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_pre_ping: bool = True,
        pool_recycle: int = 3600,
        echo: bool = False,
    ):
        """
        Initialize connection manager.

        Args:
            database_url: Database connection URL (e.g., postgresql://user:pass@host/db)
            pool_size: Number of connections to maintain in the pool
            max_overflow: Maximum number of connections to create beyond pool_size
            pool_pre_ping: If True, validate connections before using them
            pool_recycle: Number of seconds after which connections are recycled
            echo: If True, log all SQL statements
        """
        self.database_url = database_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_pre_ping = pool_pre_ping
        self.pool_recycle = pool_recycle
        self.echo = echo

        # Create engine with connection pooling
        self.engine = create_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=pool_pre_ping,
            pool_recycle=pool_recycle,
            echo=echo,
            future=True,
        )

        # Register event listeners for connection health checks
        self._register_connection_events()

        logger.info(
            f"Initialized ConnectionManager for {self._get_database_name()} "
            f"(pool_size={pool_size}, max_overflow={max_overflow})"
        )

    def _get_database_name(self) -> str:
        """Extract database name from URL for logging."""
        try:
            # Extract database name from URL (basic parsing)
            if "@" in self.database_url:
                parts = self.database_url.split("@")[-1]
                if "/" in parts:
                    return parts.split("/")[-1].split("?")[0]
            return "unknown"
        except Exception:
            return "unknown"

    def _register_connection_events(self) -> None:
        """Register SQLAlchemy event listeners for connection management."""

        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            """Set connection-level settings if needed."""
            # This can be customized per database type if needed
            pass

        @event.listens_for(Pool, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            """Handle connection checkout with health check."""
            if self.pool_pre_ping:
                # Health check is handled by pool_pre_ping
                pass

        @event.listens_for(Pool, "checkin")
        def receive_checkin(dbapi_conn, connection_record):
            """Handle connection checkin."""
            pass

    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool.

        Yields:
            Connection: SQLAlchemy connection object

        Example:
            with connection_manager.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
        """
        conn = None
        try:
            conn = self.engine.connect()
            yield conn
        except DisconnectionError as e:
            logger.warning(f"Connection lost, will retry: {e}")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            # Recreate engine and retry once
            self._recreate_engine()
            conn = self.engine.connect()
            yield conn
        except Exception as e:
            logger.error(f"Error getting connection: {e}", exc_info=True)
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
            raise
        finally:
            if conn:
                conn.close()

    def get_engine(self) -> Engine:
        """
        Get the SQLAlchemy engine.

        Returns:
            Engine: SQLAlchemy engine instance
        """
        return self.engine

    def health_check(self, max_retries: int = 3, retry_delay: float = 1.0) -> bool:
        """
        Perform a health check on the database connection.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Delay in seconds between retries

        Returns:
            True if connection is healthy, False otherwise
        """
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    conn.execute(text("SELECT 1"))
                logger.debug("Database health check passed")
                return True
            except Exception as e:
                logger.warning(
                    f"Health check failed (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    logger.error("Database health check failed after all retries")
                    return False
        return False

    def _recreate_engine(self) -> None:
        """Recreate the engine after a connection failure."""
        logger.info("Recreating database engine after connection failure")
        try:
            self.engine.dispose()
        except Exception as e:
            logger.warning(f"Error disposing old engine: {e}")

        self.engine = create_engine(
            self.database_url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=self.pool_pre_ping,
            pool_recycle=self.pool_recycle,
            echo=self.echo,
            future=True,
        )
        self._register_connection_events()

    def get_pool_status(self) -> dict:
        """
        Get connection pool status information.

        Returns:
            Dictionary with pool statistics
        """
        pool = self.engine.pool
        return {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
        }

    def close(self) -> None:
        """Close all connections in the pool."""
        logger.info("Closing connection pool")
        try:
            self.engine.dispose()
        except Exception as e:
            logger.warning(f"Error closing connection pool: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

