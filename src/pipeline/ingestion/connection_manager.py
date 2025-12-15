"""Database connection manager with connection pooling."""

import logging
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.pool import Pool

from pipeline.config.settings import DatabaseSettings

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages database connections with connection pooling and health checks."""

    def __init__(
        self,
        database_url: str,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_recycle: int = 3600,
        pool_timeout: int = 30,
        echo: bool = False,
    ):
        """
        Initialize connection manager.

        Args:
            database_url: Database connection URL
            pool_size: Number of connections to maintain in the pool
            max_overflow: Maximum number of connections to create beyond pool_size
            pool_recycle: Time in seconds after which a connection is recycled
            pool_timeout: Time in seconds to wait for a connection from the pool
            echo: Whether to log SQL statements
        """
        self.database_url = database_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_recycle = pool_recycle
        self.pool_timeout = pool_timeout
        self.echo = echo
        self._engine: Optional[Engine] = None

    @classmethod
    def from_settings(
        cls, database_url: str, settings: Optional[DatabaseSettings] = None
    ) -> "ConnectionManager":
        """
        Create ConnectionManager from settings.

        Args:
            database_url: Database connection URL
            settings: Database settings object

        Returns:
            ConnectionManager instance
        """
        if settings is None:
            from pipeline.config.settings import settings as app_settings

            settings = app_settings.database

        return cls(
            database_url=database_url,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_recycle=settings.pool_recycle,
            pool_timeout=settings.pool_timeout,
        )

    @property
    def engine(self) -> Engine:
        """Get or create the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        engine = create_engine(
            self.database_url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_recycle=self.pool_recycle,
            pool_timeout=self.pool_timeout,
            pool_pre_ping=True,  # Enable connection health checks
            echo=self.echo,
            future=True,
        )

        # Add event listeners for connection pool events
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            """Set connection-level settings if needed."""
            pass

        @event.listens_for(engine, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            """Log connection checkout."""
            logger.debug("Connection checked out from pool")

        @event.listens_for(engine, "checkin")
        def receive_checkin(dbapi_conn, connection_record):
            """Log connection checkin."""
            logger.debug("Connection returned to pool")

        return engine

    def health_check(self) -> bool:
        """
        Perform a health check on the database connection.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.debug("Database health check passed")
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool.

        Yields:
            Database connection

        Example:
            with connection_manager.get_connection() as conn:
                result = conn.execute(text("SELECT * FROM table"))
        """
        conn = self.engine.connect()
        try:
            yield conn
        except DisconnectionError:
            logger.warning("Connection lost, attempting to reconnect")
            conn.close()
            # Get a new connection
            conn = self.engine.connect()
            yield conn
        finally:
            conn.close()

    def get_pool_status(self) -> dict:
        """
        Get current connection pool status.

        Returns:
            Dictionary with pool statistics
        """
        pool: Pool = self.engine.pool
        return {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
        }

    def dispose(self) -> None:
        """Close all connections and dispose of the engine."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("Connection pool disposed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.dispose()

