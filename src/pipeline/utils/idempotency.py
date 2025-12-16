"""Redis-based idempotency key management for preventing duplicate processing."""

import hashlib
import json
import logging
import time
from typing import Any, Dict, Optional, Tuple, Union

try:
    import redis
    from redis import Redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    Redis = None  # type: ignore

from pipeline.config.settings import settings

logger = logging.getLogger(__name__)


class IdempotencyKeyError(Exception):
    """Exception raised for idempotency key errors."""

    pass


class IdempotencyManager:
    """Manages idempotency keys using Redis to prevent duplicate processing."""

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        redis_url: Optional[str] = None,
        key_prefix: str = "idempotency:",
        default_ttl_days: Optional[int] = None,
        enabled: Optional[bool] = None,
    ):
        """
        Initialize idempotency manager.

        Args:
            redis_client: Optional Redis client instance
            redis_url: Optional Redis URL (used if redis_client not provided)
            key_prefix: Prefix for idempotency keys in Redis (default: "idempotency:")
            default_ttl_days: Default TTL in days for idempotency keys (uses settings if None)
            enabled: Whether idempotency checking is enabled (uses settings if None)

        Raises:
            IdempotencyKeyError: If Redis is not available and required
        """
        if not REDIS_AVAILABLE:
            logger.warning("Redis not available. Idempotency checking will be disabled.")
            self.enabled = False
            self.redis_client = None
            return

        if redis_client is not None:
            self.redis_client = redis_client
        elif redis_url is not None:
            self.redis_client = redis.from_url(redis_url)
        else:
            # Use settings default
            try:
                self.redis_client = redis.from_url(settings.REDIS_URL)
            except Exception as e:
                logger.warning(f"Could not connect to Redis: {e}. Idempotency disabled.")
                self.redis_client = None

        self.key_prefix = key_prefix
        self.default_ttl_days = default_ttl_days or settings.IDEMPOTENCY_KEY_TTL_DAYS
        self.enabled = (
            enabled if enabled is not None else settings.IDEMPOTENCY_ENABLED
        ) and (self.redis_client is not None)

        if self.enabled:
            logger.info(
                f"IdempotencyManager initialized (ttl={self.default_ttl_days} days, "
                f"prefix={key_prefix})"
            )
        else:
            logger.info("IdempotencyManager initialized (disabled)")

    def generate_key(
        self, source_id: str, content: Union[str, bytes, Dict, Any] = ""
    ) -> str:
        """
        Generate an idempotency key from source_id and content.

        Uses SHA-256 hash of (source_id + content_hash) as per the plan.

        Args:
            source_id: Source identifier (e.g., task_id, pipeline_id, or data source)
            content: Content to hash (can be string, bytes, dict, or any serializable object)

        Returns:
            Idempotency key string
        """
        # Convert content to string for hashing
        if isinstance(content, bytes):
            content_str = content.decode("utf-8", errors="ignore")
        elif isinstance(content, dict):
            content_str = json.dumps(content, sort_keys=True)
        elif not isinstance(content, str):
            try:
                content_str = json.dumps(content, sort_keys=True, default=str)
            except (TypeError, ValueError):
                content_str = str(content)

        # Create content hash
        content_hash = hashlib.sha256(content_str.encode("utf-8")).hexdigest()

        # Combine source_id and content_hash, then hash again
        combined = f"{source_id}:{content_hash}"
        key_hash = hashlib.sha256(combined.encode("utf-8")).hexdigest()

        # Return prefixed key
        return f"{self.key_prefix}{key_hash}"

    def check_key(self, key: str) -> bool:
        """
        Check if an idempotency key exists (indicating duplicate request).

        Args:
            key: Idempotency key to check

        Returns:
            True if key exists (duplicate), False otherwise
        """
        if not self.enabled or not self.redis_client:
            return False

        try:
            exists = self.redis_client.exists(key)
            return bool(exists)
        except Exception as e:
            logger.error(f"Error checking idempotency key: {e}", exc_info=True)
            # On error, allow processing (fail open)
            return False

    def set_key(
        self, key: str, value: Optional[Dict[str, Any]] = None, ttl_days: Optional[int] = None
    ) -> bool:
        """
        Set an idempotency key in Redis with TTL.

        Args:
            key: Idempotency key to set
            value: Optional value to store with the key (for tracking)
            ttl_days: TTL in days (uses default if None)

        Returns:
            True if key was set, False otherwise
        """
        if not self.enabled or not self.redis_client:
            return False

        ttl_seconds = (ttl_days or self.default_ttl_days) * 24 * 60 * 60

        try:
            if value is not None:
                # Store value as JSON
                value_str = json.dumps(value, default=str)
                self.redis_client.setex(key, ttl_seconds, value_str)
            else:
                # Store timestamp as value
                value_str = json.dumps({"timestamp": time.time()}, default=str)
                self.redis_client.setex(key, ttl_seconds, value_str)

            logger.debug(f"Set idempotency key: {key} (ttl={ttl_days or self.default_ttl_days} days)")
            return True
        except Exception as e:
            logger.error(f"Error setting idempotency key: {e}", exc_info=True)
            return False

    def get_key(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get the value stored with an idempotency key.

        Args:
            key: Idempotency key

        Returns:
            Stored value as dictionary, or None if key doesn't exist
        """
        if not self.enabled or not self.redis_client:
            return None

        try:
            value_str = self.redis_client.get(key)
            if value_str is None:
                return None

            if isinstance(value_str, bytes):
                value_str = value_str.decode("utf-8")

            return json.loads(value_str)
        except Exception as e:
            logger.error(f"Error getting idempotency key: {e}", exc_info=True)
            return None

    def delete_key(self, key: str) -> bool:
        """
        Delete an idempotency key from Redis.

        Args:
            key: Idempotency key to delete

        Returns:
            True if key was deleted, False otherwise
        """
        if not self.enabled or not self.redis_client:
            return False

        try:
            deleted = self.redis_client.delete(key)
            logger.debug(f"Deleted idempotency key: {key}")
            return bool(deleted)
        except Exception as e:
            logger.error(f"Error deleting idempotency key: {e}", exc_info=True)
            return False

    def check_and_set(
        self,
        source_id: str,
        content: Union[str, bytes, Dict, Any] = "",
        value: Optional[Dict[str, Any]] = None,
        ttl_days: Optional[int] = None,
    ) -> Tuple[bool, str]:
        """
        Check if key exists and set it if it doesn't (atomic operation).

        This is useful for preventing duplicate processing.

        Args:
            source_id: Source identifier
            content: Content to hash
            value: Optional value to store
            ttl_days: Optional TTL in days

        Returns:
            Tuple of (is_duplicate, key)
            - is_duplicate: True if key already existed (duplicate request)
            - key: The idempotency key

        Example:
            is_dup, key = manager.check_and_set("task_123", {"data": "..."})
            if is_dup:
                return {"status": "already_processed"}
            # Process the task...
            # Key is already set, so future duplicates will be detected
        """
        key = self.generate_key(source_id, content)

        if not self.enabled or not self.redis_client:
            return False, key

        try:
            ttl_seconds = (ttl_days or self.default_ttl_days) * 24 * 60 * 60

            # Use SETNX (set if not exists) for atomic check-and-set
            if value is not None:
                value_str = json.dumps(value, default=str)
            else:
                value_str = json.dumps({"timestamp": time.time()}, default=str)

            # Try to set the key (returns 1 if set, 0 if already exists)
            was_set = self.redis_client.set(
                key, value_str, ex=ttl_seconds, nx=True
            )

            is_duplicate = not bool(was_set)

            if is_duplicate:
                logger.info(f"Duplicate request detected (key: {key})")
            else:
                logger.debug(f"Set new idempotency key: {key}")

            return is_duplicate, key

        except Exception as e:
            logger.error(f"Error in check_and_set: {e}", exc_info=True)
            # On error, allow processing (fail open)
            return False, key

    def is_enabled(self) -> bool:
        """Check if idempotency checking is enabled."""
        return self.enabled

    def test_connection(self) -> bool:
        """Test Redis connection."""
        if not self.enabled or not self.redis_client:
            return False

        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis connection test failed: {e}")
            return False


# Global idempotency manager instance (lazy initialization)
_global_manager: Optional[IdempotencyManager] = None


def get_idempotency_manager() -> IdempotencyManager:
    """
    Get or create the global idempotency manager instance.

    Returns:
        IdempotencyManager instance
    """
    global _global_manager
    if _global_manager is None:
        _global_manager = IdempotencyManager()
    return _global_manager


def idempotent(
    source_id_field: str = "id",
    content_fields: Optional[list] = None,
    ttl_days: Optional[int] = None,
    manager: Optional[IdempotencyManager] = None,
):
    """
    Decorator for making functions idempotent.

    Args:
        source_id_field: Field name in kwargs to use as source_id (default: "id")
        content_fields: List of field names in kwargs to use as content (default: all kwargs)
        ttl_days: Optional TTL in days (uses manager default if None)
        manager: Optional IdempotencyManager instance (uses global if None)

    Returns:
        Decorated function

    Example:
        @idempotent(source_id_field="task_id", content_fields=["query", "parameters"])
        def process_task(task_id, query, parameters):
            # This function will only execute once for the same task_id + query + parameters
            return execute_task(query, parameters)
    """
    def decorator(func):
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            idempotency_manager = manager or get_idempotency_manager()

            if not idempotency_manager.is_enabled():
                # If idempotency is disabled, just run the function
                return func(*args, **kwargs)

            # Extract source_id
            source_id = kwargs.get(source_id_field) or str(id(args) if args else "")

            # Extract content
            if content_fields:
                content = {field: kwargs.get(field) for field in content_fields if field in kwargs}
            else:
                content = kwargs

            # Check and set idempotency key
            is_duplicate, key = idempotency_manager.check_and_set(
                source_id, content, ttl_days=ttl_days
            )

            if is_duplicate:
                # Return cached result if available
                cached_value = idempotency_manager.get_key(key)
                if cached_value and "result" in cached_value:
                    logger.info(f"Skipping duplicate execution of {func.__name__} (key: {key})")
                    return cached_value["result"]

                # Otherwise, raise error or return None
                raise IdempotencyKeyError(
                    f"Duplicate execution detected for {func.__name__} (key: {key})"
                )

            # Execute function
            try:
                result = func(*args, **kwargs)

                # Store result in idempotency key (optional, for caching)
                if ttl_days:
                    idempotency_manager.set_key(
                        key, {"result": result, "timestamp": time.time()}, ttl_days=ttl_days
                    )

                return result
            except Exception as e:
                # On error, delete the key so it can be retried
                idempotency_manager.delete_key(key)
                raise

        return wrapper

    return decorator

