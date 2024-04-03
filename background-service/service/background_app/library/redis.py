"""Module with connection parameters to Redis service."""
import os

from redis.asyncio import ConnectionPool

__all__ = [
    'pool'
]


def create_pool() -> ConnectionPool:
    """Returns async Redis connection pool.

    Returns: ConnectionPool

    """
    host = os.getenv('REDIS_HOST')
    port = int(os.getenv('REDIS_PORT', 6379))
    password = os.getenv('REDIS_PASSWORD', '')
    db_index = int(os.getenv('REDIS_DB_INDEX', 0))

    if password:
        return ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db_index,
            decode_responses=True
        )
    else:
        return ConnectionPool(
            host=host,
            port=port,
            db=db_index,
            decode_responses=True
        )


pool = create_pool()
