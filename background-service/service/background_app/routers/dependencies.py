"""Module with  FastApi dependencies functions."""

import os
from pathlib import Path

from fastapi import Request

from background_app.library.redis import pool
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage

__all__ = [
    'get_redis_storage',
    'get_file_storage',
    'parse_body'
]


async def get_redis_storage() -> RedisStorage:
    """Returns Redis Storage connection.

    Returns: RedisStorage

    """
    storage = RedisStorage(pool=pool)
    try:
        yield storage
    finally:
        await storage.close()


async def get_file_storage() -> FileStorage:
    """Returns File Storage connection.

    Returns: FileStorage

    """
    storage = FileStorage(
        root=Path(os.getenv('STORAGE_ROOT'))
    )
    return storage


async def parse_body(request: Request) -> bytes:
    """Returns binary data from request body.

    Args:
        request: Request

    Returns: bytes

    """
    data: bytes = await request.body()
    return data
