"""Module with helper functions and classes."""

from functools import wraps
from typing import Any, Callable, List, Optional
from unittest.mock import AsyncMock

import gstream.storage.redis


def mock_decorator(func: Callable) -> Callable:
    """Decorate by doing nothing.

    Args:
        func: function for decorating

    Returns: wrapper
    """
    @wraps(func)
    async def decorated_function(**kwargs) -> object:
        """Return function without changes.

        Args:
            **kwargs: function arguments

        Returns: object
        """
        return await func(**kwargs)

    return decorated_function


class DependencyMock:
    """Class for dependencies overriding."""

    def __init__(self, task_state: Any = None,
                 log: Optional[str] = None):
        """Initialize method.

        Args:
            task_state: task_state return value
            log: log return value
        """

        self.task_state = task_state
        self.log = log

    async def override_get_redis_storage(self) -> AsyncMock:
        """Override get_redis_storage dependency.

        Returns: AsyncMock
        """
        storage = AsyncMock()
        storage.get_task_state.return_value = self.task_state
        storage.get_log.return_value = self.log

        return storage

    async def override_parse_body(self) -> List[AsyncMock]:
        """Override parse_body dependency.

        Returns: AsyncMock
        """
        data = AsyncMock()

        return [data]

    async def override_get_file_storage(self) -> AsyncMock:
        """Override get_file_storage dependency.

        Returns: AsyncMock
        """
        storage = AsyncMock()

        return storage

    async def override_redis_for_kill_task(self) -> AsyncMock:
        """Override get_redis_storage dependency for kill_task.

        Returns: AsyncMock
        """
        from gstream.storage.redis import Storage
        from unittest.mock import patch
        with patch.object(gstream.storage.redis.Storage, '__init__') as m:
            m.return_value = None
            storage = Storage()

        await storage.get_task_state(task_id=1)

        return storage


import asyncio
async def qq():
    over = DependencyMock()
    return await over.override_redis_for_kill_task()

asyncio.run(qq())
