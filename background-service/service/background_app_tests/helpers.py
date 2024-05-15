"""Module with helper functions and classes."""

from functools import wraps
from typing import Callable, List, Optional
from unittest.mock import AsyncMock, patch

from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage


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

    def __init__(self, task_state: Optional[str] = None,
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

    @staticmethod
    async def override_parse_body() -> List[AsyncMock]:
        """Override parse_body dependency.

        Returns: AsyncMock
        """

        return [AsyncMock()]

    @staticmethod
    async def override_get_file_storage() -> AsyncMock:
        """Override get_file_storage dependency.

        Returns: AsyncMock
        """

        return AsyncMock()

    @staticmethod
    async def override_get_redis_with_instance() -> Storage:
        """Override get_redis_storage dependency with real RedisStorage.

        Returns: Storage
        """
        with patch.object(Storage, '__init__') as mock_init:
            mock_init.return_value = None

            with patch.object(
                    Storage, 'get_task_state'
            ) as mock_get_task_state:
                mock_get_task_state.return_value = 'test-task'

                storage = Storage()

        return storage

    @staticmethod
    async def override_get_file_storage_with_instance() -> AsyncMock:
        """Override get_file_storage dependency with real FileStorage.

        Returns: AsyncMock
        """
        with patch.object(FileStorage, '__init__') as mock_init:
            mock_init.return_value = None

            return FileStorage()
