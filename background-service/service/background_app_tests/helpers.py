"""Module with helper functions and classes."""

from functools import wraps
from typing import Any, Callable
from unittest.mock import AsyncMock


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


class Override:
    """Class for dependencies overriding."""

    def __init__(self, return_value: Any):
        """Initialize method.

        Args:
            return_value: dependency return value
        """

        self.return_value = return_value

    async def override_get_redis_storage(self) -> AsyncMock:
        """Override dependency.

        Returns: AsyncMock
        """
        service = AsyncMock()
        service.get_task_state.return_value = self.return_value

        return service
