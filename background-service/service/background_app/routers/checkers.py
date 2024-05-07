"""Module with checker decorators for routers."""

from functools import wraps
from typing import Callable

from fastapi import HTTPException, status
from gstream.models import TaskStatus
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage


def check_task_exist(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(**kwargs) -> object:
        """Checks task existing in Redis Storage.

        Args:
            **kwargs: function arguments

        Returns: object

        """
        task_id: str = kwargs['task_id']

        for key, value in kwargs.items():
            if not isinstance(value, RedisStorage):
                continue
            redis_storage = value
            break
        else:
            raise KeyError('Redis storage not found in parameters')

        is_exist = await redis_storage.is_task_exist(task_id=task_id)
        if not is_exist:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Task not found'
            )
        return await func(**kwargs)
    return wrapper


def check_task_is_ready_for_run(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(**kwargs) -> Callable:
        """Checks task ready state for processing.

        Args:
            **kwargs: function arguments

        Returns: object

        """
        task_id: str = kwargs['task_id']
        redis_storage, file_storage = None, None

        for key, value in kwargs.items():
            if not isinstance(value, (RedisStorage, FileStorage)):
                continue
            if isinstance(value, RedisStorage):
                redis_storage = value
            if isinstance(value, FileStorage):
                file_storage = value

            if not (redis_storage is None or file_storage is None):
                break
        else:
            raise KeyError(
                'Redis and (or) file storage not found in parameters'
            )

        state = await redis_storage.get_task_state(task_id=task_id)
        if state.status != TaskStatus.NEW.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Task is has status - {state.status} not new'
            )

        input_args_filename = state.input_args_filename
        if not file_storage.is_file_exist(filename=input_args_filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Task has not input args file'
            )

        script_filename = state.script_filename
        if not file_storage.is_file_exist(filename=script_filename):
            raise HTTPException(
                status_code=status.HTTP_500_BAD_REQUEST,
                detail='Task has not running script'
            )

        return await func(**kwargs)
    return wrapper


def check_finished_task(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(**kwargs) -> Callable:
        """Checks task finishing state.

        Args:
            **kwargs: function arguments

        Returns: object

        """
        task_id: str = kwargs['task_id']
        redis_storage, file_storage = None, None

        for key, value in kwargs.items():
            if not isinstance(value, (RedisStorage, FileStorage)):
                continue

            if isinstance(value, RedisStorage):
                redis_storage = value
            if isinstance(value, FileStorage):
                file_storage = value

            if not (redis_storage is None or file_storage is None):
                break
        else:
            raise KeyError(
                'Redis and (or) file storage not found in parameters'
            )

        state = await redis_storage.get_task_state(task_id=task_id)
        if state.status != TaskStatus.FINISHED.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f'Task is has status - {state.status} (not finished!)'
            )

        output_args_filename = state.output_args_filename
        if not file_storage.is_file_exist(filename=output_args_filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Task has not output filename'
            )
        return await func(**kwargs)
    return wrapper
