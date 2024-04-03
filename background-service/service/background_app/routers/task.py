"""Module with spectrogram API routes."""

from functools import wraps
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse
from gstream.models import TaskState, TaskStatus
from gstream.node.common import convert_megabytes_to_bytes
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage

from background_app.routers.dependencies import (
    get_file_storage,
    get_redis_storage,
    parse_body
)

__all__ = [
    'APIRouter',
]

router = APIRouter()

MAXIMAL_TASKS_FOR_USER, MAXIMAL_INPUT_MEGABYTES_SIZE = 2, 1024


def check_task_exist(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(**kwargs) -> object:
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
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Task has not running script'
            )

        return await func(**kwargs)
    return wrapper


def check_finished_task(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(**kwargs) -> Callable:
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
                detail=f'Task is has status - {state.status} not finished'
            )

        output_args_filename = state.output_args_filename
        if not file_storage.is_file_exist(filename=output_args_filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail='Task has not output filename'
            )
        return await func(**kwargs)
    return wrapper


@router.post('/create')
async def create_task(
        task_type: str, user_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage)
) -> JSONResponse:
    task_ids = await redis_storage.get_user_task_ids(user_id=user_id)
    tasks_count = len(task_ids)
    if tasks_count > MAXIMAL_TASKS_FOR_USER:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail='Too many requests. Try again later.'
        )

    state = TaskState(
        user_id=user_id,
        type_=task_type
    )

    await redis_storage.add_task(task_state=state)
    return JSONResponse(
        content=state.task_id,
        status_code=status.HTTP_200_OK
    )


@router.get('/state')
@check_task_exist
async def get_status(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage)
) -> JSONResponse:
    state = await redis_storage.get_task_state(task_id=task_id)
    return JSONResponse(
        content=state.dict(by_alias=True),
        status_code=status.HTTP_200_OK
    )


@router.post('/load-args')
@check_task_exist
async def load_input_args(
        task_id: str,
        params: bytes = Depends(parse_body),
        redis_storage: RedisStorage = Depends(get_redis_storage),
        file_storage: FileStorage = Depends(get_file_storage)
) -> Response:
    if len(params) > convert_megabytes_to_bytes(
            value=MAXIMAL_INPUT_MEGABYTES_SIZE
    ):
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail='Too large parameter bytes size'
        )

    state = await redis_storage.get_task_state(task_id=task_id)
    if state.status != TaskStatus.NEW.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Task has status {state.status}'
        )

    await file_storage.save_binary_data(
        data=params,
        filename=state.input_args_filename
    )
    await redis_storage.add_log_message(
        task_id=task_id,
        text='Input arguments was loaded.'
    )
    # TODO: add generating script file

    return Response(status_code=status.HTTP_200_OK)


@router.post('/run')
@check_task_is_ready_for_run
@check_task_exist
async def run_task(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage),
        file_storage: FileStorage = Depends(get_file_storage)
) -> Response:
    state = await redis_storage.get_task_state(task_id=task_id)
    state.status = TaskStatus.READY.value
    await redis_storage.update_task_state(task_id=task_id, state=state)
    return Response(status_code=status.HTTP_200_OK)


@router.post('/kill')
@check_task_exist
async def kill_task(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage)
) -> Response:
    state = await redis_storage.get_task_state(task_id=task_id)
    state.is_need_kill = True

    await redis_storage.update_task_state(task_id=task_id, state=state)
    return Response(status_code=status.HTTP_200_OK)


@router.post('/accept')
@check_finished_task
@check_task_exist
async def accept_transfer(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage),
        file_storage: FileStorage = Depends(get_file_storage)
) -> Response:
    state = await redis_storage.get_task_state(task_id=task_id)
    state.is_transferred = True

    await redis_storage.update_task_state(task_id=task_id, state=state)
    return Response(status_code=status.HTTP_200_OK)


@router.get('/log')
@check_task_exist
async def get_log(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage)
) -> JSONResponse:
    log_text = await redis_storage.get_log(task_id=task_id)
    return JSONResponse(
        content=log_text,
        status_code=status.HTTP_200_OK
    )


@router.get('/result')
@check_finished_task
@check_task_exist
async def get_result(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage),
        file_storage: FileStorage = Depends(get_file_storage)
) -> Response:
    state = await redis_storage.get_task_state(task_id=task_id)
    output_args_filename = state.output_args_filename
    data = await file_storage.get_binary_data_from_file(
        filename=output_args_filename
    )
    return Response(
        content=data,
        headers={'Content-Type': 'application/octet-stream'}
    )
