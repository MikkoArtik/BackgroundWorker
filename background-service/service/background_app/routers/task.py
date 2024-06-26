"""Module with spectrogram API routes."""

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse

from background_app.routers.checkers import (
    check_finished_task,
    check_task_exist,
    check_task_is_ready_for_run
)
from background_app.routers.dependencies import (
    get_file_storage,
    get_redis_storage,
    parse_body
)
from gstream.files.scripts import DelaysRunnerScriptFile
from gstream.models import TaskState, TaskStatus, TaskType
from gstream.node.common import convert_megabytes_to_bytes
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage

__all__ = [
    'APIRouter',
]

router = APIRouter()

MAXIMAL_TASKS_FOR_USER, MAXIMAL_INPUT_MEGABYTES_SIZE = 200, 1024


@router.post('/create')
async def create_task(
        task_type: str, user_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage)
) -> JSONResponse:
    """Creates task in Redis Storage.

    Args:
        task_type: str
        user_id: str
        redis_storage: RedisStorage

    Returns: JSONResponse with task ID

    """
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
    """Returns task state.

    Args:
        task_id: task ID
        redis_storage: RedisStorage

    Returns: JSONResponse with task state

    """
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
    """Load input arguments for task.

    Args:
        task_id: str
        params: bytes
        redis_storage: RedisStorage
        file_storage: FileStorage

    Returns: Response

    """
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

    if state.type_ == TaskType.DELAYS.value:
        await DelaysRunnerScriptFile(
            path=Path(file_storage.root, state.script_filename),
            task_id=task_id
        ).save()

    return Response(status_code=status.HTTP_200_OK)


@router.post('/run')
@check_task_is_ready_for_run
@check_task_exist
async def run_task(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage),
        file_storage: FileStorage = Depends(get_file_storage)
) -> Response:
    """Runs task by ID.

    Args:
        task_id: str
        redis_storage: RedisStorage
        file_storage: FileStorage

    Returns: Response

    """
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
    """Kills task by ID.

    Args:
        task_id: str
        redis_storage: RedisStorage

    Returns: Response

    """
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
    """Accepts result on client side.

    Args:
        task_id: str
        redis_storage: RedisStorage
        file_storage: FileStorage

    Returns: Response

    """
    state = await redis_storage.get_task_state(task_id=task_id)
    state.is_accepted = True

    await redis_storage.update_task_state(task_id=task_id, state=state)
    return Response(status_code=status.HTTP_200_OK)


@router.get('/log')
@check_task_exist
async def get_log(
        task_id: str,
        redis_storage: RedisStorage = Depends(get_redis_storage)
) -> JSONResponse:
    """Returns log for task by id.

    Args:
        task_id: str
        redis_storage: RedisStorage

    Returns: JSONResponse with log

    """
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
    """Returns task result (bytes format) by ID.

    Args:
        task_id: str
        redis_storage: RedisStorage
        file_storage: FileStorage

    Returns: Response

    """
    state = await redis_storage.get_task_state(task_id=task_id)
    output_args_filename = state.output_args_filename
    data = await file_storage.get_binary_data_from_file(
        filename=output_args_filename
    )
    return Response(
        content=data,
        headers={'Content-Type': 'application/octet-stream'}
    )
