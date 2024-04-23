"""Module with classes for autogenerate running python-scripts."""

from pathlib import Path
from typing import Dict

from gstream.files.base import BaseTxtFileWriter

__all__ = [
    'DelaysRunnerScriptFile'
]

DELAYS_SCRIPT_BODY = """
import asyncio
from pathlib import Path

from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage
from gstream.worker.delays_finder import DelaysFinder
from redis.asyncio import ConnectionPool


async def main(task_id: str):
    redis_storage = RedisStorage(
        pool=ConnectionPool(
            host='localhost',
            port=6379,
            db=0,
            decode_responses=True
        )
    )
    file_storage = FileStorage(
        root=Path(
            '/media/mikko/Data/Work/ArtCode/AppProjects/EventLocProject/'
            'Background/BackgroundWorker/worker-service/storage'
        )
    )

    is_exist = await redis_storage.is_task_exist(task_id=task_id)
    if not is_exist:
        return

    proc = DelaysFinder(
        task_id=task_id,
        redis_storage=redis_storage,
        file_storage=file_storage
    )
    await proc.run()


if __name__ == '__main__':
    asyncio.run(main(task_id='[task-id]'))

"""


class BaseRunnerScriptFile(BaseTxtFileWriter):
    """Base class."""

    def __init__(
            self,
            path: Path,
            template_body: str,
            replace_arguments: Dict[str, str]
    ):
        """Initialize class method.

        Args:
            path: saving path
            template_body: template script body
            replace_arguments: dict
        """
        for key, value in replace_arguments.items():
            template_body = template_body.replace(key, value)

        super().__init__(path=path, body=template_body)


class DelaysRunnerScriptFile(BaseRunnerScriptFile):
    """Class for generate delays running script."""

    def __init__(self, path: Path, task_id: str):
        """Initialize class method.

        Args:
            path: saving path
            task_id: task_id [str]
        """
        super().__init__(
            path=path,
            template_body=DELAYS_SCRIPT_BODY,
            replace_arguments={'[task-id]': task_id}
        )
