import asyncio
import os
from pathlib import Path

import dotenv
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage
from gstream.worker.task_pull import TaskPull
from redis.asyncio import ConnectionPool

dotenv.load_dotenv()


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


async def main():
    redis_storage = RedisStorage(
        pool=create_pool()
    )
    file_storage = FileStorage(
        root=Path(
            os.getenv('STORAGE_ROOT')
        )
    )

    task_pull = TaskPull(
        redis_storage=redis_storage,
        file_storage=file_storage
    )
    await task_pull.run_pull()


if __name__ == '__main__':
    asyncio.run(main())
