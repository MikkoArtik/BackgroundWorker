import asyncio
import subprocess
from asyncio.queues import Queue
from pathlib import Path

import anyio
import psutil
from gstream.models import TaskStatus
from gstream.node.gpu_rig import GPURig
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage
from psutil import STATUS_ZOMBIE, Process

SLEEP_TIME_SECONDS = 10


class TaskPull:
    def __init__(
            self,
            redis_storage: RedisStorage,
            file_storage: FileStorage
    ):
        self.__redis_storage = redis_storage
        self.__file_storage = file_storage
        self.__gpu_rig = GPURig()

        self.__ready_pull = {}
        self.__kill_pull = Queue()
        self.__accepted_pull = Queue()

    @property
    def redis_storage(self) -> RedisStorage:
        return self.__redis_storage

    @property
    def file_storage(self) -> FileStorage:
        return self.__file_storage

    @property
    def gpu_rig(self) -> GPURig:
        return self.__gpu_rig

    async def synchronize_file_storage_with_redis(self):
        while True:
            redis_storage_filenames = await self.redis_storage.all_filenames
            file_storage_filenames = self.file_storage.all_filenames

            for filename in file_storage_filenames:
                if filename in redis_storage_filenames:
                    continue
                self.file_storage.remove_file(filename=filename)
            await asyncio.sleep(SLEEP_TIME_SECONDS)

    async def scan_killing_tasks(self):
        while True:
            for task_id in await self.redis_storage.all_task_ids:
                state = await self.redis_storage.get_task_state(
                    task_id=task_id
                )
                if state.status == TaskStatus.KILLED.value:
                    continue
                if not state.is_need_kill:
                    continue

                await self.__kill_pull.put(task_id)
            await asyncio.sleep(SLEEP_TIME_SECONDS)

    async def scan_ready_tasks(self):
        while True:
            for task_id in await self.redis_storage.all_task_ids:
                state = await self.redis_storage.get_task_state(
                    task_id=task_id
                )
                if state.status != TaskStatus.READY.value:
                    continue

                if state.type_ not in self.__ready_pull:
                    self.__ready_pull[state.type_] = Queue()

                self.__ready_pull[state.type_].put_nowait(task_id)

            await asyncio.sleep(SLEEP_TIME_SECONDS)

    async def scan_accepted_tasks(self):
        while True:
            for task_id in await self.redis_storage.all_task_ids:
                state = await self.redis_storage.get_task_state(
                    task_id=task_id
                )
                if state.is_accepted:
                    self.__accepted_pull.put_nowait(task_id)
            await asyncio.sleep(SLEEP_TIME_SECONDS)

    async def __kill_task(self, task_id: str):
        try:
            state = await self.redis_storage.get_task_state(task_id=task_id)
        except ValueError:
            return

        if not state.is_need_kill:
            return

        pid = state.pid
        if pid == -1:
            state.status = TaskStatus.KILLED.value
            await self.redis_storage.update_task_state(
                task_id=task_id,
                state=state
            )
            await self.redis_storage.add_log_message(
                task_id=task_id,
                text='Task was killed'
            )
            return

        try:
            process = Process(pid=pid)
        except psutil.NoSuchProcess:
            state.status = TaskStatus.KILLED.value
            await self.redis_storage.update_task_state(
                task_id=task_id,
                state=state
            )
            await self.redis_storage.add_log_message(
                task_id=task_id,
                text='Task was killed'
            )
            return

        if process.is_running():
            if process.status() != STATUS_ZOMBIE:
                process.kill()

        if process.status() == STATUS_ZOMBIE:
            state.status = TaskStatus.KILLED.value
            await self.redis_storage.update_task_state(
                task_id=task_id,
                state=state
            )
            await self.redis_storage.add_log_message(
                task_id=task_id,
                text='Task was killed'
            )

    async def kill_tasks(self):
        while True:
            if self.__kill_pull.empty():
                await asyncio.sleep(delay=SLEEP_TIME_SECONDS)
                continue

            task_id = await self.__kill_pull.get()

            await self.__kill_task(task_id=task_id)
            await asyncio.sleep(delay=SLEEP_TIME_SECONDS)

    async def __remove_task(self, task_id: str):
        try:
            state = await self.redis_storage.get_task_state(task_id=task_id)
        except ValueError:
            return

        if not state.is_accepted:
            return

        await self.redis_storage.remove_task(task_id=task_id)
        self.file_storage.remove_files(*state.all_filenames)

    async def remove_tasks(self):
        while True:
            if self.__accepted_pull.empty():
                await asyncio.sleep(delay=SLEEP_TIME_SECONDS)
                continue

            task_id = await self.__accepted_pull.get()

            await self.__remove_task(task_id=task_id)
            await asyncio.sleep(delay=SLEEP_TIME_SECONDS)

    async def __is_possible_run_any_task(self) -> bool:
        active_tasks_count = len(await self.redis_storage.active_task_ids)
        cpu_count = self.__gpu_rig.info.cpu_cores_count
        if active_tasks_count >= cpu_count:
            return False

        if not self.__gpu_rig.is_available_ram_memory:
            return False
        return True

    async def __is_possible_run_task(self, task_id: str) -> bool:
        if not self.__is_possible_run_any_task():
            return False

        try:
            state = await self.redis_storage.get_task_state(task_id=task_id)
        except ValueError:
            return False

        if state.status != TaskStatus.READY.value:
            return False

        if not self.file_storage.is_file_exist(
                filename=state.input_args_filename
        ):
            return False

        if not self.file_storage.is_file_exist(filename=state.script_filename):
            return False

        return True

    async def __run_single_task(self, task_id: str):
        if not self.__is_possible_run_task(task_id=task_id):
            return

        state = await self.redis_storage.get_task_state(task_id=task_id)
        script_full_path = Path(
            self.file_storage.root,
            state.script_filename
        )

        proc = subprocess.Popen(
            ['python3', str(script_full_path)],
            close_fds=True
        )
        state.pid = proc.pid
        state.status = TaskStatus.RUNNING.value
        await self.redis_storage.update_task_state(
            task_id=task_id,
            state=state
        )

    async def run_tasks_block(self, task_type: str):
        if task_type not in self.__ready_pull:
            raise KeyError('Invalid task type')

        while True:
            queue = self.__ready_pull.get(task_type, Queue())
            if queue.empty():
                await asyncio.sleep(delay=SLEEP_TIME_SECONDS)
                continue

            task_id = await self.__accepted_pull.get()

            await self.__run_single_task(task_id=task_id)
            await asyncio.sleep(delay=SLEEP_TIME_SECONDS)

    async def run_tasks(self):
        async with anyio.create_task_group() as group_ctx:
            for task_type in self.__ready_pull.keys():
                group_ctx.start_soon(self.run_tasks_block, task_type)

    async def run_pull(self):
        async with anyio.create_task_group() as group_ctx:
            group_ctx.start_soon(self.synchronize_file_storage_with_redis)
            group_ctx.start_soon(self.scan_killing_tasks)
            group_ctx.start_soon(self.scan_ready_tasks)
            group_ctx.start_soon(self.scan_accepted_tasks)
            group_ctx.start_soon(self.kill_tasks)
            group_ctx.start_soon(self.remove_tasks)
            group_ctx.start_soon(self.run_tasks)
        # await asyncio.gather(
        #     self.synchronize_file_storage_with_redis(),
        #     self.scan_killing_tasks(),
        #     self.scan_ready_tasks(),
        #     self.scan_accepted_tasks(),
        #     self.kill_tasks(),
        #     self.remove_tasks(),
        #     self.run_tasks()
        # )
