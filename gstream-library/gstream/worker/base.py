import sys
from pathlib import Path
from typing import List, Optional, Union

import numpy as np

import gstream
from gstream.models import TaskState, TaskStatus
from gstream.node.gpu_rig import (
    GPUCard,
    GPURig,
    NoFreeGPUCardException,
    NoFreeRAMException
)
from gstream.node.gpu_task import GPUArray, GPUTask
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage


class TaskNotReadyException(RuntimeError):
    pass


class BaseProcess:
    def __init__(
            self,
            task_id: str,
            redis_storage: RedisStorage,
            file_storage: FileStorage
    ):
        self.__task_id = task_id
        self.__redis_storage = redis_storage
        self.__file_storage = file_storage

        self.__args = None
        self._solution = np.array([])

    async def _load_args_from_file(self):
        pass

    async def _save_solution(self):
        pass

    async def _run(self):
        pass

    @property
    async def _args(self):
        if not self.__args:
            self.__args = await self._load_args_from_file()
        return self.__args

    @property
    def file_storage(self) -> FileStorage:
        return self.__file_storage

    @property
    def redis_storage(self) -> RedisStorage:
        return self.__redis_storage

    @property
    def solution(self) -> np.ndarray:
        return self._solution

    @property
    async def task_id(self) -> str:
        is_exist = await self.__redis_storage.is_task_exist(
            task_id=self.__task_id
        )
        if not is_exist:
            raise KeyError(f'Task with id {self.__task_id} not found')
        return self.__task_id

    @property
    async def task_state(self) -> TaskState:
        return await self.redis_storage.get_task_state(
            task_id=await self.task_id
        )

    async def add_log_message(self, text: str):
        await self.__redis_storage.add_log_message(
            task_id=await self.task_id,
            text=text
        )

    @property
    async def is_ready_for_running(self) -> bool:
        state = await self.task_state

        if state.status != TaskStatus.READY.value:
            return False

        input_args_filename = state.input_args_filename
        if not self.file_storage.is_file_exist(filename=input_args_filename):
            return False

        script_filename = state.script_filename
        if not self.file_storage.is_file_exist(filename=script_filename):
            return False

        return True

    async def _finalize(self):
        state = await self.task_state
        is_success = True
        if not self.file_storage.is_file_exist(
            filename=state.output_args_filename
        ):
            is_success *= False

        if not is_success:
            state.status = TaskStatus.FAILED.value
            await self.add_log_message(text='Failed task processing')
        else:
            state.status = TaskStatus.FINISHED.value
            await self.add_log_message(text='Task successfully completed')

        await self.redis_storage.update_task_state(
            task_id=await self.task_id,
            state=state
        )
        await self.add_log_message(text='Task was closed')

    async def _rollback(self):
        task_id = await self.task_id
        state = await self.redis_storage.get_task_state(
            task_id=task_id
        )

        state.rollback()
        await self.redis_storage.update_task_state(
            task_id=task_id,
            state=state
        )

    async def run(self):
        if not await self.is_ready_for_running:
            raise TaskNotReadyException

        await self.add_log_message(text='Task running...')

        try:
            await self._run()
        except NoFreeRAMException:
            await self.add_log_message(
                text='RAM is busy now. Process not run now but will run later'
            )
            await self._rollback()
            return
        except BaseException as e:
            task_id = await self.task_id
            await self.add_log_message(
                text=f'Error in task  with id {task_id}: exception {e}'
            )

            state = await self.redis_storage.get_task_state(
                task_id=task_id
            )
            state.status = TaskStatus.FAILED
            await self.redis_storage.update_task_state(
                task_id=task_id,
                state=state
            )
            return
        await self._save_solution()
        await self._finalize()


class GPUProcess(BaseProcess):
    def __init__(
            self,
            task_id: str,
            redis_storage: RedisStorage,
            file_storage: FileStorage
    ):
        super().__init__(
            task_id=task_id,
            redis_storage=redis_storage,
            file_storage=file_storage
        )

        self.__gpu_card = None
        self.__task = None
        self.__prepared_args = []

    async def _prepare_args(self):
        pass

    async def _create_task(self):
        pass

    @property
    def __kernels_root(self) -> Path:
        path = Path(
            Path(gstream.__file__).parent,
            'kernels'
        )
        if not path.exists():
            raise OSError('Kernels folder not found')
        return path

    def _get_kernel_core(self, kernel_filename: str) -> str:
        path = Path(self.__kernels_root, kernel_filename)
        if not path.exists():
            raise FileNotFoundError(f'Kernel file {kernel_filename} not found')

        with path.open() as file_ctx:
            return file_ctx.read()

    @property
    async def _task(self) -> GPUTask:
        if self.__task is None:
            self.__task = await self._create_task()
        return self.__task

    async def __get_gpu_card(self) -> GPUCard:
        gpu_rig = GPURig()
        ram_memory_info = gpu_rig.info.ram_memory_info
        required_memory_size = int(await self._args_bytes_size)
        if ram_memory_info.permitted_volume < required_memory_size:
            await self.add_log_message(
                text='No free RAM size. Process not run now but will run later'
            )
            raise NoFreeRAMException

        try:
            gpu_card = GPURig().get_free_gpu_card(
                required_memory_size=required_memory_size
            )
            await self.add_log_message(text='Found free GPUCard')
            return gpu_card
        except NoFreeGPUCardException:
            await self.add_log_message(
                text='All GPU cards are busy now. '
                     'Process not run now but will run later'
            )
            raise

    @property
    async def gpu_card(self) -> Optional[GPUCard]:
        if self.__gpu_card is None:
            try:
                self.__gpu_card = await self.__get_gpu_card()
            except (NoFreeRAMException, NoFreeGPUCardException):
                self.__gpu_card = None
        return self.__gpu_card

    @property
    async def _prepared_args(self) -> List[Union[int, float, GPUArray]]:
        if not self.__prepared_args:
            self.__prepared_args = await self._prepare_args()
        return self.__prepared_args

    @property
    async def _args_bytes_size(self) -> int:
        prepared_args, total_size = await self._prepared_args, 0
        if not prepared_args:
            return total_size

        for arg in prepared_args:
            if isinstance(arg, GPUArray):
                total_size += arg.bytes_size
            else:
                total_size += sys.getsizeof(arg)
        return total_size

    async def run(self):
        gpu_card = await self.gpu_card
        try:
            await self._run()
        except NoFreeRAMException:
            await self.add_log_message(
                text=f'GPU card with uuid={gpu_card.uuid} is busy '
                     f'now. Process not run now but will run later'
            )
            await self._rollback()
            return
        except NoFreeGPUCardException:
            await self.add_log_message(
                text=f'GPU card with uuid={gpu_card.uuid} is busy now.'
                     f'Process not run now but will run later'
            )
            await self._rollback()
            return
        except BaseException as e:
            task_id = await self.task_id
            await self.add_log_message(
                text=f'Error in task with id {task_id}: exception {e}'
            )
            await self._rollback()
            return

        await self._save_solution()
        await self._finalize()

    async def _release_args(self):
        for i in range(len(self.__prepared_args)):
            if isinstance(self.__prepared_args[i], GPUArray):
                self.__prepared_args[i].release()
        await self.add_log_message(
            text='GPU card is clear from task arguments'
        )
