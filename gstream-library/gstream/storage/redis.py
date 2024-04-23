import json
from datetime import datetime, timedelta
from typing import List, Optional, Set, Union

from gstream.models import DATETIME_FORMAT, TaskState, TaskStatus
from redis.asyncio import ConnectionPool, Redis


def format_message(message: str) -> str:
    datetime_fmt = datetime.now().strftime(DATETIME_FORMAT)
    return f'[{datetime_fmt}] {message}\n'


class Storage:
    def __init__(
            self,
            pool: ConnectionPool,
            key_expiration: timedelta = timedelta(hours=3)
    ):
        self.__adapter = Redis(
            connection_pool=pool,
            decode_responses=False
        )
        self.__key_expiration = key_expiration

    @staticmethod
    def __create_flatten_dict(src_dict: dict) -> dict:
        flatten_dict = {}

        def run_recursion(
                dict_obj: dict, parent_key: str = '', sep: str = ':'
        ):
            for key, value in dict_obj.items():
                if parent_key:
                    new_key = f'{parent_key}{sep}{key}'
                else:
                    new_key = key

                if isinstance(value, dict):
                    run_recursion(dict_obj=value, parent_key=new_key)
                else:
                    if isinstance(value, bool):
                        if value:
                            value = 1
                        else:
                            value = 0
                    flatten_dict[new_key] = value
        run_recursion(dict_obj=src_dict)
        return flatten_dict

    @property
    def adapter(self) -> Redis:
        return self.__adapter

    async def __add_dict(self, value: dict):
        flatten_dict = self.__create_flatten_dict(src_dict=value)
        await self.adapter.mset(mapping=flatten_dict)

    async def __get_keys(
            self,
            pattern: str,
            is_strip: bool = False
    ) -> Optional[Union[str, List[str]]]:
        keys: List[str] = await self.adapter.keys(pattern=pattern)
        if is_strip:
            if not keys:
                return None
            if len(keys) == 1:
                return keys[0]
        return keys

    async def __set_time_expiration(self, keys_pattern: str):
        keys = await self.__get_keys(pattern=keys_pattern)
        for key in keys:
            await self.adapter.expire(
                name=key,
                time=self.__key_expiration
            )

    async def add_log_message(self, task_id: str, text: str):
        key = await self.__get_keys(pattern=f'Log:{task_id}', is_strip=True)
        if not key:
            await self.adapter.set(
                name=f'Log:{task_id}',
                value=format_message(message=text)
            )
            await self.__set_time_expiration(
                keys_pattern=f'Log:{task_id}',
            )
        else:
            await self.adapter.append(
                key=key,
                value=format_message(message=text)
            )

    async def get_log(self, task_id: str) -> str:
        key = await self.__get_keys(pattern=f'Log:{task_id}', is_strip=True)
        if not key:
            return 'Log not found'
        return await self.adapter.get(name=key)

    async def is_task_exist(self, task_id: str) -> bool:
        selected_keys = await self.adapter.keys(pattern=f'*Task:{task_id}:*')
        if not selected_keys:
            return False
        return True

    async def add_task(self, task_state: TaskState):
        if await self.is_task_exist(task_id=task_state.task_id):
            raise KeyError(
                f'Task with id {task_state.task_id} already exist'
            )
        await self.__add_dict(
            value=self.__create_flatten_dict(
                src_dict=task_state.dict_view
            )
        )
        await self.__set_time_expiration(
            keys_pattern=f'User:{task_state.user_id}:Task:'
                         f'{task_state.task_id}*'
        )
        await self.add_log_message(
            task_id=task_state.task_id,
            text='Task was created'
        )

    @property
    async def active_users(self) -> Set[str]:
        unique_users = set()
        for key in await self.__get_keys(pattern='User:*'):
            user_id = key.split(':')[1]
            unique_users.add(user_id)
        return unique_users

    @property
    async def all_task_ids(self) -> List[str]:
        task_ids = []
        for key in await self.__get_keys(pattern='*Task:*'):
            task_id = key.split(':')[3]
            task_ids.append(task_id)
        return task_ids

    @property
    async def active_task_ids(self) -> List[str]:
        task_ids = []
        for task_id in await self.all_task_ids:
            state = await self.get_task_state(task_id=task_id)
            if state.status != TaskStatus.RUNNING.value:
                continue
            task_ids.append(task_id)
        return task_ids

    @property
    async def all_filenames(self) -> Set[str]:
        filenames = set()
        for task_id in await self.all_task_ids:
            state = await self.get_task_state(task_id=task_id)
            filenames |= set(state.all_filenames)
        return filenames

    async def get_user_id(self, task_id: str) -> str:
        for key in await self.__get_keys(pattern=f'*Task:{task_id}*'):
            current_user_id = key.split(':')[1]
            current_task_id = key.split(':')[3]
            if current_task_id == task_id:
                return current_user_id
        raise KeyError('Task is not found')

    async def get_user_task_ids(self, user_id: str) -> Set[str]:
        task_ids = set()
        for key in await self.__get_keys(pattern=f'User:{user_id}:Task:*'):
            task_id = key.split(':')[3]
            task_ids.add(task_id)
        return task_ids

    async def get_task_state(self, task_id: str) -> TaskState:
        if not await self.is_task_exist(task_id=task_id):
            raise ValueError('Task not found')

        key_pattern = f'*Task:{task_id}:State'
        full_key = await self.__get_keys(pattern=key_pattern, is_strip=True)
        json_content = json.loads(
            s=await self.adapter.get(name=full_key),
        )
        user_id = await self.get_user_id(task_id=task_id)
        return TaskState(user_id=user_id, task_id=task_id, **json_content)

    async def get_task_input_args_filename(self, task_id: str) -> str:
        if not await self.is_task_exist(task_id=task_id):
            raise ValueError('Task not found')

        key_pattern = f'*Task:{task_id}:InputArgumentsFilename'
        full_key = await self.__get_keys(pattern=key_pattern, is_strip=True)
        return await self.adapter.get(name=full_key)

    async def get_task_script_filename(self, task_id: str) -> str:
        if not await self.is_task_exist(task_id=task_id):
            raise ValueError('Task not found')

        key_pattern = f'*Task:{task_id}:ScriptFilename'
        full_key = await self.__get_keys(pattern=key_pattern, is_strip=True)
        return await self.adapter.get(name=full_key)

    async def get_task_output_args_filename(self, task_id: str) -> str:
        if not await self.is_task_exist(task_id=task_id):
            raise ValueError('Task not found')

        key_pattern = f'*Task:{task_id}:OutputArgs'
        full_key = await self.__get_keys(pattern=key_pattern, is_strip=True)
        return await self.adapter.get(name=full_key)

    async def update_task_state(self, task_id: str, state: TaskState):
        full_key = await self.__get_keys(
            pattern=f'*Task:{task_id}:State',
            is_strip=True
        )

        await self.adapter.set(
            name=full_key,
            value=json.dumps(state.dict(by_alias=True))
        )
        await self.add_log_message(
            task_id=task_id,
            text='Task state was updated'
        )

    async def close(self):
        await self.adapter.aclose()

    async def remove_task(self, task_id: str):
        await self.adapter.delete(f'*Task:{task_id}:State')
        await self.adapter.delete(f'Log:{task_id}')
