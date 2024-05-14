import os
from importlib import reload
from typing import Callable
from unittest.mock import MagicMock, Mock, patch

import pytest
from dotenv import load_dotenv
from fastapi import FastAPI, status
from gstream.models import TaskState
from gstream.storage.redis import Storage as RedisStorage
from hamcrest import assert_that, equal_to

from background_app.routers import task
from background_app.routers.dependencies import get_redis_storage
from background_app_tests.helpers import DependencyMock, mock_decorator

load_dotenv()
APP_HOST = os.getenv('APP_HOST')
APP_PORT = os.getenv('APP_PORT')
URL_PATTERN = 'http://{host}:{port}/{root_path}/{endpoint}'


class TestTask:

    @pytest.mark.negative
    @patch.object(RedisStorage, 'get_user_task_ids')
    @patch('background_app.routers.dependencies.get_redis_storage')
    @pytest.mark.asyncio
    async def test_create_task_negative(self, mock_get_redis_storage: Mock,
                                        mock_get_user_task_ids: Mock,
                                        get_async_client: Callable):
        mock_get_redis_storage.return_value = 'test'
        mock_get_user_task_ids.return_value = list(
            range(task.MAXIMAL_TASKS_FOR_USER + 1)
        )
        url = URL_PATTERN.format(
            host=APP_HOST,
            port=APP_PORT,
            root_path='background',
            endpoint='create'
        )
        params = {
            'task_type': 'task_type',
            'user_id': 'user_id'
        }
        app = FastAPI(root_path='/background')
        app.include_router(task.router)
        response = await get_async_client(app=app).post(url=url, params=params)
        expected_value = 'Too many requests. Try again later.'

        assert_that(
            actual_or_assertion=response.status_code,
            matcher=equal_to(status.HTTP_429_TOO_MANY_REQUESTS)
        )
        assert_that(
            actual_or_assertion=response.json()['detail'],
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('uuid.uuid4')
    @patch('gstream.models.check_task_type')
    @patch.object(RedisStorage, 'add_task')
    @patch.object(RedisStorage, 'get_user_task_ids')
    @patch('background_app.routers.dependencies.get_redis_storage')
    @pytest.mark.asyncio
    async def test_create_task_positive(self, mock_get_redis_storage: Mock,
                                        mock_get_user_task_ids: Mock,
                                        mock_add_task: Mock,
                                        mock_check_task_type: Mock,
                                        mock_hex: Mock,
                                        get_async_client: Callable):
        mock_get_redis_storage.return_value = 'test-redis'
        mock_get_user_task_ids.return_value = [1, 2]
        mock_check_task_type.return_value = 'test-type'
        mock_add_task.return_value = None

        expected_value = 'test'
        mock_hex.return_value = MagicMock(hex=expected_value)

        url = URL_PATTERN.format(
            host=APP_HOST,
            port=APP_PORT,
            root_path='background',
            endpoint='create'
        )
        task_type = 'task_type'
        user_id = 'user_id'
        params = {
            'task_type': task_type,
            'user_id': user_id
        }
        app = FastAPI(root_path='/background')
        app.include_router(task.router)
        response = await get_async_client(app=app).post(url=url, params=params)

        mock_get_user_task_ids.assert_called_with(user_id=user_id)
        mock_add_task.assert_called_once_with(
            task_state=TaskState(
                user_id=user_id,
                type_=task_type
            )
        )
        assert_that(
            actual_or_assertion=response.status_code,
            matcher=equal_to(status.HTTP_200_OK)
        )
        assert_that(
            actual_or_assertion=response.json(),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('gstream.models.check_task_type')
    @pytest.mark.asyncio
    async def test_get_status_positive(self,
                                       mock_check_task_type: Mock,
                                       get_async_client: Callable):
        url = URL_PATTERN.format(
            host=APP_HOST,
            port=APP_PORT,
            root_path='background',
            endpoint='state'
        )
        params = {
            'task_id': 'task_id',
        }
        task_type = 'test-type'
        mock_check_task_type.return_value = task_type

        expected_value = TaskState(
            user_id='test-id',
            type_='test-type'
        )
        override = DependencyMock(return_value=expected_value)
        app = FastAPI(root_path='/background')

        with patch(
            'background_app.routers.checkers.check_task_exist', mock_decorator
        ):
            reload(task)
            app.include_router(task.router)
            app.dependency_overrides[
                get_redis_storage
            ] = override.override_get_redis_storage
            response = await get_async_client(app=app).get(
                url=url,
                params=params
            )
        assert_that(
            actual_or_assertion=response.json(),
            matcher=equal_to(expected_value.dict(by_alias=True))
        )
