import uuid

import fastapi.responses
from dotenv import load_dotenv
import pytest
from hamcrest import assert_that, equal_to
from httpx import AsyncClient


import gstream.models
from background_app.routers.task import MAXIMAL_TASKS_FOR_USER
from main import app
from unittest.mock import Mock, patch, PropertyMock, MagicMock
from gstream.storage.redis import Storage
from gstream import models
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException
import os
from gstream.models import TaskState

load_dotenv()
URL_PATTERN = 'http://{host}:{port}/{root_path}/{endpoint}'


class TestTask:

    @pytest.mark.negative
    @patch.object(Storage, 'get_user_task_ids')
    @patch('background_app.routers.dependencies.get_redis_storage')
    @pytest.mark.asyncio
    async def test_create_task_negative(self, mock_get_redis_storage: Mock,
                                        mock_get_user_task_ids: Mock,
                                        create_async_client: AsyncClient):
        url = URL_PATTERN.format(
            host=os.getenv('APP_HOST'),
            port=os.getenv('APP_PORT'),
            root_path='background',
            endpoint='create'
        )
        mock_get_redis_storage.return_value = 'test'
        mock_get_user_task_ids.return_value = list(
            range(MAXIMAL_TASKS_FOR_USER + 1)
        )
        params = {
            'task_type': 'task_type',
            'user_id': 'user_id'
        }
        response = await create_async_client.post(url=url, params=params)
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
    @patch.object(Storage, 'add_task')
    @patch.object(Storage, 'get_user_task_ids')
    @patch('background_app.routers.dependencies.get_redis_storage')
    @pytest.mark.asyncio
    async def test_create_task_positive(self, mock_get_redis_storage: Mock,
                                        mock_get_user_task_ids: Mock,
                                        mock_add_task: Mock,
                                        mock_check_task_type: Mock,
                                        mock_hex: Mock,
                                        create_async_client: AsyncClient):
        url = URL_PATTERN.format(
            host=os.getenv('APP_HOST'),
            port=os.getenv('APP_PORT'),
            root_path='background',
            endpoint='create'
        )
        mock_get_redis_storage.return_value = 'test'
        mock_get_user_task_ids.return_value = [1, 2]
        mock_check_task_type.return_value = 'q'
        mock_add_task.return_value = None

        expected_value = 'test'
        mock_hex.return_value = MagicMock(hex=expected_value)

        params = {
            'task_type': 'task_type',
            'user_id': 'user_id'
        }
        response = await create_async_client.post(url=url, params=params)

        assert_that(
            actual_or_assertion=response.json(),
            matcher=equal_to(expected_value)
        )
