from typing import Callable

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def get_async_client() -> Callable:
    def create_async_client(app: FastAPI) -> AsyncClient:
        transport = ASGITransport(app=app)

        return AsyncClient(transport=transport)

    return create_async_client
