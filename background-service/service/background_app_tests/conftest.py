from typing import Callable

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def get_async_client() -> Callable:
    def wrap(app: FastAPI) -> AsyncClient:
        transport = ASGITransport(app=app)

        return AsyncClient(transport=transport)

    return wrap
