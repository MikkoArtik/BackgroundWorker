import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from main import app


import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

# TODO: research problem with app creation
@pytest.fixture(scope='session', autouse=True)
def create_async_client() -> AsyncClient:

    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport)

