import pytest
from httpx import ASGITransport, AsyncClient

from main import app


# TODO: fix app importing
@pytest.fixture(scope='session', autouse=True)
def create_async_client() -> AsyncClient:
    transport = ASGITransport(app=app)

    return AsyncClient(transport=transport)
