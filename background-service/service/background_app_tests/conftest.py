import pytest
from httpx import ASGITransport, AsyncClient

from background_app.routers.dependencies import get_redis_storage
from main import app


async def override_get_redis_storage_dependency():
    return


# TODO: temp fixture
@pytest.fixture(scope='session', autouse=True)
def create_async_client() -> AsyncClient:

    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport)


# TODO: research problem with app creation
@pytest.fixture
def create_async_client_without_dependencies() -> AsyncClient:
    app.dependency_overrides[
        get_redis_storage
    ] = override_get_redis_storage_dependency

    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport)
