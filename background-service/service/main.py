"""Microservice main module."""

import os

import dotenv
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from background_app.routers import task

__all__ = []

dotenv.load_dotenv()


def get_app(root_path: str) -> FastAPI:
    try:
        if bool(int(os.getenv('IS_DEBUG'))):
            return FastAPI()
    except TypeError:
        pass
    return FastAPI(root_path=root_path)


app = get_app(root_path='/background')
app.include_router(task.router)


@app.get('/ping')
async def check_service_alive() -> JSONResponse:
    """Return ping-pong response.

    Returns: dict object with status and message

    """
    return JSONResponse(
        content='Service is alive'
    )


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        reload=True,
        host=os.getenv('APP_HOST'),
        port=int(os.getenv('APP_PORT'))
    )
