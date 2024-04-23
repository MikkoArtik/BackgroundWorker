"""Module with classes for working binary and txt files."""

from pathlib import Path

import aiofiles as async_file

__all__ = [
    'BaseBinaryFileReader',
    'BaseBinaryFileWriter',
    'BaseTxtFileWriter'
]


class BaseBinaryFileReader:
    """Class for reading of binary file."""

    def __init__(self, path: Path):
        """Initialize class method.

        Args:
            path: path to file
        """
        if not path.exists():
            raise FileNotFoundError('File not found')
        self.__path = path
        self.__src_data = None

    async def convert_to_py_object(self):
        """Convert binary data to Python object.

        Returns: object

        """
        pass

    async def __read(self) -> bytes:
        """Reads bytes file content.

        Returns: bytes

        """
        async with async_file.open(self.__path, 'rb') as file_ctx:
            return await file_ctx.read()

    @property
    async def src_data(self) -> bytes:
        """Return bytes file content.

        Returns: bytes

        """
        if self.__src_data is None:
            self.__src_data = await self.__read()
        return self.__src_data


class BaseBinaryFileWriter:
    """Class for writing of binary file."""

    def __init__(self, path: Path, data: object):
        """Initialize class method.

        Args:
            path: path to file
            data: Python object
        """
        if not path.parent.exists():
            raise OSError('Folder for saving is not found')

        self.__path = path
        self.__data = data

    @property
    def _data(self):
        """Returns writing data.

        Returns: object

        """
        return

    def _convert_to_bytes(self) -> bytes:
        """Converts Python object to bytes.

        Returns: bytes

        """
        pass

    async def save(self) -> None:
        """Saves binary data to file.

        Returns: None

        """
        async with async_file.open(self.__path, 'wb') as file_ctx:
            await file_ctx.write(self._convert_to_bytes())


class BaseTxtFileWriter:
    """Class for writing of text file."""

    def __init__(self, path: Path, body: str = ''):
        """Initialize class method.

        Args:
            path: path to file
            body: str
        """
        self.__path = path
        self.__body = body

    async def save(self) -> None:
        """Saves content to file.

        Returns: None

        """
        async with async_file.open(self.__path, 'w') as file_ctx:
            await file_ctx.write(self.__body)
