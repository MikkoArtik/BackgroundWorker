"""Module for working output binary files."""

from pathlib import Path

from gstream.files.base import BaseBinaryFileWriter
from gstream.models import Array, DelaysFinderParameters

__all__ = [
    'DelaysFinderArgsBinaryFile',
    'DelaysFinderResultBinaryFile'
]


class DelaysFinderArgsBinaryFile(BaseBinaryFileWriter):
    """Class for operations with binary output file."""

    def __init__(self, path: Path, data: DelaysFinderParameters):
        """Initialize class method.

        Args:
            path: path to file
            data: writing python object
        """
        super().__init__(path=path, data=data)

    @property
    def _data(self) -> DelaysFinderParameters:
        """Returns source data (python instance of DelaysFinderParameters).

        Returns: DelaysFinderParameters

        """
        return self._BaseFileWriter__data

    def _convert_to_bytes(self) -> bytes:
        """Convert python object to bytes.

        Returns: bytes

        """
        return self._data.convert_to_bytes()


class DelaysFinderResultBinaryFile(BaseBinaryFileWriter):
    """Class for operations with binary output file."""

    def __init__(self, path: Path, data: Array):
        """Initialize class method.

        Args:
            path: path to file
            data: writing python object
        """
        super().__init__(path=path, data=data)

    @property
    def _data(self) -> Array:
        """Returns source data (python instance of Array).

        Returns: Array

        """
        return self._BaseBinaryFileWriter__data

    def _convert_to_bytes(self) -> bytes:
        """Convert python object (Array) to bytes.

        Returns: bytes

        """
        return self._data.convert_to_bytes()
