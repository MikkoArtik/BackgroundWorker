"""Module for working with input binary files."""

from pathlib import Path

from gstream.files.base import BaseBinaryFileReader
from gstream.models import DelaysFinderParameters

__all__ = [
    'DelaysFinderArgsBinaryFile'
]


class DelaysFinderArgsBinaryFile(BaseBinaryFileReader):
    """Class for operations with binary input file."""

    def __init__(self, path: Path):
        """Initialize class method.

        Args:
            path: path to file
        """
        super().__init__(path=path)

    async def convert_to_py_object(self) -> DelaysFinderParameters:
        """Convert file content to python object.

        Returns: DelaysFinderParameters

        """
        return DelaysFinderParameters.create_from_bytes(
            bytes_obj=await self.src_data
        )
