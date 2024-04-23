from pathlib import Path
from typing import Set

import aiofiles as async_file


class Storage:
    def __init__(self, root: Path):
        if not root.exists():
            raise OSError('Storage root not found')

        if not root.is_dir():
            raise OSError('Storage root is not directory')

        self.__root = root

    @property
    def root(self) -> Path:
        return self.__root

    @property
    def all_filenames(self) -> Set[str]:
        filenames = set()
        for item in self.root.iterdir():
            if Path(self.root, item).is_dir():
                continue
            filenames.add(item.name)
        return filenames

    def is_file_exist(self, filename: str) -> bool:
        path = Path(self.root, filename)
        return path.exists()

    async def save_binary_data(self, data: bytes, filename: str):
        path = Path(self.root, filename)
        if path.exists():
            raise FileExistsError(f'Binary file {filename} is exist')

        async with async_file.open(path, 'wb') as file_ctx:
            await file_ctx.write(data)

    async def get_binary_data_from_file(self, filename: str) -> bytes:
        path = Path(self.root, filename)
        if not path.exists():
            raise FileNotFoundError(f'Binary file {filename} not found')

        async with async_file.open(path, 'rb') as file_ctx:
            return await file_ctx.read()

    def remove_file(self, filename: str):
        if not self.is_file_exist(filename=filename):
            return
        Path(self.root, filename).unlink()

    def remove_files(self, *filenames):
        for filename in filenames:
            self.remove_file(filename=filename)
