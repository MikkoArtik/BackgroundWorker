import pathlib
from unittest.mock import AsyncMock, Mock, patch

import pytest
from hamcrest import assert_that, equal_to, is_

from gstream.storage.file_system import Storage


class TestStorage:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        root = Mock()
        root.exists.return_value = True
        root.is_dir.return_value = True

        assert_that(
            actual_or_assertion=Storage(root=root)._Storage__root,
            matcher=equal_to(root)
        )

    @pytest.mark.negative
    @pytest.mark.parametrize(
        ['is_exists', 'is_dir'], [(True, False), (False, True)]
    )
    def test_correct_attributes_negative(self, is_exists: bool, is_dir: bool):
        root = Mock()

        if is_exists:
            root.exists.return_value = False
            expected_value = 'Storage root not found'

        if is_dir:
            root.exists.return_value = True
            root.is_dir.return_value = False
            expected_value = 'Storage root is not directory'

        with pytest.raises(OSError) as error:
            Storage(root=root)

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to(expected_value)
            )

    @pytest.mark.positive
    def test_root_positive(self):
        root = Mock()
        root.exists.return_value = True
        root.is_dir.return_value = True

        assert_that(
            actual_or_assertion=Storage(root=root).root,
            matcher=equal_to(root)
        )

    @pytest.mark.positive
    @patch.object(pathlib.PurePath, '_from_parts')
    def test_all_filenames_positive(self, mock_from_parts: Mock):
        path = Mock()
        path.is_dir.return_value = True
        mock_from_parts._flavour.is_supported = path
        mock_from_parts.return_value.is_dir.return_value = False
        root = Mock()
        expected_value = 'test'
        item = Mock()
        item.return_value.name = expected_value
        root.iterdir.return_value = [item]
        assert_that(
            actual_or_assertion=Storage(root=root).all_filenames,
            matcher=equal_to({item.name})
        )

    @pytest.mark.positive
    @patch.object(pathlib.PurePath, '_from_parts')
    def test_is_file_exist_positive(self, mock_from_parts: Mock):
        path = Mock()
        path.return_value.exists.return_value = 'test'
        mock_from_parts._flavour.is_supported = path

        assert_that(
            actual_or_assertion=Storage(root=Mock()).is_file_exist(
                filename=Mock()
            ),
            matcher=equal_to(mock_from_parts().exists())
        )

    @pytest.mark.positive
    @pytest.mark.asyncio
    @patch('aiofiles.open')
    @patch.object(pathlib.PurePath, '_from_parts')
    async def test_save_binary_data_positive(
            self,
            mock_from_parts: Mock,
            mock_open: AsyncMock
    ):
        mock_from_parts.return_value.exists.return_value = False
        mock_from_parts._flavour.is_supported = True

        mock_file = AsyncMock()
        mock_file.write.return_value = None

        mock_open.return_value.__aenter__.return_value = mock_file
        data = b'data'

        await Storage(root=Mock()).save_binary_data(data=data, filename='')
        mock_file.write.assert_called_once_with(data)

    @pytest.mark.negative
    @pytest.mark.asyncio
    @patch.object(pathlib.PurePath, '_from_parts')
    async def test_save_binary_data_negative(self, mock_from_parts: Mock):
        mock_from_parts.return_value.exists.return_value = True
        mock_from_parts._flavour.is_supported = True
        filename = 'test'

        with pytest.raises(FileExistsError) as error:
            await Storage(root=Mock()).save_binary_data(
                data=b'test',
                filename=filename
            )
            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to(f'Binary file {filename} is exist')
            )

    @pytest.mark.positive
    @pytest.mark.asyncio
    @patch('aiofiles.open')
    @patch.object(pathlib.PurePath, '_from_parts')
    async def test_get_binary_data_from_file_positive(
            self,
            mock_from_parts: Mock,
            mock_open: AsyncMock
    ):
        mock_from_parts.return_value.exists.return_value = True
        mock_from_parts._flavour.is_supported = True

        expected_value = b'test'
        mock_file = AsyncMock()
        mock_file.read.return_value = expected_value

        mock_open.return_value.__aenter__.return_value = mock_file

        assert_that(
            actual_or_assertion=await Storage(
                root=Mock()
            ).get_binary_data_from_file(filename=''),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.negative
    @pytest.mark.asyncio
    @patch.object(pathlib.PurePath, '_from_parts')
    async def test_get_binary_data_from_file_negative(
            self,
            mock_from_parts: Mock
    ):
        mock_from_parts.return_value.exists.return_value = False
        mock_from_parts._flavour.is_supported = True
        filename = 'test'

        with pytest.raises(FileNotFoundError) as error:
            await Storage(
                root=Mock()
            ).get_binary_data_from_file(filename=filename)

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to(f'Binary file {filename} not found')
            )

    @pytest.mark.positive
    @patch.object(pathlib.PurePath, '_from_parts')
    def test_remove_file_positive(
            self,
            mock_from_parts: Mock
    ):
        mock_from_parts.return_value.unlink.return_value = True
        mock_from_parts._flavour.is_supported = True
        Storage(root=Mock()).remove_file(filename='test')
        mock_from_parts.return_value.unlink.assert_called_once()

    @pytest.mark.negative
    @patch.object(Storage, 'is_file_exist')
    def test_remove_file_negative(
            self,
            mock_is_file_exist: Mock
    ):
        mock_is_file_exist.return_value = False

        assert_that(
            actual_or_assertion=Storage(root=Mock()).remove_file(filename=''),
            matcher=is_(None)
        )

    @pytest.mark.positive
    @patch.object(Storage, 'remove_file')
    def test_remove_files_positive(self, mock_remove_files: Mock):
        filenames = list(range(5))
        mock_remove_files.side_effect = filenames
        Storage(root=Mock()).remove_files(filenames)
        mock_remove_files.assert_called_with(filename=filenames)
