from unittest.mock import AsyncMock, Mock, patch

import pytest
from gstream.files.base import (
    BaseBinaryFileReader,
    BaseBinaryFileWriter,
    BaseTxtFileWriter
)
from hamcrest import assert_that, equal_to, is_


class TestBaseBinaryFileReader:
    file_reader = BaseBinaryFileReader(path=Mock())

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        path = Mock()
        path.exists.return_value = True
        file_reader = BaseBinaryFileReader(path=path)

        assert_that(
            actual_or_assertion=file_reader._BaseBinaryFileReader__path,
            matcher=equal_to(path)
        )
        assert_that(
            actual_or_assertion=file_reader._BaseBinaryFileReader__src_data,
            matcher=equal_to(None)
        )

    @pytest.mark.negative
    def test_correct_attributes_negative(self):
        path = Mock()
        path.exists.return_value = False

        with pytest.raises(FileNotFoundError) as error:
            BaseBinaryFileReader(path=path)

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to('File not found')
            )

    @pytest.mark.positive
    @pytest.mark.asyncio
    async def test_convert_to_py_object_positive(self):
        assert_that(
            actual_or_assertion=await self.file_reader.convert_to_py_object(),
            matcher=is_(None)
        )

    @pytest.mark.positive
    @patch.object(BaseBinaryFileReader, '_BaseBinaryFileReader__read')
    @pytest.mark.asyncio
    async def test_src_data(self,
                            mock_read: Mock):
        expected_value = 'test-stc-data'
        mock_read.return_value = expected_value

        assert_that(
            actual_or_assertion=await self.file_reader.src_data,
            matcher=equal_to(expected_value)
        )

    @patch('aiofiles.open')
    @pytest.mark.asyncio
    async def test_read_file(self,
                             mock_open: Mock):
        expected_value = b'test-content'
        mock_file = AsyncMock()
        mock_file.read.return_value = expected_value

        mock_open.return_value.__aenter__.return_value = mock_file

        file_reader = BaseBinaryFileReader(path=Mock())
        file_reader._BaseBinaryFileReader__path = 'path/to/file'

        result = await file_reader._BaseBinaryFileReader__read()

        assert_that(
            actual_or_assertion=result,
            matcher=equal_to(expected_value)
        )


class TestBaseBinaryFileWriter:
    file_writer = BaseBinaryFileWriter(path=Mock(), data='test-data')

    @pytest.mark.positive
    @pytest.mark.asyncio
    async def test_correct_attributes_positive(self):
        path = Mock()
        data = 'test-data'
        file_writer = BaseBinaryFileWriter(path=path, data=data)
        assert_that(
            actual_or_assertion=file_writer._BaseBinaryFileWriter__path,
            matcher=equal_to(path)
        )
        assert_that(
            actual_or_assertion=file_writer._BaseBinaryFileWriter__data,
            matcher=equal_to(data)
        )

    @pytest.mark.negative
    @pytest.mark.asyncio
    async def test_correct_attributes_negative(self):
        path = Mock()
        path.parent.return_value = None
        path.parent.exists.return_value = False

        with pytest.raises(OSError) as error:
            BaseBinaryFileWriter(path=path, data='test-data')

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to('Folder for saving is not found')
            )

    @pytest.mark.positive
    @pytest.mark.asyncio
    async def test_data_positive(self):
        assert_that(
            actual_or_assertion=self.file_writer._data,
            matcher=is_(None)
        )

    @pytest.mark.positive
    @pytest.mark.asyncio
    async def test_convert_to_bytes_positive(self):
        assert_that(
            actual_or_assertion=self.file_writer._convert_to_bytes(),
            matcher=is_(None)
        )

    @pytest.mark.positive
    @patch('aiofiles.open')
    @pytest.mark.asyncio
    async def test_save_positive(self,
                                 mock_open: Mock):
        mock_file = AsyncMock()
        mock_file.write.return_value = None
        mock_open.return_value.__aenter__.return_value = mock_file

        await self.file_writer.save()

        mock_file.write.assert_called_once_with(
            self.file_writer._convert_to_bytes()
        )


class TestBaseTxtFileWriter:

    @pytest.mark.positive
    @pytest.mark.asyncio
    async def test_correct_attributes_positive(self):
        path = 'test-path'
        body = 'test-body'
        file_writer = BaseTxtFileWriter(path=path, body=body)

        assert_that(
            actual_or_assertion=file_writer._BaseTxtFileWriter__path,
            matcher=equal_to(path)
        )
        assert_that(
            actual_or_assertion=file_writer._BaseTxtFileWriter__body,
            matcher=equal_to(body)
        )

    @pytest.mark.positive
    @patch('aiofiles.open')
    @pytest.mark.asyncio
    async def test_save_positive(self, mock_open: Mock):
        mock_file = AsyncMock()
        mock_file.write.return_value = None
        mock_open.return_value.__aenter__.return_value = mock_file

        body = 'test-body'
        await BaseTxtFileWriter(path='test-path', body=body).save()

        mock_file.write.assert_called_once_with(body)
