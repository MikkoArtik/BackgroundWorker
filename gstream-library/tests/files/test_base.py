from unittest.mock import Mock, patch

import pytest
from gstream.files.base import BaseBinaryFileReader
from hamcrest import assert_that, equal_to, is_


class TestBaseBinaryFileReader:
    file_reader = BaseBinaryFileReader(path=Mock())

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        path = Mock()
        path.exists.return_value = True
        src_data = None
        file_reader = BaseBinaryFileReader(path=path)

        assert_that(
            actual_or_assertion=file_reader._BaseBinaryFileReader__path,
            matcher=equal_to(path)
        )
        assert_that(
            actual_or_assertion=file_reader._BaseBinaryFileReader__src_data,
            matcher=equal_to(src_data)
        )

    @pytest.mark.negative
    def test_correct_attributes_negative(self):
        path = Mock()
        path.exists.return_value = False

        with pytest.raises(FileNotFoundError):
            BaseBinaryFileReader(path=path)

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
