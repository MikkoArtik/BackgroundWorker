from unittest.mock import Mock, patch

import pytest
from hamcrest import assert_that, equal_to

import gstream.models
from gstream.files.readers import DelaysFinderArgsBinaryFile
from gstream.models import DelaysFinderParameters


class TestDelaysFinderArgsBinaryFile:

    @pytest.mark.positive
    @patch.object(
        gstream.files.base.BaseBinaryFileReader, '_BaseBinaryFileReader__read'
    )
    @patch.object(
        gstream.models.DelaysFinderParameters,
        '__init__'
    )
    @patch.object(gstream.models.DelaysFinderParameters, 'create_from_bytes')
    @pytest.mark.asyncio
    async def test_convert_to_py_object_positive(
            self,
            mock_create_from_bytes: Mock,
            mock_check_arguments: Mock,
            mock_read: Mock
    ):
        mock_check_arguments.return_value = None
        expected_value = DelaysFinderParameters(
            signals=list(range(10)),
            window_size=10,
            scanner_size=2,
            min_correlation=2,
            base_station_index=0
        )
        mock_create_from_bytes.return_value = expected_value
        mock_read.return_value = None
        obj = DelaysFinderArgsBinaryFile(path=Mock())
        actual_value = await obj.convert_to_py_object()

        mock_create_from_bytes.assert_called_once_with(
            bytes_obj=await obj.src_data
        )
        assert_that(
            actual_or_assertion=actual_value,
            matcher=equal_to(expected_value)
        )
