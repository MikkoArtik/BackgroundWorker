from unittest.mock import Mock

import pytest
from hamcrest import assert_that, equal_to

from gstream.files.writers import (
    DelaysFinderArgsBinaryFile,
    DelaysFinderResultBinaryFile
)


class TestDelaysFinderArgsBinaryFile:

    @pytest.mark.positive
    def test_data_positive(self):
        expected_value = 'test-data'

        assert_that(
            actual_or_assertion=DelaysFinderArgsBinaryFile(
                path=Mock(),
                data=expected_value
            )._data,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    def test_convert_to_bytes_positive(self):
        expected_value = b'test'
        data = Mock()
        data.convert_to_bytes.return_value = expected_value

        assert_that(
            actual_or_assertion=DelaysFinderArgsBinaryFile(
                path=Mock(),
                data=data
            )._convert_to_bytes(),
            matcher=equal_to(expected_value)
        )


class TestDelaysFinderResultBinaryFile:

    @pytest.mark.positive
    def test_data_positive(self):
        expected_value = 'test-data'

        assert_that(
            actual_or_assertion=DelaysFinderResultBinaryFile(
                path=Mock(),
                data=expected_value
            )._data,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    def test_convert_to_bytes_positive(self):
        expected_value = b'test'
        data = Mock()
        data.convert_to_bytes.return_value = expected_value

        assert_that(
            actual_or_assertion=DelaysFinderResultBinaryFile(
                path=Mock(),
                data=data
            )._convert_to_bytes(),
            matcher=equal_to(expected_value)
        )
