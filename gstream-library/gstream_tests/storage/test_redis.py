import datetime
from unittest.mock import Mock, patch
import pytest
from hamcrest import assert_that, equal_to
from gstream.storage.redis import Storage, DATETIME_FORMAT, format_message


class TestFormatMessage:

    @pytest.mark.positive
    def test_format_message_positive(self):
        message = 'test'

        assert_that(
            actual_or_assertion=format_message(message=message),
            matcher=equal_to(
                f'[{datetime.datetime.now().strftime(DATETIME_FORMAT)}]'
                f' {message}\n'
            )
        )


class TestStorage:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        key_expiration = 'key_expiration'
        adapter = 'test'
        obj = Storage(pool=Mock(), key_expiration=key_expiration)
        obj._Storage__adapter = adapter

        assert_that(
            actual_or_assertion=obj._Storage__key_expiration,
            matcher=equal_to(key_expiration)
        )
        assert_that(
            actual_or_assertion=obj._Storage__adapter,
            matcher=equal_to(adapter)
        )
