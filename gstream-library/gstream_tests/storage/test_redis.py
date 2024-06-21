import datetime
from unittest.mock import Mock, patch, AsyncMock, PropertyMock
import pytest
from hamcrest import assert_that, equal_to
from gstream.storage.redis import Storage, DATETIME_FORMAT, format_message
from redis.asyncio import ConnectionPool, Redis


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

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['src_dict', 'expected_value'],
        [
            ({'key_': 1}, {'key_': 1}),
            ({'key_': {'key2': 2}}, {'key_:key2': 2}),
            ({'key_': {'key2': {'key3': 3}}}, {'key_:key2:key3': 3}),
            ({'key_': True}, {'key_': 1}),
            ({'key_': False}, {'key_': 0})
         ]
    )
    def test_create_flatten_dict_positive(
            self,
            src_dict: dict,
            expected_value: dict
    ):
        obj = Storage(pool=Mock(), key_expiration='test')

        assert_that(
            actual_or_assertion=obj._Storage__create_flatten_dict(
                src_dict=src_dict
            ),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    def test_adapter_positive(self):
        adapter = 'adapter'
        obj = Storage(pool=Mock(), key_expiration='test')
        obj._Storage__adapter = adapter

        assert_that(
            actual_or_assertion=obj.adapter,
            matcher=equal_to(adapter)
        )

    @pytest.mark.positive
    @pytest.mark.asyncio
    @patch.object(Storage, 'adapter', new_callable=PropertyMock)
    @patch.object(Storage, '_Storage__create_flatten_dict')
    async def test_add_dict_positive(
            self,
            mock_create_dict: Mock,
            mock_adapter: Mock
    ):
        flatten_dict = {1: 1}
        mock_create_dict.return_value = flatten_dict
        mset = AsyncMock()
        mset.return_value = None
        mock_adapter.return_value = mset
        await Storage(
            pool=AsyncMock(),
            key_expiration='test'
        )._Storage__add_dict(value='q')

        mock_adapter.return_value.mset.assert_called_once_with(
            mapping=flatten_dict
        )
