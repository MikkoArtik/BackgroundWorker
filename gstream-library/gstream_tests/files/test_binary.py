from typing import List, Union
from unittest.mock import Mock, patch

import pytest
from gstream.files.binary import (
    MAX_FLOAT,
    MAX_INT,
    MIN_FLOAT,
    MIN_INT,
    CharType,
    DoubleType,
    IntType
)
from hamcrest import assert_that, equal_to, is_


class TestCharType:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        char_type = CharType()
        assert_that(
            actual_or_assertion=char_type._CharType__label,
            matcher=equal_to('s')
        )
        assert_that(
            actual_or_assertion=char_type.byte_size,
            matcher=equal_to(1)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [('test-value', True), (1, False), ('', False)]
    )
    def test_is_correct_value_positive(
            self,
            value: Union[int, str],
            expected_value: bool
    ):
        assert_that(
            actual_or_assertion=CharType._is_correct_value(obj=value),
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            ('w', CharType()._CharType__label),
            ('test', f'4{CharType()._CharType__label}')
        ]
    )
    def test_generate_format_string_positive(
            self,
            value: str,
            expected_value: str
    ):
        assert_that(
            actual_or_assertion=CharType()._generate_format_string(
                obj=value
            ),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.negative
    def test_generate_format_string_negative(self):
        with pytest.raises(ValueError) as error:
            CharType()._generate_format_string(obj=1)

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to('Value has invalid type or empty')
            )

    @pytest.mark.positve
    def test_pack_positive(self):
        expected_value = 'test'
        assert_that(
            actual_or_assertion=CharType().pack(obj=expected_value),
            matcher=equal_to(expected_value.encode())
        )

    @pytest.mark.positive
    def test_unpack_positive(self):
        expected_value = 'test'
        assert_that(
            actual_or_assertion=CharType().unpack(
                value=expected_value.encode(),
                symbols_count=len(expected_value)
            ),
            matcher=equal_to(expected_value)
        )


class TestIntType:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        int_type = IntType()
        assert_that(
            actual_or_assertion=int_type._IntType__label,
            matcher=equal_to('i')
        )
        assert_that(
            actual_or_assertion=int_type.byte_size,
            matcher=equal_to(4)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            (MIN_INT - 1, False),
            (MIN_INT, True),
            ((MIN_INT + MAX_INT) // 2, True),
            ((MIN_INT + MAX_INT) / 2, True),
            (MAX_INT - 1, True),
            (MAX_INT, True),
            (MAX_INT + 1, False)
        ]
    )
    def test_is_in_range_positive(self, value: int, expected_value: bool):
        assert_that(
            actual_or_assertion=IntType._IntType__is_in_range(value=value),
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            ((MIN_INT + MAX_INT) // 2, True),
            ((MIN_INT + MAX_INT) / 2, False),
            ([], False),
            (['test'], False),
            ([MIN_INT - 1], False),
            ([(MIN_INT + MAX_INT) // 2, 'test'], False),
            ([(MIN_INT + MAX_INT) // 2, (MIN_INT + MAX_INT) / 2], False),
            ([(MIN_INT + MAX_INT) // 2], True),
            ('test', False)
        ]
    )
    def test_is_correct_value_positive(
            self,
            value: Union[str, int, List[Union[str, int]]],
            expected_value: bool
    ):
        assert_that(
            actual_or_assertion=IntType._is_correct_value(obj=value),
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            ([(MIN_INT + MAX_INT) // 2], IntType()._IntType__label),
            ([MIN_INT + 1, MIN_INT + 10], f'2{IntType()._IntType__label}'),
            ((MIN_INT + MAX_INT) // 2, IntType()._IntType__label)
        ]
    )
    def test_generate_format_string_positive(
            self,
            value: Union[List[int], int],
            expected_value: str
    ):
        assert_that(
            actual_or_assertion=IntType._generate_format_string(obj=value),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.negative
    def test_generate_format_string_negative(self):
        with pytest.raises(ValueError) as error:
            IntType._generate_format_string(obj='')

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to('Value has invalid type or empty')
            )

    @pytest.mark.positive
    @patch('struct.pack')
    @pytest.mark.parametrize(
        'is_obj_list', [True, False]
    )
    def test_pack_positive(self, mock_pack: Mock, is_obj_list: bool):
        if is_obj_list:
            obj = list(range(10))

        else:
            obj = 777

        expected_value = b'test'
        mock_pack.return_value = expected_value
        actual_value = IntType.pack(obj=obj)
        mock_pack.assert_called_once()

        assert_that(
            actual_or_assertion=actual_value,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('struct.unpack')
    @pytest.mark.parametrize(
        'is_return_list', [True, False]
    )
    def test_unpack_positive(self, mock_unpack: Mock, is_return_list: bool):
        if is_return_list:
            numbers_count = 2
            expected_value = [12, 34]
            mock_unpack.return_value = expected_value

        else:
            numbers_count = 1
            expected_value = 1234
            mock_unpack.return_value = [expected_value]

        fmt = f'{numbers_count}{IntType._IntType__label}'
        value = b'test'
        actual_value = IntType.unpack(
            value=value,
            numbers_count=numbers_count
        )
        mock_unpack.assert_called_once_with(fmt, value)

        assert_that(
            actual_or_assertion=actual_value,
            matcher=equal_to(expected_value)
        )


class TestDoubleType:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        double_type = DoubleType()
        assert_that(
            actual_or_assertion=double_type._DoubleType__label,
            matcher=equal_to('d')
        )
        assert_that(
            actual_or_assertion=double_type.byte_size,
            matcher=equal_to(8)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            (MIN_FLOAT - 1, False),
            (MIN_FLOAT, True),
            ((MIN_FLOAT + MAX_FLOAT) / 2, True),
            ((MIN_FLOAT + MAX_FLOAT) // 2, True),
            (MAX_FLOAT - 1, True),
            (MAX_FLOAT, True),
            (MAX_FLOAT + 1, False)
        ]
    )
    def test_is_in_range_positive(self, value: int, expected_value: bool):
        assert_that(
            actual_or_assertion=DoubleType._DoubleType__is_in_range(
                value=value
            ),
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            ((MIN_FLOAT + MAX_FLOAT) / 2, True),
            ((MIN_FLOAT + MAX_FLOAT) // 2, True),
            ([], False),
            (['test'], False),
            ([MIN_FLOAT - 1], False),
            ([(MIN_FLOAT + MAX_FLOAT) / 2, 'test'], False),
            (
                [(MIN_FLOAT + MAX_FLOAT) / 2, (MIN_FLOAT + MAX_FLOAT) // 2],
                True
            ),
            ([(MIN_FLOAT + MAX_FLOAT) / 2], True),
            ('test', False)
        ]
    )
    def test_is_correct_value_positive(
            self,
            value: Union[str, int, List[Union[str, int]]],
            expected_value: bool
    ):
        assert_that(
            actual_or_assertion=DoubleType._is_correct_value(obj=value),
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['value', 'expected_value'],
        [
            ([(MIN_FLOAT + MAX_FLOAT) // 2], DoubleType()._DoubleType__label),
            (
                [MIN_FLOAT + 1, MIN_FLOAT + 10],
                f'2{DoubleType()._DoubleType__label}'
            ),
            ((MIN_FLOAT + MAX_FLOAT) // 2, DoubleType()._DoubleType__label)
        ]
    )
    def test_generate_format_string_positive(
            self,
            value: Union[List[int], int],
            expected_value: str
    ):
        assert_that(
            actual_or_assertion=DoubleType._generate_format_string(obj=value),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.negative
    def test_generate_format_string_negative(self):
        with pytest.raises(ValueError) as error:
            DoubleType._generate_format_string(obj='')

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to('Value has invalid type or empty')
            )

    @pytest.mark.positive
    @patch('struct.pack')
    @pytest.mark.parametrize(
        'is_obj_list', [True, False]
    )
    def test_pack_positive(self, mock_pack: Mock, is_obj_list: bool):
        if is_obj_list:
            obj = [i / 2 for i in range(10)]

        else:
            obj = 77.7

        expected_value = b'test'
        mock_pack.return_value = expected_value
        actual_value = DoubleType.pack(obj=obj)
        mock_pack.assert_called_once()

        assert_that(
            actual_or_assertion=actual_value,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('struct.unpack')
    @pytest.mark.parametrize(
        'is_return_list', [True, False]
    )
    def test_unpack_positive(self, mock_unpack: Mock, is_return_list: bool):
        if is_return_list:
            numbers_count = 2
            expected_value = [12, 34]
            mock_unpack.return_value = expected_value

        else:
            numbers_count = 1
            expected_value = 1234
            mock_unpack.return_value = [expected_value]

        fmt = f'{numbers_count}{DoubleType._DoubleType__label}'
        value = b'test'
        actual_value = DoubleType.unpack(
            value=value,
            numbers_count=numbers_count
        )
        mock_unpack.assert_called_once_with(fmt, value)

        assert_that(
            actual_or_assertion=actual_value,
            matcher=equal_to(expected_value)
        )
