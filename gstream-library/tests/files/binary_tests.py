import pytest
from hamcrest import assert_that, equal_to, is_
from gstream.files.binary import CharType
from typing import Union


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
        [('w', 's'), ('test', '4s')]
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
