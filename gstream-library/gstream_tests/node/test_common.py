import pytest
from hamcrest import assert_that, equal_to, is_
from gstream.node.common import convert_megabytes_to_bytes, MemoryInfo, USING_MEMORY_COEFFICIENT


@pytest.mark.positive
@pytest.mark.parametrize(
    ['value', 'expected_value'],
    [(0, 0), (17, 17825792)]
)
def test_convert_megabytes_to_bytes_positive(value: int, expected_value: int):
    assert_that(
        actual_or_assertion=convert_megabytes_to_bytes(value=value),
        matcher=equal_to(expected_value)
    )


@pytest.mark.negative
def test_convert_megabytes_to_bytes_negative():
    with pytest.raises(ValueError) as error:
        convert_megabytes_to_bytes(value=-20)

        assert_that(
            actual_or_assertion=error.value,
            matcher=equal_to('Value can`t be negative')
        )


class TestMemoryInfo:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        total_volume = 100
        used_volume = 10
        obj = MemoryInfo(total_volume=total_volume, used_volume=used_volume)

        assert_that(
            actual_or_assertion=obj.total_volume,
            matcher=equal_to(total_volume)
        )
        assert_that(
            actual_or_assertion=obj.used_volume,
            matcher=equal_to(used_volume)
        )

    @pytest.mark.positive
    def test_permitted_volume_positive(self):

