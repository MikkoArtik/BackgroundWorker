import numpy as np
import pytest
from hamcrest import assert_that, equal_to

from gstream.node.common import (
    USING_MEMORY_COEFFICIENT,
    MemoryInfo,
    convert_megabytes_to_bytes
)


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
    @pytest.mark.parametrize(
        'is_volume_zero', [True, False]
    )
    def test_permitted_volume_positive(self, is_volume_zero: bool):
        total_volume = 100
        if is_volume_zero:
            used_volume = total_volume * USING_MEMORY_COEFFICIENT + 10
            expected_value = 0
        else:
            used_volume = total_volume * USING_MEMORY_COEFFICIENT - 10
            expected_value = 10

        assert_that(
            actual_or_assertion=MemoryInfo(
                total_volume=total_volume,
                used_volume=used_volume
            ).permitted_volume,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    def test_free_volume_positive(self):
        total_volume = 100
        used_volume = 10

        assert_that(
            actual_or_assertion=MemoryInfo(
                total_volume=total_volume,
                used_volume=used_volume
            ).free_volume,
            matcher=equal_to(90)
        )

    @pytest.mark.positive
    def test_get_max_array_size_positive(self):
        obj = MemoryInfo(total_volume=100, used_volume=77)
        element_byte_size = np.float32(1.0).nbytes

        assert_that(
            actual_or_assertion=obj.get_max_array_size(),
            matcher=equal_to(int(obj.permitted_volume / element_byte_size))
        )
