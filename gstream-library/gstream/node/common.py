"""Module with common service methods."""

from dataclasses import dataclass

import numpy as np

USING_MEMORY_COEFFICIENT = 0.95

__all__ = [
    'convert_megabytes_to_bytes',
    'MemoryInfo',
    'USING_MEMORY_COEFFICIENT'
]


def convert_megabytes_to_bytes(value: int) -> int:
    """Convert megabytes values to bytes.

    Args:
        value: int

    Returns: int

    """
    return value * 1024 ** 2


@dataclass
class MemoryInfo:
    """Container with memory info.

    Args:
        total_volume: general volume in bytes
        used_volume: busy volume in bytes

    """
    total_volume: int
    used_volume: int

    @property
    def permitted_volume(self) -> int:
        """Return permitted volume for processing in bytes.

        Returns: int

        """
        permitted_volume = int(
            self.total_volume * USING_MEMORY_COEFFICIENT - self.used_volume
        )
        return max(0, permitted_volume)

    @property
    def free_volume(self) -> int:
        return self.total_volume - self.used_volume

    def get_max_array_size(self, arr_type: type = np.float32) -> int:
        """Return maximal possible 1D-array size.

        Args:
            arr_type: array type

        Returns: int

        """
        element_byte_size = arr_type(1.0).nbytes
        return int(self.permitted_volume / element_byte_size)
