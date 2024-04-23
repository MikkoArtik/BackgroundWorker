"""Module for working with binary data."""

import struct
from dataclasses import dataclass
from typing import List, Union

__all__ = [
    'CharType',
    'IntType',
    'DoubleType'
]


@dataclass
class CharType:
    """Char type class.

    Args:
        __label: C label type
        __byte_size: int

    """

    __label = 's'
    __byte_size = 1

    @staticmethod
    def _is_correct_value(obj: str) -> bool:
        if isinstance(obj, str):
            if not obj:
                return False
            return True
        return False

    def _generate_format_string(self, obj: str) -> str:
        """Returns type format string.

        Args:
            obj: str

        Returns: str

        """
        if not self._is_correct_value(obj=obj):
            raise ValueError('Value has invalid type or empty')
        if len(obj) == 1:
            return self.__label
        return f'{len(obj)}{self.__label}'

    def pack(self, obj: str) -> bytes:
        """Pack string to byte object.

        Args:
            obj: str

        Returns: bytes

        """
        return struct.pack(
            self._generate_format_string(obj=obj),
            obj.encode()
        )

    def unpack(self, value: bytes, symbols_count: int) -> str:
        """Unpack bytes to string.

        Args:
            value: bytes
            symbols_count: int

        Returns: string

        """
        fmt = f'{symbols_count}{self.__label}'
        return struct.unpack(fmt, value)[0].decode('utf-8')


@dataclass
class IntType:
    """Integer type class.

    Args:
        __label: C label type
        __byte_size: int

    """
    __label = 'i'
    __byte_size = 4

    @staticmethod
    def __is_in_range(value: int) -> bool:
        """Check value by range.

        Args:
            value: int

        Returns: bool

        """
        if value < -2_000_000_000 or value > 2_000_000_000:
            return False
        return True

    def _is_correct_value(self, obj: Union[List[int], int]) -> bool:
        """Check correction by value.

        Args:
            obj: Union[List[int], int]

        Returns: bool

        """
        if isinstance(obj, int):
            return self.__is_in_range(value=obj)
        elif isinstance(obj, list):
            if not obj:
                return False

            for item in obj:
                if not isinstance(item, int):
                    return False
                if not self.__is_in_range(value=item):
                    return False
            return True
        else:
            return False

    def _generate_format_string(self, obj: Union[List[int], int]) -> str:
        """Returns type format string.

        Args:
            obj: Union[List[int], int]

        Returns: str

        """
        if not self._is_correct_value(obj=obj):
            raise ValueError('Value has invalid type or empty')

        if isinstance(obj, list):
            if len(obj) == 1:
                return self.__label
            return f'{len(obj)}{self.__label}'
        else:
            return self.__label

    def pack(self, obj: Union[List[int], int]) -> bytes:
        """Pack integer values to byte object.

        Args:
            obj: Union[List[int], int]

        Returns: bytes

        """
        if not isinstance(obj, list):
            obj = [obj]
        return struct.pack(self._generate_format_string(obj=obj), *obj)

    def unpack(
            self,
            value: bytes,
            numbers_count: int
    ) -> Union[List[int], int]:
        """Unpack bytes to integer values.

        Args:
            value: bytes
            numbers_count: int

        Returns: Union[List[int], int]

        """
        fmt = f'{numbers_count}{self.__label}'
        values = list(struct.unpack(fmt, value))
        if numbers_count == 1:
            return values[0]
        return values


@dataclass
class DoubleType:
    """Float (Double) type class.

    Args:
        __label: C label type
        __byte_size: int

    """
    __label = 'd'
    __byte_size = 8

    @staticmethod
    def __is_in_range(value: float) -> bool:
        """Check value by range.

        Args:
            value: float

        Returns: bool

        """
        if value < -1e14 or value > 1e14:
            return False
        return True

    def _is_correct_value(self, obj: Union[List[float], float]) -> bool:
        """Check correction by value.

        Args:
            obj: Union[List[float], float]

        Returns: bool

        """
        if isinstance(obj, (int, float)):
            return self.__is_in_range(value=obj)
        elif isinstance(obj, list):
            if not obj:
                return False

            for item in obj:
                if not isinstance(item, (int, float)):
                    return False
                if not self.__is_in_range(value=item):
                    return False
            return True
        else:
            return False

    def _generate_format_string(self, obj: Union[List[float], float]) -> str:
        """Returns type format string.

        Args:
            obj: Union[List[float], float]

        Returns: str

        """
        if not self._is_correct_value(obj=obj):
            raise ValueError('Value has invalid type or empty')

        if isinstance(obj, list):
            if len(obj) == 1:
                return self.__label
            return f'{len(obj)}{self.__label}'
        else:
            return self.__label

    def pack(self, obj: Union[List[float], float]) -> bytes:
        """Pack integer values to byte object.

        Args:
            obj: Union[List[float], float]

        Returns: bytes

        """
        if not isinstance(obj, list):
            obj = [float(obj)]
        else:
            for i in range(len(obj)):
                obj[i] = float(obj[i])
        return struct.pack(self._generate_format_string(obj=obj), *obj)

    def unpack(
            self,
            value: bytes,
            numbers_count: int
    ) -> Union[List[float], float]:
        """Unpack bytes to integer values.

        Args:
            value: bytes
            numbers_count: int

        Returns: Union[List[int], int]

        """
        fmt = f'{numbers_count}{self.__label}'
        values = list(struct.unpack(fmt, value))
        if numbers_count == 1:
            return values[0]
        return values
