import datetime
import json
import struct
import uuid
from enum import Enum
from time import time
from typing import Tuple

import numpy as np
from gstream.files.binary import CharType, DoubleType, IntType
from pydantic import BaseModel, Field, root_validator, validator


class TaskType(Enum):
    DELAYS = 'delays'
    LOCATION = 'location'
    FAULT = 'fault'


class TaskStatus(Enum):
    NEW = 'new'
    READY = 'ready'
    RUNNING = 'running'
    FAILED = 'failed'
    FINISHED = 'finished'
    KILLED = 'killed'


class ArrayType(Enum):
    INT32 = 'int32'
    FLOAT32 = 'float32'


def check_task_type(type_: str) -> str:
    if type_ in TaskType._value2member_map_:
        return type_
    raise KeyError('Task type is invalid')


def check_array_type(type_: str) -> str:
    if type_ in ArrayType._value2member_map_:
        return type_
    raise KeyError('Type array is invalid')


class CustomBaseModel(BaseModel):
    class Config:
        """Model configuration."""

        allow_population_by_field_name = True


class TaskState(CustomBaseModel):
    user_id: str = Field(
        alias='UserID',
        exclude=True
    )
    task_id: str = Field(
        alias='TaskID',
        default_factory=lambda: uuid.uuid4().hex,
        exclude=True
    )
    type_: str = Field(alias='Type')
    status: str = Field(alias='Status', default=TaskStatus.NEW.value)
    is_accepted: bool = Field(alias='IsAccepted', default=False)
    pid: int = Field(alias='PID', default=-1)
    is_need_kill: bool = Field(alias='IsNeedKill', default=False)
    modified_unix_time: int = Field(
        alias='ModifiedUnixTime',
        default_factory=lambda: int(time())
    )
    init_script_filename: str = Field(
        alias='InitScriptFilename',
        default_factory=lambda: f'{uuid.uuid4().hex}.py'
    )
    input_args_filename: str = Field(
        alias='InputArgumentsFilename',
        default_factory=lambda: uuid.uuid4().hex
    )
    output_args_filename: str = Field(
        alias='OutputArgumentsFilename',
        default_factory=lambda: uuid.uuid4().hex
    )
    script_filename: str = Field(
        alias='ScriptFilename',
        default_factory=lambda: uuid.uuid4().hex + '.py'
    )

    _check_type = validator(
        'type_', allow_reuse=True
    )(
        lambda value: check_task_type(type_=value)
    )

    @root_validator
    def set_modified_datetime(cls, values: dict) -> dict:
        values['modified_unix_time'] = int(time())
        return values

    @property
    def modified_datetime(self) -> datetime:
        return datetime.datetime.fromtimestamp(
            timestamp=self.modified_unix_time
        )

    @property
    def dict_view(self) -> dict:
        return {
            'User': {
                self.user_id: {
                    'Task': {
                        self.task_id: {
                            'State': json.dumps(self.dict(by_alias=True))
                        }
                    }
                }
            }
        }

    @property
    def all_filenames(self) -> Tuple[str, str, str]:
        return (
            self.input_args_filename,
            self.script_filename,
            self.output_args_filename
        )

    def rollback(self):
        self.status = TaskStatus.READY.value
        self.pid = -1


class ArraySize(CustomBaseModel):
    rows_count: int = Field(alias='RowsCount')
    cols_count: int = Field(alias='ColsCount')

    @property
    def tuple_view(self) -> Tuple[int, int]:
        return self.rows_count, self.cols_count


class Array(CustomBaseModel):
    type_: str = Field(alias='Type')
    shape: ArraySize = Field(alias='Shape')
    data: bytes = Field(alias='Data')

    _check_type = validator(
        'type_', allow_reuse=True
    )(
        lambda value: check_array_type(type_=value)
    )

    @property
    def dtype(self) -> np.dtype:
        if self.type_ == ArrayType.INT32.value:
            return np.int32
        elif self.type_ == ArrayType.FLOAT32.value:
            return np.float32
        else:
            pass

    @property
    def bytes_size(self) -> int:
        bytes_size = CharType.byte_size * len(self.type_)
        bytes_size += len(self.shape.tuple_view) * IntType.byte_size
        return bytes_size + len(self.data)

    def convert_to_numpy_format(self) -> np.ndarray:
        vector = np.frombuffer(self.data, self.dtype)
        if self.shape.rows_count == 0 or self.shape.cols_count == 0:
            shape = max(self.shape.rows_count, self.shape.cols_count)
        else:
            shape = self.shape.tuple_view
        return np.reshape(vector, shape)

    def convert_to_bytes(self) -> bytes:
        bytes_value = CharType.pack(obj=self.type_)
        bytes_value += IntType.pack(obj=list(self.shape.tuple_view))
        bytes_value += self.data
        return bytes_value

    @staticmethod
    def create_from_numpy_array(arr: np.ndarray) -> 'Array':
        if arr.dtype == np.int32:
            array_type = ArrayType.INT32.value
        elif arr.dtype == np.float32:
            array_type = ArrayType.FLOAT32.value
        else:
            raise TypeError('Unsupported array type')

        return Array(
            type_=array_type,
            shape=ArraySize(
                rows_count=arr.shape[0],
                cols_count=arr.shape[1]
            ),
            data=arr.tobytes()
        )

    @staticmethod
    def __get_type_from_bytes(bytes_obj: bytes) -> str:
        for type_name in ArrayType._value2member_map_:
            symbols_count = len(type_name)
            try:
                actual_type = CharType.unpack(
                    value=bytes_obj[:symbols_count * CharType.byte_size],
                    symbols_count=symbols_count
                )
                if actual_type == type_name:
                    return type_name
            except struct.error:
                continue
        else:
            raise TypeError('Unsupported array type')

    @classmethod
    def create_from_bytes(cls, bytes_obj: bytes) -> 'Array':
        array_type = cls.__get_type_from_bytes(bytes_obj=bytes_obj)

        left_edge = len(array_type) * CharType.byte_size
        right_edge = left_edge + 2 * IntType.byte_size
        rows_count, cols_count = IntType().unpack(
            value=bytes_obj[left_edge:right_edge],
            numbers_count=2
        )

        element_size = np.dtype(array_type).itemsize

        actual_array_bytes_size = len(bytes_obj) - right_edge
        excepted_array_bytes_size = rows_count * cols_count * element_size
        if excepted_array_bytes_size > actual_array_bytes_size:
            raise ValueError('Invalid input bytes object')

        return Array(
            type_=array_type,
            shape=ArraySize(
                rows_count=rows_count,
                cols_count=cols_count
            ),
            data=bytes_obj[right_edge:right_edge + excepted_array_bytes_size]
        )


class DelaysFinderParameters(CustomBaseModel):
    """Pydantic model for delays finder parameters.

    Args:
        # signals: signals array
        window_size: window size
        scanner size: scanner size
        min_correlation: minimal correlation value
        base_station_index: base station index

    """

    signals: Array = Field(alias='Signals')
    window_size: int = Field(alias='WindowSize')
    scanner_size: int = Field(alias='ScannerSize')
    min_correlation: float = Field(alias='MinCorrelation')
    base_station_index: int = Field(alias='BaseStationIndex')

    @property
    def signals_length(self) -> int:
        return self.signals.shape.cols_count

    @property
    def stations_count(self) -> int:
        return self.signals.shape.rows_count

    @property
    def buffer(self) -> int:
        return self.window_size + self.scanner_size

    def convert_to_bytes(self) -> bytes:
        bytes_value = IntType.pack(
            obj=[self.window_size, self.scanner_size]
        )
        bytes_value += DoubleType.pack(obj=self.min_correlation)
        bytes_value += IntType.pack(
            obj=self.base_station_index
        )
        bytes_value += self.signals.convert_to_bytes()
        return bytes_value

    @staticmethod
    def create_from_bytes(bytes_obj: bytes) -> 'DelaysFinderParameters':
        left_index, right_index = 0, 2 * IntType.byte_size
        window_size, scanner_size = IntType.unpack(
            value=bytes_obj[left_index:right_index],
            numbers_count=2
        )

        left_index = right_index
        right_index += DoubleType.byte_size
        min_correlation = DoubleType.unpack(
            value=bytes_obj[left_index:right_index],
            numbers_count=1
        )

        left_index = right_index
        right_index += IntType.byte_size
        base_station_index = IntType.unpack(
            value=bytes_obj[left_index:right_index],
            numbers_count=1
        )

        left_index = right_index

        right_index += CharType.byte_size * len(ArrayType.FLOAT32.value)
        array_type = CharType.unpack(
            value=bytes_obj[left_index: right_index],
            symbols_count=len(ArrayType.FLOAT32.value)
        )

        left_index = right_index
        right_index += 2 * IntType.byte_size
        rows_count, cols_count = IntType.unpack(
            value=bytes_obj[left_index: right_index],
            numbers_count=2
        )

        left_index = right_index
        array_bytes = bytes_obj[left_index:]

        return DelaysFinderParameters(
            signals=Array(
                type_=array_type,
                shape=ArraySize(
                    rows_count=rows_count,
                    cols_count=cols_count
                ),
                data=array_bytes
            ),
            window_size=window_size,
            scanner_size=scanner_size,
            min_correlation=min_correlation,
            base_station_index=base_station_index
        )

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        arr: Array = values['signals']
        if values['base_station_index'] >= arr.shape.rows_count:
            raise IndexError('Invalid base station index')
        return values


class PullConfig(BaseModel):
    sleep_time_seconds: int = Field(
        alias='SleepTimeSeconds',
        default=10
    )
    max_tasks_count_per_user: int = Field(
        alias='MaxTasksCountPerUser',
        default=2
    )
    max_input_args_megabytes_size: int = Field(
        alias='MaxInputArgsMegabytesSize',
        default=1024
    )
