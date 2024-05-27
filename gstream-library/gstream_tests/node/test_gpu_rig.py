import pathlib
from typing import List, Union
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest
from hamcrest import assert_that, equal_to, is_

from gstream.node.common import MemoryInfo, convert_megabytes_to_bytes
from gstream.node.gpu_rig import (
    FREE_MEMORY_SIZE_KEY,
    MEMORY_SIZE_UNIT_IN_BYTES,
    TOTAL_MEMORY_SIZE_KEY,
    GPUCardInfo,
    GPURigInfo
)


class TestGPUCardInfo:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        uuid = 'test-uuid'
        bus_id = 1
        memory = 'test-memory'
        obj = GPUCardInfo(uuid=uuid, bus_id=bus_id, memory=memory)

        assert_that(
            actual_or_assertion=obj.uuid,
            matcher=equal_to(uuid)
        )
        assert_that(
            actual_or_assertion=obj.bus_id,
            matcher=equal_to(bus_id)
        )
        assert_that(
            actual_or_assertion=obj.memory,
            matcher=equal_to(memory)
        )


class TestGPURigInfo:

    @pytest.mark.positive
    def test_parse_line_positive(self):
        uuid, bus_id = 'test-uuid', 7
        used_memory, total_memory = 10, 100

        line = f'{uuid}, test:{bus_id}, {used_memory}, 0, {total_memory}'
        expected_value = GPUCardInfo(
            uuid=uuid,
            bus_id=bus_id,
            memory=MemoryInfo(
                total_volume=convert_megabytes_to_bytes(
                    value=total_memory - 1
                ),
                used_volume=convert_megabytes_to_bytes(value=used_memory + 1)
            )
        )

        assert_that(
            actual_or_assertion=GPURigInfo._GPURigInfo__parse_line(line=line),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.negative
    @pytest.mark.parametrize(
        'line', [
            'test',
            'test, test, test, test, test',
            'test, test:test, test, test, test',
            'test, test:1, test, test, test'
        ]
    )
    def test_parse_line_negative(self, line: str):
        assert_that(
            actual_or_assertion=GPURigInfo._GPURigInfo__parse_line(
                line=line),
            matcher=is_(None)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['line', 'expected_value'],
        [
            (
                '123, test:2, 10, 0, 100',
                [
                    GPUCardInfo(
                        uuid='123',
                        bus_id=2,
                        memory=MemoryInfo(
                            total_volume=103809024,
                            used_volume=11534336
                        )
                    )
                ]
            ),
            (
                '123, test:2, 10, 0, 100\n123, test:3, 1, 0, 90',
                [
                    GPUCardInfo(
                        uuid='123',
                        bus_id=2,
                        memory=MemoryInfo(
                            total_volume=103809024,
                            used_volume=11534336
                        )
                    ),
                    GPUCardInfo(
                        uuid='123',
                        bus_id=3,
                        memory=MemoryInfo(
                            total_volume=93323264,
                            used_volume=2097152
                        )
                    )
                ]
            ),
            (
                '123, test:2, 10, 0, 100\ntest\n123, test:1, 2, 0, 50',
                [
                    GPUCardInfo(
                        uuid='123',
                        bus_id=2,
                        memory=MemoryInfo(
                            total_volume=103809024,
                            used_volume=11534336
                        )
                    ),
                    GPUCardInfo(
                        uuid='123',
                        bus_id=1,
                        memory=MemoryInfo(
                            total_volume=51380224,
                            used_volume=3145728
                        )
                    )
                ]
            )
        ]
    )
    @patch('subprocess.Popen')
    def test_get_gpu_cards_info_positive(
            self,
            mock_popen: Mock,
            line: str,
            expected_value: List[GPUCardInfo]
    ):
        mock_popen.return_value.communicate.return_value = line.encode(), None
        assert_that(
            actual_or_assertion=GPURigInfo()._GPURigInfo__get_gpu_cards_info(),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        'error', [OSError, ValueError]
    )
    @patch('subprocess.Popen')
    def test_get_empty_gpu_cards_info_positive(
            self,
            mock_popen: Mock,
            error: Union[OSError, ValueError]
    ):
        mock_popen.return_value.communicate.side_effect = error

        assert_that(
            actual_or_assertion=GPURigInfo()._GPURigInfo__get_gpu_cards_info(),
            matcher=equal_to([])
        )

    @pytest.mark.positive
    @patch.object(GPURigInfo, '_GPURigInfo__get_gpu_cards_info')
    def test_gpu_cards_info_positive(self, mock_get_gpu_cards_info: Mock):
        expected_value = 'test-info'
        mock_get_gpu_cards_info.return_value = expected_value

        assert_that(
            actual_or_assertion=GPURigInfo().gpu_cards_info,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('os.uname')
    def test_hostname_positive(self, mock_nodename: Mock):
        expected_value = 'test-node'
        mock_nodename.return_value = MagicMock(nodename=expected_value)

        assert_that(
            actual_or_assertion=GPURigInfo().hostname,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch.object(pathlib.Path, 'open')
    def test_ram_memory_info_positive(self, mock_open: Mock):
        total_memory, free_memory = 10, 1
        memory_file = [
            f'{TOTAL_MEMORY_SIZE_KEY} {total_memory}\n',
            f'{FREE_MEMORY_SIZE_KEY} {free_memory}'
        ]
        mock_open.return_value.__enter__.return_value = memory_file
        total_volume = total_memory * MEMORY_SIZE_UNIT_IN_BYTES

        assert_that(
            actual_or_assertion=GPURigInfo().ram_memory_info,
            matcher=equal_to(
                MemoryInfo(
                    total_volume=total_volume,
                    used_volume=total_volume - (
                        free_memory * MEMORY_SIZE_UNIT_IN_BYTES
                    )
                )
            )
        )

    @pytest.mark.negative
    @patch.object(pathlib.Path, 'exists')
    def test_ram_memory_info_negative(self, mock_exists: Mock):
        mock_exists.return_value = None

        with pytest.raises(OSError) as error:
            GPURigInfo().ram_memory_info()

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to('Meminfo file not found')
            )

    @pytest.mark.positive
    @patch('os.cpu_count')
    def test_cpu_cores_count_positive(self, mock_cpu_count: Mock):
        expected_value = 777
        mock_cpu_count.return_value = expected_value

        assert_that(
            actual_or_assertion=GPURigInfo().cpu_cores_count,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch.object(GPURigInfo, 'gpu_cards_info', new_callable=PropertyMock)
    def test_gpu_cards_count_positive(self, mock_gpu_cards_info: Mock):
        gpu_cards_info = ['test-info']
        mock_gpu_cards_info.return_value = len(gpu_cards_info)

        assert_that(
            actual_or_assertion=GPURigInfo().gpu_cards_info,
            matcher=equal_to(len(gpu_cards_info))
        )
