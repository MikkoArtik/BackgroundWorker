import pathlib
from typing import List, Union
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pyopencl
import pytest
from hamcrest import assert_that, equal_to, is_

from gstream.node.common import MemoryInfo, convert_megabytes_to_bytes
from gstream.node.gpu_rig import (
    FREE_MEMORY_SIZE_KEY,
    MEMORY_SIZE_UNIT_IN_BYTES,
    TOTAL_MEMORY_SIZE_KEY,
    BusIdNotFound,
    GPUCard,
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


class TestGPUCard:

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_cl_gpu_device_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        expected_value = 'test-device'
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=expected_value
            ).cl_gpu_device,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch.object(GPUCard, 'cl_gpu_device', new_callable=PropertyMock)
    @patch.object(GPURigInfo, 'gpu_cards_info', new_callable=PropertyMock)
    @patch.object(GPUCard, '__init__')
    def test_get_bus_id_and_uuid_positive(
            self,
            mock_init: Mock,
            mock_gpu_cards_info: Mock,
            mock_cl_gpu_device: Mock
    ):
        mock_init.return_value = None
        cl_bus_id = 'test-bus-id'
        uuid = 'test-uuid'
        gpu_cards_info = [Mock(bus_id=cl_bus_id, uuid=uuid)]
        mock_gpu_cards_info.return_value = gpu_cards_info
        mock_cl_gpu_device.return_value = Mock(pci_bus_id_nv=cl_bus_id)

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=Mock()
            )._GPUCard__get_bus_id_and_uuid(),
            matcher=equal_to((cl_bus_id, uuid))
        )

    @pytest.mark.negative
    @pytest.mark.parametrize(
        ['is_absent_gpu_device', 'is_absent_bus_id'],
        [(True, False), (False, True)]
    )
    @patch.object(GPUCard, 'cl_gpu_device', new_callable=PropertyMock)
    @patch.object(GPURigInfo, 'gpu_cards_info', new_callable=PropertyMock)
    @patch.object(GPUCard, '__init__')
    def test_get_bus_id_and_uuid_negative(
            self,
            mock_init: Mock,
            mock_gpu_cards_info: Mock,
            mock_cl_gpu_device: Mock,
            is_absent_bus_id: bool,
            is_absent_gpu_device: bool
    ):
        mock_init.return_value = None
        uuid = 'test-uuid'

        if is_absent_gpu_device:
            gpu_cards_info = None
            expected_value = 'No GPU devices'

        if is_absent_bus_id:
            cl_bus_id = 'test-bus-id'
            gpu_cards_info = [Mock(bus_id='test-bus-id', uuid=uuid)]
            expected_value = f'CL Device bus id {cl_bus_id} not found'

        mock_gpu_cards_info.return_value = gpu_cards_info
        mock_cl_gpu_device.return_value = Mock(pci_bus_id_nv='other-bus-id')

        with pytest.raises(BusIdNotFound) as error:
            GPUCard(cl_gpu_device=Mock())._GPUCard__get_bus_id_and_uuid()

            assert_that(
                actual_or_assertion=error.value,
                matcher=equal_to(expected_value)
            )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_eq_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        uuid = 'test-uuid'
        mock_get_bus_id_and_uuid.return_value = ('test-bus', uuid)
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        obj = GPUCard(cl_gpu_device=Mock())
        other_obj = GPUCard(cl_gpu_device=Mock())

        assert_that(
            actual_or_assertion=obj == other_obj,
            matcher=is_(True)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_ne_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        uuid = 'test-uuid'
        mock_get_bus_id_and_uuid.return_value = ('test-bus', uuid)
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        obj = GPUCard(cl_gpu_device=Mock())
        other_obj = GPUCard(cl_gpu_device=Mock())
        other_obj._GPUCard__uuid = 'other-test-uuid'

        assert_that(
            actual_or_assertion=obj != other_obj,
            matcher=is_(True)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    @patch('logging.getLogger')
    def test_logger_positive(
            self,
            mock_logger: Mock,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock,
    ):
        expected_value = Mock()
        mock_logger.return_value = expected_value
        mock_logger.return_value.debug.return_value = None
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).logger,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_bus_id_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        expected_value = 'test-bus-id'
        mock_get_bus_id_and_uuid.return_value = (expected_value, 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).bus_id,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_uuid_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        expected_value = 'test-uuid'
        mock_get_bus_id_and_uuid.return_value = ('test-bus-id', expected_value)
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).uuid,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_cl_context_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        expected_value = 'test-cl-context'
        mock_get_bus_id_and_uuid.return_value = ('test-bus-id', 'test-uuid')
        mock_context.return_value = expected_value
        mock_queue.return_value = Mock()

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).cl_context,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_cl_queue_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        expected_value = 'test-cl-queue'
        mock_get_bus_id_and_uuid.return_value = ('test-bus-id', 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = expected_value

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).cl_queue,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch.object(pyopencl.Program, 'build')
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_compile_cl_core_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock,
            mock_build: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus-id', 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        expected_value = 'test'
        mock_build.return_value = expected_value

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).compile_cl_core(
                core='test-core'
            ),
            matcher=equal_to(expected_value)
        )

    @pytest.mark.negative
    @patch.object(pyopencl.Program, 'build')
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_cl_queue_negative(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock,
            mock_build: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus-id', 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = Mock()

        expected_value = pyopencl.RuntimeError
        mock_build.side_effect = expected_value

        with pytest.raises(expected_value):
            GPUCard(cl_gpu_device=Mock()).compile_cl_core(core='test-core')

    @pytest.mark.positive
    @patch.object(GPURigInfo, 'gpu_cards_info', new_callable=PropertyMock)
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_memory_info_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock,
            mock_gpu_cards_info: Mock
    ):
        expected_value = 'test-memory-info'
        bus_id = 'test-bus-id'
        mock_get_bus_id_and_uuid.return_value = (bus_id, 'test-uuid')
        mock_context.return_value = Mock()
        mock_queue.return_value = expected_value
        mock_gpu_cards_info.return_value = [
            Mock(bus_id=bus_id, memory=expected_value)
        ]

        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).memory_info,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['is_free', 'permitted_volume'], [(True, 1), (False, 0), (False, -1)]
    )
    @patch.object(GPUCard, 'memory_info', new_callable=PropertyMock)
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_is_free_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock,
            mock_memory_info: Mock,
            is_free: bool,
            permitted_volume: int
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = None
        mock_queue.return_value = None

        mock_memory_info.return_value = MagicMock(
            permitted_volume=permitted_volume
        )
        assert_that(
            actual_or_assertion=GPUCard(cl_gpu_device=Mock()).is_free,
            matcher=is_(is_free)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_max_dimension_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = None
        mock_queue.return_value = None
        expected_value = 'test'

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=Mock(max_work_item_dimensions=expected_value)
            ).max_dimension,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_max_warp_size_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = None
        mock_queue.return_value = None
        expected_value = 'test'

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=Mock(warp_size_nv=expected_value)
            ).max_warp_size,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_max_block_size_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = None
        mock_queue.return_value = None
        expected_value = 'test'

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=Mock(max_work_group_size=expected_value)
            ).max_block_size,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_max_grid_size_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = None
        mock_queue.return_value = None
        expected_value = 'test'

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=Mock(max_work_item_sizes=expected_value)
            ).max_grid_size,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('pyopencl.CommandQueue')
    @patch('pyopencl.Context')
    @patch.object(GPUCard, '_GPUCard__get_bus_id_and_uuid')
    def test_grid_cells_count_positive(
            self,
            mock_get_bus_id_and_uuid: Mock,
            mock_context: Mock,
            mock_queue: Mock
    ):
        mock_get_bus_id_and_uuid.return_value = ('test-bus', 'test-uuid')
        mock_context.return_value = None
        mock_queue.return_value = None
        max_grid_size = [2]

        assert_that(
            actual_or_assertion=GPUCard(
                cl_gpu_device=Mock(max_work_item_sizes=max_grid_size)
            ).grid_cells_count,
            matcher=equal_to(max_grid_size[0])
        )
