from unittest.mock import Mock, patch

import pytest
from hamcrest import assert_that, equal_to, is_

from gstream.node.common import MemoryInfo, convert_megabytes_to_bytes
from gstream.node.gpu_rig import GPUCardInfo, GPURigInfo


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
        uuid = 'test-uuid'
        bus_id = 7
        used_memory = 10
        total_memory = 100

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
    @patch.object(GPURigInfo, '_GPURigInfo__get_gpu_cards_info')
    def test_gpu_cards_info_positive(self, mock_get_gpu_cards_info: Mock):
        expected_value = 'test-info'
        mock_get_gpu_cards_info.return_value = expected_value

        assert_that(
            actual_or_assertion=GPURigInfo().gpu_cards_info,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    @patch('os')
    def test_hostname_positive(self, mock_os: Mock):
        expected_value = 'test-nodename'
        mock_os.uname.return_value = Mock(nodename=expected_value)

        assert_that(
            actual_or_assertion=GPURigInfo.hostname,
            matcher=equal_to(expected_value)
        )
