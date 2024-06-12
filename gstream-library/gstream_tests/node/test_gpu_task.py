from unittest.mock import MagicMock, Mock, PropertyMock, patch

import numpy as np
import pyopencl as cl
import pytest
from hamcrest import assert_that, equal_to, is_

from gstream.node.gpu_rig import GPUCard, NoFreeGPUCardException
from gstream.node.gpu_task import GPUArray, GPUTask


class TestGPUArray:

    @pytest.mark.positive
    def test_correct_attributes_positive(self):
        src = np.arange(9)
        obj = GPUArray(src=src)

        assert_that(
            actual_or_assertion=np.allclose(obj._GPUArray__src, src),
            matcher=is_(True)
        )
        assert_that(
            actual_or_assertion=obj._GPUArray__is_copy,
            matcher=is_(False)
        )
        assert_that(
            actual_or_assertion=obj._GPUArray__cl_buffer,
            matcher=is_(None)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['obj', 'other_obj', 'expected_value'],
        [
            (GPUArray(src=np.arange(9)), GPUArray(src=np.arange(9)), True),
            (GPUArray(src=np.arange(9)), GPUArray(src=np.arange(2, 11)), False)
        ]
    )
    def test_eq_positive(
            self,
            obj: GPUArray,
            other_obj: GPUArray,
            expected_value: bool
    ):
        assert_that(
            actual_or_assertion=obj == other_obj,
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        ['obj', 'other_obj', 'expected_value'],
        [
            (GPUArray(src=np.arange(9)), GPUArray(src=np.arange(9)), False),
            (GPUArray(src=np.arange(9)), GPUArray(src=np.arange(2, 11)), True)
        ]
    )
    def test_ne_positive(
            self,
            obj: GPUArray,
            other_obj: GPUArray,
            expected_value: bool
    ):
        assert_that(
            actual_or_assertion=obj != other_obj,
            matcher=is_(expected_value)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        'is_copy', [True, False]
    )
    def test_flags_positive(self, is_copy: bool):
        src = np.arange(9)
        if is_copy:
            obj = GPUArray(src=src, is_copy=True)
            expected_value = 36

        else:
            obj = GPUArray(src=src)
            expected_value = 2

        assert_that(
            actual_or_assertion=obj._GPUArray__flags,
            matcher=equal_to(expected_value)
        )

    @pytest.mark.positive
    def test_bytes_size_positive(self):
        src = np.arange(9)
        assert_that(
            actual_or_assertion=GPUArray(src=src).bytes_size,
            matcher=equal_to(src.shape[0] * 8)
        )

    @pytest.mark.positive
    def test_cl_buffer_positive(self):
        assert_that(
            actual_or_assertion=GPUArray(src=np.arange(9)).cl_buffer,
            matcher=is_(None)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        'is_copy', [True, False]
    )
    @patch('pyopencl.Buffer')
    @pytest.mark.asyncio
    async def test_load_to_gpu_positive(
            self,
            mock_buffer: Mock,
            is_copy: bool
    ):
        src = np.arange(9)
        cl_context = Mock()

        if is_copy:
            await GPUArray(src=src, is_copy=True).load_to_gpu(
                cl_context=cl_context
            )
            mock_buffer.assert_called_once_with(
                context=cl_context,
                flags=cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR,
                hostbuf=src
            )
        else:
            await GPUArray(src=src).load_to_gpu(cl_context=cl_context)

            mock_buffer.assert_called_once_with(
                context=cl_context,
                flags=cl.mem_flags.WRITE_ONLY,
                size=src.nbytes
            )

    @pytest.mark.negative
    @patch('pyopencl.Buffer')
    @pytest.mark.asyncio
    async def test_load_to_gpu_negative(
            self,
            mock_buffer: Mock
    ):
        mock_buffer.side_effect = cl.MemoryError

        with pytest.raises(NoFreeGPUCardException):
            await GPUArray(src=np.arange(9)).load_to_gpu(cl_context=Mock())

    @pytest.mark.positive
    @pytest.mark.parametrize(
        'is_buffer_none', [True, False]
    )
    @patch.object(cl, 'enqueue_copy')
    @pytest.mark.asyncio
    async def test_get_from_gpu_positive(
            self,
            mock_enqueue_copy: Mock,
            is_buffer_none: bool
    ):
        src = np.arange(9)
        obj = GPUArray(src=src)
        mock_enqueue_copy.return_value = None

        if is_buffer_none:
            expected_value = np.array([])

        else:
            obj._GPUArray__cl_buffer = 'test'
            expected_value = src

        assert_that(
            actual_or_assertion=np.allclose(
                await obj.get_from_gpu(cl_queue=Mock()),
                expected_value
            ),
            matcher=equal_to(True)
        )

    @pytest.mark.positive
    @pytest.mark.parametrize(
        'is_logic_error', [True, False]
    )
    @patch.object(GPUArray, 'cl_buffer', new_callable=PropertyMock)
    def test_release_positive(
            self,
            mock_cl_buffer: Mock,
            is_logic_error: bool
    ):
        mock_buffer = MagicMock(spec=cl.Buffer)
        mock_cl_buffer.return_value = mock_buffer

        if is_logic_error:
            mock_cl_buffer.return_value.release.side_effect = cl.LogicError

        obj = GPUArray(src=np.arange(9))
        obj.release()

        mock_buffer.release.assert_called_once()


class TestGPUTask:

    @pytest.mark.positive
    @patch.object(GPUCard, 'compile_cl_core')
    def test_correct_attributes_positive(self, mock_compile_cl_core: Mock):
        gpu_card = Mock()
        mock_compile_cl_core.return_value = None
        core = 'test-core'
        obj = GPUTask(gpu_card=gpu_card, core=core)

        assert_that(
            actual_or_assertion=obj._GPUTask__gpu_card,
            matcher=equal_to(gpu_card)
        )
        assert_that(
            actual_or_assertion=obj.core,
            matcher=equal_to(core)
        )
        assert_that(
            actual_or_assertion=obj._GPUTask__gpu_args,
            matcher=equal_to([])
        )

    @pytest.mark.positive
    @patch.object(GPUCard, 'compile_cl_core')
    def test_gpu_card_positive(self, mock_compile_cl_core: Mock):
        gpu_card = Mock()
        mock_compile_cl_core.return_value = None

        assert_that(
            actual_or_assertion=GPUTask(
                gpu_card=gpu_card,
                core='test-core'
            ).gpu_card,
            matcher=equal_to(gpu_card)
        )

    @pytest.mark.positive
    @patch.object(GPUCard, 'compile_cl_core')
    def test_gpu_args_positive(self, mock_compile_cl_core: Mock):
        mock_compile_cl_core.return_value = None

        assert_that(
            actual_or_assertion=GPUTask(
                gpu_card=Mock(),
                core='test-core'
            ).gpu_args,
            matcher=equal_to([])
        )
