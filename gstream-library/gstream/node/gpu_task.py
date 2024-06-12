"""Module with classes for running processing tasks on GPU."""

from functools import singledispatchmethod
from typing import List, Union

import numpy as np
import pyopencl as cl

from gstream.node.gpu_rig import GPUCard, NoFreeGPUCardException

__all__ = [
    'GPUArray',
    'GPUTask'
]


class GPUArray:
    """Class for custom GPU array."""

    def __init__(self, src: np.ndarray, is_copy: bool = False):
        """Initialize class method.

        Args:
            src: source numpy array
            is_copy: is copy to gpu from cpu [bool]
        """
        self.__src = src

        self.__is_copy = is_copy
        self.__cl_buffer = None

    def __eq__(self, other: 'GPUArray') -> bool:
        """Compares GPUArray object with other GPUArray object for equality.

        Args:
            other: The GPUArray object to compare

        Returns:
            True if both GPUArray objects are equal, otherwise - False
        """
        return np.array_equal(self.__src, other.__src)

    def __ne__(self, other: 'GPUArray') -> bool:
        """Compares GPUArray object with other GPUArray object for inequality.

        Args:
            other: The GPUArray object to compare

        Returns:
            True if both GPUArray objects are not equal, otherwise - False
        """
        return True if not np.array_equal(self.__src, other.__src) else False

    @property
    def __flags(self) -> int:
        """Return CL array memory flag.

        Returns: int

        """
        if self.__is_copy:
            return cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR
        else:
            return cl.mem_flags.WRITE_ONLY

    @property
    def bytes_size(self) -> int:
        return self.__src.nbytes

    @property
    def cl_buffer(self) -> Union[cl.Buffer, None]:
        """Return CL buffer.

        Returns: Union[cl.Buffer, None]

        """
        return self.__cl_buffer

    async def load_to_gpu(self, cl_context: cl.Context) -> None:
        """Load array to GPU memory.

        Args:
            cl_context: CL context

        Returns: None

        """
        try:
            if self.__is_copy:
                self.__cl_buffer = cl.Buffer(
                    context=cl_context,
                    flags=self.__flags,
                    hostbuf=self.__src
                )
            else:
                self.__cl_buffer = cl.Buffer(
                    context=cl_context,
                    flags=cl.mem_flags.WRITE_ONLY,
                    size=self.__src.nbytes
                )
        except cl.MemoryError:
            raise NoFreeGPUCardException

    async def get_from_gpu(self, cl_queue: cl.CommandQueue) -> np.ndarray:
        """Copy array from GPU memory to CPU.

        Args:
            cl_queue: CL queue

        Returns: numpy array

        """
        if self.cl_buffer is None:
            return np.array([])

        cl.enqueue_copy(cl_queue, self.__src, self.cl_buffer)
        return self.__src

    def release(self) -> None:
        """Release CL buffer.

        Returns: None

        """
        if isinstance(self.cl_buffer, cl.Buffer):
            try:
                self.cl_buffer.release()
            except cl.LogicError:
                pass


class GPUTask:
    """Class for comfort running gpu cores."""

    def __init__(self, gpu_card: GPUCard, core: str):
        """Initialize class method.

        Args:
            gpu_card: GPUCard
            core: src core [str]
        """
        self.__gpu_card = gpu_card
        self.core = core
        self.__cl_module = gpu_card.compile_cl_core(core=self.core)
        self.__gpu_args = []

    def __eq__(self, other: 'GPUTask') -> bool:
        """Compares GPUTask object with other GPUCard object for equality.

        Args:
            other: The GPUTask object to compare

        Returns:
            True if both GPUTask objects are equal, otherwise - False
        """
        return self.__gpu_card == other.gpu_card and self.core == other.core

    def __ne__(self, other: 'GPUTask') -> bool:
        """Compares GPUTask object with other GPUTask object for inequality.

        Args:
            other: The GPUTask object to compare

        Returns:
            True if both GPUTask objects are not equal, otherwise - False
        """
        return self.__gpu_card != other.gpu_card and self.core != other.core

    @property
    def gpu_card(self) -> GPUCard:
        """Return using GPU card.

        Returns: GPUCard

        """
        return self.__gpu_card

    @property
    def gpu_args(self) -> List[Union[np.int32, np.float32, cl.Buffer]]:
        """Return list of arguments for OpenCL task.

        Returns: List[Union[np.int32, np.float32, cl.Buffer]]

        """
        return self.__gpu_args

    @singledispatchmethod
    async def __convert_to_gpu_type(self, arg: object) -> object:
        raise TypeError(f'Unregistered argument type - {type(arg)}')

    @__convert_to_gpu_type.register
    async def __convert_from(self, arg: int) -> np.int32:
        return np.int32(arg)

    @__convert_to_gpu_type.register
    async def __convert_from(self, arg: float) -> np.float32:
        return np.float32(arg)

    @__convert_to_gpu_type.register
    async def __convert_from(self, arg: GPUArray) -> cl.Buffer:
        if arg.cl_buffer is None:
            await arg.load_to_gpu(cl_context=self.gpu_card.cl_context)
        return arg.cl_buffer

    async def __load_args(
            self,
            args: List[Union[int, float, GPUArray]]
    ) -> None:
        """Load list of gpu args.

        Args:
            args: args list

        Returns: gpu args list
        """
        gpu_args = []
        for i in range(len(args)):
            gpu_args.append(await self.__convert_to_gpu_type(args[i]))
        self.__gpu_args = gpu_args

    async def run(self, function_name: str, args: list) -> None:
        """Run gpu task.

        Args:
            function_name: function name
            args: args list

        Returns: None

        """
        cl_function = getattr(self.__cl_module, function_name)
        try:
            await self.__load_args(args=args)
        except NoFreeGPUCardException:
            raise

        try:
            cl_function(
                self.gpu_card.cl_queue, self.gpu_card.max_grid_size, None,
                *self.gpu_args
            )
        except cl.RuntimeError:
            raise NoFreeGPUCardException
