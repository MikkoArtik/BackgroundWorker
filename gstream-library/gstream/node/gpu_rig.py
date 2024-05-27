"""Module with GPU wrapper for running CL kernels."""

import logging
import os
import subprocess
from dataclasses import dataclass
from logging import Logger
from pathlib import Path
from typing import List, Tuple, Union

import pyopencl as cl

from gstream.node.common import MemoryInfo, convert_megabytes_to_bytes

MEMORY_FILE_STATS = Path('/proc/meminfo')
TOTAL_MEMORY_SIZE_KEY = 'MemTotal'
FREE_MEMORY_SIZE_KEY = 'MemFree'
MEMORY_SIZE_UNIT_IN_BYTES = 1024

__all__ = [
    'GPUCardInfo',
    'GPUCard',
    'GPURigInfo',
    'GPURig',
    'BusIdNotFound',
    'MEMORY_FILE_STATS',
    'MEMORY_SIZE_UNIT_IN_BYTES',
    'TOTAL_MEMORY_SIZE_KEY',
    'FREE_MEMORY_SIZE_KEY'
]

GPU_QUERY_CMD = [
    'nvidia-smi',
    '--query-gpu=uuid,gpu_bus_id,memory.used,memory.free,memory.total',
    '--format=csv,noheader,nounits'
]


class BusIdNotFound(Exception):
    """Exception when BusId not found."""
    pass


class NoFreeGPUCardException(RuntimeError):
    pass


class NoFreeRAMException(RuntimeError):
    pass


@dataclass
class GPUCardInfo:
    """Container with GPU card information.

    Args:
        uuid: GPU card uuid [str]
        bus_id: GPU bus id [int]
        memory: GPU card memory information [MemoryInfo]

    """
    uuid: str
    bus_id: int
    memory: MemoryInfo


class GPURigInfo:
    """Class for getting information about all gpu cards in node."""

    def __init__(self):
        """Initialize class method."""
        pass

    @staticmethod
    def __parse_line(line: str) -> Union[GPUCardInfo, None]:
        """Process GPU information from the given string.

        Args:
            line:  string containing GPU information

        Returns: GPUCardInfo or None
        """
        tmp_list = line.split(', ')
        if len(tmp_list) < 5:
            return

        uuid_val, full_bus_id = tmp_list[0], tmp_list[1]
        try:
            bus_id = int(full_bus_id.split(':')[1])
        except (IndexError, ValueError):
            return

        try:
            memory_info = [int(x) for x in tmp_list[2:]]
        except ValueError:
            return

        used_memory = convert_megabytes_to_bytes(value=memory_info[0] + 1)
        total_memory = convert_megabytes_to_bytes(
            value=max(0, memory_info[2] - 1)
        )
        memory_info = MemoryInfo(
            total_volume=total_memory,
            used_volume=used_memory
        )
        return GPUCardInfo(uuid=uuid_val, bus_id=bus_id, memory=memory_info)

    def __get_gpu_cards_info(self) -> List[GPUCardInfo]:
        """Return information about all gpu cards in node.

        Returns: List[GPUCardInfo]

        """
        cards_info = []

        try:
            process = subprocess.Popen(GPU_QUERY_CMD, stdout=subprocess.PIPE)
            output_info, error = process.communicate()
        except (OSError, ValueError):
            return cards_info

        for line in output_info.decode().split('\n'):
            if not line:
                continue

            gpu_card_info = self.__parse_line(line=line)
            if not gpu_card_info:
                continue

            cards_info.append(gpu_card_info)

        return cards_info

    @property
    def gpu_cards_info(self) -> List[GPUCardInfo]:
        """Return get_gpu_cards_info.

        Returns: SeismicModel

        """
        return self.__get_gpu_cards_info()

    @property
    def hostname(self) -> str:
        """Return hostname of node.

        Returns: str

        """
        return os.uname().nodename

    @property
    def ram_memory_info(self) -> MemoryInfo:
        """Return CPU memory info.

        Returns: MemoryInfo

        """
        if not MEMORY_FILE_STATS.exists():
            raise OSError('Meminfo file not found')

        total_memory, free_memory = 0, 0
        with MEMORY_FILE_STATS.open() as f:
            for line in f:
                if TOTAL_MEMORY_SIZE_KEY in line:
                    size_val = int(''.join([x for x in line if x.isdigit()]))
                    total_memory = size_val * MEMORY_SIZE_UNIT_IN_BYTES

                if FREE_MEMORY_SIZE_KEY in line:
                    size_val = int(''.join([x for x in line if x.isdigit()]))
                    free_memory = size_val * MEMORY_SIZE_UNIT_IN_BYTES
                if total_memory and free_memory:
                    break

        used_memory = total_memory - free_memory

        return MemoryInfo(total_volume=total_memory, used_volume=used_memory)

    @property
    def cpu_cores_count(self) -> int:
        return os.cpu_count()

    @property
    def gpu_cards_count(self) -> int:
        return len(self.gpu_cards_info)


class GPUCard:
    """Class for wrapping operations with running CL kernels on GPU."""

    def __init__(self, cl_gpu_device: cl.Device):
        """Initialize class method.

        Args:
            cl_gpu_device: CL GPU device
        """
        self.__logger = logging.getLogger('GPUCard')
        self.__cl_gpu_device = cl_gpu_device
        self.__bus_id, self.__uuid = self.__get_bus_id_and_uuid()
        self.__cl_context = cl.Context(devices=[cl_gpu_device])
        self.__cl_queue = cl.CommandQueue(self.__cl_context)

        self.logger.debug(f'Card with uuid {self.__uuid} was activated')

    def __eq__(self, other: 'GPUCard') -> bool:
        """Compares GPUCard object with other GPUCard object for equality.

        Args:
            other: The GPUCard object to compare

        Returns:
            True if both GPUCard objects are equal, otherwise - False
        """
        return self.uuid == other.uuid

    def __ne__(self, other: 'GPUCard') -> bool:
        """Compares GPUCard object with other GPUCard object for inequality.

        Args:
            other: The GPUCard object to compare

        Returns:
            True if both GPUCard objects are not equal, otherwise - False
        """
        return self.uuid != other.uuid

    def __get_bus_id_and_uuid(self) -> Tuple[int, str]:
        """Return uuid and bus id for current GPU card.

        Returns: pair with bus id [int] and uuid [str]

        """
        gpu_cards_info = GPURigInfo().gpu_cards_info
        if not gpu_cards_info:
            raise BusIdNotFound('No GPU devices')

        cl_bus_id = self.cl_gpu_device.pci_bus_id_nv
        for card_info in gpu_cards_info:
            if card_info.bus_id == cl_bus_id:
                return cl_bus_id, card_info.uuid
        else:
            raise BusIdNotFound(f'CL Device bus id {cl_bus_id} not found')

    @property
    def logger(self) -> Logger:
        """Return GPU card logger.

        Returns: logger

        """
        return self.__logger

    @property
    def cl_gpu_device(self) -> cl.Device:
        """Return CL GPU device.

        Returns: cl.Device

        """
        return self.__cl_gpu_device

    @property
    def bus_id(self) -> int:
        """Return bus id.

        Returns: int

        """
        return self.__bus_id

    @property
    def uuid(self) -> str:
        """Return uuid value.

        Returns: str

        """
        return self.__uuid

    @property
    def cl_context(self) -> cl.Context:
        """Return CL context on current GPU device.

        Returns: cl.Context

        """
        return self.__cl_context

    @property
    def cl_queue(self) -> cl.CommandQueue:
        """Return CL Queue on current GPU card.

        Returns: cl.CommandQueue

        """
        return self.__cl_queue

    def compile_cl_core(self, core: str) -> cl.Program:
        """Return compiled CL kernel.

        Args:
            core: kernel in line format

        Returns: cl.Program

        """
        try:
            module = cl.Program(self.cl_context, core).build()
        except cl.RuntimeError:
            raise
        return module

    @property
    def memory_info(self) -> MemoryInfo:
        """Return GPU card memory info.

        Returns: MemoryInfo

        """
        for card_info in GPURigInfo().gpu_cards_info:
            if card_info.bus_id == self.bus_id:
                return card_info.memory

    @property
    def is_free(self) -> bool:
        """Return GPU status for running new processing.

        Returns: bool

        """
        return self.memory_info.permitted_volume > 0

    @property
    def max_dimension(self) -> int:
        """Return max GPU array dimension.

        Returns: int

        """
        return self.cl_gpu_device.max_work_item_dimensions

    @property
    def max_warp_size(self) -> int:
        """Return max GPU warp size.

        Returns: int

        """
        return self.cl_gpu_device.warp_size_nv

    @property
    def max_block_size(self) -> int:
        """Return max block (group) size.

        Returns: int

        """
        return self.cl_gpu_device.max_work_group_size

    @property
    def max_grid_size(self) -> List[int]:
        """Return max GPU grid size.

        Returns: int

        """
        return self.cl_gpu_device.max_work_item_sizes

    @property
    def grid_cells_count(self) -> int:
        """Return max grid cells count on GPU.

        Returns: int

        """
        result = 1
        for item in self.max_grid_size:
            result *= item
        return result


class GPURig:
    """Class with description cluster node."""

    def __init__(self):
        """Initialize class method."""
        self.__gpu__cards: List[GPUCard] = [
            GPUCard(
                cl_gpu_device=cl_gpu
            ) for cl_gpu in self.__get_cl_gpu_devices()
        ]

    @property
    def __cl_platforms(self) -> List[cl.Platform]:
        """Return list of CL platforms.

        Returns: List[cl.Platform]

        """
        return cl.get_platforms()

    def __get_cl_gpu_devices(self) -> List[cl.Device]:
        """Return all GPU devices in node.

        Returns: List[cl.Device]

        """
        all_gpu_devices = []
        for platform in self.__cl_platforms:
            all_gpu_devices += platform.get_devices(
                device_type=cl.device_type.GPU
            )
        return all_gpu_devices

    @property
    def is_available_ram_memory(self) -> bool:
        """Return available CPU memory.

        Returns: bool

        """
        return self.info.ram_memory_info.permitted_volume > 0

    @property
    def gpu_cards(self) -> List[GPUCard]:
        """Return list of GPU cards.

        Returns: List[GPUCard]

        """
        return self.__gpu__cards

    def get_gpu_card_by_bus_id(self, bus_id_value: int) -> GPUCard:
        """Return GPUCard by bus_id value.

        Args:
            bus_id_value: int

        Returns: GPUCard

        """
        for gpu_card in self.gpu_cards:
            if gpu_card.bus_id == bus_id_value:
                return gpu_card
        else:
            raise BusIdNotFound(f'Bus id {bus_id_value} is not exist')

    def get_gpu_card_by_uuid(self, uuid_value: str) -> GPUCard:
        """Return GPUCard by bus_id value.

        Args:
            uuid_value: str

        Returns: GPUCard

        """
        for gpu_card in self.gpu_cards:
            if gpu_card.uuid == uuid_value:
                return gpu_card
        else:
            raise BusIdNotFound(f'Bus uuid {uuid_value} is not exist')

    @property
    def info(self) -> GPURigInfo:
        """Return node info.

        Returns: NodeInfo

        """
        return GPURigInfo()

    def get_free_gpu_card(self, required_memory_size: int) -> GPUCard:
        for gpu_card in self.gpu_cards:
            if not gpu_card.is_free:
                continue

            if gpu_card.memory_info.free_volume > required_memory_size:
                return gpu_card
        else:
            raise NoFreeGPUCardException('All GPU card are busy now')
