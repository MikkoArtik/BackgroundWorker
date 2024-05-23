from math import inf as INF
from typing import List, Tuple, Union

import numpy as np

from gstream.models import DiffFunctionParameters
from gstream.node.gpu_task import GPUArray, GPUTask
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage
from gstream.worker.base import GPUProcess

KERNEL_FILENAME = 'diff_function.c'
FUNCTION_NAME = 'get_diff_function_cube'
EMPTY_ID, NULL_VALUE = -1, -9999


class SolutionColumn:
    x = 0
    y = 1
    altitude = 2
    delta_function = 3


class DiffFunction(GPUProcess):
    """Class-wrapper for processing diff function cube."""

    def __init__(
            self,
            task_id: str,
            redis_storage: RedisStorage,
            file_storage: FileStorage,
            parameters: DiffFunctionParameters
    ):
        super().__init__(
            task_id=task_id,
            redis_storage=redis_storage,
            file_storage=file_storage
        )
        self.__args = parameters

    @property
    async def _args(self) -> DiffFunctionParameters:
        return self.__args

    async def _prepare_args(self) -> List[Union[int, float, GPUArray]]:
        input_args: DiffFunctionParameters = await self._args

        seismic_model_gpu = GPUArray(
            src=input_args.seismic_model.convert_to_numpy_format(),
            is_copy=True
        )

        real_delays_gpu = GPUArray(
            src=input_args.real_delays.astype(np.int32),
            is_copy=True
        )

        station_coordinates_gpu = GPUArray(
            src=input_args.observation_system.convert_to_numpy_format(),
            is_copy=True
        )

        stepping = input_args.search_space.get_stepping(
            spacing=input_args.spacing
        )
        offsets = np.array(
            [
                [
                    -input_args.spacing.nx * stepping.dx / 2,
                    -input_args.spacing.ny * stepping.dy / 2,
                    -input_args.spacing.nz * stepping.dz / 2,
                ]
            ] * input_args.events_count,
            dtype=np.float32
        )
        search_origins = input_args.search_space_centers + offsets

        search_origins_gpu = GPUArray(
            src=search_origins.astype(np.float32),
            is_copy=True
        )

        total_nodes_count = (
            input_args.spacing.nodes_count * input_args.events_count
        )
        error_cubes_nodes = np.zeros(
            shape=total_nodes_count,
            dtype=np.float32
        )

        error_cubes_nodes_gpu = GPUArray(
            src=error_cubes_nodes,
            is_copy=False
        )

        output_args = [
            seismic_model_gpu,
            int(input_args.seismic_model.layers_count),
            real_delays_gpu,
            int(input_args.observation_system.stations_count),
            int(input_args.events_count),
            station_coordinates_gpu,
            float(input_args.observation_system.minimal_altitude),
            search_origins_gpu,
            float(stepping.dx),
            float(stepping.dy),
            float(stepping.dz),
            int(input_args.spacing.nx),
            int(input_args.spacing.ny),
            int(input_args.spacing.nz),
            float(input_args.accuracy),
            int(input_args.signal_frequency),
            int(
                input_args.observation_system.get_station_index_by_number(
                    number=input_args.base_station_number
                )
            ),
            error_cubes_nodes_gpu
        ]
        return output_args

    async def _create_task(self) -> GPUTask:
        await self.add_log_message(text='Creating GPU task...')

        task = GPUTask(
            gpu_card=await self.gpu_card,
            core=self._get_kernel_core(kernel_filename=KERNEL_FILENAME)
        )

        await self.add_log_message(text='GPU task was created')
        return task

    async def run(self):
        await self.add_log_message(
            text='Getting diff function cube starting ...'
        )

        prepared_args = await self._prepared_args
        task = await self._task
        await task.run(
            function_name=FUNCTION_NAME,
            args=prepared_args
        )

        gpu_solution: GPUArray = prepared_args[-1]
        self._solution = await gpu_solution.get_from_gpu(
            cl_queue=task.gpu_card.cl_queue
        )
        await self.add_log_message(
            text='Diff function cube was extracted successfully'
        )
        await self._release_args()

    async def __preprocess_solution(self) -> Tuple[np.ndarray, np.ndarray]:
        input_args: DiffFunctionParameters = await self._args

        node_ids = np.zeros(
            shape=input_args.events_count,
            dtype=np.int32
        )
        diff_function_values = np.zeros(
            shape=input_args.events_count,
            dtype=np.float32
        )

        cube_size = input_args.spacing.nodes_count
        for i in range(input_args.events_count):
            start_index = i * cube_size
            best_node_id, best_diff_function = EMPTY_ID, INF
            for j in range(input_args.spacing.nodes_count):
                diff_function_value = self._solution[j + start_index]
                if diff_function_value == NULL_VALUE:
                    continue

                if diff_function_value < best_diff_function:
                    best_node_id = j
                    best_diff_function = diff_function_value

            if best_node_id == EMPTY_ID:
                best_diff_function = NULL_VALUE

            node_ids[i] = best_node_id
            diff_function_values[i] = best_diff_function
        return node_ids, diff_function_values

    @property
    async def solution(self) -> np.ndarray:
        """Return result of processing.

        Returns: np.ndarray

        """
        node_ids, diff_function_values = await self.__preprocess_solution()
        minimization_data = np.zeros(
            shape=(0, 4),
            dtype=np.float32
        )

        input_args: DiffFunctionParameters = await self._args
        for event_id in range(node_ids.shape[0]):
            node_id = int(node_ids[event_id])
            if node_id == EMPTY_ID:
                continue

            spacing = input_args.spacing
            ix, iy, iz = spacing.get_node_id(node_id=node_id)
            stepping = input_args.search_space.get_stepping(
                spacing=input_args.spacing
            )
            dx = stepping.dx * (ix - spacing.nx / 2)
            dy = stepping.dy * (iy - spacing.ny / 2)
            dz = stepping.dz * (iz - spacing.nz / 2)

            x = input_args.search_space_centers[event_id, 0] + dx
            y = input_args.search_space_centers[event_id, 1] + dy
            z = input_args.search_space_centers[event_id, 2] + dz

            minimization_data = np.vstack(
                (
                    minimization_data,
                    (x, y, z, diff_function_values[event_id])
                )
            )
        return minimization_data
