from pathlib import Path

import numpy as np
from gstream.files.writers import DelaysFinderResultBinaryFile
from gstream.models import Array, ArraySize, ArrayType, DelaysFinderParameters
from gstream.node.gpu_task import GPUArray, GPUTask
from gstream.storage.file_system import Storage as FileStorage
from gstream.storage.redis import Storage as RedisStorage
from gstream.worker.base import GPUProcess

KERNEL_FILENAME = 'delays_finder.c'
FUNCTION_NAME = 'get_real_delays'
SIMILARITY_COEFFICIENT = 0.8
TIME_EPSILON = 5
NULL_VALUE = -9999


def get_similarity_coeff(row_a: np.ndarray, row_b: np.ndarray,
                         time_epsilon: int) -> float:
    if row_a.shape[0] != row_b.shape[0]:
        raise ValueError('Different sizes of rows')

    diff_values = np.abs(row_a - row_b)
    t = diff_values[
        (diff_values <= time_epsilon) + (diff_values > abs(NULL_VALUE) / 2)
    ]
    return t.shape[0] / row_a.shape[0]


class DelaysFinder(GPUProcess):
    def __init__(
            self,
            task_id: str,
            redis_storage: RedisStorage,
            file_storage: FileStorage
    ):
        super().__init__(
            task_id=task_id,
            redis_storage=redis_storage,
            file_storage=file_storage
        )

    async def _load_args_from_file(self) -> DelaysFinderParameters:
        state = await self.task_state
        bytes_obj = await self.file_storage.get_binary_data_from_file(
            filename=state.input_args_filename
        )
        return DelaysFinderParameters.create_from_bytes(bytes_obj=bytes_obj)

    async def _prepare_args(self):
        args: DelaysFinderParameters = await self._args
        gpu_signals = GPUArray(
            src=args.signals.convert_to_numpy_format(),
            is_copy=True
        )

        processing_signal_length = args.signals_length - args.buffer
        stations_count = args.stations_count

        result_array = np.zeros(
            shape=(processing_signal_length, stations_count + 1),
            dtype=np.int32
        )
        gpu_solution = GPUArray(src=result_array)

        return [
            gpu_signals,
            args.signals_length,
            args.stations_count,
            args.scanner_size,
            args.window_size,
            float(args.min_correlation),
            args.base_station_index,
            gpu_solution
        ]

    async def _create_task(self) -> GPUTask:
        await self.add_log_message(text='Creating GPU task...')

        task = GPUTask(
            gpu_card=await self.gpu_card,
            core=self._get_kernel_core(kernel_filename=KERNEL_FILENAME)
        )

        await self.add_log_message(text='GPU task was created')
        return task

    async def _save_solution(self):
        state = await self.task_state
        solution = await self.solution

        writer = DelaysFinderResultBinaryFile(
            path=Path(self.file_storage.root, state.output_args_filename),
            data=Array(
                type_=ArrayType.INT32.value,
                shape=ArraySize(
                    rows_count=solution.shape[0],
                    cols_count=solution.shape[1]
                ),
                data=solution.tobytes()
            )
        )
        await writer.save()

    async def _run(self):
        await self.add_log_message(text='Finding real delays starting ...')
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
            text='Real delays array was extract successfully'
        )
        await self._release_args()

    @property
    async def solution(self) -> np.ndarray:
        """Return result of processing.

        Returns: np.ndarray

        """
        additional_columns = np.zeros(
            shape=(self._solution.shape[0], 2),
            dtype=np.int32
        )
        additional_columns[:, 0] = np.arange(
            0, self._solution.shape[0], 1
        )
        processing_parameters: DelaysFinderParameters = await self._args
        additional_columns[:, 1] = processing_parameters.window_size

        solution = np.column_stack((additional_columns, self._solution))
        solution = solution[solution[:, 2] == 1]
        solution = np.delete(solution, 2, 1)

        skipped_indexes, selected_indexes = set(), []

        for i in range(solution.shape[0]):
            if i in skipped_indexes:
                continue

            selected_indexes.append(i)
            row_a = solution[i, 2:]
            max_j_index = min(
                i + processing_parameters.scanner_size + 1, solution.shape[0]
            )
            duration_index = i
            for j in range(i + 1, max_j_index):
                if j in skipped_indexes:
                    continue

                row_b = solution[j, 2:]
                similarity_coeff = get_similarity_coeff(
                    row_a=row_a,
                    row_b=row_b,
                    time_epsilon=TIME_EPSILON
                )
                if similarity_coeff >= SIMILARITY_COEFFICIENT:
                    skipped_indexes.add(j)
                    duration_index = j

            solution[i, 1] = (
                duration_index - i + processing_parameters.window_size
            )
        solution = solution[selected_indexes]
        return solution
