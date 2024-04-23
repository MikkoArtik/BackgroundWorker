#define NULL_VALUE -9999
#define MIN_STATIONS_COUNT 3


int get_global_thread_id()
{
    int block_id = get_group_id(0) + get_group_id(1) * get_num_groups(0) + get_group_id(2) * get_num_groups(0) * get_num_groups(1);
    int thread_local_id = get_local_id(0) + get_local_id(1) * get_local_size(0) + get_local_id(2) * get_local_size(0) * get_local_size(1);
    return block_id * get_local_size(0) * get_local_size(1) * get_local_size(2) + thread_local_id;
}

bool is_good_signal_part(global const float *signals,
						int start_index, int window_size){
	// TODO: нет контроля выхода за пределы массива
	int counter = 0;
	float last_value = signals[start_index];
	for (int i = start_index + 1; i < start_index + window_size; i++){
		if (signals[i] == last_value){
			counter ++;
		}
		last_value = signals[i];
	}
	return counter == 0;
}


kernel void get_real_delays(global const float *signals, int signal_length,
							int stations_count, int scanner_size,
							int window_size, float min_correlation,
							int base_station_index,
							global int *real_delays){
	int time_index = get_global_thread_id();

	if (time_index > signal_length - window_size - scanner_size - 1){
		return;
	}

	int base_signal_index = base_station_index * signal_length + time_index;

	if (!is_good_signal_part(signals, base_signal_index, window_size)){
		return;
	}

	float sum_a = 0;
	float sum_qa = 0;
	float min_value = 0;
	float max_value = 0;

	for (int i = 0; i < window_size; i++){
		int index = base_signal_index + i;
		float val = signals[index];
		min_value = min(min_value, val);
		max_value = max(max_value, val);
		sum_a += val;
		sum_qa += pown(val, 2);
	}

	if (min_value == max_value){
		return;
	}

	int selection_stations_count = 0;
	for (int station_index = 0; station_index < stations_count; station_index++){
		if (station_index == base_station_index){
			continue;
		}

		float max_value_correlation = -1;
		int optimal_delay = NULL_VALUE;
		for (int delay_index = 0; delay_index < scanner_size; delay_index++){
			float sum_b = 0;
			float sum_qb = 0;
			float sum_ab = 0;

			int current_signal_index = station_index * signal_length + time_index + delay_index;
			if (!is_good_signal_part(signals, current_signal_index, window_size)){
				continue;
			}
			for (int j = 0; j < window_size; j++){
				// TODO: нет контроля выхода за пределы массива
				int moment_index_i = base_signal_index + j;
				int moment_index_j = current_signal_index + j;
				float val_a = signals[moment_index_i];
				float val_b = signals[moment_index_j];
				sum_b += val_b;
				sum_qb += pown(val_b, 2);
				sum_ab += val_a * val_b;
			}
			float numerator = sum_ab * window_size - sum_a * sum_b;
			if (numerator < 0){
				continue;
			}

			float denominator =  sqrt((sum_qa * window_size - pown(sum_a, 2)) * (sum_qb * window_size - pown(sum_b, 2)));
			if (denominator == 0){
				continue;
			}
			
			float corr = numerator / denominator;

			if ((min_correlation <= corr) && (max_value_correlation < corr)){
				max_value_correlation = corr;
				optimal_delay = delay_index;
			}
		}

		real_delays[time_index * (stations_count + 1) + station_index + 1] = optimal_delay;
		if (optimal_delay != NULL_VALUE){
			selection_stations_count++;
		}
	}

	if (selection_stations_count > MIN_STATIONS_COUNT){
		real_delays[time_index * (stations_count + 1)] = 1;
	}
	else{
		real_delays[time_index * (stations_count + 1)] = 0;
	}
}
