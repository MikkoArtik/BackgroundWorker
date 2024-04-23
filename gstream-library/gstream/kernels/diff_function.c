#define NULL_VALUE -9999
#define MODEL_COLUMNS_COUNT 3
#define COORDINATE_COLUMNS_COUNT 2
#define SEARCH_ORIGINS_COLUMNS_COUNT 3
#define MAX_ITERATIONS_COUNT 10
#define POSITIVE_DIRECTION 1
#define NEGATIVE_DIRECTION -1


int get_global_thread_id(){
    int block_id = get_group_id(0) + get_group_id(1) * get_num_groups(0) + get_group_id(2) * get_num_groups(0) * get_num_groups(1);
    int thread_local_id = get_local_id(0) + get_local_id(1) * get_local_size(0) + get_local_id(2) * get_local_size(0) * get_local_size(1);
    return block_id * get_local_size(0) * get_local_size(1) * get_local_size(2) + thread_local_id;
}


int get_model_layer_index_by_altitude(global const float *model,
								      int layers_count,
								      float target_altitude){
	for (int i=0; i < layers_count; i++){
		float bottom_altitude = model[MODEL_COLUMNS_COUNT * i];
		float top_altitude = model[MODEL_COLUMNS_COUNT * i + 1];

		if ((bottom_altitude <= target_altitude) && (target_altitude < top_altitude)){
			return i;
		}
	}
	return NULL_VALUE;
}


double get_ray_constant(double incident_angle, float vp){
	return sin(incident_angle) / vp;
}


bool is_ray_reflected(global const float *model, int layers_count,
                      float source_altitude, float target_altitude,
                      double incident_angle){
	int source_layer_index = get_model_layer_index_by_altitude(model, layers_count, source_altitude);
	int target_layer_index = get_model_layer_index_by_altitude(model, layers_count, target_altitude);

	if ((source_layer_index == NULL_VALUE) || (target_layer_index == NULL_VALUE)){
		return true;
	}

	double ray_constant = get_ray_constant(incident_angle, model[source_layer_index * MODEL_COLUMNS_COUNT + 2]);
	for (int i=source_layer_index; i > target_layer_index - 1; i--){
		if (ray_constant * model[i * layers_count + 2] > 1){
			return true;
		}
	}
	return false;
}


float3 get_ray_trace(global const float *model, int layers_count,
				     float source_r, float source_altitude, float target_altitude,
                     double incident_angle, int lateral_direction,
                     int frequency){
	float3 last_trace_point = {NULL_VALUE, NULL_VALUE, NULL_VALUE};
	if (is_ray_reflected(model, layers_count, source_altitude,
	                     target_altitude, incident_angle)){
		return last_trace_point;
	}

	int source_layer_index = get_model_layer_index_by_altitude(model, layers_count, source_altitude);
	int target_layer_index = get_model_layer_index_by_altitude(model, layers_count, target_altitude);

	last_trace_point.s0 = source_r;
	last_trace_point.s1 = source_altitude;
	last_trace_point.s2 = 0;

	double ray_constant = get_ray_constant(incident_angle, model[source_layer_index * MODEL_COLUMNS_COUNT + 2]);
	for (int i=source_layer_index; i > target_layer_index - 1; i--){
		float thickness = 0;
		if (i == source_layer_index){
			thickness = model[i * MODEL_COLUMNS_COUNT + 1] - source_altitude;
		}
		else if (i == target_layer_index) {
			thickness = target_altitude - model[i * MODEL_COLUMNS_COUNT];
		}
		else{
			thickness = model[i * MODEL_COLUMNS_COUNT + 1] - model[i * MODEL_COLUMNS_COUNT];
		}

		double refraction_angle = asin(ray_constant * model[i * MODEL_COLUMNS_COUNT + 2]);

		float dr_offset = thickness * tan(refraction_angle) * lateral_direction;

		float dl = sqrt(pown(dr_offset, 2) + pown(thickness, 2));
		float dt = dl / model[i * MODEL_COLUMNS_COUNT + 2];

		last_trace_point.s0 += dr_offset;
		last_trace_point.s1 += thickness;
		last_trace_point.s2 += dt * frequency;
	}
	return last_trace_point;
}


double get_min_angle(float delta_altitudes, float accuracy){
	return atan2((double) 0.5 * accuracy, (double) delta_altitudes);
}


double get_max_angle(float delta_altitudes, float r_offset){
	return atan2((double) r_offset, (double) delta_altitudes);
}


int get_ray_time(global const float *model, int layers_count,
				   float source_r, float source_altitude,
				   float receiver_r, float receiver_altitude,
				   float accuracy, int frequency){
	float delta_altitudes = fabs(source_altitude - receiver_altitude);
	double min_angle = get_min_angle(delta_altitudes, accuracy);

	int source_layer_index = get_model_layer_index_by_altitude(model, layers_count, source_altitude);
	float layer_delta_altitudes = model[source_layer_index * MODEL_COLUMNS_COUNT + 1] - source_altitude;
	float r_offset = fabs(source_r - receiver_r);
	double max_angle = get_max_angle(layer_delta_altitudes, r_offset);

	int lateral_direction = 0;
	if (receiver_r >= 0){
		lateral_direction = POSITIVE_DIRECTION;
	}
	else{
		lateral_direction = NEGATIVE_DIRECTION;
	}

	float3 ray = get_ray_trace(
		model, layers_count, 0, source_altitude, receiver_altitude,
        max_angle, lateral_direction, frequency
    );

	for (int i = 0; i < MAX_ITERATIONS_COUNT; i++){
		float3 min_ray = get_ray_trace(
			model, layers_count, source_r, source_altitude, receiver_altitude,
            min_angle, lateral_direction, frequency
        );

        float dr = fabs(min_ray.s0 - receiver_r);
        if (dr < accuracy){
        	return (int)min_ray.s2;
        }

        double middle_angle = (min_angle + max_angle) / 2;

        float3 middle_ray = get_ray_trace(
			model, layers_count, source_r, source_altitude, receiver_altitude,
            middle_angle, lateral_direction, frequency
        );

        dr = fabs(middle_ray.s0 - receiver_r);
        if (dr < accuracy){
        	return (int)middle_ray.s2;
        }

        float3 max_ray = get_ray_trace(
			model, layers_count, source_r, source_altitude, receiver_altitude,
            max_angle, lateral_direction, frequency
        );

        dr = fabs(max_ray.s0 - receiver_r);
        if (dr < accuracy){
        	return (int)max_ray.s2;
        }

        if (lateral_direction == POSITIVE_DIRECTION){
        	if ((min_ray.s0 < receiver_r) && (receiver_r < middle_ray.s0)){
        		max_angle = middle_angle;
        	}
        	else if ((middle_ray.s0 < receiver_r) && (receiver_r < max_ray.s0)){
        		min_angle = middle_angle;
        	}
        	else{
        		break;
        	}
        }
        else{
        	if ((max_ray.s0 < receiver_r) && (receiver_r < middle_ray.s0)){
        		min_angle = middle_angle;
        	}
        	else if ((middle_ray.s0 < receiver_r) && (receiver_r < middle_ray.s0)){
        		max_angle = middle_angle;
        	}
        	else{
        		break;
        	}
        }
	}
	return NULL_VALUE;
}


float get_diff_function(global const float *model, int layers_count,
	global const int *real_delays, int stations_count, int events_count,
	int event_id, global const float *station_coordinates,
	float stations_altitude, float3 node_coordinate, float accuracy,
	int frequency, int base_station_index
){
	float2 base_coordinate = {
		station_coordinates[base_station_index * COORDINATE_COLUMNS_COUNT],
		station_coordinates[base_station_index * COORDINATE_COLUMNS_COUNT + 1]
	};

	float offset = sqrt(
		pown(base_coordinate.s0 - node_coordinate.s0, 2) +
		pown(base_coordinate.s1 - node_coordinate.s1, 2)
	);

	int base_time = get_ray_time(model, layers_count, 0, node_coordinate.s2,
								   offset, stations_altitude, accuracy,
								   frequency);
	if (base_time == NULL_VALUE){
		return NULL_VALUE;
	}

	float diff_function_value = 0;
	int using_stations_count = 0;
	for (int i=0; i < stations_count; i++){
		float2 coordinate = {
			station_coordinates[i * COORDINATE_COLUMNS_COUNT],
			station_coordinates[i * COORDINATE_COLUMNS_COUNT + 1]
		};

		offset = sqrt(
			pown(coordinate.s0 - node_coordinate.s0, 2) +
			pown(coordinate.s1 - node_coordinate.s1, 2)
		);

		int time = get_ray_time(model, layers_count, 0, node_coordinate.s2,
								offset, stations_altitude, accuracy,
								frequency);



		if (time == NULL_VALUE){
			continue;
		}

		int theor_time_diff = time - base_time;

		if (theor_time_diff < 0){
			continue;
		}

		int real_time_diff = real_delays[event_id * stations_count + i];
		int delta_diff = theor_time_diff - real_time_diff;
		diff_function_value += delta_diff * delta_diff;
		using_stations_count++;
	}

	if (using_stations_count < 3){
		return NULL_VALUE;
	}
	return sqrt(diff_function_value) / using_stations_count;
}


kernel void get_diff_function_cube(global const float *model,
	int layers_count, global const int *real_delays, int stations_count, int
	events_count, global const float *station_coordinates,
	float stations_altitude,
	global const float *search_origins,
	float dx, float dy, float dz,
	int nx, int ny, int nz, float accuracy, int frequency,
	int base_station_index, global float *diff_func_cube_values
){
	int global_id = get_global_thread_id();
	int all_nodes_count = nx * ny * nz;
	if (global_id > all_nodes_count * events_count - 1){
		return;
	}

	int event_id = global_id / (nx * ny * nz);
	int node_id = global_id % (nx * ny * nz);

	int3 node_index = {
		(node_id % (nx * ny)) % nx,
		(node_id % (nx * ny)) / nx,
		node_id / (nx * ny)
	};

	float3 node_coordinate = {
		node_index.s0 * dx + search_origins[event_id * SEARCH_ORIGINS_COLUMNS_COUNT],
		node_index.s1 * dy + search_origins[event_id * SEARCH_ORIGINS_COLUMNS_COUNT + 1],
		node_index.s2 * dz + search_origins[event_id * SEARCH_ORIGINS_COLUMNS_COUNT + 2]
	};

	float min_model_altitude = model[(layers_count - 1) * MODEL_COLUMNS_COUNT];
	float max_model_altitude = model[1];


	if (node_coordinate.s2 < min_model_altitude){
		diff_func_cube_values[global_id] = NULL_VALUE;
	}
	else if (node_coordinate.s2 > max_model_altitude){
		diff_func_cube_values[global_id] = NULL_VALUE;
	}
	else{
		diff_func_cube_values[global_id] = get_diff_function(
			model, layers_count, real_delays, stations_count, events_count,
			event_id, station_coordinates, stations_altitude, node_coordinate,
			accuracy, frequency, base_station_index
		);
	}
}


kernel void get_minimal_nodes(global float *diff_func_values,
							 int nodes_count, int events_count,
							 global int *minimal_nodes,
							 global float *error){
	int global_id = get_global_thread_id();

	if (global_id > events_count - 1){
		return;
	}

	int min_node_id = global_id * nodes_count;

	int minimal_node = NULL_VALUE;
	float min_diff_function = INFINITY;
	for (int i = 0; i < nodes_count; i++){
		float diff_func_value = diff_func_values[min_node_id + i];
		if (diff_func_value == NULL_VALUE){
			continue;
		}
		if (diff_func_value < min_diff_function){
			min_diff_function = diff_func_value;
			minimal_node = i;
		}
	}

	minimal_nodes[global_id] = minimal_node;
	error[global_id] = min_diff_function;
}


kernel void test_get_model_layer_index_by_altitude(
	global const float *model, int layers_count, float target_altitude
	){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	int index = get_model_layer_index_by_altitude(
		model, layers_count, target_altitude
	);

	printf("%i\n", index);
}

kernel void test_get_ray_constant(float incident_angle, float vp){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	printf("%f\n", get_ray_constant((double)incident_angle, vp));
}

kernel void test_is_ray_reflected(global const float *model,
	int layers_count, float source_altitude,
	float target_altitude, float incident_angle
	){
    int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	bool is_reflected = is_ray_reflected(model, layers_count, source_altitude,
	target_altitude, (double)incident_angle
	);

	if (is_reflected){
		printf("1\n");
	}
	else{
		printf("0\n");
	}
}

kernel void test_get_ray_trace(global const float *model, int layers_count,
	float source_r, float source_altitude, float target_altitude,
    float incident_angle, int lateral_direction, int frequency
){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	float3 ray_point = get_ray_trace(
		model, layers_count, source_r, source_altitude, target_altitude,
    	(double)incident_angle, lateral_direction, frequency
	);
	printf("%f %f %f\n", ray_point.s0, ray_point.s1, ray_point.s2);
}

kernel void test_get_min_angle(float delta_altitudes, float accuracy){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	double min_angle = get_min_angle(delta_altitudes, accuracy);
	printf("%f\n", min_angle);
}

kernel void test_get_max_angle(float delta_altitudes, float r_offset){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	double max_angle = get_max_angle(delta_altitudes, r_offset);
	printf("%f\n", max_angle);
}

kernel void test_get_ray_time(global const float *model, int layers_count,
	float source_r, float source_altitude, float receiver_r,
	float receiver_altitude, float accuracy, int frequency
){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	float time = get_ray_time(model, layers_count, source_r, source_altitude,
		receiver_r, receiver_altitude, accuracy, frequency);

	printf("%f\n", time);
}

kernel void test_get_diff_function(global const float *model, int layers_count,
	global const int *real_delays, int stations_count, int events_count,
	int event_id, global const float *station_coordinates,
	float stations_altitude, float x_node, float y_node, float z_node,
	float accuracy, int frequency, int base_station_index
){
	int global_id = get_global_thread_id();

	if (global_id > 0){
		return;
	}

	float3 node_coordinate = {x_node, y_node, z_node};

	float diff_function_value = get_diff_function(model, layers_count,
		real_delays, stations_count, events_count, event_id,
		station_coordinates, stations_altitude,
		node_coordinate, accuracy, frequency, base_station_index
		);

	printf("%f\n", diff_function_value);
}
