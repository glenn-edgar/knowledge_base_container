#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "json_node_decoder.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"
#include "user_function_headers.h"

void activate_valve_one_shot_fn(void *handle, unsigned node_index){
    
    
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    const char *state;
   
    
    
    
    json_decoder_init_from_runtime(runtime, node_index);
    
    // Step 2: Extract the value using path notation
    json_extract_string_runtime(runtime, "node_dict.state", &state);
    
    // Step 3: Use the value (state now points to "open")
    // No need to free - string points into flash memory
    
    if (strcmp(state, "open") == 0) {
        printf("Valve is open\n");
    }

}



void wait_for_event_error_one_shot_fn(void *handle, unsigned node_index){
    (void)node_index;
    (void)handle;
    printf("wait_for_event_error_one_shot_fn node index: %d\n", node_index);
    exit(0);
}

void verify_error_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_verify_fn_data_t *ptr = (cfl_verify_fn_data_t *)cfl_heap_arena_get_node_ptr( runtime->arena_system, node_index);
    printf("verify_error_one_shot_fn failure_data: %s\n", ptr->failure_data);
    
}
#if 0

typedef struct {
    int32_t finalize_function_id;
    int32_t try_node_count;
    uint16_t *try_node_indexes;
    void *auxiliary_data;
  } sequence_aggregate_data_t;

#endif






void initialize_sequence_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    
   
}

void display_sequence_till_result_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    const char *message;

    sequence_start_fn_data_t *ptr = (sequence_start_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    json_decoder_init_from_runtime(runtime, node_index);
    
    json_extract_string_runtime(runtime, "node_dict.column_data.user_data.message", &message);
    printf("display_sequence_till_result_one_shot_fn message: %s\n", message);
    printf("sequence_type: %d\n", ptr->sequence_type);
    printf("sequence_result: %d\n", ptr->final_status);
    for(int i = 0; i < ptr->sequence_number; i++){
        printf("node_index: [%d] sequence_result: %d\n", ptr->sequence_result_data_array[i].node_index, ptr->sequence_result_data_array[i].sequence_result);
    }
    printf("now examinng nodes that set sequence_results\n");
    for(int i = 0; i < ptr->sequence_number; i++){
        node_index = ptr->sequence_result_data_array[i].node_index;
        printf("dictonary node_index: %d\n", node_index);
        json_decoder_init_from_runtime(runtime, node_index);
        json_print_node_data_runtime(runtime, node_index);
    }
    
    
}

void display_sequence_result_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    sequence_aggregate_data_t *ptr = (sequence_aggregate_data_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    printf("try_node_count: %d\n", ptr->try_node_count);
    for(int i = 0; i < ptr->try_node_count; i++){
        printf("try_node_indexes[%d]: %d\n", i, ptr->try_node_indexes[i]);
        display_sequence_till_result_one_shot_fn(handle, ptr->try_node_indexes[i]);
    }
    

}

void display_failure_window_result_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_supervisor_data_t *ptr = (cfl_supervisor_data_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    int32_t uplink_node_id;
    printf("failed_link_index: %d\n", ptr->failed_link_index);
    printf("failed_node_index: %d\n", ptr->supervisor_failure_array[ptr->failed_link_index].node_id);
    json_decoder_init_from_runtime(runtime, node_index);
    json_extract_int32_runtime(runtime, "node_dict.column_data.user_data.uplink_node_id", &uplink_node_id);
    printf("uplink_node_id: %d if communicating with uplink node\n", uplink_node_id);
    json_decoder_init_from_runtime(runtime, ptr->supervisor_failure_array[ptr->failed_link_index].node_id);
    printf("dump of json data for failed node \n");
    json_print_node_data_runtime(runtime, ptr->supervisor_failure_array[ptr->failed_link_index].node_id);

}

void watch_dog_time_out_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime, node_index);
    
    bool  reset;
    const char *message;
    json_extract_bool_runtime(runtime, "node_dict.wd_reset", &reset);
    json_extract_string_runtime(runtime, "node_dict.wd_fn_data.message", &message);
    printf("watch_dog_time_out_one_shot_fn reset: %d\n", reset);
    printf("watch_dog_time_out_one_shot_fn message: %s\n", message);
    
}


#include "cfl_exception_support.h"

void exception_logging_one_shot_fn(void *handle, unsigned node_index){

    
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    cfl_exception_support_data_t *ptr = (cfl_exception_support_data_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if (ptr == NULL) {
        EXCEPTION("exception_logging_one_shot_fn: ptr is NULL");
    }
    
    if (ptr->logging_data == NULL) {
        ptr->logging_data = (void *)cfl_additional_arena_alloc(runtime, node_index, sizeof(void *));
       json_decoder_init_from_runtime(runtime, node_index);
       json_extract_string_runtime(runtime, "node_dict.column_data.logging_function_data.logging_function_data", (const char **)&ptr->logging_data);
    }
    
    
    printf("*********** exception_logging_one_shot_fn ***********\n");
    printf("original_node_id: %d\n", ptr->original_node_id);
    printf("logging_data: %s\n", (const char *)ptr->logging_data);
    printf("exception_type: %d\n", ptr->exception_type);   
    printf("*********** exception_logging_one_shot_fn ***********\n");
    
}


void sm_event_filtering_init_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    sm_event_filtering_init_fn_data_t *ptr = (sm_event_filtering_init_fn_data_t *)cfl_smart_arena_alloc(runtime, node_index, 
              sizeof(sm_event_filtering_init_fn_data_t));
    if (ptr == NULL) {
        EXCEPTION("sm_event_filtering_init_one_shot_fn: ptr is NULL");
    }
    ptr->event_id = ct_get_event_index(runtime->flash_handle, "TEST_EVENT_1");
    if (ptr->event_id == -1) {
        EXCEPTION("sm_event_filtering_init_one_shot_fn: event_id not found");
    }
    
}


