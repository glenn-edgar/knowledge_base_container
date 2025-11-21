#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "json_node_decoder.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"

bool cfl_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return false;
}


bool cfl_bool_false_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    return false;
    
}

bool cfl_column_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return false;
    
}

bool cfl_gate_node_null_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)node_index;
    (void)event_id;
    (void)event_type;
    (void)event_data;
    
    return false;
    
}

typedef struct{
    double timestamp_timeout;
} cfl_verify_time_out_boolean_fn_data_t;

bool cfl_verify_time_out_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)event_data;
    (void)event_type;

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_verify_fn_data_t *ptr = (cfl_verify_fn_data_t *)cfl_heap_arena_get_node_ptr( runtime_handle->arena_system, node_index);
    if(event_id == CFL_INIT_EVENT){
        float time_out;

        json_decoder_init_from_runtime(runtime_handle, node_index);
        cfl_verify_time_out_boolean_fn_data_t *auxiliary_data = (cfl_verify_time_out_boolean_fn_data_t *)cfl_heap_malloc_pointer(runtime_handle->heap, sizeof(cfl_verify_time_out_boolean_fn_data_t));
        json_extract_float32_runtime(runtime_handle, "node_dict.fn_data.time_out", &time_out);
        auxiliary_data->timestamp_timeout = cfl_timer_get_timestamp(runtime_handle->timer_handle) + (double)time_out;
        ptr->auxiliary_data = auxiliary_data;
        return false;
    }
    if(event_id == CFL_TERMINATE_EVENT){
    
        cfl_heap_free_pointer(runtime_handle->heap, ptr->auxiliary_data);
        return false;
    }
    if(event_id == CFL_TIMER_EVENT){
        
        cfl_verify_time_out_boolean_fn_data_t *auxiliary_data = (cfl_verify_time_out_boolean_fn_data_t *)ptr->auxiliary_data;
        double compare_timestamp = auxiliary_data->timestamp_timeout;
        
        if(cfl_timer_get_timestamp(runtime_handle->timer_handle) >= compare_timestamp){
        
            
            return false;
        }
    }
    
    
    return true;
}


typedef struct {
    int32_t event_id;
    int32_t event_count;
} cfl_wait_for_event_boolean_fn_data_t;
  


bool cfl_wait_for_event_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)event_data;
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_wait_fn_data_t *ptr = (cfl_wait_fn_data_t *)cfl_heap_arena_get_node_ptr( runtime_handle->arena_system, node_index);

    if(event_id == CFL_INIT_EVENT){
    
       cfl_wait_for_event_boolean_fn_data_t *auxiliary_data =
        (cfl_wait_for_event_boolean_fn_data_t *) cfl_heap_malloc_pointer(runtime_handle->heap, sizeof(cfl_wait_for_event_boolean_fn_data_t));
        json_decoder_init_from_runtime(runtime_handle, node_index);
        json_extract_int32_runtime(runtime_handle, "node_dict.wait_fn_data.event_id", &auxiliary_data->event_id);
        json_extract_int32_runtime(runtime_handle, "node_dict.wait_fn_data.event_count", &auxiliary_data->event_count);
        ptr->auxiliary_data = auxiliary_data;
       return false;

    }
    if(event_id == CFL_TERMINATE_EVENT){
        
        cfl_heap_free_pointer(runtime_handle->heap, ptr->auxiliary_data);

        return false;

    }
    cfl_wait_for_event_boolean_fn_data_t *auxiliary_data = (cfl_wait_for_event_boolean_fn_data_t *)ptr->auxiliary_data;
    if((unsigned)event_id == (unsigned)auxiliary_data->event_id){
        auxiliary_data->event_count--;
        if(auxiliary_data->event_count == 0){
            return true;
        }
    }
    return false;
}
/*
wait_fn_data:
  event_id: 12
  event_count: 1

*/