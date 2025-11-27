#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>


#include "cfl_runtime.h"
#include "cfl_common_function_headers.h"
#include "json_node_decoder.h"





bool while_test_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)event_type;
    (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_while_fn_data_t *ptr = (cfl_while_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (event_id == CFL_INIT_EVENT) {
        int32_t *loop_count = (int32_t *)cfl_heap_malloc_pointer(runtime_handle->heap, sizeof(uint32_t));
        
        ptr->auxiliary_data = (void *)loop_count;
        json_decoder_init_from_runtime(runtime_handle, node_index);
        json_extract_int32_runtime(runtime_handle, "node_dict.user_data.count", loop_count);
    
        return false;
    }
    if (event_id == CFL_TERMINATE_EVENT) {
        cfl_heap_free_pointer(runtime_handle->heap, ptr->auxiliary_data);
        return false;
    }
    int32_t *loop_count = (int32_t *)ptr->auxiliary_data;
    if(ptr->current_iteration >= *loop_count) {
        return false;
    }

    return true;
}


bool catch_all_exception_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)handle;
    (void)node_index;
    (void)event_id;
    (void)event_type;
    (void)event_data;
    printf("catch_all_exception_boolean_fn\n");
    exit(0);
    return false;
}

bool exception_filter_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)handle;
    (void)node_index;
    (void)event_id;
    (void)event_type;
    (void)event_data;
    printf("exception_filter_boolean_fn\n");
    exit(0);
    return false;
}

bool user_skip_condition_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)handle;
    (void)node_index;
    (void)event_id;
    (void)event_type;
    (void)event_data;
    printf("user_skip_condition_boolean_fn\n");
    exit(0);
    return false;
}




