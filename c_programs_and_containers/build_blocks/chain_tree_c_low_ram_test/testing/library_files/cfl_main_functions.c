#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include "cfl_runtime.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"


unsigned cfl_null_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){

    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    

    return 0;
}



unsigned cfl_disable_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    
    return CFL_DISABLE;
}



unsigned cfl_column_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    if (event_id != CFL_TIMER_EVENT) {
        return CFL_CONTINUE;
    }
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    const boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    if (result == true) {

        return CFL_DISABLE;
    }

    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);

    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    link_table = runtime_handle->flash_handle->link_table;
    
    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
    
        if (cfl_engine_node_is_enabled(runtime_handle, link_id) == true) {
        
            return CFL_CONTINUE;
        }
    }
    return CFL_DISABLE;
}
  
unsigned cfl_gate_node_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    
    return cfl_column_main_main_fn(handle, bool_function_index, node_index, event_type, event_id, event_data);
}

unsigned cfl_halt_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_HALT;
    
    
}
unsigned cfl_reset_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_RESET;
    
}
unsigned cfl_terminate_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_TERMINATE;
    
}
unsigned cfl_terminate_system_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    return CFL_TERMINATE_SYSTEM;
}



unsigned cfl_verify_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    printf("result: %d\n", result);
    if (result == true) {
        return CFL_CONTINUE;
    }
    
    cfl_verify_fn_data_t *ptr = (cfl_verify_fn_data_t *)cfl_heap_arena_get_node_ptr( runtime_handle->arena_system, node_index);
    one_shot_function_t one_shot_function = runtime_handle->flash_handle->one_shot_functions[ptr->error_function];
    one_shot_function(runtime_handle, node_index);
    if (ptr->reset_flag == true) {
        
        return CFL_RESET;
    }
    return CFL_DISABLE;
    
}
unsigned cfl_wait_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    if (result == true) {
        return CFL_DISABLE;
    }
    cfl_wait_fn_data_t *ptr = (cfl_wait_fn_data_t *)cfl_heap_arena_get_node_ptr( runtime_handle->arena_system, node_index);
    if (ptr->timeout == 0) {
        return CFL_HALT;
    }
    if (ptr->time_out_event == event_id) {
        ptr->event_count++;
        if (ptr->event_count >= ptr->timeout) {
            one_shot_function_t one_shot_function = runtime_handle->flash_handle->one_shot_functions[ptr->error_function];
            one_shot_function(runtime_handle, node_index);
            if (ptr->reset_flag == true) {
                return CFL_RESET;
            }
            return CFL_DISABLE;
        }
        return CFL_HALT;
    }
    return CFL_HALT;
    
}




unsigned cfl_wait_time_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, 
    unsigned event_type, unsigned event_id, void *event_data){
    (void)bool_function_index;
    (void)event_type;
    (void)event_data;
    if (event_id != CFL_TIMER_EVENT) {
        return CFL_HALT;
    }

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_wait_time_out_data_t *ptr = (cfl_wait_time_out_data_t *)cfl_heap_arena_get_node_ptr( runtime_handle->arena_system, node_index);
    if (ptr->wait_time_out >= cfl_timer_get_timestamp(runtime_handle->timer_handle)) {
        
        return CFL_HALT;
    }
    printf("CFL_DISABLE\n");
    return CFL_DISABLE;  
    
}



