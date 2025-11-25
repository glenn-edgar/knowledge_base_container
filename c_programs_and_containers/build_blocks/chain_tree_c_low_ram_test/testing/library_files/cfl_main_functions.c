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
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_column_main_main_fn: node_index out of bounds");
        return CFL_TERMINATE_SYSTEM;
    }
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    const boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    if (result == true) {
        return CFL_DISABLE;
    }

    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_column_main_main_fn: link_start out of bounds");
            return CFL_TERMINATE_SYSTEM;
        }
        if (link_start + link_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_column_main_main_fn: link range exceeds table size");
            return CFL_TERMINATE_SYSTEM;
        }
    }

    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    
    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_column_main_main_fn: link_id out of bounds");
            return CFL_TERMINATE_SYSTEM;
        }
    
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
    if (result == true) {
        return CFL_CONTINUE;
    }
    
    cfl_verify_fn_data_t *ptr = (cfl_verify_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_verify_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
    one_shot_function_t one_shot_function = runtime_handle->flash_handle->one_shot_functions[ptr->error_function];
    
    one_shot_function(runtime_handle, node_index);
    if (ptr->reset_flag == true) {
        
        return CFL_RESET; 
    }
    
    return CFL_TERMINATE;
    
}
unsigned cfl_wait_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    if (result == true) {
        return CFL_DISABLE;
    }
    
    cfl_wait_fn_data_t *ptr = (cfl_wait_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_wait_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
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
        
            return CFL_TERMINATE;
        }
        return CFL_HALT;
    }
    return CFL_HALT;
    
}




unsigned cfl_wait_time_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, 
    unsigned event_type, unsigned event_id, void *event_data){
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    if (result == true) {
        return CFL_DISABLE;
    }
    if (event_id != CFL_TIMER_EVENT) {
        return CFL_HALT;
    }

    
    cfl_wait_time_out_data_t *ptr = (cfl_wait_time_out_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_wait_time_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
    if (ptr->wait_time_out >= cfl_timer_get_timestamp(runtime_handle->timer_handle)) {
        
        return CFL_HALT;
    }
    
    return CFL_DISABLE;  
    
}


unsigned cfl_event_logger_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)bool_function_index;
    (void)event_type;
    (void)event_data;

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    double timestamp = cfl_timer_get_timestamp(runtime_handle->timer_handle);
    cfl_event_logger_fn_data_t *ptr = (cfl_event_logger_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_event_logger_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
    if (ptr->event_count == 0) {
        return CFL_DISABLE;
    }
    
    if (!ptr->event_ids) {
        EXCEPTION("cfl_event_logger_main_fn: event_ids array is NULL");
        return CFL_TERMINATE_SYSTEM;
    }
    
    for (uint32_t i = 0; i < ptr->event_count; i++) {
        if (ptr->event_ids[i] == (int32_t)event_id) {
            printf("++++++++++ timestamp %f ,node id: %d event id: %d message: %s\n", timestamp, node_index, event_id, ptr->event_logger_message);
        }
    }
    return CFL_CONTINUE;
    
}


unsigned cfl_state_machine_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    unsigned return_value = CFL_CONTINUE;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_state_machine_main_main_fn: node_index out of bounds");
        return CFL_TERMINATE_SYSTEM;
    }
    
    cfl_state_machine_column_data_t *ptr = (cfl_state_machine_column_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_state_machine_main_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint32_t node_count = node->link_count & LINK_COUNT_MASK;
    uint32_t link_start = node->link_start;
    
    /* Validate link_start and node_count */
    if (node_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_main_main_fn: link_start out of bounds");
            return CFL_TERMINATE_SYSTEM;
        }
        if (link_start + node_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_main_main_fn: link range exceeds table size");
            return CFL_TERMINATE_SYSTEM;
        }
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    
    if (ptr->current_state != ptr->new_state) {
        /* Validate state indices */
        if (ptr->current_state < 0 || ptr->current_state >= (int32_t)node_count) {
            EXCEPTION("cfl_state_machine_main_main_fn: current_state out of range");
            return CFL_TERMINATE_SYSTEM;
        }
        if (ptr->new_state < 0 || ptr->new_state >= (int32_t)node_count) {
            EXCEPTION("cfl_state_machine_main_main_fn: new_state out of range");
            return CFL_TERMINATE_SYSTEM;
        }
        
        /* Validate link_id for current_state */
        uint16_t current_link_id = link_table[link_start + ptr->current_state];
        if (current_link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_state_machine_main_main_fn: current_state link_id out of bounds");
            return CFL_TERMINATE_SYSTEM;
        }
        
        /* Validate link_id for new_state */
        uint16_t new_link_id = link_table[link_start + ptr->new_state];
        if (new_link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_state_machine_main_main_fn: new_state link_id out of bounds");
            return CFL_TERMINATE_SYSTEM;
        }
        
        /* BUG FIX: Only terminate current_state ONCE, not in a loop */
        cfl_terminate_node_tree(runtime_handle, current_link_id);
        cfl_enable_node(runtime_handle, new_link_id);
        ptr->current_state = ptr->new_state;
    }
    
    boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
    bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
    if (result == true) {
        return_value = CFL_SKIP_CONTINUE;
    }
    

    for (unsigned i = 0; i < node_count; i++) {
        unsigned int link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_state_machine_main_main_fn: link_id out of bounds");
            return CFL_TERMINATE_SYSTEM;
        }
    
        if (cfl_engine_node_is_enabled(runtime_handle, link_id) == true) {
            
            return return_value;
        }
    }
    
    return CFL_DISABLE;
}

unsigned cfl_fork_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    return cfl_column_main_main_fn(handle, bool_function_index, node_index, event_type, event_id, event_data);
    
}

unsigned cfl_join_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)bool_function_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    uint32_t *ptr = (uint32_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_join_main_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    if(cfl_engine_node_is_enabled(runtime_handle, (uint16_t)*ptr) == true) {
        return CFL_HALT;
    }
    return CFL_DISABLE;
}

unsigned cfl_join_sequence_element_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    return cfl_join_main_main_fn(handle, bool_function_index, node_index, event_type, event_id, event_data);
}

unsigned cfl_sequence_pass_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    if (bool_function_index != 0) {

        boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
        bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
        if (result == true) {
            return CFL_DISABLE;
        }
    }
    sequence_start_fn_data_t *ptr = (sequence_start_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_sequence_pass_main_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
    if (event_id != CFL_TIMER_EVENT) {
        return CFL_CONTINUE;;
    }

    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;

    
    uint16_t active_link = link_table[link_start + ptr->current_sequence_index];
    
    if (cfl_engine_node_is_enabled(runtime_handle, active_link) == true) {
        return CFL_CONTINUE;
    }
    
    if(ptr->recorded_sequence_index != ptr->current_sequence_index){
        EXCEPTION("cfl_sequence_pass_main_main_fn: recorded_sequence_index != current_sequence_index");
        return CFL_TERMINATE_SYSTEM;
    }
    ptr->final_status = ptr->sequence_result_data_array[ptr->current_sequence_index].sequence_result;
    if( ptr->current_sequence_index+1 >= link_count){
    
        return CFL_DISABLE;
    }
    if (ptr->sequence_result_data_array[ptr->current_sequence_index].sequence_result == false) {
        cfl_terminate_node_tree(runtime_handle, active_link);
        ptr->current_sequence_index++;
        cfl_enable_node(runtime_handle, link_table[link_start + ptr->current_sequence_index]);
        return CFL_CONTINUE;

    }
    // test has passed
    return CFL_DISABLE;
}
unsigned cfl_sequence_fail_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
      cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
      
      if (bool_function_index != 0) {

        boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
        bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
        if (result == true) {
            return CFL_DISABLE;
        }
    }
    
    sequence_start_fn_data_t *ptr = (sequence_start_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    
    if (!ptr) {
        EXCEPTION("cfl_sequence_pass_main_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    
    if (event_id != CFL_TIMER_EVENT) {
        return CFL_CONTINUE;;
    }

    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;

    
    uint16_t active_link = link_table[link_start + ptr->current_sequence_index];
    
    if (cfl_engine_node_is_enabled(runtime_handle, active_link) == true) {
        return CFL_CONTINUE;
    }
    
    if(ptr->recorded_sequence_index != ptr->current_sequence_index){
        EXCEPTION("cfl_sequence_pass_main_main_fn: recorded_sequence_index != current_sequence_index");
        return CFL_TERMINATE_SYSTEM;
    }
    ptr->final_status = ptr->sequence_result_data_array[ptr->current_sequence_index].sequence_result;
    if( ptr->current_sequence_index+1 >= link_count){
    
        return CFL_DISABLE;
    }
    if (ptr->sequence_result_data_array[ptr->current_sequence_index].sequence_result == true) {
        cfl_terminate_node_tree(runtime_handle, active_link);
        ptr->current_sequence_index++;
        cfl_enable_node(runtime_handle, link_table[link_start + ptr->current_sequence_index]);
        return CFL_CONTINUE;

    }
    // test has passed
    return CFL_DISABLE;
}
unsigned cfl_sequence_start_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    return cfl_column_main_main_fn(handle, bool_function_index, node_index, event_type, event_id, event_data);
}