#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include "cfl_runtime.h"
#include "cfl_exception.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_header.h"

#include "cfl_sm_functions.h"

void cfl_change_state(cfl_runtime_handle_t *handle, uint16_t node_index, int32_t sm_node_id, const char *new_state, bool sync_flag, int32_t sync_event_id){
    (void)node_index;
    
    /* Validate sm_node_id */
    if (sm_node_id < 0 || (unsigned)sm_node_id >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_change_state: sm_node_id out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &handle->flash_handle->nodes[sm_node_id];
   
    if (node->main_function_index != handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_STATE_MACHINE]){
        EXCEPTION("cfl_change_state: Node is not a state machine");
        return;
    }
    
    cfl_state_machine_column_data_t *ptr = (cfl_state_machine_column_data_t *)cfl_heap_arena_get_node_ptr(handle->arena_system, sm_node_id);
    if (!ptr) {
        EXCEPTION("cfl_change_state: failed to get node pointer");
        return;
    }
    
    if (!ptr->state_names) {
        EXCEPTION("cfl_change_state: state_names is NULL");
        return;
    }
    
    bool state_found = false;
    uint16_t state_count = node->link_count & LINK_COUNT_MASK;
    
    for(uint16_t i = 0; i < state_count; i++){
        if (strcmp(ptr->state_names[i], new_state) == 0){
            ptr->new_state = i;
            state_found = true;
            break;
        }
    }
    
    if (!state_found){
        EXCEPTION("cfl_change_state: State not found");
        return;
    }
    
    /* Validate new_state is within range */
    if (ptr->new_state < 0 || ptr->new_state >= (int32_t)state_count) {
        EXCEPTION("cfl_change_state: new_state out of range");
        return;
    }
    
    if (sync_flag){
        ptr->sync_event_id_valid = true;
        ptr->sync_event_id = sync_event_id;
        cfl_send_null_event(
            handle->event_queue,
            CFL_EVENT_PRIORITY_LOW,
            sm_node_id,
            sync_event_id);
       
    }
    else{
        ptr->sync_event_id_valid = false;
        ptr->sync_event_id = 0;
    }
}

oid cfl_terminate_state_machine(cfl_runtime_handle_t *handle, uint16_t node_index, int32_t sm_node_id){
    (void)node_index;
    
    /* Validate sm_node_id */
    if (sm_node_id < 0 || (unsigned)sm_node_id >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_terminate_state_machine: sm_node_id out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &handle->flash_handle->nodes[sm_node_id];
   
    if (node->main_function_index != ct_get_main_function_index(handle->flash_handle, "CFL_STATE_MACHINE_MAIN")){
        EXCEPTION("cfl_terminate_state_machine: Node is not a state machine");
        return;
    }
    
    cfl_terminate_node_tree(handle, sm_node_id);

}



unsigned cfl_sm_envelope_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    cfl_column_main_main_fn(handle, bool_function_index, node_index, event_type, event_id, event_data);
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


void cfl_state_machine_term_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_state_machine_term_one_shot_fn: node_index out of bounds");
        return;
    }
    
    
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    uint16_t link_start = node->link_start;
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_term_one_shot_fn: link_start out of bounds");
            return;
        }
        if (link_start + link_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_term_one_shot_fn: link range exceeds table size");
            return;
        }
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    
    /* BUG FIX: Terminate link_table[link_start + i], not link_start + i */
    for (uint32_t i = 0; i < link_count; i++) {
        uint16_t link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_state_machine_term_one_shot_fn: link_id out of bounds");
            continue;
        }
        
        cfl_terminate_node_tree(runtime_handle, link_id);
    }    
}

void cfl_terminate_state_machine_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    int32_t sm_node_id;
    json_extract_int32_runtime(runtime_handle, "node_dict.sm_node_id", &sm_node_id);

    cfl_terminate_state_machine(runtime_handle, node_index, sm_node_id);


}