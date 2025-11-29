#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include "cfl_engine.h"

#include "cfl_heap_arena_allocate.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"

void cfl_uint16_to_str(uint16_t value, char* buffer) {
    char temp[6];  // Max 5 digits + null
    int i = 0;
    
    if (value == 0) {
        buffer[0] = '0';
        buffer[1] = '\0';
        return;
    }
    
    while (value > 0) {
        temp[i++] = '0' + (value % 10);
        value /= 10;
    }
    
    // Reverse into output buffer
    int j;
    for (j = 0; j < i; j++) {
        buffer[j] = temp[i - 1 - j];
    }
    buffer[j] = '\0';
}

bool cfl_allocate_state(cfl_runtime_handle_t *handle, uint16_t node_index){
    uint16_t memory_index = cfl_heap_arena_get_node_memory_index(( cfl_heap_arena_system_t *)handle->arena_system, node_index);
    if (memory_index == 0xFFFF){
        return false;
    }
    return true;
}

void *cfl_additional_arena_alloc(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t size){
    // Use cfl_arena_additional_alloc, NOT cfl_arena_alloc_from_active
    void *ptr = cfl_arena_additional_alloc((CflHeapArenaSystem*)handle->arena_system, node_index, size);
    if (!ptr){
        EXCEPTION("cfl_additional_arena_alloc: Failed to allocate memory");
        return NULL;
    }
    return ptr;
}

void *cfl_smart_arena_alloc(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t size){
    
    uint16_t memory_index = cfl_heap_arena_get_node_memory_index(( cfl_heap_arena_system_t *)handle->arena_system, node_index);
    if (memory_index == 0xFFFF){
        return  cfl_arena_system_alloc(handle->arena_system, node_index, size);
    }
    return cfl_heap_arena_get_node_ptr(handle->arena_system, node_index);
    
}

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

void cfl_terminate_state_machine(cfl_runtime_handle_t *handle, uint16_t node_index, int32_t sm_node_id){
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


void cfl_enable_all_nodes(cfl_runtime_handle_t *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_enable_all_nodes: node_index out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_enable_all_nodes: link_start out of bounds");
            return;
        }
        if (link_start + link_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_enable_all_nodes: link range exceeds table size");
            return;
        }
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_enable_all_nodes: link_id out of bounds");
            return;
        }
        
        cfl_enable_node(runtime_handle, link_id);
    }
   
}

unsigned cfl_verify_active_children(cfl_runtime_handle_t *handle, uint16_t node_index)
{
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    const uint16_t *link_table = handle->flash_handle->link_table;
    for (uint16_t i = 0; i < link_count; i++) {
        uint16_t link_id = link_table[link_start + i];
        if (cfl_engine_node_is_enabled(handle, link_id) == true) {
            return CFL_CONTINUE;
        }
    }
    return CFL_DISABLE;
}