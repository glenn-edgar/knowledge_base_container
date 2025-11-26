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


void cfl_mark_supervisor_node_failure(cfl_runtime_handle_t *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    uint16_t ref_node_index = node_index;
    uint16_t previous_node_index = node_index;

    bool loop_flag = true;
    while(loop_flag){
        const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
        
        if (node->main_function_index == handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_SUPERVISOR_MAIN]){
            loop_flag = false;
            uint16_t link_count = node->link_count & LINK_COUNT_MASK;
        
            for(uint16_t i = 0; i < link_count; i++){
                uint16_t link_id = runtime_handle->flash_handle->link_table[node->link_start + i];
            
                if (link_id == previous_node_index){
                    cfl_supervisor_data_t *ptr = (cfl_supervisor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
                    ptr->failed_link_index = i;
                
                    ptr->supervisor_failure_array[i].node_id = ref_node_index;
                    return;
                }
            }
            
        }
        previous_node_index = node_index;
        node_index = node->parent_index;
        if (node_index == 0xFFFF){
            EXCEPTION("cfl_mark_supervisor_node_failure: no CFL_SUPERVISOR_MAIN found");
            return;
        }
    }
    EXCEPTION("cfl_mark_supervisor_node_failure: failed to mark supervisor node failure");
    return;
}





unsigned cfl_handle_supervisor_node_failure(cfl_runtime_handle_t *handle, uint16_t node_index, unsigned bool_function_index, 
        unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)node_index;
    (void)bool_function_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
     printf("cfl_handle_supervisor_node_failure: node_index: %d bool_function_index: %d event_type: %d event_id: %d\n", node_index, bool_function_index, event_type, event_id);
    exit(0);
    return CFL_CONTINUE;
}
#if 0
typedef struct {
    uint8_t  subchain_id;
    uint8_t  node_id;
    uint8_t  error_code;
    uint8_t  bucket;           // failure "credits"
    uint32_t last_tick;
} supervisor_failure_t;

#define BUCKET_MAX        3
#define LEAK_INTERVAL     1000   // lose 1 credit per second
#define BUCKET_ESCALATE   3

bool supervisor_leaky_bucket_check(supervisor_failure_t *f, uint8_t subchain, uint8_t node,
                    uint8_t err, uint32_t now_tick) {
    f->subchain_id = subchain;
    f->node_id = node;
    f->error_code = err;
    
    // Leak: subtract credits based on elapsed time
    uint32_t elapsed = now_tick - f->last_tick;
    uint8_t leak = (uint8_t)(elapsed / LEAK_INTERVAL);
    f->bucket = (leak >= f->bucket) ? 0 : (f->bucket - leak);
    
    // Add failure credit
    f->bucket++;
    f->last_tick = now_tick;
    
    return (f->bucket >= BUCKET_ESCALATE);
}

uint8_t find_subchain_link(uint16_t failed_node_idx, uint16_t supervisor_idx) {
    uint16_t current = failed_node_idx;
    uint16_t parent_idx = ct_nodes[current].parent_idx;
    
    // Walk up until parent is supervisor
    while (parent_idx != supervisor_idx) {
        current = parent_idx;
        parent_idx = ct_nodes[current].parent_idx;
    }
    
    // Now 'current' is direct child of supervisor
    // Find which link slot it occupies
    const ct_node_t *sup = &ct_nodes[supervisor_idx];
    for (uint8_t i = 0; i < sup->link_count; i++) {
        if (ct_pst2dztu_link_table[sup->link_start + i] == current) {
            return i;
        }
    }
    
    return 0xFF;  // sho
}
    #endif