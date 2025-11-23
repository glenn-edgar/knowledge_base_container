#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "cfl_runtime.h"
#include "chaintree_support.h"
#include "json_node_decoder.h"

static unsigned int cfl_calculate_max_level(cfl_runtime_handle_t* handle);
static void cfl_set_timer_reference(cfl_runtime_handle_t* handle);
static void cfl_send_system_event_to_test(cfl_runtime_handle_t* handle, 
    uint16_t kb_idx, unsigned event_id, unsigned event_type, bool malloc_flag, void *data);
static void cfl_generate_timer_events(cfl_runtime_handle_t* handle, uint16_t kb_idx, cfl_tick_result_t* result);
static void cfl_init_test_system(cfl_runtime_handle_t* handle);

cfl_runtime_create_params_t* cfl_runtime_create_params_create(void) {
    cfl_runtime_create_params_t* params = (cfl_runtime_create_params_t*)calloc(1, sizeof(cfl_runtime_create_params_t));
    if (!params) {
        EXCEPTION("cfl_runtime_create_params_create: Failed to allocate memory for params");
    }
    memset(params, 0, sizeof(cfl_runtime_create_params_t));
    return params;
}

void cfl_runtime_create_params_destroy(cfl_runtime_create_params_t* params) {
    if (!params) {
        EXCEPTION("cfl_runtime_create_params_destroy: NULL params pointer");
    }
    free(params);
}

cfl_runtime_handle_t* cfl_runtime_create(cfl_perm_t* perm, cfl_runtime_create_params_t* params, 
                                         const chaintree_handle_t* flash_handle) {
    
    // Validate that parameter node count matches flash handle
    if (params->total_node_count != flash_handle->node_count) {
        EXCEPTION("cfl_runtime_create: params->total_node_count doesn't match flash_handle->node_count");
    }
    
    cfl_perm_set_instance(perm);
    cfl_perm_init(perm, params->perm_buffer, params->perm_buffer_size);
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t*)cfl_perm_alloc_pointer(perm, (uint16_t)sizeof(cfl_runtime_handle_t));
    if (!handle) {
        EXCEPTION("cfl_runtime_create: Failed to allocate memory for handle");
    }
    
    handle->flash_handle = flash_handle;
    handle->perm = perm;
    handle->heap = cfl_heap_init(perm, params->heap_size);
    handle->arena_system = cfl_heap_arena_system_create(perm, handle->heap, params->max_allocator_count, 
                                                        params->total_node_count, params->allocator_0_size);
    handle->event_queue = cfl_create_event_queue(params->event_queue_high_priority_size, 
                                                 params->event_queue_low_priority_size, perm);
    
    // Validate flags array size before allocating (prevent uint16_t overflow)
    size_t flags_size = sizeof(uint8_t) * params->total_node_count;
    if (flags_size > 65535) {
        EXCEPTION("cfl_runtime_create: Flags array size exceeds uint16_t limit");
    }
    
    handle->flags = (uint8_t*)cfl_perm_alloc_pointer(perm, (uint16_t)flags_size);
    handle->timer_handle = cfl_timer_create(params->delta_time, perm);
    handle->delta_time = params->delta_time;
    handle->max_level = cfl_calculate_max_level(handle);
    
    // Validate stack sizes before allocating (prevent uint16_t overflow)
    size_t stack_size = sizeof(CT_StackEntry) * handle->max_level;
    if (stack_size > 65535) {
        EXCEPTION("cfl_runtime_create: Stack size exceeds uint16_t limit");
    }
    
    handle->stack = (CT_StackEntry*)cfl_perm_alloc_pointer(perm, (uint16_t)stack_size);
    handle->nested_stack = (CT_StackEntry*)cfl_perm_alloc_pointer(perm, (uint16_t)stack_size);
    handle->walker = (CT_TreeWalker*)cfl_perm_alloc_pointer(handle->perm, sizeof(CT_TreeWalker));
    handle->backup_flags = (uint8_t*)cfl_perm_alloc_pointer(perm, (uint16_t)flags_size);
    handle->walker_context_ptr = (CT_WalkerContext*)cfl_perm_alloc_pointer(perm, (uint16_t)(sizeof(CT_WalkerContext)));
    handle->json_decoder_ctx = (json_decoder_ctx_t*)cfl_perm_alloc_pointer(perm, (uint16_t)(sizeof(json_decoder_ctx_t)));
    
    memset((void*)handle->flags, 0, params->total_node_count);
    
    cfl_init_test_system(handle);
    cfl_engine_create(handle);
    
    unsigned bytes_used = cfl_perm_used_bytes(perm);
    printf("bytes used: %d\n", bytes_used);
    unsigned bytes_free = cfl_perm_free_bytes(perm);
    printf("bytes free: %d\n", bytes_free);
    
    return handle;
}

void cfl_runtime_reset(cfl_runtime_handle_t* handle) {
    if (!handle) {
        EXCEPTION("cfl_runtime_reset: NULL handle pointer");
    }
    handle->max_level = cfl_calculate_max_level(handle);
    
    cfl_heap_arena_system_reset(handle->arena_system);
    cfl_clear_queue(handle->event_queue);
    cfl_engine_init(handle);
    memset((void*)handle->flags, 0, handle->flash_handle->node_count);
    
    // Reinitialize all active tests using bitmap system
    for(uint16_t kb_idx = 0; kb_idx < handle->flash_handle->kb_count; kb_idx++) {
        if (TEST_IS_ACTIVE(handle, kb_idx)) {
            const chaintree_kb_info_t* kb = &handle->flash_handle->kb_table[kb_idx];
            cfl_engine_init_test(handle, kb->start_index, kb->node_count);
        }
    }
    
    printf("heap used bytes: %d free bytes: %d\n", 
           cfl_heap_used_bytes(handle->heap), cfl_heap_free_bytes(handle->heap));
}

bool cfl_runtime_run(cfl_runtime_handle_t* handle) {
    CFL_EVENT_DATA_T event_data;
    cfl_tick_result_t tick_result;
    
    if (!handle) {
        EXCEPTION("cfl_runtime_run: NULL handle pointer");
    }
    
    printf("---------------------------------start of runtime run---------------------------------\n");
    printf("cfl_perm_used_bytes: %d\n", cfl_perm_used_bytes(handle->perm));
    printf("cfl_perm_free_bytes: %d\n", cfl_perm_free_bytes(handle->perm));
    printf("arena 0 : used bytes: %d free bytes: %d\n", 
           cfl_heap_arena_used_bytes(handle->arena_system, 0), 
           cfl_heap_arena_free_bytes(handle->arena_system,0 ));
    
    cfl_set_timer_reference(handle);
    
    bool loop_flag = true;
    while(loop_flag) {
        // Wait for timer once per cycle - OUTSIDE the test loop
        double delta_time = handle->future_time_stamp - cfl_timer_get_timestamp(handle->timer_handle);
    
        cfl_timer_wait(handle->timer_handle, delta_time, &tick_result);
        loop_flag = false;
        for(uint16_t kb_idx = 0; kb_idx < handle->flash_handle->kb_count; kb_idx++) {
    
            if (!TEST_IS_ACTIVE(handle, kb_idx)) continue;
            loop_flag = true;
            handle->current_kb_idx = kb_idx;
            handle->kb_start_index = handle->flash_handle->kb_table[kb_idx].start_index;
            handle->kb_node_count = handle->flash_handle->kb_table[kb_idx].node_count;
            handle->kb_max_level = handle->flash_handle->kb_table[kb_idx].max_depth+1;
            
            cfl_generate_timer_events(handle, kb_idx, &tick_result);
        
            while(cfl_total_event_count(handle->event_queue) > 0) {
                cfl_peek_event(handle->event_queue, &event_data);
                
                if (event_data.event_id == CFL_TERMINATE_SYSTEM_EVENT) {
                    printf("terminate system\n");
                    goto exit;
                }
        
                handle->event_data_ptr = &event_data;
                
                if(cfl_execute_event(handle) == false) {
                    printf("terminate test\n");
                    cfl_delete_test_by_index(handle, handle->current_kb_idx);
                }
                
                cfl_pop_event(handle->event_queue, &event_data);
            }
            
            // ADD THIS: Check if start node is still enabled after processing all events
            if (!cfl_engine_node_is_enabled(handle, handle->kb_start_index)) {
                printf("Test %d start node disabled, deleting test\n", kb_idx);
                cfl_delete_test_by_index(handle, kb_idx);
            }
        }
        
        
        
        // Update timestamp once per cycle - AFTER all tests processed
        handle->future_time_stamp = handle->future_time_stamp + handle->delta_time;
        if (loop_flag == false) {
            goto exit;
        }
    }  
    
exit:
    printf("---------------------------------end of runtime run---------------------------------\n");
    printf("cfl_perm_used_bytes: %d\n", cfl_perm_used_bytes(handle->perm));
    printf("cfl_perm_free_bytes: %d\n", cfl_perm_free_bytes(handle->perm));
    printf("heap used bytes: %d free bytes: %d\n", 
           cfl_heap_used_bytes(handle->heap), cfl_heap_free_bytes(handle->heap));
    printf("high priority count: %d low priority count: %d\n", 
           cfl_high_priority_count(handle->event_queue), cfl_low_priority_count(handle->event_queue));
    printf("arena 0 : used bytes: %d free bytes: %d\n", 
           cfl_heap_arena_used_bytes(handle->arena_system,0),
           cfl_heap_arena_free_bytes(handle->arena_system,0));
    printf("runtime run completed\n");
    
    return true;
}

static void cfl_set_timer_reference(cfl_runtime_handle_t* handle) {
    cfl_tick_result_t result;
    cfl_timer_wait(handle->timer_handle, .001, &result);
    handle->future_time_stamp = cfl_timer_get_timestamp(handle->timer_handle) + handle->delta_time;
}

/**
 * Send a system event to a specific test
 */
static void cfl_send_system_event_to_test(cfl_runtime_handle_t* handle, 
    uint16_t kb_idx, unsigned event_id, unsigned event_type, bool malloc_flag, void *data) {
    
    if (!TEST_IS_ACTIVE(handle, kb_idx)) return;
    
    const chaintree_kb_info_t* kb = &handle->flash_handle->kb_table[kb_idx];
    
    if(cfl_engine_node_is_enabled(handle, kb->start_index)) {
        cfl_send_event(handle->event_queue, CFL_EVENT_PRIORITY_LOW, 
            kb->start_index, event_type, malloc_flag, event_id, data);
    }
}

/**
 * Generate timer events for a specific test
 * Timer wait is now done ONCE in the main loop before calling this
 */
static void cfl_generate_timer_events(cfl_runtime_handle_t* handle, uint16_t kb_idx, cfl_tick_result_t* result) {
    cfl_send_system_event_to_test(handle, kb_idx, CFL_TIMER_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    
    // Send second event if second changed
    if (result->changed_mask & CFL_CHANGED_SECOND) {
        cfl_send_system_event_to_test(handle, kb_idx, CFL_SECOND_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    }
    
    // Send minute event if minute changed
    if (result->changed_mask & CFL_CHANGED_MINUTE) { 
        cfl_send_system_event_to_test(handle, kb_idx, CFL_MINUTE_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    }
    
    // Send hour event if hour changed
    if (result->changed_mask & CFL_CHANGED_HOUR) {
        cfl_send_system_event_to_test(handle, kb_idx, CFL_HOUR_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    }
    
    // Send day event if day changed
    if (result->changed_mask & CFL_CHANGED_DAY) {
        cfl_send_system_event_to_test(handle, kb_idx, CFL_DAY_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    }
    
    // Send week event if week changed
    if (result->changed_mask & CFL_CHANGED_DOW) {
        cfl_send_system_event_to_test(handle, kb_idx, CFL_WEEK_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    }
    
    // Send year event if year changed
    if (result->changed_mask & CFL_CHANGED_DOY) {
        cfl_send_system_event_to_test(handle, kb_idx, CFL_YEAR_EVENT, CFL_EVENT_TYPE_PTR, false, result);
    }
}

static void cfl_init_test_system(cfl_runtime_handle_t* handle) {
    unsigned bitmap_size = (handle->flash_handle->kb_count + 31) / 32;
    handle->active_test_bitmap = cfl_perm_alloc_pointer(handle->perm, 
                                                        bitmap_size * sizeof(uint32_t));
    memset((void*)handle->active_test_bitmap, 0, bitmap_size * sizeof(uint32_t));
    // Allocate test arena tracking arrays
    handle->kb_allocator_ids = (cfl_heap_allocator_id_t*)cfl_perm_alloc_pointer(handle->perm,
        handle->flash_handle->kb_count * sizeof(cfl_heap_allocator_id_t));
    handle->test_has_arena = (uint8_t*)cfl_perm_alloc_pointer(handle->perm,
        handle->flash_handle->kb_count * sizeof(uint8_t));

    // Initialize to invalid/unallocated state
    for (uint16_t i = 0; i < handle->flash_handle->kb_count; i++) {
        handle->kb_allocator_ids[i] = 0xff;  // -1 indicates no arena
        handle->test_has_arena[i] = 0;   // false
    }
    handle->active_test_count = 0;
}

bool cfl_add_test_by_index(cfl_runtime_handle_t* handle, uint16_t kb_index) {
    if (kb_index >= handle->flash_handle->kb_count) return false;
    if (TEST_IS_ACTIVE(handle, kb_index)) return false;  // Already active
    
    const chaintree_kb_info_t* kb = &handle->flash_handle->kb_table[kb_index];
    cfl_heap_allocator_id_t arena_id = cfl_heap_arena_create(handle->arena_system, kb->start_index, kb->node_count * 10);
    if (arena_id == 0xff) {
        // Arena allocation failed
        EXCEPTION("cfl_add_test_by_index: Arena allocation failed");
    }
    
    for(uint16_t i = kb->start_index; i < kb->start_index + kb->node_count; i++) {
        cfl_heap_arena_set_node_allocator_id(handle->arena_system, i, arena_id);
    }
    handle->kb_allocator_ids[kb_index] = arena_id;
    handle->test_has_arena[kb_index] = true;
    cfl_engine_init_test(handle, kb->start_index, kb->node_count);
    
    TEST_ACTIVE_SET(handle, kb_index);
    handle->active_test_count++;
    
    return true;
}

bool cfl_delete_test_by_index(cfl_runtime_handle_t* handle, uint16_t kb_index) {
    if (kb_index >= handle->flash_handle->kb_count) return false;
    if (!TEST_IS_ACTIVE(handle, kb_index)) return false;
    if (handle->test_has_arena[kb_index] == true) {
        unsigned used_bytes = cfl_heap_arena_used_bytes(handle->arena_system, handle->kb_allocator_ids[kb_index]);
        printf("used bytes: %d\n", used_bytes);
        unsigned free_bytes = cfl_heap_arena_free_bytes(handle->arena_system, handle->kb_allocator_ids[kb_index]);
        printf("free bytes: %d\n", free_bytes);
        cfl_heap_arena_destroy(handle->arena_system, handle->kb_allocator_ids[kb_index], handle->flash_handle->kb_table[kb_index].start_index);
        for(uint16_t i = handle->flash_handle->kb_table[kb_index].start_index;
             i < handle->flash_handle->kb_table[kb_index].start_index + handle->flash_handle->kb_table[kb_index].node_count; i++) {
            cfl_heap_arena_set_node_allocator_id(handle->arena_system, i, 0xff);
        }
        handle->test_has_arena[kb_index] = false;
        handle->kb_allocator_ids[kb_index] = 0xff;
    }
    TEST_ACTIVE_CLR(handle, kb_index);
    handle->active_test_count--;
    
    return true;
}

static unsigned int cfl_calculate_max_level(cfl_runtime_handle_t* runtime_handle) {
    unsigned int max_depth = 0;
    unsigned number_of_kbs = runtime_handle->flash_handle->kb_count;
    
    for(unsigned i = 0; i < number_of_kbs; i++) {
        const chaintree_kb_info_t* kb = &runtime_handle->flash_handle->kb_table[i];
        if(kb->max_depth > max_depth) {
            max_depth = kb->max_depth;
        }
    }
    
    return max_depth + 1; // for safety margin
}

uint16_t cfl_calculate_arrena_number(const chaintree_handle_t* flash_handle) {
    return flash_handle->kb_count + 1;
    // later we will scan for number of node defined allegators
}

