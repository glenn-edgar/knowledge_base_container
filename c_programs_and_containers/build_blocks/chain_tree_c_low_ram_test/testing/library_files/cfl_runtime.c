
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "cfl_runtime.h"
#include "chaintree_support.h"

static bool cfl_check_for_active_nodes(cfl_runtime_handle_t* handle);
static void cfl_set_timer_reference(cfl_runtime_handle_t* handle);

static void cfl_queue_internal_system_event(cfl_runtime_handle_t *handle, unsigned event_id, 
    unsigned event_type,  bool malloc_flag, void *data);

static void cfl_generate_timer_events(cfl_runtime_handle_t* handle);
static void cfl_init_test_system(cfl_runtime_handle_t* handle) ;

cfl_runtime_create_params_t* cfl_runtime_create_params_create(void);

void cfl_runtime_create_params_destroy(cfl_runtime_create_params_t* params);

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

cfl_runtime_handle_t* cfl_runtime_create( cfl_perm_t* perm, cfl_runtime_create_params_t* params, const chaintree_handle_t* flash_handle) {
    
    cfl_perm_set_instance(perm);
    cfl_perm_init(perm, params->perm_buffer, params->perm_buffer_size);
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t*)cfl_perm_alloc_pointer(perm,(uint16_t) sizeof(cfl_runtime_handle_t));
    if (!handle) {
        EXCEPTION("cfl_runtime_create: Failed to allocate memory for handle");
    }
    handle->flash_handle = flash_handle;
    handle->perm = perm;
    handle->heap = cfl_heap_init(perm, params->heap_size);
    uint16_t perm_used_bytes = cfl_perm_used_bytes(perm);
    uint16_t perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after heap init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->arena_system = cfl_heap_arena_system_create(perm, handle->heap, params->max_allocator_count, params->total_node_count, params->allocator_0_size);
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after arena system init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->event_queue = cfl_create_event_queue(params->event_queue_high_priority_size, params->event_queue_low_priority_size, perm);
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after event queue init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->flags = (uint8_t*)cfl_perm_alloc_pointer(perm, (uint16_t) (sizeof(uint8_t) * params->total_node_count));
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after flags init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->timer_handle = cfl_timer_create(params->delta_time, perm);
    handle->delta_time = params->delta_time;
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after timer handle init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    handle->flash_handle = flash_handle;
    cfl_engine_create(handle);
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after engine  init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    cfl_init_test_system(handle);
    perm_used_bytes = cfl_perm_used_bytes(perm);
    perm_free_bytes = cfl_perm_free_bytes(perm);
    printf("perm_used_bytes after test system init: %d, perm_free_bytes: %d\n", perm_used_bytes, perm_free_bytes);
    return handle;
}

void cfl_runtime_reset(cfl_runtime_handle_t* handle) {
    
    if (!handle) {
        EXCEPTION("cfl_runtime_reset: NULL handle pointer");
    }
    
    cfl_heap_arena_system_reset(handle->arena_system);
    cfl_clear_queue(handle->event_queue);
    cfl_reset_active_tests(handle);
    cfl_engine_init(handle);
    for(unsigned i = 0; i < handle->test_count; i++) {
        cfl_engine_init_test(handle, handle->test_controls[i].start_index, handle->test_controls[i].node_count);
    }
   }



bool cfl_runtime_run(cfl_runtime_handle_t* handle) {
    CFL_EVENT_DATA_T event_data;
    
    if (!handle) {
        EXCEPTION("cfl_runtime_run: NULL handle pointer");
    }
    
    cfl_set_timer_reference(handle);
    for( int i = 0; i < 1000; i++) {
        cfl_generate_timer_events(handle);

        while(cfl_total_event_count(handle->event_queue) > 0) {
            cfl_peek_event(handle->event_queue, &event_data);
            printf("event_id %d\n", event_data.event_id);
            if (event_data.event_id == CFL_TERMINATE_SYSTEM) {
                printf("terminate system\n");
                return false;
            }
            // handle stop test start test

            handle->event_data_ptr = &event_data;
            if(cfl_execute_event(handle) == false) {
                printf("event execution terminated system\n");
                return false;
            }
            cfl_pop_event(handle->event_queue, &event_data);
        }
        if(cfl_check_for_active_nodes(handle) == false) {
            printf("no active nodes\n");
            return false;
        }
    }
    printf("runtime run completed\n");
    return true;
}

static bool cfl_check_for_active_nodes(cfl_runtime_handle_t* handle){
    for(unsigned i = 0; i < handle->test_count; i++) {
        if(cfl_engine_node_is_enabled(handle, handle->test_controls[i].start_index)) {
            return true;
        }
    }
    return false;
}


static double future_time_stamp;

static void cfl_set_timer_reference(cfl_runtime_handle_t* handle){
    cfl_tick_result_t result;
    cfl_timer_wait(
        handle->timer_handle,
        .001,
        &result
    );
    future_time_stamp = cfl_timer_get_timestamp(handle->timer_handle) + handle->delta_time;
}



static void cfl_generate_timer_events(cfl_runtime_handle_t* handle) {
    cfl_tick_result_t result;
    double delta_time = future_time_stamp - cfl_timer_get_timestamp(handle->timer_handle);
    cfl_timer_wait(
        handle->timer_handle,
        delta_time,
        &result
    );
    cfl_queue_internal_system_event(handle, CFL_TIMER_EVENT,  CFL_EVENT_TYPE_PTR , false, &result);
    if (result.changed_mask & CFL_CHANGED_SECOND) {
        cfl_queue_internal_system_event(handle, CFL_SECOND_EVENT,  CFL_EVENT_TYPE_PTR  ,false, &result);
    }
    if (result.changed_mask & CFL_CHANGED_MINUTE) {
        cfl_queue_internal_system_event(handle, CFL_MINUTE_EVENT,  CFL_EVENT_TYPE_PTR  ,false, &result);
    }
    if (result.changed_mask & CFL_CHANGED_HOUR) {
        cfl_queue_internal_system_event(handle, CFL_HOUR_EVENT, CFL_EVENT_TYPE_PTR  , false, &result);
    }
    if (result.changed_mask & CFL_CHANGED_DAY) {
        cfl_queue_internal_system_event(handle, CFL_DAY_EVENT,  CFL_EVENT_TYPE_PTR  ,false, &result);
    }
    if (result.changed_mask & CFL_CHANGED_DOW) {
        cfl_queue_internal_system_event(handle, CFL_WEEK_EVENT,  CFL_EVENT_TYPE_PTR  ,false, &result);
    }
    if (result.changed_mask & CFL_CHANGED_DOY) {
        cfl_queue_internal_system_event(handle, CFL_YEAR_EVENT,  CFL_EVENT_TYPE_PTR  ,false, &result);
    }
    future_time_stamp = future_time_stamp + handle->delta_time;
}





static void cfl_init_test_system(cfl_runtime_handle_t* handle) {
    unsigned bitmap_size = (handle->flash_handle->kb_count + 31) / 32;
    handle->active_test_bitmap = cfl_perm_alloc_pointer(handle->perm, 
                                             bitmap_size * sizeof(uint32_t));
    memset(handle->active_test_bitmap, 0, bitmap_size * sizeof(uint32_t));
    handle->active_test_count = 0;
}




// Initialize (allocate bitmap based on kb_count)


// Add test
bool cfl_add_test_by_index(cfl_runtime_handle_t* handle, uint16_t kb_index) {
    if (kb_index >= handle->flash_handle->kb_count) return false;
    if (TEST_IS_ACTIVE(handle, kb_index)) return false;  // Already active
    
    const chaintree_kb_info_t* kb = &handle->flash_handle->kb_table[kb_index];
    
    cfl_engine_init_test(handle, kb->start_index, kb->node_count);
    
    TEST_ACTIVE_SET(handle, kb_index);
    handle->active_test_count++;
    return true;
}

// Delete test
bool cfl_delete_test_by_index(cfl_runtime_handle_t* handle, uint16_t kb_index) {
    if (kb_index >= handle->flash_handle->kb_count) return false;
    if (!TEST_IS_ACTIVE(handle, kb_index)) return false;
    
    //const chaintree_kb_info_t* kb = &handle->flash_handle->kb_table[kb_index];
    

    
    TEST_ACTIVE_CLR(handle, kb_index);
    
    handle->active_test_count--;
    return true;
}

// Dispatch system events
static void cfl_queue_internal_system_event(cfl_runtime_handle_t* handle, 
    unsigned event_id, unsigned event_type, bool malloc_flag, void *data) {
    
    const chaintree_handle_t* flash = handle->flash_handle;
    
    for(uint16_t kb_idx = 0; kb_idx < flash->kb_count; kb_idx++) {
        if (!TEST_IS_ACTIVE(handle, kb_idx)) continue;
        
        const chaintree_kb_info_t* kb = &flash->kb_table[kb_idx];
        
        if(cfl_engine_node_is_enabled(handle, kb->start_index)) {
            cfl_send_event(handle->event_queue, CFL_EVENT_PRIORITY_LOW, 
                kb->start_index, event_type, malloc_flag, event_id, data);
        }
    }
}