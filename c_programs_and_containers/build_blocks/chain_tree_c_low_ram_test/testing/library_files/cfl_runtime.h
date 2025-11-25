#ifndef CFL_RUNTIME_H
#define CFL_RUNTIME_H

#ifdef __cplusplus
extern "C" {
#endif

#include "cfl_global_definitions.h"
#include "cfl_exception.h"
#include "cfl_heap.h"
#include "cfl_heap_arena_allocate.h"
#include "cfl_perm.h"
#include "cfl_event_queue.h"
#include "CT_Tree_Walker.h"
#include "cfl_timer_system.h"
#include "chaintree_support.h"
#include "cfl_engine.h"

// Bitmap-based test tracking macros
#define TEST_ACTIVE_SET(handle, kb_idx) \
    ((handle)->active_test_bitmap[(kb_idx)/32] |= (1u << ((kb_idx) % 32)))
    
#define TEST_ACTIVE_CLR(handle, kb_idx) \
    ((handle)->active_test_bitmap[(kb_idx)/32] &= ~(1u << ((kb_idx) % 32)))
    
#define TEST_IS_ACTIVE(handle, kb_idx) \
    (((handle)->active_test_bitmap[(kb_idx)/32] & (1u << ((kb_idx) % 32))) != 0)

typedef struct CFL_RUNTIME_CREATE_PARAMS {
    cfl_perm_t* perm;
    char* perm_buffer;
    uint16_t perm_buffer_size;
    uint16_t heap_size;
    uint16_t max_allocator_count;
    uint16_t total_node_count;
    uint16_t allocator_0_size;
    uint16_t event_queue_high_priority_size;
    uint16_t event_queue_low_priority_size;
    double delta_time;
} cfl_runtime_create_params_t;

// Runtime creation and management
cfl_runtime_create_params_t* cfl_runtime_create_params_create(void);
void cfl_runtime_create_params_destroy(cfl_runtime_create_params_t* params);

cfl_runtime_handle_t* cfl_runtime_create(cfl_perm_t* perm, 
                                         cfl_runtime_create_params_t* params, 
                                         const chaintree_handle_t* flash_handle);
void cfl_runtime_reset(cfl_runtime_handle_t* handle);
bool cfl_runtime_run(cfl_runtime_handle_t* handle);

// Test management using bitmap system
bool cfl_add_test_by_index(cfl_runtime_handle_t* handle, uint16_t kb_index);
bool cfl_delete_test_by_index(cfl_runtime_handle_t* handle, uint16_t kb_index);

uint16_t cfl_calculate_arrena_number(const chaintree_handle_t* flash_handle);

#ifdef __cplusplus
}
#endif

#endif  // CFL_RUNTIME_H