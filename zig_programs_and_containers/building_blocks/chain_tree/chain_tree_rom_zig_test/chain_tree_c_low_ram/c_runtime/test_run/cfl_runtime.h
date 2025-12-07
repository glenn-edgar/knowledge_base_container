#ifndef CFL_RUNTIME_H
#define CFL_RUNTIME_H

#ifdef __cplusplus
extern "C" {
#endif


#include "cfl_global_definitions.h"
#include "cfl_exception.h"
#include "cfl_common_functions.h"
#include "cfl_heap.h"
#include "cfl_heap_arena_allocate.h"
#include "cfl_perm.h"
#include "cfl_event_queue.h"
#include "CT_Tree_Walker.h"
#include "cfl_timer_system.h"

typedef struct CFL_RUNTIME_HANDLE {
    cfl_perm_t *perm;      /* Pointer to perm */
    cfl_heap_t *heap;      /* Pointer to heap */
    cfl_heap_arena_system_t* arena_system; /* Pointer to arena system */
    cfl_event_queue_t *event_queue; /* Pointer to event queue */
    uint8_t* flags; /* Pointer to flags */
    cfl_timer_handle_t timer_handle; /* Pointer to timer handle */
    double delta_time; /* Delta time */
} cfl_runtime_handle_t;


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


cfl_runtime_create_params_t* cfl_runtime_create_params_create(void);
void cfl_runtime_create_params_destroy(cfl_runtime_create_params_t* params);


cfl_runtime_handle_t* cfl_runtime_create(cfl_perm_t* perm, cfl_runtime_create_params_t* params);
void cfl_runtime_reset(cfl_runtime_handle_t* handle);

bool cfl_runtime_run(cfl_runtime_handle_t* handle);

#ifdef __cplusplus
}
#endif

#endif 