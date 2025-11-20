#ifndef CFL_ENGINE_H
#define CFL_ENGINE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "cfl_perm.h"
#include "cfl_heap.h"
#include "cfl_heap_arena_allocate.h"
#include "cfl_event_queue.h"
#include "cfl_timer_system.h"
#include "cfl_event_queue.h"
#include "chaintree_support.h"
#include "CT_Tree_Walker.h"



typedef enum {
    CFL_INIT_EVENT = 0,
    CFL_TERMINATE_EVENT = 1,
    CFL_START_TESTS = 2,
    CFL_TERMINATE_TESTS = 3,
    CFL_TIMER_EVENT = 4,
    CFL_SECOND_EVENT = 5,
    CFL_MINUTE_EVENT = 6,
    CFL_HOUR_EVENT = 7,
    CFL_DAY_EVENT = 8,
    CFL_WEEK_EVENT = 9,
    CFL_MONTH_EVENT = 10,
    CFL_YEAR_EVENT = 11,
} cfl_engine_event_t;



/*
  Chain Tree Main Function Return Codes

*/
#define CFL_CONTINUE 0
#define CFL_HALT 1
#define CFL_TERMINATE 2
#define CFL_RESET 3
#define CFL_DISABLE 4
#define CFL_SKIP_CONTINUE 5
#define CFL_TERMINATE_SYSTEM 6



typedef struct cfl_test_control_t {
    unsigned start_index;
    unsigned node_count;
} cfl_test_control_t;


 
 typedef struct {
    const json_record_t *records;      // From flash_handle->node_data_records
    uint32_t records_count;            // From flash_handle->node_data_records_count
    const char *strings;               // From flash_handle->node_data_strings
    uint32_t strings_size;             // From flash_handle->node_data_strings_size
    const record_control_t *controls;  // From flash_handle->node_data_controls
    uint32_t controls_count;           // From flash_handle->node_data_controls_count
    
    // Current operation state
    uint32_t current_control_idx;      // Active control region index
    int error_code;                    // Last error code
} json_decoder_ctx_t;

typedef struct CFL_RUNTIME_HANDLE cfl_runtime_handle_t;
struct CFL_RUNTIME_HANDLE {
    volatile cfl_perm_t *perm;      /* Pointer to perm */
    volatile cfl_heap_t *heap;      /* Pointer to heap */
    volatile cfl_heap_arena_system_t* arena_system; /* Pointer to arena system */
    volatile cfl_event_queue_t *event_queue; /* Pointer to event queue */
    volatile uint8_t* flags; /* Pointer to flags */
    volatile cfl_timer_handle_t timer_handle; /* Pointer to timer handle */
    volatile double delta_time; /* Delta time */
    volatile unsigned test_count; /* Test count */
    volatile uint32_t *active_test_bitmap;    // One bit per kb_table entry
    volatile unsigned active_test_count;   
    volatile cfl_test_control_t *test_controls;
    volatile CT_TreeWalker* walker; /* Pointer to walker */
    volatile CFL_EVENT_DATA_T *event_data_ptr; /* Pointer to event data */
    bool cfl_engine_flag;
    unsigned cfl_node_execution_count;
    unsigned max_level;
    volatile CT_StackEntry*  stack;
    volatile CT_StackEntry*  nested_stack;
    volatile json_decoder_ctx_t *json_decoder_ctx; /* Pointer to json decoder context */
    volatile uint8_t* backup_flags; /* Pointer to backup flags */
    volatile CT_WalkerContext* walker_context_ptr; /* Pointer to walker context */
    volatile const chaintree_handle_t* flash_handle; /* Pointer to flash hanle */
/* Pointer to engine handle */
};


void cfl_engine_create(cfl_runtime_handle_t *handle);

void cfl_engine_init(cfl_runtime_handle_t *handle);

void cfl_engine_init_test(cfl_runtime_handle_t *handle,unsigned start_node, unsigned node_count);

bool cfl_engine_node_is_enabled(cfl_runtime_handle_t *handle, unsigned node_index);

bool cfl_engine_node_is_initialized(cfl_runtime_handle_t *handle, unsigned node_index);

bool cfl_execute_event(cfl_runtime_handle_t *handle);
void cfl_enable_node(cfl_runtime_handle_t *handle, unsigned node_index);

#ifdef __cplusplus
}
#endif

#endif 