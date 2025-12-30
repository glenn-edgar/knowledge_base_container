#include <stdlib.h>
#include <stdio.h>
#include "user_function_headers.h"
#include "cfl_runtime.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"
#include "cfl_engine.h"


unsigned sm_event_filtering_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data){
    (void)bool_function_index;
    (void)event_type;
    (void)event_data;

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    sm_event_filtering_init_fn_data_t *ptr = (sm_event_filtering_init_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime->arena_system, node_index);
    if (ptr == NULL) {
        EXCEPTION("sm_event_filtering_main_main_fn: ptr is NULL");
    }
    if(event_id == (unsigned) ptr->event_id){
        
        return CFL_CONTINUE;
    }
    return CFL_CONTINUE;

}