#include <stdlib.h>
#include <stdio.h>

#include "cfl_runtime.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"
#include "cfl_sm_functions.h"
#include "cfl_engine.h"

typedef struct {
    int32_t event_id;
} sm_event_filtering_init_fn_data_t;

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

/* =====================================================================
 * Register application-specific main functions with binary image loader.
 * Warns if a function is not found in the image.
 * ===================================================================== */

 #include "cfl_image_loader.h"
extern unsigned avro_verify_packet_main_fn(void*, unsigned, unsigned, unsigned, unsigned, void*);
 void cfl_register_user_main_functions(cfl_image_loader_t *img)
 {
     int missing = 0;
     int rc;
 
     rc = cfl_image_register_main(img, "sm_event_filtering_main_main", sm_event_filtering_main_main_fn);
     if (rc < 0) { fprintf(stderr, "  WARN: app main not found: sm_event_filtering_main_main\n"); missing++; }
 
     rc = cfl_image_register_main(img, "avro_verify_packet_main", avro_verify_packet_main_fn);
     if (rc < 0) { fprintf(stderr, "  WARN: app main not found: avro_verify_packet_main\n"); missing++; }
 
 }