#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"
#include "json_node_decoder.h"




void cfl_set_exception_step_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_set_exception_step_one_shot_fn\n");
    exit(0);
}
void cfl_heartbeat_event_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_heartbeat_event_one_shot_fn node_index: %d\n", node_index);
    exit(0);
}
void cfl_raise_exception_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_raise_exception_one_shot_fn node_index: %d\n", node_index);
    exit(0);
}
void cfl_turn_heartbeat_off_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_turn_heartbeat_off_one_shot_fn node_index: %d\n", node_index);
    exit(0);
}
void   cfl_turn_heartbeat_on_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_turn_heartbeat_on_one_shot_fn node_index: %d\n", node_index);
    exit(0);
}

void cfl_recovery_init_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_recovery_init_one_shot_fn\n");
    exit(0);
}

void cfl_recovery_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_recovery_term_one_shot_fn\n");
    exit(0);
}

unsigned cfl_recovery_main_main_fn(void *handle, unsigned node_index, unsigned bool_function_id, unsigned event_type, unsigned event_id, void *event_data){
    (void)handle;
    (void)node_index;
    (void)bool_function_id;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("cfl_recovery_main_main_fn\n");
    exit(0);
    return CFL_CONTINUE;
}



unsigned cfl_exception_catch_all_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, 
    unsigned event_type, unsigned event_id, void *event_data){
    (void)bool_function_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    (void)handle;
    (void)node_index;
    printf("cfl_exception_catch_all_main_main_fn node_index: %d\n", node_index);
    exit(0);
    return CFL_CONTINUE;
}

void cfl_catch_all_exception_init_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_catch_all_exception_init_one_shot_fn\n");
    exit(0);
}

void cfl_catch_all_exception_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    printf("cfl_catch_all_exception_term_one_shot_fn\n");
    exit(0);
}



unsigned cfl_exception_catch_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, 
    unsigned event_type, unsigned event_id, void *event_data){
    (void)bool_function_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    (void)handle;
    (void)node_index;
    printf("cfl_exception_catch_main_main_fn node_index: %d\n", node_index);
    exit(0);
    return CFL_CONTINUE;
}


void cfl_exception_catch_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_exception_catch_init_one_shot_fn node_index: %d\n", node_index);
    exit(0);
}
void cfl_exception_catch_term_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_exception_catch_term_one_shot_fn node_index: %d\n", node_index);
    exit(0);
}
