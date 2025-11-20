#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "json_node_decoder.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"


static void cfl_enable_auto_start_nodes(cfl_runtime_handle_t *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    for (unsigned i = 0; i < (link_count&LINK_COUNT_MASK); i++) {
        unsigned int link_id = link_table[link_start + i];
        const chaintree_node_t *link_node = &runtime_handle->flash_handle->nodes[link_id];
        if ((link_node->link_count & AUTO_START_BIT) != 0) {
    
            cfl_enable_node(runtime_handle, link_id);
        }
    }
   
}

static void cfl_enable_all_nodes(cfl_runtime_handle_t *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    for (unsigned i = 0; i < link_count; i++) {
            unsigned int link_id = link_table[link_start + i];
            cfl_enable_node(runtime_handle, link_id);

    }
   
}

void cfl_null_one_shot_fn(void *handle, uint16_t node_index){
 (void)handle;
 (void)node_index;
}

void cfl_column_init_one_shot_fn(void *handle, uint16_t node_index){

    cfl_enable_all_nodes(handle, node_index);

 }

void cfl_column_term_one_shot_fn(void *handle, uint16_t node_index){

    (void)handle;
    (void)node_index;
   
    ; //do nothing
    }


void cfl_gate_node_init_one_shot_fn(void *handle, uint16_t node_index){
    
    cfl_enable_auto_start_nodes(handle, node_index);
}

void cfl_gate_node_term_one_shot_fn(void *handle, uint16_t node_index){

 
    (void)handle;
    (void)node_index;
   
    
    }

void cfl_log_message_one_shot_fn(void *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime = (cfl_runtime_handle_t *)handle;
    const char *message;
    double timestamp;

    timestamp = cfl_timer_get_timestamp(runtime->timer_handle);
    
    // Step 1: Initialize decoder for this node's data
    json_decoder_init_from_runtime(runtime, node_index);
    
    // Step 2: Extract the value using path notation
    json_extract_string_runtime(runtime, "node_dict.message", &message);
    
    // Step 3: Use the value (state now points to "open")
    // No need to free - string points into flash memory
    
    printf("Timestamp: %f, Node Index: %d, Message: %s  \n", timestamp, node_index, message );
    
}

void cfl_send_named_event_one_shot_fn(void *handle, uint16_t node_index){

 
    (void)handle;
    (void)node_index;
   
    printf("cfl_send_named_event_one_shot_fn\n");
    exit(0);
    }





void cfl_verify_init_one_shot_fn(void *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_verify_fn_data_t *ptr = (cfl_verify_fn_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_verify_fn_data_t));
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_bool_runtime(runtime_handle, "node_dict.reset_flag", &ptr->reset_flag);
    json_extract_int32_runtime(runtime_handle, "node_dict.error_function_id", (int32_t*)&ptr->error_function);
    json_extract_string_runtime(runtime_handle, "node_dict.error_data.failure_data",(const char**) &ptr->failure_data);
    ptr->auxiliary_data = NULL;

}



void cfl_verify_term_one_shot_fn(void *handle, uint16_t node_index){
   (void)handle;
   (void)node_index;
    
}

void cfl_wait_init_one_shot_fn(void *handle, uint16_t node_index){
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_wait_fn_data_t *ptr = (cfl_wait_fn_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index,
         sizeof(cfl_wait_fn_data_t));
    
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_bool_runtime(runtime_handle, "node_dict.reset_flag", &ptr->reset_flag);
    json_extract_int32_runtime(runtime_handle, "node_dict.timeout", (int32_t*)&ptr->timeout);
    json_extract_int32_runtime(runtime_handle, "node_dict.time_out_event", (int32_t*)&ptr->time_out_event);
    json_extract_string_runtime(runtime_handle, "node_dict.error_data.error_message",(const char**) &ptr->error_message);
    json_extract_int32_runtime(runtime_handle, "node_dict.error_function_id", (int32_t*)&ptr->error_function);
    ptr->event_count = 0;
    ptr->auxiliary_data = NULL;
}


void cfl_wait_term_one_shot_fn(void *handle, uint16_t node_index){


    (void)handle;
    (void)node_index;


}

void cfl_wait_time_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    float time_delay;

    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_float32_runtime(runtime_handle, "node_dict.time_delay", &time_delay);
    cfl_wait_time_out_data_t *ptr = cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_wait_time_out_data_t));
    ptr->wait_time_out = (double)time_delay+cfl_timer_get_timestamp(runtime_handle->timer_handle);
   
}
