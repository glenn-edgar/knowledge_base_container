#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "json_node_decoder.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"


static void cfl_enable_auto_start_nodes(cfl_runtime_handle_t *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_enable_auto_start_nodes: node_index out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_enable_auto_start_nodes: link_start out of bounds");
            return;
        }
        if (link_start + link_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_enable_auto_start_nodes: link range exceeds table size");
            return;
        }
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_enable_auto_start_nodes: link_id out of bounds");
            return;
        }
        
        const chaintree_node_t *link_node = &runtime_handle->flash_handle->nodes[link_id];
        if ((link_node->link_count & AUTO_START_BIT) != 0) {
            cfl_enable_node(runtime_handle, link_id);
        }
    }
   
}

static void cfl_enable_all_nodes(cfl_runtime_handle_t *handle, uint16_t node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_enable_all_nodes: node_index out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_enable_all_nodes: link_start out of bounds");
            return;
        }
        if (link_start + link_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_enable_all_nodes: link range exceeds table size");
            return;
        }
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_enable_all_nodes: link_id out of bounds");
            return;
        }
        
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
    int32_t event_id;
    int32_t event_node_index;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_int32_runtime(runtime_handle, "node_dict.event_id", &event_id);
    json_extract_int32_runtime(runtime_handle, "node_dict.node_id", &event_node_index);
    cfl_send_json_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW, (unsigned)event_node_index, (unsigned)event_id, node->node_data_id);
    
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

void cfl_disable_nodes_one_shot_fn(void *handle, uint16_t node_index){
    uint32_t count;
    int32_t node_id;
    uint32_t array_nodes_record;

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    json_decoder_init_from_runtime(runtime_handle, node_index);
    const record_control_t *region = &runtime_handle->json_decoder_ctx->controls[runtime_handle->json_decoder_ctx->current_control_idx];
    
    
    // Navigate to the array
    json_navigate_path(runtime_handle->json_decoder_ctx, region->start_position, "node_dict.nodes", &array_nodes_record);
    
    // Get child count
    json_get_child_count(runtime_handle->json_decoder_ctx, array_nodes_record, &count);
    
    // Loop through array elements
    for (uint32_t i = 0; i < count; i++) {
        uint32_t element_record;
        json_get_array_child(runtime_handle->json_decoder_ctx, array_nodes_record, i, &element_record);
        
        json_get_int32(runtime_handle->json_decoder_ctx, element_record, &node_id);
        
        /* Validate node_id before terminating */
        if (node_id < 0 || (unsigned)node_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_disable_nodes_one_shot_fn: node_id out of bounds");
            continue;
        }
        
        cfl_terminate_node_tree(runtime_handle, (unsigned)node_id);
    }
    

}

void cfl_enable_nodes_one_shot_fn(void *handle, uint16_t node_index){
    uint32_t count;
    int32_t node_id;
    uint32_t array_nodes_record;

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    json_decoder_init_from_runtime(runtime_handle, node_index);
    const record_control_t *region = &runtime_handle->json_decoder_ctx->controls[runtime_handle->json_decoder_ctx->current_control_idx];
    
    
    // Navigate to the array
    json_navigate_path(runtime_handle->json_decoder_ctx, region->start_position, "node_dict.nodes", &array_nodes_record);
    
    // Get child count
    json_get_child_count(runtime_handle->json_decoder_ctx, array_nodes_record, &count);
    
    // Loop through array elements
    for (uint32_t i = 0; i < count; i++) {
        uint32_t element_record;
        json_get_array_child(runtime_handle->json_decoder_ctx, array_nodes_record, i, &element_record);
        
        json_get_int32(runtime_handle->json_decoder_ctx, element_record, &node_id);
        
        /* Validate node_id before enabling */
        if (node_id < 0 || (unsigned)node_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_enable_nodes_one_shot_fn: node_id out of bounds");
            continue;
        }
        
        cfl_enable_node(runtime_handle, (unsigned)node_id);
    }
    

}


void cfl_event_logger_init_one_shot_fn(void *handle, uint16_t node_index){
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    bool allocator_state = cfl_allocate_state(runtime_handle, node_index);
    cfl_event_logger_fn_data_t *ptr = cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_event_logger_fn_data_t));
    
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    
    const  json_decoder_ctx_t *ctx = runtime_handle->json_decoder_ctx;
    const record_control_t *region = &ctx->controls[ctx->current_control_idx];
    uint32_t root_record = region->start_position;
    
    // Extract the message string
    
    json_extract_string(ctx, root_record, "node_dict.message", (const char**) &ptr->event_logger_message);
    
    
    // Navigate to the events array
    uint32_t node_dict_record;
    json_find_object_child(ctx, root_record, "node_dict", &node_dict_record);
    
    uint32_t events_array_record;
    json_find_object_child(ctx, node_dict_record, "events", &events_array_record);
    
    // Get array count
    json_get_child_count(ctx, events_array_record, &ptr->event_count);
    
    /* Check for overflow in allocation size */
    size_t alloc_size = ptr->event_count * sizeof(int32_t);
    if (alloc_size > 65535) {
        EXCEPTION("cfl_event_logger_init_one_shot_fn: event_ids allocation size exceeds uint16_t limit");
        ptr->event_count = 0;
        ptr->event_ids = NULL;
        return;
    }
    if (allocator_state == false){
    
        ptr->event_ids = (int32_t *)cfl_additional_arena_alloc(runtime_handle, node_index, (uint16_t)alloc_size);
        if (!ptr->event_ids) {
            EXCEPTION("cfl_event_logger_init_one_shot_fn: failed to allocate event_ids");
            return;
        }else
        {
            if(ptr->event_ids == NULL){
                EXCEPTION("cfl_event_logger_init_one_shot_fn: event_ids is not NULL");
                return;
            }
        }
    }

    
    for (uint32_t i = 0; i < ptr->event_count; i++) {
        uint32_t element_record;
        json_get_array_child(ctx, events_array_record, i, &element_record);
        
        json_get_int32(ctx, element_record, &ptr->event_ids[i]);
    }
    
}


void cfl_event_logger_term_one_shot_fn(void *handle, uint16_t node_index){
    (void)handle;
    (void)node_index;
    
}




/* Extract node_id, new_state, and sync_event_id (which can be null or integer) */
void json_extract_new_state_data(
    cfl_runtime_handle_t *runtime,
    uint16_t node_index,
    int32_t *out_node_id,
    const char **out_new_state,
    bool *out_sync_flag,
    int32_t *out_sync_event_id)
{
    if (!runtime || !out_node_id || !out_new_state || !out_sync_flag || !out_sync_event_id) {
        EXCEPTION("json_extract_node_transition_data: NULL parameter");
    }
    
    json_decoder_init_from_runtime(runtime, node_index);
    
    const  json_decoder_ctx_t *ctx = runtime->json_decoder_ctx;
    const record_control_t *region = &ctx->controls[ctx->current_control_idx];
    uint32_t root_record = region->start_position;
    
    // Navigate to node_dict
    uint32_t node_dict_record;
    json_find_object_child(ctx, root_record, "node_dict", &node_dict_record);
    
    // Extract node_id
    json_extract_int32(ctx, node_dict_record, "node_id", out_node_id);
    
    // Extract new_state (pointer to string table)
    json_extract_string(ctx, node_dict_record, "new_state", out_new_state);
    
    // Extract sync_event_id - check if it's null or integer
    uint32_t sync_event_id_record;
    json_find_object_child(ctx, node_dict_record, "sync_event_id", &sync_event_id_record);
    
    if (json_is_null(ctx, sync_event_id_record)) {
        *out_sync_flag = false;
        *out_sync_event_id = 0;  // Default value when null
        
    } else {
        *out_sync_flag = true;
        json_get_int32(ctx, sync_event_id_record, out_sync_event_id);
        
    }
}


void cfl_change_state_one_shot_fn(void *handle, uint16_t node_index){
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    int32_t sm_node_id;
    const char *new_state;
    bool sync_flag;
    int32_t sync_event_id;
    
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_new_state_data(runtime_handle, node_index, &sm_node_id, &new_state, &sync_flag, &sync_event_id);
    
    cfl_change_state(runtime_handle, node_index, sm_node_id, new_state, sync_flag, sync_event_id);
}





void cfl_state_machine_init_one_shot_fn(void *handle, uint16_t node_index){
    uint32_t node_count;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_state_machine_init_one_shot_fn: node_index out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    node_count = node->link_count & LINK_COUNT_MASK;
    
    /* Validate link_start and node_count */
    if (node_count > 0) {
        if (node->link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_init_one_shot_fn: link_start out of bounds");
            return;
        }
        if (node->link_start + node_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_init_one_shot_fn: link range exceeds table size");
            return;
        }
    }
    bool allocator_state = cfl_allocate_state(runtime_handle, node_index);
    cfl_state_machine_column_data_t *ptr = cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_state_machine_column_data_t));
    
    ptr->sync_event_id_valid = false;
    ptr->sync_event_id = 0;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    
    const  json_decoder_ctx_t *ctx = runtime_handle->json_decoder_ctx;
    const record_control_t *region = &ctx->controls[ctx->current_control_idx];
    uint32_t root_record = region->start_position;
    
    // Extract initial_state_number
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.initial_state_number", &ptr->current_state);
    ptr->new_state = ptr->current_state;
    
    /* Validate initial_state_number */
    if (ptr->current_state < 0 || (unsigned)ptr->current_state >= node_count) {
        EXCEPTION("cfl_state_machine_init_one_shot_fn: initial_state_number out of range");
        return;
    }
    
    /* Check for overflow in state_names allocation */
    size_t alloc_size = node_count * sizeof(const char *);
    if (alloc_size > 65535) {
        EXCEPTION("cfl_state_machine_init_one_shot_fn: state_names allocation size exceeds uint16_t limit");
        return;
    }
    if(allocator_state == false){
        ptr->state_names = (const char **)cfl_arena_system_alloc(runtime_handle->arena_system, node_index, (uint16_t)alloc_size);
        if (!ptr->state_names) {
            EXCEPTION("cfl_state_machine_init_one_shot_fn: failed to allocate state_names");
            return;
        }
        else
        {
            if(ptr->state_names != NULL){
                EXCEPTION("cfl_state_machine_init_one_shot_fn: state_names is not NULL");
                return;
            }
        }
    }
    

    
    // Navigate to node_dict.column_data
    uint32_t node_dict_record;
    json_find_object_child(ctx, root_record, "node_dict", &node_dict_record);
    
    uint32_t column_data_record;
    json_find_object_child(ctx, node_dict_record, "column_data", &column_data_record);
    
    // Navigate to state_names array
    uint32_t state_names_array;
    json_find_object_child(ctx, column_data_record, "state_names", &state_names_array);
    
    // Extract each state name pointer from the string table
    for (uint32_t i = 0; i < node_count; i++) {
        uint32_t element_record;
        json_get_array_child(ctx, state_names_array, i, &element_record);
        json_get_string_value(ctx, element_record, &ptr->state_names[i]);
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    
    // Terminate all state nodes first
    for (uint32_t i = 0; i < node_count; i++) {
        uint16_t link_id = link_table[node->link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_state_machine_init_one_shot_fn: link_id out of bounds");
            continue;
        }
        
        cfl_terminate_node_tree(runtime_handle, link_id);
    }
    
    /* Validate link_id for initial state before enabling */
    uint16_t initial_link_id = link_table[node->link_start + ptr->current_state];
    if (initial_link_id >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_state_machine_init_one_shot_fn: initial state link_id out of bounds");
        return;
    }
    
    cfl_enable_node(runtime_handle, initial_link_id);
}



void cfl_state_machine_term_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    /* Validate node_index */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_state_machine_term_one_shot_fn: node_index out of bounds");
        return;
    }
    
    
    
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    uint16_t link_start = node->link_start;
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_term_one_shot_fn: link_start out of bounds");
            return;
        }
        if (link_start + link_count > runtime_handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_state_machine_term_one_shot_fn: link range exceeds table size");
            return;
        }
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    
    /* BUG FIX: Terminate link_table[link_start + i], not link_start + i */
    for (uint32_t i = 0; i < link_count; i++) {
        uint16_t link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_state_machine_term_one_shot_fn: link_id out of bounds");
            continue;
        }
        
        cfl_terminate_node_tree(runtime_handle, link_id);
    }    
}

void cfl_terminate_state_machine_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    int32_t sm_node_id;
    json_extract_int32_runtime(runtime_handle, "node_dict.sm_node_id", &sm_node_id);

    cfl_terminate_state_machine(runtime_handle, node_index, sm_node_id);


}


void cfl_fork_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    cfl_enable_all_nodes(runtime_handle, node_index);
}

void cfl_fork_term_one_shot_fn(void *handle, uint16_t node_index){
    (void)handle;
    (void)node_index;
}






void cfl_sequence_pass_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    bool allocator_state = cfl_allocate_state(runtime_handle, node_index);
    
    sequence_start_fn_data_t *ptr = (sequence_start_fn_data_t*)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(sequence_start_fn_data_t));

    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    ptr->sequence_number = link_count;

    ptr->current_sequence_index = 0;
    ptr->recorded_sequence_index = -1;
    if (allocator_state == false){
      
      ptr->sequence_result_data_array = (sequence_result_data_t *)cfl_additional_arena_alloc(runtime_handle,node_index,
            (uint16_t)(sizeof(sequence_result_data_t) * ptr->sequence_number));
    } else{
       if (ptr->sequence_result_data_array == NULL){
        EXCEPTION("cfl_sequence_pass_init_one_shot_fn: failed to allocate sequence_result_data_array");
        return;
       }
    }
    
    for (int32_t i = 0; i < link_count; i++) {
        uint16_t link_id = link_table[node->link_start + i];
        if (link_id >= runtime_handle->flash_handle->node_count) {
            EXCEPTION("cfl_sequence_pass_init_one_shot_fn: link_id out of bounds");
            continue;
        }
        
        ptr->sequence_result_data_array[i].sequence_result = false;
        ptr->sequence_result_data_array[i].node_index = link_id;
        
        if (i == 0) {
            cfl_enable_node(runtime_handle, link_id);
        }else
        {
            cfl_terminate_node_tree(runtime_handle, link_id);
        }
    }
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.finalize_function_id", &ptr->finalize_function_id);
   

}

void cfl_sequence_pass_term_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    sequence_start_fn_data_t *ptr = (sequence_start_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (!ptr) {
        EXCEPTION("cfl_sequence_pass_term_one_shot_fn: failed to get node pointer");
        return;
    }
    printf("ptr->finalize_function_id: %d\n", ptr->finalize_function_id);
    one_shot_function_t finalize_function = runtime_handle->flash_handle->one_shot_functions[ptr->finalize_function_id];
    finalize_function(runtime_handle, node_index);
    printf("finalize_function(runtime_handle, node_index) returned\n");
    exit(0);
}

void cfl_sequence_fail_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_sequence_pass_init_one_shot_fn(handle, node_index);
}
void cfl_sequence_fail_term_one_shot_fn(void *handle, uint16_t node_index){
    cfl_sequence_pass_term_one_shot_fn(handle, node_index);
}



void cfl_sequence_start_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    sequence_aggregate_data_t *ptr = (sequence_aggregate_data_t*)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(sequence_aggregate_data_t));
    ptr->auxiliary_data = NULL;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.finalize_function_id", &ptr->finalize_function_id);
    int32_t initialize_function_id;
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.initialize_function_id", &initialize_function_id);
    one_shot_function_t initialize_function = runtime_handle->flash_handle->one_shot_functions[initialize_function_id];
    initialize_function(runtime_handle, node_index);
    cfl_enable_all_nodes(runtime_handle, node_index);


}

void cfl_sequence_start_term_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("sequence_start_term_one_shot_fn\n");
    exit(0);
}


void cfl_join_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    int32_t *ptr = (int32_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(int32_t));
   
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    json_extract_int32_runtime(runtime_handle, "node_dict.parent_node_name", ptr);
    
}

void cfl_join_sequence_element_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_join_init_one_shot_fn(handle, node_index);
}

void cfl_join_sequence_element_term_one_shot_fn(void *handle, uint16_t node_index){
    (void)handle;
    (void)node_index;
}

void cfl_join_term_one_shot_fn(void *handle, uint16_t node_index){
    (void)handle;
    (void)node_index;
}


void cfl_mark_sequence_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    int32_t result;
    int32_t parent_node_name;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_int32_runtime(runtime_handle, "node_dict.result", &result);
    json_extract_int32_runtime(runtime_handle, "node_dict.parent_node_name", &parent_node_name);
    sequence_start_fn_data_t *ptr = (sequence_start_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, parent_node_name);
    if (!ptr) {
        EXCEPTION("cfl_mark_sequence_one_shot_fn: failed to get node pointer");
        return;
    }
    if (result == 1) {
        ptr->sequence_result_data_array[ptr->recorded_sequence_index].sequence_result = true;
    } else {
        ptr->sequence_result_data_array[ptr->recorded_sequence_index].sequence_result = false;
    }
    ptr->recorded_sequence_index = ptr->current_sequence_index;

} 