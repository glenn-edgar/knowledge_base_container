#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include "cfl_common_function_headers.h"
#include "cfl_exception_support.h"
#include "json_node_decoder.h"




static unsigned find_parent_node_exception(cfl_runtime_handle_t *runtime_handle, uint16_t node_id, bool terminate_flag){
    // find parent node id
    bool found_flag = false;
    uint16_t parent_node_id = node_id;
    uint16_t search_node_id = node_id;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[search_node_id];
    search_node_id = node->parent_index;
    parent_node_id = search_node_id;
    while (!found_flag) {
        
       
        node = &runtime_handle->flash_handle->nodes[search_node_id];
        parent_node_id = node->parent_index;
        
        if (parent_node_id == 0xFFFF) {
            if (terminate_flag) {
                cfl_terminate_node_tree(runtime_handle,search_node_id);
            }
            return 0xFFFF;
        }
       
        if( node->main_function_index == runtime_handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_EXCEPTION_CATCH_ALL_MAIN]) {
            
            return search_node_id;
        }
        if( node->main_function_index == runtime_handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_EXCEPTION_CATCH_MAIN]) {
            
            return search_node_id;
        }
        if( node->main_function_index == runtime_handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_CONTROLLED_NODE_MAIN]) {
            
            return search_node_id;
        }
        else{
            search_node_id = parent_node_id;
        }
    }
    return 0xFFFF;   
}


void cfl_raise_json_exception_event(cfl_runtime_handle_t *runtime_handle, uint16_t node_id, uint16_t parent_node_id )
{
  // find parent node id
  if (parent_node_id == 0xFFFF) {
     parent_node_id = find_parent_node_exception(runtime_handle, node_id, true);
    
  }
  
  cfl_send_node_id_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_HIGH, parent_node_id, CFL_RAISE_EXCEPTION_EVENT, node_id);

}



void cfl_forward_exception_event(cfl_runtime_handle_t *runtime_handle, unsigned node_index , unsigned parent_node_id , 
               uint16_t record_index ){
    // find parent node id
    if (parent_node_id == 0xFFFF) {
        parent_node_id = find_parent_node_exception(runtime_handle, node_index, true);
        if (parent_node_id == 0xFFFF) {
            EXCEPTION("cfl_forward_exception_event: parent_node_id is 0xFFFF");
        }
    }
    cfl_send_node_id_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_HIGH, parent_node_id, CFL_RAISE_EXCEPTION_EVENT,record_index);
    
}

void cfl_set_exception_step_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    int32_t step_count;
    json_extract_int32_runtime(runtime_handle, "node_dict.step", &step_count);
    uint16_t parent_node_id = find_parent_node_exception(runtime_handle, node_index,  false);
    if (parent_node_id == 0xFFFF) {
        EXCEPTION("cfl_set_exception_step_one_shot_fn: parent_node_id is 0xFFFF");
    }
    
    cfl_send_unsigned_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_HIGH, parent_node_id, 
        CFL_SET_EXCEPTION_STEP_EVENT, step_count);
}

void cfl_raise_exception_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_raise_json_exception_event(runtime_handle, node_index, 0xFFFF);
   

}

void cfl_heartbeat_event_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    uint16_t node_id = find_parent_node_exception(runtime_handle, node_index,  false);
    if (node_id == 0xFFFF) {
        EXCEPTION("cfl_heartbeat_event_one_shot_fn: node_id is 0xFFFF");
    }
    cfl_send_unsigned_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW, node_id, 
        CFL_HEARTBEAT_EVENT, 0);
}
void cfl_turn_heartbeat_off_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    uint16_t node_id = find_parent_node_exception(runtime_handle, node_index,  false);
    if (node_id == 0xFFFF) {
        EXCEPTION("cfl_heartbeat_event_one_shot_fn: node_id is 0xFFFF");
    }
    cfl_send_unsigned_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW, node_id, 
        CFL_TURN_HEARTBEAT_OFF_EVENT, 0);
}

void   cfl_turn_heartbeat_on_one_shot_fn(void *handle, uint16_t node_index){
    int32_t time_out;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_extract_int32_runtime(runtime_handle, "node_dict.time_out", &time_out);
    uint16_t node_id = find_parent_node_exception(runtime_handle, node_index,  false);
    if (node_id == 0xFFFF) {
        EXCEPTION("cfl_turn_heartbeat_on_one_shot_fn: node_id is 0xFFFF");
    }
    cfl_send_unsigned_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_LOW, node_id, 
        CFL_TURN_HEARTBEAT_ON_EVENT, time_out);

}

void cfl_recovery_init_one_shot_fn(void *handle, unsigned node_index){

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t parent_node_id = node->parent_index;
    if (parent_node_id == 0xFFFF) {
        EXCEPTION("cfl_recovery_init_one_shot_fn: parent_node_id is 0xFFFF");
    }
    cfl_exception_support_data_t *exception_support_data = 
         (cfl_exception_support_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, parent_node_id);
    
    if (exception_support_data == NULL) {
        EXCEPTION("cfl_recovery_init_one_shot_fn: exception_support_data is NULL");
    }

    
    
    json_decoder_init_from_runtime(runtime_handle, node_index);
    

    int32_t temp_int32;
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.max_steps", &temp_int32);
    exception_support_data->max_steps = (uint8_t)temp_int32;
    
    if (exception_support_data->max_steps < exception_support_data->step_count) {
        EXCEPTION("cfl_recovery_init_one_shot_fn: max_steps is less than step_count");
    }
    if (node->link_count < exception_support_data->max_steps+1) {
        EXCEPTION("cfl_recovery_init_one_shot_fn: link_number is less than max_steps+2");
    }
    
    exception_support_data->recovery_step_count = exception_support_data->max_steps - exception_support_data->step_count;
    
    // Initialize new state machine fields
    exception_support_data->current_step = exception_support_data->recovery_step_count;
    exception_support_data->recovery_state = CFL_RECOVERY_SEQ_EVAL;
    
}

void cfl_recovery_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
}


unsigned cfl_recovery_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, 
         unsigned event_type, unsigned event_id, void *event_data) {
    
    (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t parent_node_id = node->parent_index;
    
    if (event_id != CFL_TIMER_EVENT) {
        return CFL_CONTINUE;
    }
    
    cfl_exception_support_data_t *exception_support_data = 
        (cfl_exception_support_data_t *)cfl_heap_arena_get_node_ptr(
            runtime_handle->arena_system, parent_node_id);
    if (exception_support_data == NULL) {
        EXCEPTION("cfl_recovery_main_main_fn: exception_support_data is NULL");
    }
    
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    boolean_function_t boolean_function = 
        runtime_handle->flash_handle->boolean_functions[bool_function_index];
    
    switch (exception_support_data->recovery_state) {
        
        case CFL_RECOVERY_SEQ_EVAL: {
            while (exception_support_data->current_step <= exception_support_data->max_steps ) {
                uint16_t step_node_id = link_table[link_start + exception_support_data->current_step];
                
                bool filter_matched = boolean_function(
                    runtime_handle, node_index, event_type, CFL_RECOVERY_CHECK_EVENT, NULL);
                
                if (filter_matched) {
                    cfl_enable_node(runtime_handle, step_node_id);
                    exception_support_data->recovery_state = CFL_RECOVERY_SEQ_WAIT;
                    return CFL_CONTINUE;
                }
                
                exception_support_data->current_step++;
            }
        }
        /* FALLTHROUGH */
        
        case CFL_RECOVERY_PARALLEL_ENABLE: {
            uint16_t sequential_steps = exception_support_data->max_steps + 1;
            uint16_t parallel_count = link_count - sequential_steps;
            
            if (parallel_count > 0) {
                for (uint16_t i = sequential_steps; i < link_count; i++) {
                    cfl_enable_node(runtime_handle, link_table[link_start + i]);
                }
                exception_support_data->recovery_state = CFL_RECOVERY_PARALLEL_WAIT;
                return CFL_CONTINUE;
            }
            return CFL_DISABLE;
        }
            
        case CFL_RECOVERY_SEQ_WAIT:
            if (cfl_verify_active_children(runtime_handle, node_index) == CFL_DISABLE) {
                exception_support_data->current_step++;
                if (exception_support_data->current_step > exception_support_data->max_steps + 1) {
                    exception_support_data->recovery_state = CFL_RECOVERY_PARALLEL_ENABLE;
                } else {
                    exception_support_data->recovery_state = CFL_RECOVERY_SEQ_EVAL;
                }
            }
            return CFL_CONTINUE;
            
        case CFL_RECOVERY_PARALLEL_WAIT:
            return cfl_verify_active_children(runtime_handle, node_index);
            
        default:
            return CFL_DISABLE;
    }
}




unsigned cfl_exception_catch_all_main_main_fn(void *handle, unsigned bool_function_index, unsigned node_index, 
    unsigned event_type, unsigned event_id, void *event_data){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    

    switch(event_id)
    {
        case CFL_RAISE_EXCEPTION_EVENT:
            
            boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
        
            bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
            if (result == true) {
                
                return CFL_CONTINUE;
            }
            
            return CFL_DISABLE;
            break;
        default:
            break;
    }
    return cfl_verify_active_children(runtime_handle, node_index);
    
}

void cfl_catch_all_exception_init_one_shot_fn(void *handle, unsigned node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_enable_all_nodes(runtime_handle, node_index);
    
}

void cfl_catch_all_exception_term_one_shot_fn(void *handle, unsigned node_index){
    (void)handle;
    (void)node_index;
    
}



unsigned cfl_exception_catch_main_main_fn(
    void *handle, 
    unsigned bool_function_index, 
    unsigned node_index, 
    unsigned event_type, 
    unsigned event_id, 
    void *event_data)
{
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_exception_support_data_t *exception_support_data = 
        (cfl_exception_support_data_t *)cfl_heap_arena_get_node_ptr(
            runtime_handle->arena_system, node_index);

    switch (event_id) {
        
        case CFL_RAISE_EXCEPTION_EVENT: {
            if (event_type != CFL_EVENT_TYPE_JSON_RECORD) {
                EXCEPTION("cfl_exception_catch_main_fn: event_type is not CFL_EVENT_TYPE_JSON_RECORD");
            }
            
            uint16_t original_node_id = (uint32_t)((size_t)event_data);
            exception_support_data->original_node_id = original_node_id;
            exception_support_data->exception_type = CFL_EXCEPTION_RAISED;
            
            if (exception_support_data->logging_function_id != 0) {
                one_shot_function_t one_shot_function = 
                    runtime_handle->flash_handle->one_shot_functions[exception_support_data->logging_function_id];
                one_shot_function(runtime_handle, node_index);
            }
           
            boolean_function_t boolean_function = 
                runtime_handle->flash_handle->boolean_functions[bool_function_index];
            bool filter_matched = boolean_function(
                runtime_handle, node_index, event_type, event_id, event_data);
            
            // Filter matched - forward exception to parent
            if (filter_matched) {
                cfl_forward_exception_event(runtime_handle, node_index, 
                    exception_support_data->parent_node_id, original_node_id);
                return CFL_DISABLE;
            }
            
            // Filter not matched - handle based on current stage
            if (exception_support_data->exception_stage == CFL_EXCEPTION_MAIN_LINK) {
                // Transition: MAIN -> RECOVERY
                exception_support_data->exception_stage = CFL_EXCEPTION_RECOVERY_LINK;
                cfl_terminate_node_tree(runtime_handle, 
                    exception_support_data->exception_catch_links[CFL_EXCEPTION_MAIN_LINK]);
                cfl_enable_node(runtime_handle, 
                    exception_support_data->exception_catch_links[CFL_EXCEPTION_RECOVERY_LINK]);
                return CFL_CONTINUE;
            }
            
            // RECOVERY or FINALIZE stage - forward to parent
            cfl_forward_exception_event(runtime_handle, node_index, 
                exception_support_data->parent_node_id, original_node_id);
            return CFL_DISABLE;
        }
        
        case CFL_SET_EXCEPTION_STEP_EVENT: {
            exception_support_data->step_count = (uint16_t)((size_t)event_data);
            return CFL_CONTINUE;
        }
        
        case CFL_TURN_HEARTBEAT_ON_EVENT: {
            exception_support_data->heartbeat_enabled = true;
            exception_support_data->heartbeat_time_out = (uint16_t)((size_t)event_data);
            exception_support_data->heartbeat_count = 0;
            return CFL_CONTINUE;
        }
        
        case CFL_TURN_HEARTBEAT_OFF_EVENT: {
            exception_support_data->heartbeat_enabled = false;
            return CFL_CONTINUE;
        }
        
        case CFL_HEARTBEAT_EVENT: {
            exception_support_data->heartbeat_count = 0;
            return CFL_CONTINUE;
        }
        
        case CFL_TIMER_EVENT: {
            if (exception_support_data->heartbeat_enabled) {
                exception_support_data->heartbeat_count++;
                if (exception_support_data->heartbeat_count >= exception_support_data->heartbeat_time_out) {
                    exception_support_data->heartbeat_enabled = false;
                    exception_support_data->original_node_id = node_index;
                    exception_support_data->exception_type = CFL_EXCEPTION_HEARTBEAT_TIMEOUT;
                    
                    if (exception_support_data->logging_function_id != 0) {
                        one_shot_function_t one_shot_function = 
                            runtime_handle->flash_handle->one_shot_functions[exception_support_data->logging_function_id];
                        one_shot_function(runtime_handle, node_index);
                    }
                    
                    if (exception_support_data->exception_stage == CFL_EXCEPTION_MAIN_LINK) {
                        // Transition: MAIN -> RECOVERY
                        exception_support_data->exception_stage = CFL_EXCEPTION_RECOVERY_LINK;
                        cfl_terminate_node_tree(runtime_handle, 
                            exception_support_data->exception_catch_links[CFL_EXCEPTION_MAIN_LINK]);
                        cfl_enable_node(runtime_handle, 
                            exception_support_data->exception_catch_links[CFL_EXCEPTION_RECOVERY_LINK]);
                        return CFL_CONTINUE;
                    }
                    
                    // RECOVERY or FINALIZE - forward to parent and terminate
                    cfl_forward_exception_event(runtime_handle, node_index, 
                        exception_support_data->parent_node_id, node_index);
                    return CFL_DISABLE;
                }
            }
            break;
        }
        
        default:
            break;
    }

    // Manage normal execution sequencing
    unsigned return_code = cfl_verify_active_children(runtime_handle, node_index);
    
    if (return_code == CFL_DISABLE) {
        switch (exception_support_data->exception_stage) {
            case CFL_EXCEPTION_MAIN_LINK:
                // MAIN completed normally -> FINALIZE (skip RECOVERY)
                exception_support_data->exception_stage = CFL_EXCEPTION_FINALIZE_LINK;
                cfl_enable_node(runtime_handle, 
                    exception_support_data->exception_catch_links[CFL_EXCEPTION_FINALIZE_LINK]);
                return CFL_CONTINUE;
                
            case CFL_EXCEPTION_RECOVERY_LINK:
                // RECOVERY completed -> FINALIZE
                exception_support_data->exception_stage = CFL_EXCEPTION_FINALIZE_LINK;
                cfl_enable_node(runtime_handle, 
                    exception_support_data->exception_catch_links[CFL_EXCEPTION_FINALIZE_LINK]);
                return CFL_CONTINUE;
                
            case CFL_EXCEPTION_FINALIZE_LINK:
                return CFL_DISABLE;
                
            default:
                return CFL_CONTINUE;
        }
    }
    
    return CFL_CONTINUE;
}

static void json_extract_exception_links(
    const cfl_runtime_handle_t *runtime,
    uint16_t *exception_catch_links)
{
    if (!runtime) {
        EXCEPTION("json_extract_exception_support_data: NULL runtime");
    }
    
    
    
    if (!runtime->json_decoder_ctx) {
        EXCEPTION("json_extract_exception_support_data: NULL json_decoder_ctx");
    }
    
    const json_decoder_ctx_t *ctx = runtime->json_decoder_ctx;
    
    if (ctx->current_control_idx >= ctx->controls_count) {
        EXCEPTION("json_extract_exception_support_data: Invalid control index");
    }
    
    const record_control_t *region = &ctx->controls[ctx->current_control_idx];
    uint32_t root = region->start_position;
    
    // Navigate to node_dict.column_data.exception_catch_links
    uint32_t node_dict_record;
    json_find_object_child(ctx, root, "node_dict", &node_dict_record);
    
    uint32_t column_data_record;
    json_find_object_child(ctx, node_dict_record, "column_data", &column_data_record);
    
    uint32_t links_array_record;
    json_find_object_child(ctx, column_data_record, "exception_catch_links", &links_array_record);
    
    // Get array count
    uint32_t link_count;
    json_get_child_count(ctx, links_array_record, &link_count);
    
    // Cap at 3 elements (struct limit)
    if (link_count != 3) {
        EXCEPTION("json_extract_exception_links: Invalid link count");
        return;
    }
    
    // Extract each link value
    for (uint32_t i = 0; i < link_count; i++) {
        uint32_t elem_record;
        json_get_array_child(ctx, links_array_record, i, &elem_record);
        
        int32_t link_value;
        json_get_int32(ctx, elem_record, &link_value);
        
        exception_catch_links[i] = (uint16_t)link_value;
    }
    
}

void cfl_exception_catch_init_one_shot_fn(void *handle, uint16_t node_index){
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_exception_support_data_t *exception_support_data = 
          (cfl_exception_support_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_exception_support_data_t));
    exception_support_data->heartbeat_enabled = false;
    exception_support_data->heartbeat_time_out = 0;
    exception_support_data->exception_stage = CFL_EXCEPTION_MAIN_LINK;
    exception_support_data->max_steps = 0;
    exception_support_data->step_count = 0;
    exception_support_data->logging_data = NULL;
    exception_support_data->auxiliary_data = NULL;
    exception_support_data->parent_node_id = find_parent_node_exception(runtime_handle, node_index, false);
    json_decoder_init_from_runtime(runtime_handle, node_index);
    
    int32_t temp_int32;
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.logging_function_id",
         &temp_int32);
    exception_support_data->logging_function_id = (uint16_t)temp_int32;
    json_extract_exception_links(runtime_handle, exception_support_data->exception_catch_links);
    
    cfl_enable_node(runtime_handle, exception_support_data->exception_catch_links[CFL_EXCEPTION_MAIN_LINK]);
    
}
void cfl_exception_catch_term_one_shot_fn(void *handle, uint16_t node_index){
   (void)handle;
   (void)node_index;
    
}
