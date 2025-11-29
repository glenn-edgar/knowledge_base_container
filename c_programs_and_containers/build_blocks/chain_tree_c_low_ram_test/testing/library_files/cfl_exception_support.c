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
    while (!found_flag) {
        
       
        const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[search_node_id];
        parent_node_id = node->parent_index;
        
        if (parent_node_id == 0xFFFF) {
            if (terminate_flag) {
                cfl_terminate_node_tree(runtime_handle,search_node_id);
            }
            return 0xFFFF;
        }
       
        if( node->main_function_index == runtime_handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_EXCEPTION_CATCH_ALL_MAIN]) {
            found_flag = true;
        }
        if( node->main_function_index == runtime_handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_EXCEPTION_CATCH_MAIN]) {
            found_flag = true;
        }
        else{
            search_node_id = parent_node_id;
        }
    }
    return search_node_id;
}


void cfl_raise_json_exception_event(cfl_runtime_handle_t *runtime_handle, uint16_t node_id, uint16_t parent_node_id )
{
  // find parent node id
  if (parent_node_id == 0xFFFF) {
     parent_node_id = find_parent_node_exception(runtime_handle, node_id, true);
    
  }
  
  cfl_send_json_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_HIGH, parent_node_id, CFL_RAISE_EXCEPTION_EVENT, node_id);

}



void cfl_forward_exception_event(cfl_runtime_handle_t *runtime_handle, unsigned node_index , unsigned parent_node_id , 
               uint16_t record_index ){
    // find parent node id
    if (parent_node_id == 0xFFFF) {
        parent_node_id = find_parent_node_exception(runtime_handle, node_index, true);
        return;
    }
    cfl_send_json_event(runtime_handle->event_queue, CFL_EVENT_PRIORITY_HIGH, parent_node_id, CFL_RAISE_EXCEPTION_EVENT,record_index);
    
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
    uint16_t parent_node_id = find_parent_node_exception(runtime_handle, node_index, false);
    if (parent_node_id == 0xFFFF) {
        EXCEPTION("cfl_raise_exception_one_shot_fn: parent_node_id is 0xFFFF");
    }
    
    cfl_raise_json_exception_event(runtime_handle, node_index, parent_node_id);

}

void cfl_heartbeat_event_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    json_decoder_init_from_runtime(runtime_handle, node_index);
    json_print_node_data_runtime(runtime_handle, node_index);
    printf("cfl_heartbeat_event_one_shot_fn node_index: %d\n", node_index);
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

   
    printf("cfl_recovery_main_main_fn node_index: %d\n", node_index);
    exit(0);
    return CFL_CONTINUE;
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
                return CFL_DISABLE;
            }
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
            if (exception_support_data->logging_function_id != 0) {
                one_shot_function_t one_shot_function = runtime_handle->flash_handle->one_shot_functions[exception_support_data->logging_function_id];
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
                // MAIN completed normally -> RECOVERY
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
                // FINALIZE completed -> done
                return CFL_DISABLE;
                
            default:
                return CFL_DISABLE;
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
