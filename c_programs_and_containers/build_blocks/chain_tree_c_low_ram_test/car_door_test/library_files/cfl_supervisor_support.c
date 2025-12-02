
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
#include "cfl_supervisor_support.h"


static void cfl_links_handle_link_termination(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t link_index, cfl_supervisor_data_t *ptr);
static bool cfl_links_still_active(cfl_runtime_handle_t *handle, uint16_t node_index);
//static void cfl_enable_terminated_nodes(cfl_runtime_handle_t *handle, uint16_t node_index);

static void cfl_mark_supervisor_node_failure(cfl_runtime_handle_t *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    uint16_t ref_node_index = node_index;
    uint16_t previous_node_index = node_index;

    bool loop_flag = true;
    while(loop_flag){
        const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
        
        if (node->main_function_index == handle->main_function_data->main_function_ids[CFL_FUNCTION_ID_SUPERVISOR_MAIN]){
            loop_flag = false;
            uint16_t link_count = node->link_count & LINK_COUNT_MASK;
        
            for(uint16_t i = 0; i < link_count; i++){
                uint16_t link_id = runtime_handle->flash_handle->link_table[node->link_start + i];
            
                if (link_id == previous_node_index){
                    cfl_supervisor_data_t *ptr = (cfl_supervisor_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
                    ptr->failed_link_index = i;
                
                    ptr->supervisor_failure_array[i].node_id = ref_node_index;
                    return;
                }
            }
            
        }
        previous_node_index = node_index;
        node_index = node->parent_index;
        if (node_index == 0xFFFF){
            EXCEPTION("cfl_mark_supervisor_node_failure: no CFL_SUPERVISOR_MAIN found");
            return;
        }
    }
    EXCEPTION("cfl_mark_supervisor_node_failure: failed to mark supervisor node failure");
    return;
}





static bool cfl_leaky_bucket_check(cfl_supervisor_data_t *ptr,uint16_t link_index){
    uint32_t now_tick = ptr->now_tick;
    uint32_t last_tick = ptr->supervisor_failure_array[link_index].last_tick;
    uint32_t elapsed = now_tick - last_tick;
    uint32_t leak = (uint32_t)(elapsed / ptr->reset_window);
    uint8_t bucket = ptr->supervisor_failure_array[link_index].bucket;
    bucket = (leak >= bucket) ? 0 : (bucket - leak);
    ptr->supervisor_failure_array[link_index].bucket = bucket;
    ptr->supervisor_failure_array[link_index].last_tick = ptr->now_tick;
    return (bucket >= ptr->max_reset_number);
}

static unsigned cfl_handle_supervisor_node_failure(cfl_runtime_handle_t *handle, uint16_t node_index,uint16_t link_index, cfl_supervisor_data_t *ptr){
 
          
    
        cfl_links_handle_link_termination(handle,node_index,link_index,ptr);
        
        if( cfl_links_still_active(handle,node_index) == false){
            one_shot_function_t one_shot_function = handle->flash_handle->one_shot_functions[ptr->finalize_function_id];
            one_shot_function(handle,node_index);
            return CFL_DISABLE;
        }
        return CFL_CONTINUE;
}




static bool cfl_links_still_active(cfl_runtime_handle_t *handle, uint16_t node_index){
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_index];
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    for(uint16_t i = 0; i < link_count; i++){
        uint16_t link_id = handle->flash_handle->link_table[node->link_start + i];
        if(cfl_engine_node_is_enabled(handle,link_id) == true){
            return true;
        }
    }
    return false;
}

#if 0
static void cfl_enable_terminated_nodes(cfl_runtime_handle_t *handle, uint16_t node_index){
  
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_index];
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    for(uint16_t i = 0; i < link_count; i++){
        uint16_t link_id = handle->flash_handle->link_table[node->link_start + i];
        if(cfl_engine_node_is_enabled(handle,link_id) == false){
            cfl_enable_node(handle,link_id);
        }
    }
    
}
#endif

static void cfl_links_handle_link_termination(cfl_runtime_handle_t *handle, uint16_t node_index, uint16_t link_index, cfl_supervisor_data_t *ptr){

    const chaintree_node_t *node = &handle->flash_handle->nodes[node_index];
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    uint16_t link_start = node->link_start;
    
    switch(ptr->termination_type){
        case 0: // one for onde
           uint16_t link_id = handle->flash_handle->link_table[link_start + link_index];
           cfl_terminate_node_tree(handle,link_id);
           if(ptr->restart_enabled == true){
            cfl_enable_node(handle,link_id);
           }
           break;
        case 1: // one for one
           for(uint16_t i = 0; i < link_count; i++){
            uint16_t link_id = handle->flash_handle->link_table[link_start + i];
    
            cfl_terminate_node_tree(handle,link_id);
            if(ptr->restart_enabled == true){
                cfl_enable_node(handle,link_id);
            }
            
                
           }
           break;
       
        case 2: // one for all
           for(uint16_t i =link_index; i < link_count; i++){
            uint16_t link_id = handle->flash_handle->link_table[link_start + i];
        
            cfl_terminate_node_tree(handle,link_id);
            if(ptr->restart_enabled == true){
                cfl_enable_node(handle,link_id);
            }
           
           }
           break;
        default:
            EXCEPTION("cfl_links_handle_link_termination: invalid termination type");
            
    }

}


void cfl_supervisor_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
   
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    uint16_t link_count = node->link_count & LINK_COUNT_MASK;
    //uint16_t link_start = node->link_start;

    bool allocator_state = cfl_allocate_state(runtime_handle, node_index);
    cfl_supervisor_data_t *ptr = (cfl_supervisor_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(cfl_supervisor_data_t));
    if (allocator_state == false){
        ptr->supervisor_failure_array = (cfl_supervisor_failure_t *)cfl_additional_arena_alloc(runtime_handle, node_index, sizeof(cfl_supervisor_failure_t) * link_count);
    }
    for (uint32_t i = 0; i < link_count; i++) {
        ptr->supervisor_failure_array[i].node_id = -1;
        ptr->supervisor_failure_array[i].bucket = 0;
        ptr->supervisor_failure_array[i].last_tick = 0;
        ptr->supervisor_failure_array[i].active_node = true;
    }


    
    
    json_decoder_init_from_runtime(runtime_handle, node_index);

    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.supervisor_data.termination_type", &ptr->termination_type);
    json_extract_bool_runtime(runtime_handle, "node_dict.column_data.supervisor_data.reset_limited_enabled", &ptr->reset_limited_enabled);
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.supervisor_data.max_reset_number", &ptr->max_reset_number);
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.supervisor_data.reset_window", &ptr->reset_window);
    json_extract_int32_runtime(runtime_handle, "node_dict.column_data.supervisor_data.finalize_function_id", &ptr->finalize_function_id);
    json_extract_bool_runtime(runtime_handle, "node_dict.column_data.supervisor_data.restart_enabled", &ptr->restart_enabled);
    cfl_enable_all_nodes(runtime_handle, node_index);
    
}
void cfl_supervisor_term_one_shot_fn(void *handle, uint16_t node_index){
    (void)handle;
    (void)node_index;
    

}


void cfl_mark_supervisor_node_failure_init_one_shot_fn(void *handle, uint16_t node_index){
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_mark_supervisor_node_failure(runtime_handle, node_index);
}

unsigned cfl_supervisor_main_main_fn(void *handle, unsigned bool_function_index,
                                     unsigned node_index, unsigned event_type,
                                     unsigned event_id, void *event_data)
{
    

    if (event_id != CFL_TIMER_EVENT) {
        return CFL_CONTINUE;
    }

    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_supervisor_data_t *ptr = cfl_heap_arena_get_node_ptr(
        runtime_handle->arena_system, node_index);
    
    if (!ptr) {
        EXCEPTION("cfl_supervisor_main_main_fn: failed to get node pointer");
        return CFL_TERMINATE_SYSTEM;
    }
    ptr->now_tick +=1;
    const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
    const uint16_t *link_table = runtime_handle->flash_handle->link_table;
    uint16_t node_count = node->link_count & LINK_COUNT_MASK;
    uint16_t link_start = node->link_start;

    bool any_active = false;
    if(ptr->reset_limited_enabled == true){
        for(uint32_t i = 0; i < node_count; i++){
            cfl_leaky_bucket_check(ptr,i);  // clearing time window for each link
        }
    }
    
    for (uint32_t i = 0; i < node_count; i++) {
        uint16_t link_id = link_table[link_start + i];
        bool is_active = ptr->supervisor_failure_array[i].active_node;

        if (is_active) {
            any_active = true;
        
            if (!cfl_engine_node_is_enabled(runtime_handle, link_id)) {
                ptr->supervisor_failure_array[i].bucket++;
                ptr->failed_link_index = i;
                boolean_function_t boolean_function = runtime_handle->flash_handle->boolean_functions[bool_function_index];
                bool result = boolean_function(runtime_handle, node_index, event_type, event_id, event_data);
                if (result == true) {
                    return CFL_DISABLE;
                }
                if(ptr->reset_limited_enabled == true){
                    printf("cfl_leaky_bucket_check link_id: %d, i: %d\n", link_id, i);
                    if(cfl_leaky_bucket_check(ptr,i) == true){
                        one_shot_function_t one_shot_function = runtime_handle->flash_handle->one_shot_functions[ptr->finalize_function_id];
                        one_shot_function(runtime_handle,node_index);
                        return CFL_DISABLE;
                    }
                }
                
                return cfl_handle_supervisor_node_failure(runtime_handle, node_index,i,ptr);
                
            }
        }
    }

    // All children inactive - supervisor complete
    if (!any_active) {
        return CFL_DISABLE;
    }

    return CFL_CONTINUE;
}