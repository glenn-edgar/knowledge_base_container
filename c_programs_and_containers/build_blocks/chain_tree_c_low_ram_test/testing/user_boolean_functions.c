#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>


#include "cfl_runtime.h"
#include "cfl_exception.h"
#include "cfl_common_function_headers.h"
#include "cfl_common_functions.h"
#include "json_node_decoder.h"





bool while_test_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)event_type;
    (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_while_fn_data_t *ptr = (cfl_while_fn_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    if (event_id == CFL_INIT_EVENT) {
        int32_t *loop_count = (int32_t *)cfl_heap_malloc_pointer(runtime_handle->heap, sizeof(uint32_t));
        
        ptr->auxiliary_data = (void *)loop_count;
        json_decoder_init_from_runtime(runtime_handle, node_index);
        json_extract_int32_runtime(runtime_handle, "node_dict.user_data.count", loop_count);
    
        return false;
    }
    if (event_id == CFL_TERMINATE_EVENT) {
        cfl_heap_free_pointer(runtime_handle->heap, ptr->auxiliary_data);
        return false;
    }
    int32_t *loop_count = (int32_t *)ptr->auxiliary_data;
    if(ptr->current_iteration >= *loop_count) {
        return false;
    }

    return true;
}

#include "cfl_exception_support.h"

typedef struct {
    const char *aux_data;
} user_catch_all_exception_data_t;

bool catch_all_exception_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    user_catch_all_exception_data_t *user_catch_all_exception_data = NULL;
    switch(event_id)
    {
        case CFL_INIT_EVENT:
            user_catch_all_exception_data = 
                (user_catch_all_exception_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(user_catch_all_exception_data_t));
            json_decoder_init_from_runtime(runtime_handle, node_index);
            
            json_extract_string_runtime(runtime_handle, "node_dict.column_data.aux_data",
                &user_catch_all_exception_data->aux_data);
            return false;
        case CFL_TERMINATE_EVENT:
            return false;
        case CFL_RAISE_EXCEPTION_EVENT:
              user_catch_all_exception_data = 
                (user_catch_all_exception_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
              printf("*********** catch_all_exception_boolean_fn ***********\n");
              printf("Raise exception event\n");
              printf("Aux data: %s\n", user_catch_all_exception_data->aux_data);
              printf("original node id: %d\n", (uint16_t)((size_t)event_data));
              printf("catch the exception\n");
              printf("*********** catch_all_exception_boolean_fn ***********\n");
              return true;
        default:
            EXCEPTION("Unexpected event in catch_all_exception_boolean_fn");
            return false;
    }
    return false;
}

typedef struct {
    const char *exception_filter_data;
} user_exception_filter_data_t;

bool exception_filter_boolean_fn(void *handle, unsigned node_index, unsigned event_type, unsigned event_id, void *event_data) {
    (void)event_type;
    (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_exception_support_data_t *exception_support_data = 
        (cfl_exception_support_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
   
    if (event_id == CFL_INIT_EVENT) {
        if (exception_support_data->auxiliary_data != NULL){
            return false;  // already initialized
        }
        exception_support_data->auxiliary_data = 
        (void *)cfl_additional_arena_alloc(runtime_handle, node_index, sizeof(user_exception_filter_data_t));
        user_exception_filter_data_t *user_exception_filter_data = 
            (user_exception_filter_data_t *)exception_support_data->auxiliary_data;
        json_decoder_init_from_runtime(runtime_handle, node_index);
        json_extract_string_runtime(runtime_handle, "node_dict.column_data.aux_function_data.exception_filter_data",
             &user_exception_filter_data->exception_filter_data);
        
        return false;
    }
    
    if (event_id == CFL_TERMINATE_EVENT) {
    
        
        return false;
    }
    
    switch (event_id) {
        case CFL_RAISE_EXCEPTION_EVENT: {
            user_exception_filter_data_t *user_data = 
                (user_exception_filter_data_t *)exception_support_data->auxiliary_data;
            printf("*********** Exception filter event function ***********\n");
            printf("Exception filter event function\n");
            printf("Raise exception originating node %d %s\n",(uint16_t)((size_t)event_data),user_data->exception_filter_data);
            printf("exception_type: %d\n", exception_support_data->exception_type);
            printf("Returning false\n");
            printf("*********** Exception filter event function ***********\n");
            return false;
        }
        default:
            return false;
    }
}

typedef struct {
    const char *skip_condition_message;
    uint16_t parent_node_index;
} user_skip_condition_data_t;

bool user_skip_condition_boolean_fn(void *handle, unsigned node_index, unsigned event_type,unsigned event_id,void *event_data){
    
    (void)event_type;
    (void)event_data;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    user_skip_condition_data_t *user_skip_condition_data;
    switch(event_id)
    {
        case CFL_INIT_EVENT:
            user_skip_condition_data = 
                (user_skip_condition_data_t *)cfl_smart_arena_alloc(runtime_handle, node_index, sizeof(user_skip_condition_data_t));
            const chaintree_node_t *node = &runtime_handle->flash_handle->nodes[node_index];
            user_skip_condition_data->parent_node_index = node->parent_index;
            json_decoder_init_from_runtime(runtime_handle, node_index);
            json_extract_string_runtime(runtime_handle, "node_dict.column_data.skip_condition_data.skip_condition_data",
                &user_skip_condition_data->skip_condition_message);
            return false;

        case CFL_TERMINATE_EVENT:
            return false;

        case CFL_RECOVERY_CHECK_EVENT:
             user_skip_condition_data = 
                (user_skip_condition_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);


             cfl_exception_support_data_t *exception_support_data = 
                (cfl_exception_support_data_t *)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, user_skip_condition_data->parent_node_index);
            
             if(exception_support_data->recovery_state == CFL_RECOVERY_SEQ_EVAL) {
                printf("*********** Recovery step check ***********\n");
                printf("Recovery step check\n");
                printf("Recovery step message: %s\n", user_skip_condition_data->skip_condition_message);
                printf("Recovery step state: %d \n", (exception_support_data->max_steps)-exception_support_data->current_step);
                printf("*********** Recovery step check ***********\n");
             }
             return true;
        default:
             EXCEPTION("Unexpected event in user_skip_condition_boolean_fn");
             return false;
                
    }
    
    return false;
}



