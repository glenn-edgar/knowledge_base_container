#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>


#include "cfl_engine.h"
#include "CT_Tree_Walker.h"


static void cfl_disable_all_nodes(cfl_runtime_handle_t *handle);

static void cfl_disable_node(cfl_runtime_handle_t *handle, unsigned node_index);
static void cfl_set_node_initialization_flag(cfl_runtime_handle_t *handle, unsigned node_index);

static void cfl_terminate_node_tree(cfl_runtime_handle_t *handle, unsigned parent_id);
static void cfl_reset_node_id(cfl_runtime_handle_t *handle, unsigned parent_id);


static unsigned int cfl_get_forward_enabled_links(void* user_handle, unsigned int node_id, unsigned int* links_out, 
    unsigned int max_links);

static CT_ReturnCode cfl_execute_node(void* user_handle, unsigned int node_id, unsigned int level, uint8_t* flags);


void cfl_engine_create(cfl_runtime_handle_t *handle){
    handle->walker = (CT_TreeWalker*)cfl_perm_alloc_pointer(handle->perm, sizeof(CT_TreeWalker));
    ct_walker_init(handle->walker, handle->flash_handle->node_count, handle->flags,
           cfl_get_forward_enabled_links, cfl_execute_node);
}


void cfl_engine_init(cfl_runtime_handle_t *handle){

    cfl_disable_all_nodes(handle);
    
}



void cfl_engine_init_test(cfl_runtime_handle_t *handle,unsigned start_node, unsigned node_count){
    for (unsigned i = start_node; i < start_node + node_count; i++) {
        cfl_disable_node(handle, i);
    }
    cfl_enable_node(handle, start_node);
    
}

CT_StackEntry stack[1024];
     

bool cfl_execute_event(cfl_runtime_handle_t *handle){
     unsigned node_index = handle->event_data_ptr->node_id;
     if(cfl_engine_node_is_enabled(handle, node_index) == false) {
        return false;
     }
     handle->max_level = 0;
     handle->cfl_engine_flag = true;
     handle->cfl_node_execution_count = 0;
     CT_ReturnCode return_code = ct_walker_walk(
        handle->walker,
        handle,
        node_index,
        CT_ITERATIVE,
        stack,
        1024,
        0xFFFF
    );
    printf("max_level: %d\n", handle->max_level);
    printf("return_code: %d\n", return_code);
    exit(0);
     return false;
}




static CT_ReturnCode cfl_execute_node(void* user_handle, unsigned int node_id, unsigned int level, uint8_t* flags){
    (void)flags;
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t *)user_handle;
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    const main_function_t main_function = handle->flash_handle->main_functions[node->main_function_index];
    const one_shot_function_t one_shot_function = handle->flash_handle->one_shot_functions[node->init_function_index];
    const boolean_function_t boolean_function = handle->flash_handle->boolean_functions[node->aux_function_index];
 
    if (handle->max_level < level) {
        handle->max_level = level;
    }
    printf("cfl_execute_node node index: %d\n", node_id);
    
    if (cfl_engine_node_is_initialized(handle, node_id) == false) {
        one_shot_function(handle, node_id);
        boolean_function(handle, node_id, handle->event_data_ptr->event_type,
                                            handle->event_data_ptr->event_id, (void*)handle->event_data_ptr->data.ptr);
        cfl_set_node_initialization_flag(handle, node_id);
    }
    printf("cfl_execute_node node index: %d\n", node_id);
    
    unsigned return_code = main_function(handle, node->aux_function_index, node_id, handle->event_data_ptr->event_type,
                                            handle->event_data_ptr->event_id, (void*)handle->event_data_ptr->data.ptr);
    printf("main function return code: %d\n", return_code);
    printf("level: %d\n", level);
    printf("node id: %d\n", node_id);
    
    handle->cfl_node_execution_count++;
    handle->cfl_engine_flag = true;

    switch (return_code) {
        case CFL_CONTINUE:
            return CT_CONTINUE;

        case CFL_HALT:
            return CT_STOP_SIBLINGS;
        
        case CFL_RESET:
            cfl_terminate_node_tree(handle, node->parent_index);
            cfl_reset_node_id(handle, node->parent_index);
            return CT_STOP_SIBLINGS;

        case CFL_DISABLE:
            cfl_terminate_node_tree(handle, node->parent_index);
            return CT_CONTINUE;

        case CFL_SKIP_CONTINUE:
            return CT_STOP_LEVEL;

        case CFL_TERMINATE:
            if (node->parent_index != 0xffff){
                cfl_terminate_node_tree(handle, node->parent_index);
                handle->cfl_engine_flag = true;
                handle->cfl_node_execution_count = 0;
                return CT_STOP_SIBLINGS;

            }
            cfl_disable_node(handle, node_id);
            return CT_STOP_ALL;
        
        case CFL_TERMINATE_SYSTEM:
            handle->cfl_engine_flag = false;
            handle->cfl_node_execution_count = 0;
            return CT_STOP_ALL;
 
        default:
            EXCEPTION("cfl_execute_node: invalid return code");
            return CT_STOP_ALL;
    }
   
}




static unsigned int cfl_get_forward_enabled_links(void* user_handle, unsigned int node_id, unsigned int* links_out, 
                 unsigned int max_links){
    printf("cfl_get_forward_enabled_links node id: %d\n", node_id);
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t *)user_handle;
    unsigned int return_value = 0;
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
 
    const uint16_t *link_table = handle->flash_handle->link_table;
    printf("link count: %d\n", link_count);
    printf("link start: %d\n", link_start);
    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
        printf("checking link %d\n", link_id);
        if (cfl_engine_node_is_enabled(handle, link_id)) {
            printf("link %d is enabled\n", link_id);
            links_out[return_value] = link_id;
            return_value++;
            if (return_value >= max_links) {
                EXCEPTION("cfl_get_forward_enabled_links: max_links exceeded");
                return 0;
            }
        }
    }
    printf("return value: %d\n", return_value);

    return return_value;
}

bool cfl_engine_node_is_enabled(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint8_t *flags = handle->flags;
    return flags[node_index] & CT_FLAG_USER_BIT3;
}

bool cfl_engine_node_is_initialized(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint8_t *flags = handle->flags;
    return (flags[node_index] & CT_FLAG_USER_BIT2) && (flags[node_index] & CT_FLAG_USER_BIT3);
}



static void cfl_terminate_node_tree(cfl_runtime_handle_t *handle, unsigned parent_id){
  (void)handle;
  (void)parent_id;

    // TBD
}

static void cfl_reset_node_id(cfl_runtime_handle_t *handle, unsigned parent_id){
    if (cfl_engine_node_is_enabled(handle, parent_id) == false) {
        EXCEPTION("parent node is not enabled for enabling a node");
        return;
    }
    cfl_enable_node(handle, parent_id);
}
 







static void cfl_disable_all_nodes(cfl_runtime_handle_t *handle)
{
   uint8_t *flags = handle->flags;
   for (unsigned i = 0; i < handle->flash_handle->node_count; i++) {
    flags[i] &=  ~CT_FLAG_USER_MASK;
   }
}

/* used to mark nodes which will be terminated later*/
static void cfl_disable_node_flag(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint8_t *flags = handle->flags;
    flags[node_index]  |= CT_FLAG_USER_BIT1;
}

void cfl_enable_node(cfl_runtime_handle_t *handle, unsigned node_index)

{
    uint8_t *flags = handle->flags;
    flags[node_index] &= ~CT_FLAG_USER_MASK;
    flags[node_index] |= CT_FLAG_USER_BIT3;
}

static void cfl_disable_node(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint8_t *flags = handle->flags;
    flags[node_index] &= ~CT_FLAG_USER_BIT3;
}

static void cfl_set_node_initialization_flag(cfl_runtime_handle_t *handle, unsigned node_index)
{
    uint8_t *flags = handle->flags;
    flags[node_index] |= CT_FLAG_USER_BIT2;
}