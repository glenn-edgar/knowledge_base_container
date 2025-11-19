#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>


#include "cfl_engine.h"
#include "CT_Tree_Walker.h"



static void cfl_disable_node(cfl_runtime_handle_t *handle, unsigned node_index);


static void cfl_disable_all_node_flags(cfl_runtime_handle_t *handle);

static void cfl_disable_node_flag(cfl_runtime_handle_t *handle, unsigned node_index);
static void cfl_set_node_initialization_flag(cfl_runtime_handle_t *handle, unsigned node_index);

static void cfl_terminate_node_tree(cfl_runtime_handle_t *handle, unsigned parent_id);
static void cfl_reset_node_id(cfl_runtime_handle_t *handle, unsigned parent_id);


static unsigned int cfl_get_forward_enabled_links(void* user_handle, unsigned int node_id, unsigned int* links_out, 
    unsigned int max_links);

static CT_ReturnCode cfl_execute_node(void* user_handle, unsigned int node_id, unsigned int level, uint8_t* flags);


void cfl_engine_create(cfl_runtime_handle_t *handle){
    
    ct_walker_init(handle->walker, handle->flash_handle->node_count, handle->flags,
           cfl_get_forward_enabled_links, cfl_execute_node);
}


void cfl_engine_init(cfl_runtime_handle_t *handle){

    cfl_disable_all_node_flags(handle);
    
}



void cfl_engine_init_test(cfl_runtime_handle_t *handle,unsigned start_node, unsigned node_count){
    for (unsigned i = start_node; i < start_node + node_count; i++) {
        cfl_disable_node_flag(handle, i);
    }
    cfl_enable_node(handle, start_node);
    
}


     

bool cfl_execute_event(cfl_runtime_handle_t *handle){
     unsigned node_index = handle->event_data_ptr->node_id;
     if(cfl_engine_node_is_enabled(handle, node_index) == false) {
        return false;
     }
     printf("cfl_execute_event node count: %d\n", handle->flash_handle->node_count);
     handle->cfl_engine_flag = true;
     handle->cfl_node_execution_count = 0;
     ct_walker_walk(
        handle->walker,
        handle,
        node_index,
        handle->stack,
        handle->max_level,
        handle->walker->max_level,
        handle->flash_handle->node_count
    );
    return false;
}




static CT_ReturnCode cfl_execute_node(void* user_handle, unsigned int node_id, unsigned int level, uint8_t* flags){
    (void)flags;
    (void)level;
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t *)user_handle;
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    const main_function_t main_function = handle->flash_handle->main_functions[node->main_function_index];
    const one_shot_function_t one_shot_function = handle->flash_handle->one_shot_functions[node->init_function_index];
    const boolean_function_t boolean_function = handle->flash_handle->boolean_functions[node->aux_function_index];
 
   
    printf("cfl_execute_node node index: %d\n", node_id);
    
    if (cfl_engine_node_is_initialized(handle, node_id) == false) {
    
        if (node->init_function_index != 0) {
            one_shot_function(handle, node_id);
        }
        printf("aux_function_index: %d\n", node->aux_function_index);
        if (node->aux_function_index != 0) {
            boolean_function(handle, node_id, handle->event_data_ptr->event_type,
                                            handle->event_data_ptr->event_id, (void*)handle->event_data_ptr->data.ptr);
        }
        cfl_set_node_initialization_flag(handle, node_id);
    }
    
    
    unsigned return_code = main_function(handle, node->aux_function_index, node_id, handle->event_data_ptr->event_type,
                                            handle->event_data_ptr->event_id, (void*)handle->event_data_ptr->data.ptr);
 
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
            cfl_terminate_node_tree(handle, node_id);
            return CT_CONTINUE;

        case CFL_SKIP_CONTINUE:
            return CT_STOP_LEVEL;

        case CFL_TERMINATE:
            if (node->parent_index != 0xffff){
                cfl_terminate_node_tree(handle, node->parent_index);
                handle->cfl_engine_flag = true;
                handle->cfl_node_execution_count = 0;
                printf("cfl_execute_node: terminate node tree parent index: %d\n", node->parent_index);
            
                return CT_STOP_LEVEL;
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

    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t *)user_handle;
    unsigned int return_value = 0;
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
 
    const uint16_t *link_table = handle->flash_handle->link_table;

    for (unsigned i = 0; i < link_count; i++) {
        unsigned int link_id = link_table[link_start + i];
    
        links_out[return_value] = link_id;
        return_value++;
        if (return_value >= max_links) {
             EXCEPTION("cfl_get_forward_enabled_links: max_links exceeded");
                return 0;
        }
        
    }
    

    return return_value;
}

bool cfl_engine_node_is_enabled(cfl_runtime_handle_t *handle, unsigned node_index)
{
    volatile uint8_t *flags = handle->flags;
    if ((flags[node_index] & CT_FLAG_USER3) == 0){
        return false;
    }
    return true;
}

bool cfl_engine_node_is_initialized(cfl_runtime_handle_t *handle, unsigned node_index)
{
    volatile uint8_t *flags = handle->flags;
    bool return_value = (flags[node_index] & CT_FLAG_USER2) && (flags[node_index] & CT_FLAG_USER3);

    return return_value;
}


/* used to mark nodes which will be terminated later*/
static CT_ReturnCode cfl_mark_node_for_termination(void *handle, unsigned node_index,unsigned int level, uint8_t* flags)
{
    (void)level;
    (void)handle;
    if ((flags[node_index] & (  CT_FLAG_USER2 | CT_FLAG_USER3 )) == (CT_FLAG_USER2 | CT_FLAG_USER3)) {
        flags[node_index] |= CT_FLAG_USER1;
    }
    return CT_CONTINUE;
}

static void cfl_disable_node(cfl_runtime_handle_t *handle, unsigned node_index)
{
    if ((handle->flags[node_index] & CT_FLAG_USER1) != 0) {
        return;
    }
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_index];
    unsigned int termination_function_index = node->term_function_index;
    if (termination_function_index != 0) {
       handle->flash_handle->one_shot_functions[termination_function_index](handle, node_index);
    }
    cfl_disable_node_flag(handle, node_index);
}


static void cfl_terminate_node_tree(cfl_runtime_handle_t *handle, unsigned node_id){
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    if ((node->link_count & LINK_COUNT_MASK) == 0){
        cfl_disable_node(handle, node_id);
        return;
    }

    ct_walker_save_context(handle->walker, handle->walker_context_ptr, handle->backup_flags);
        
    /* Switch function and walk subtree */
    ct_walker_update_functions(handle->walker,cfl_mark_node_for_termination, cfl_get_forward_enabled_links);
    ct_walker_walk(
        handle->walker,
        handle,
        node_id,
        handle->nested_stack,
        handle->max_level,
        handle->walker->max_level,
        handle->flash_handle->node_count
    );
    ct_walker_restore_context(handle->walker, handle->walker_context_ptr);
    for (int i =handle->flash_handle->node_count-1; i>= (int) node_id; i--) {
        if (handle->flags[i] & CT_FLAG_USER1) {
            cfl_disable_node(handle, i);
        }
    }
    
    
}

static void cfl_reset_node_id(cfl_runtime_handle_t *handle, unsigned parent_id){
    if (cfl_engine_node_is_enabled(handle, parent_id) == false) {
        EXCEPTION("parent node is not enabled for enabling a node");
        return;
    }
    cfl_enable_node(handle, parent_id);
}
 







static void cfl_disable_all_node_flags(cfl_runtime_handle_t *handle)
{
   volatile uint8_t *flags = handle->flags;
   for (unsigned i = 0; i < handle->flash_handle->node_count; i++) {
    flags[i] &=  ~CT_FLAG_USER_MASK;
   }
}



void cfl_enable_node(cfl_runtime_handle_t *handle, unsigned node_index)

{
    volatile uint8_t *flags = handle->flags;
    flags[node_index] &= ~CT_FLAG_USER_MASK;
    flags[node_index] |= CT_FLAG_USER3;
}

static void cfl_disable_node_flag(cfl_runtime_handle_t *handle, unsigned node_index)
{
    volatile uint8_t *flags = handle->flags;
    flags[node_index] &= ~CT_FLAG_USER_MASK;
}

static void cfl_set_node_initialization_flag(cfl_runtime_handle_t *handle, unsigned node_index)
{
    volatile uint8_t *flags = handle->flags;
    flags[node_index] |= CT_FLAG_USER2;
}