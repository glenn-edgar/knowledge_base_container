#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include "cfl_engine.h"
#include "CT_Tree_Walker.h"

/*==============================================================================
 * FLAG DEFINITIONS
 * 
 * CT_FLAG_USER1 (0x20) - Mark for termination flag (used during tree termination)
 * CT_FLAG_USER2 (0x40) - Node initialization flag (set after init/aux functions called)
 * CT_FLAG_USER3 (0x80) - Node enabled flag (must be set for node to execute)
 *============================================================================*/

static void cfl_disable_node(cfl_runtime_handle_t *handle, unsigned node_index);
static void cfl_disable_all_node_flags(cfl_runtime_handle_t *handle);
static void cfl_disable_node_flag(cfl_runtime_handle_t *handle, unsigned node_index);
static void cfl_set_node_initialization_flag(cfl_runtime_handle_t *handle, unsigned node_index);

static void cfl_reset_node_id(cfl_runtime_handle_t *handle, unsigned parent_id);

static unsigned int cfl_get_forward_enabled_links(void* user_handle, unsigned int node_id, 
    unsigned int* links_out, unsigned int max_links);

static CT_ReturnCode cfl_execute_node(void* user_handle, unsigned int node_id, unsigned int level, 
    uint8_t* flags);

/*==============================================================================
 * PUBLIC API IMPLEMENTATION
 *============================================================================*/

void cfl_engine_create(cfl_runtime_handle_t *handle) {
    if (!handle) {
        EXCEPTION("cfl_engine_create: handle is NULL");
    }
    if (!handle->walker) {
        EXCEPTION("cfl_engine_create: walker is NULL");
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_engine_create: flash_handle is NULL");
    }
    if (!handle->flags) {
        EXCEPTION("cfl_engine_create: flags is NULL");
    }
    
    ct_walker_init(handle->walker, handle->flash_handle->node_count, handle->flags,
           cfl_get_forward_enabled_links, cfl_execute_node);
}

void cfl_engine_init(cfl_runtime_handle_t *handle) {
    if (!handle) {
        EXCEPTION("cfl_engine_init: handle is NULL");
    }
    
    cfl_disable_all_node_flags(handle);
}

void cfl_engine_init_test(cfl_runtime_handle_t *handle, unsigned start_node, unsigned node_count) {
    if (!handle) {
        EXCEPTION("cfl_engine_init_test: handle is NULL");
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_engine_init_test: flash_handle is NULL");
    }
    
    /* Validate range */
    if (start_node >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_engine_init_test: start_node out of bounds");
    }
    
    /* Check for overflow and bounds */
    if (node_count > 0 && start_node > handle->flash_handle->node_count - node_count) {
        EXCEPTION("cfl_engine_init_test: range exceeds node_count");
    }
    
    for (unsigned i = start_node; i < start_node + node_count; i++) {
        cfl_disable_node_flag(handle, i);
    }
    cfl_enable_node(handle, start_node);
}

bool cfl_execute_event(cfl_runtime_handle_t *handle) {
    
    if (!handle) {
        EXCEPTION("cfl_execute_event: handle is NULL");
    }
    if (!handle->event_data_ptr) {
        EXCEPTION("cfl_execute_event: event_data_ptr is NULL");
    }
    if (!handle->walker) {
        EXCEPTION("cfl_execute_event: walker is NULL");
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_execute_event: flash_handle is NULL");
    }
    if (!handle->stack) {
        EXCEPTION("cfl_execute_event: stack is NULL");
    }
    
    unsigned node_index = handle->event_data_ptr->node_id;
    if (node_index < handle->kb_start_index) {
        EXCEPTION("cfl_execute_event: node_id out of bounds too low");
    }
    
    /* Check for overflow in range calculation */
    if (handle->kb_node_count > 0 && 
        handle->kb_start_index > UINT_MAX - handle->kb_node_count) {
        EXCEPTION("cfl_execute_event: kb range calculation overflow");
    }
    
    if (node_index >= handle->kb_start_index + handle->kb_node_count) {
        EXCEPTION("cfl_execute_event: node_id out of bounds too high");
    }
    /* Validate node_index */
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_execute_event: node_id out of bounds");
    }
    
    if (cfl_engine_node_is_enabled(handle, node_index) == false) {
    
        return false;
    }
    
    handle->cfl_engine_flag = true;
    handle->cfl_node_execution_count = 0;
    handle->node_start_index = node_index;
    
    ct_walker_walk(
        handle->walker,
        handle,
        node_index,
        handle->stack,
        handle->max_level,
        handle->walker->max_level,
        handle->flash_handle->node_count
    );
   
    /* If no nodes were executed but the engine flag is still true,
     * something went wrong - clear the flag and return false */
    if ((handle->cfl_node_execution_count == 0) || (handle->cfl_engine_flag == false)) {
        handle->cfl_engine_flag = false;
    }
    
    return handle->cfl_engine_flag;
}

/*==============================================================================
 * TREE WALKER CALLBACKS
 *============================================================================*/

static CT_ReturnCode cfl_execute_node(void* user_handle, unsigned int node_id, unsigned int level, 
    uint8_t* flags) {
    (void)flags;
    (void)level;
    
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t *)user_handle;
    
    if (!handle || !handle->flash_handle) {
        EXCEPTION("cfl_execute_node: invalid handle");
        return CT_STOP_ALL;
    }
    
    /* Validate node_id bounds */
    if (node_id >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_execute_node: node_id out of bounds");
        return CT_STOP_ALL;
    }
    
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    
    /* Validate function indices */
    if (node->main_function_index >= handle->flash_handle->main_function_count) {
        EXCEPTION("cfl_execute_node: main_function_index out of bounds");
        return CT_STOP_ALL;
    }
    if (node->init_function_index >= handle->flash_handle->one_shot_function_count) {
        EXCEPTION("cfl_execute_node: init_function_index out of bounds");
        return CT_STOP_ALL;
    }
    if (node->aux_function_index >= handle->flash_handle->boolean_function_count) {
        EXCEPTION("cfl_execute_node: aux_function_index out of bounds");
        return CT_STOP_ALL;
    }
    
    const main_function_t main_function = handle->flash_handle->main_functions[node->main_function_index];
    const one_shot_function_t one_shot_function = handle->flash_handle->one_shot_functions[node->init_function_index];
    const boolean_function_t boolean_function = handle->flash_handle->boolean_functions[node->aux_function_index];
    
    
    /* Check if node is enabled */
    if (cfl_engine_node_is_enabled(handle, node_id) == false) {
        
        return CT_SKIP_CHILDREN;
    }
    
    /* Node is executing - increment counter once per actual execution */
    handle->cfl_node_execution_count++;
    
    /* Initialize node if not already initialized */
    if (cfl_engine_node_is_initialized(handle, node_id) == false) {
        if (node->init_function_index != 0) {
            one_shot_function(handle, node_id);
        }

        if (node->aux_function_index != 0) {
            boolean_function(handle, node_id, CFL_EVENT_TYPE_NULL, CFL_INIT_EVENT, NULL);
        }
        cfl_set_node_initialization_flag(handle, node_id);
    }
    
    /* Validate event_data_ptr before dereferencing */
    if (!handle->event_data_ptr) {
        EXCEPTION("cfl_execute_node: event_data_ptr is NULL");
        return CT_STOP_ALL;
    }
    
    /* Execute main function */
    unsigned return_code = main_function(handle, node->aux_function_index, node_id, 
                                        handle->event_data_ptr->event_type,
                                        handle->event_data_ptr->event_id, 
                                        (void*)handle->event_data_ptr->data.ptr);
    
    handle->cfl_engine_flag = true;
    
    /* Process return code */
    switch (return_code) {
        case CFL_CONTINUE:
            return CT_CONTINUE;

        case CFL_HALT:
            return CT_STOP_SIBLINGS;
        
        case CFL_RESET:
            cfl_terminate_node_tree(handle, node->parent_index);
            cfl_reset_node_id(handle, node->parent_index);
            return CT_CONTINUE;

        case CFL_DISABLE:
            cfl_terminate_node_tree(handle, node_id);
            return CT_SKIP_CHILDREN;

        case CFL_SKIP_CONTINUE:
            return CT_SKIP_CHILDREN;

        case CFL_TERMINATE:
            if (node->parent_index != 0xffff) {
                cfl_terminate_node_tree(handle, node->parent_index);
                return CT_SKIP_CHILDREN;
            }
            
            cfl_disable_node(handle, node_id);
            return CT_STOP_ALL;
        
        case CFL_TERMINATE_SYSTEM:
            handle->cfl_engine_flag = false;
            cfl_terminate_node_tree(handle, handle->node_start_index);
            return CT_STOP_ALL;
 
        default:
            EXCEPTION("cfl_execute_node: invalid return code");
            return CT_STOP_ALL;
    }
}

static unsigned int cfl_get_forward_enabled_links(void* user_handle, unsigned int node_id, 
    unsigned int* links_out, unsigned int max_links) {
    
    cfl_runtime_handle_t *handle = (cfl_runtime_handle_t *)user_handle;
    
    if (!handle || !handle->flash_handle) {
        EXCEPTION("cfl_get_forward_enabled_links: invalid handle");
        return 0;
    }
    
    /* Validate node_id */
    if (node_id >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_get_forward_enabled_links: node_id out of bounds");
        return 0;
    }
    
    unsigned int return_value = 0;
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    uint16_t link_start = node->link_start;
    uint16_t link_count = (node->link_count & LINK_COUNT_MASK);
    
    /* Validate link_start and link_count */
    if (link_count > 0) {
        if (link_start >= handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_get_forward_enabled_links: link_start out of bounds");
            return 0;
        }
        if (link_start + link_count > handle->flash_handle->link_table_size) {
            EXCEPTION("cfl_get_forward_enabled_links: link range exceeds table size");
            return 0;
        }
    }
    
    const uint16_t *link_table = handle->flash_handle->link_table;

    for (unsigned i = 0; i < link_count; i++) {
        /* Check buffer space BEFORE writing */
        if (return_value >= max_links) {
            EXCEPTION("cfl_get_forward_enabled_links: max_links exceeded");
            return 0;
        }
        
        unsigned int link_id = link_table[link_start + i];
        
        /* Validate link_id */
        if (link_id >= handle->flash_handle->node_count) {
            EXCEPTION("cfl_get_forward_enabled_links: link_id out of bounds");
            return 0;
        }
        
        links_out[return_value] = link_id;
        return_value++;
    }
    
    return return_value;
}

/*==============================================================================
 * NODE STATE QUERIES
 *============================================================================*/

bool cfl_engine_node_is_enabled(cfl_runtime_handle_t *handle, unsigned node_index) {
    if (!handle || !handle->flags) {
        EXCEPTION("cfl_engine_node_is_enabled: invalid handle");
        return false;
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_engine_node_is_enabled: flash_handle is NULL");
        return false;
    }
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_engine_node_is_enabled: node_index out of bounds");
        return false;
    }
    
    volatile uint8_t *flags = handle->flags;
    if ((flags[node_index] & CT_FLAG_USER3) == 0) {
        return false;
    }
    return true;
}

bool cfl_engine_node_is_initialized(cfl_runtime_handle_t *handle, unsigned node_index) {
    if (!handle || !handle->flags) {
        EXCEPTION("cfl_engine_node_is_initialized: invalid handle");
        return false;
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_engine_node_is_initialized: flash_handle is NULL");
        return false;
    }
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_engine_node_is_initialized: node_index out of bounds");
        return false;
    }
    
    volatile uint8_t *flags = handle->flags;
    bool return_value = (flags[node_index] & CT_FLAG_USER2) && (flags[node_index] & CT_FLAG_USER3);
    
    return return_value;
}

/*==============================================================================
 * NODE TERMINATION
 *============================================================================*/

/* Used to mark nodes which will be terminated later */
static CT_ReturnCode cfl_mark_node_for_termination(void *handle, unsigned node_index,
    unsigned int level, uint8_t* flags) {
    (void)level;
    
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    
    if (!runtime_handle || !runtime_handle->backup_flags) {
        EXCEPTION("cfl_mark_node_for_termination: invalid handle");
        return CT_STOP_ALL;
    }
    
    if (!runtime_handle->flash_handle) {
        EXCEPTION("cfl_mark_node_for_termination: flash_handle is NULL");
        return CT_STOP_ALL;
    }
    
    /* Validate node_index before accessing arrays */
    if (node_index >= runtime_handle->flash_handle->node_count) {
        EXCEPTION("cfl_mark_node_for_termination: node_index out of bounds");
        return CT_STOP_ALL;
    }
    
    if ((flags[node_index] & (CT_FLAG_USER2 | CT_FLAG_USER3)) == (CT_FLAG_USER2 | CT_FLAG_USER3)) {
        runtime_handle->backup_flags[node_index] |= CT_FLAG_USER1;
    }
    
    return CT_CONTINUE;
}

static void cfl_disable_node(cfl_runtime_handle_t *handle, unsigned node_index) {
    if (!handle || !handle->flags || !handle->flash_handle) {
        EXCEPTION("cfl_disable_node: invalid handle");
        return;
    }
    
    /* Validate node_index */
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_disable_node: node_index out of bounds");
        return;
    }
    
    /* Only disable if node is initialized and enabled */
    if ((handle->flags[node_index] & (CT_FLAG_USER3 | CT_FLAG_USER2)) != 
        (CT_FLAG_USER3 | CT_FLAG_USER2)) {
        return;
    }
    
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_index];
    unsigned int termination_function_index = node->term_function_index;
    unsigned int aux_function_index = node->aux_function_index;
    
    /* Validate function indices */
    if (termination_function_index != 0) {
        if (termination_function_index >= handle->flash_handle->one_shot_function_count) {
            EXCEPTION("cfl_disable_node: termination_function_index out of bounds");
            return;
        }
        handle->flash_handle->one_shot_functions[termination_function_index](handle, node_index);
    }
    
    if (aux_function_index != 0) {
        if (aux_function_index >= handle->flash_handle->boolean_function_count) {
            EXCEPTION("cfl_disable_node: aux_function_index out of bounds");
            return;
        }
        handle->flash_handle->boolean_functions[aux_function_index](handle, node_index, 
            CFL_EVENT_TYPE_NULL, CFL_TERMINATE_EVENT, NULL);
    }
    
    cfl_disable_node_flag(handle, node_index);
}

void cfl_terminate_node_tree(cfl_runtime_handle_t *handle, unsigned node_id) {
    if (!handle || !handle->flash_handle) {
        EXCEPTION("cfl_terminate_node_tree: invalid handle");
        return;
    }
    
    /* Validate node_id */
    if (node_id >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_terminate_node_tree: node_id out of bounds");
        return;
    }
    
    const chaintree_node_t *node = &handle->flash_handle->nodes[node_id];
    
    if ((node->link_count & LINK_COUNT_MASK) == 0) {
        cfl_disable_node(handle, node_id);
        return;
    }
    
    if (!handle->walker || !handle->nested_stack || !handle->walker_context_ptr || 
        !handle->backup_flags) {
        EXCEPTION("cfl_terminate_node_tree: required handle members are NULL");
        return;
    }

    ct_walker_save_context(handle->walker, handle->walker_context_ptr, handle->backup_flags);
    
    /* Switch function and walk subtree */
    ct_walker_update_functions(handle->walker, cfl_mark_node_for_termination, 
        cfl_get_forward_enabled_links);
    
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
    
    /* Validate loop bounds to prevent underflow/overflow */
    if (handle->kb_node_count == 0) {
        return;  /* Nothing to terminate */
    }
    
    /* Check for overflow in range calculation */
    if (handle->kb_start_index > UINT_MAX - handle->kb_node_count) {
        EXCEPTION("cfl_terminate_node_tree: kb range calculation overflow");
        return;
    }
    
    unsigned int end_index = handle->kb_start_index + handle->kb_node_count - 1;
    
    /* Validate end_index is within bounds */
    if (end_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_terminate_node_tree: end_index out of bounds");
        return;
    }
    
    /* Ensure node_id is within the KB range */
    if (node_id < handle->kb_start_index) {
        EXCEPTION("cfl_terminate_node_tree: node_id below kb_start_index");
        return;
    }
    
    /* Terminate marked nodes in reverse order */
    for (int i = (int)end_index; i >= (int)node_id; i--) {
        if (handle->flags[i] & CT_FLAG_USER1) {
            handle->flags[i] &= ~CT_FLAG_USER1;
            cfl_disable_node(handle, i);
        }
    }
}

void cfl_reset_node_id(cfl_runtime_handle_t *handle, unsigned node_id) {
    if (!handle) {
        EXCEPTION("cfl_reset_node_id: handle is NULL");
        return;
    }
    
    cfl_enable_node(handle, node_id);
}

/*==============================================================================
 * FLAG MANIPULATION
 *============================================================================*/

static void cfl_disable_all_node_flags(cfl_runtime_handle_t *handle) {
    if (!handle || !handle->flags || !handle->flash_handle) {
        EXCEPTION("cfl_disable_all_node_flags: invalid handle");
        return;
    }
    
    volatile uint8_t *flags = handle->flags;
    for (unsigned i = 0; i < handle->flash_handle->node_count; i++) {
        flags[i] &= ~CT_FLAG_USER_MASK;
    }
}

void cfl_enable_node(cfl_runtime_handle_t *handle, unsigned node_index) {
    if (!handle || !handle->flags) {
        EXCEPTION("cfl_enable_node: invalid handle");
        return;
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_enable_node: flash_handle is NULL");
        return;
    }
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_enable_node: node_index out of bounds");
        return;
    }
    
    volatile uint8_t *flags = handle->flags;
    flags[node_index] &= ~CT_FLAG_USER_MASK;
    flags[node_index] |= CT_FLAG_USER3;
}

static void cfl_disable_node_flag(cfl_runtime_handle_t *handle, unsigned node_index) {
    if (!handle || !handle->flags) {
        EXCEPTION("cfl_disable_node_flag: invalid handle");
        return;
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_disable_node_flag: flash_handle is NULL");
        return;
    }
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_disable_node_flag: node_index out of bounds");
        return;
    }
    
    volatile uint8_t *flags = handle->flags;
    flags[node_index] &= ~CT_FLAG_USER_MASK;
}

static void cfl_set_node_initialization_flag(cfl_runtime_handle_t *handle, unsigned node_index) {
    if (!handle || !handle->flags) {
        EXCEPTION("cfl_set_node_initialization_flag: invalid handle");
        return;
    }
    if (!handle->flash_handle) {
        EXCEPTION("cfl_set_node_initialization_flag: flash_handle is NULL");
        return;
    }
    if (node_index >= handle->flash_handle->node_count) {
        EXCEPTION("cfl_set_node_initialization_flag: node_index out of bounds");
        return;
    }
    
    volatile uint8_t *flags = handle->flags;
    flags[node_index] |= CT_FLAG_USER2;
}