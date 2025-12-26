#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "cfl_s_engine_interface.h"
#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "json_node_decoder.h"
#include "cfl_common_functions.h"
#include "cfl_s_one_shot_functions.h"
#include "cfl_s_boolean_functions.h"
#include "cfl_s_main_functions.h"


static void* s_expr_malloc(void* handle, uint16_t ct_node_id, size_t size) {
    (void)ct_node_id;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    return cfl_heap_malloc_pointer(runtime_handle->heap, size);
}

static void s_expr_free(void* handle, uint16_t ct_node_id, void* ptr) {
    (void)ct_node_id;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    cfl_heap_free_pointer(runtime_handle->heap, ptr);
}

static s_expr_allocator_t s_expr_allocator = {
    .malloc = s_expr_malloc,
    .free = s_expr_free
};


// ============================================================================
// DEBUG FUNCTION
// ============================================================================

static void fn_debug(s_expr_tree_instance_t* inst, const char* message) {
    cfl_runtime_handle_t* runtime = (cfl_runtime_handle_t*)inst->handle;
    (void)runtime;
    uint16_t node_id = inst->ct_node_id;
    
    printf("  [node %u DBG] %s\n", node_id, message);
}


// ============================================================================
// S-ENGINE INITIALIZATION (Phase 1)
// ============================================================================

void cfl_initialize_s_engine(cfl_runtime_handle_t *handle, const s_expr_module_def_t *module_def) {
    // Allocate module struct from heap
    s_expr_module_t* mod = (s_expr_module_t*)cfl_heap_malloc_pointer(
        handle->heap, sizeof(s_expr_module_t)
    );
    
    if (!mod) {
        EXCEPTION("ERROR: Module allocation failed");
    }
    
    // Phase 1: Initialize module structure (no function resolution yet)
    uint8_t err = s_expr_module_init(mod, module_def, s_expr_allocator, handle);
    
    if (err != S_EXPR_MOD_OK) {
        printf("ERROR: %s\n", s_expr_module_error_str(err));
        cfl_heap_free_pointer(handle->heap, mod);
        EXCEPTION("S-Engine module initialization failed");
    }
    
    // Store module pointer
    handle->s_expr_module_ptr = mod;
    
    // Set debug function
    s_expr_module_set_debug(mod, fn_debug);
    
    // Phase 2a: Load system function tables
    uint16_t loaded_oneshot = s_expr_module_load_oneshot(mod, &system_oneshot);
    uint16_t loaded_boolean = s_expr_module_load_boolean(mod, &system_boolean);
    // uint16_t loaded_main = s_expr_module_load_main(mod, &system_main);
    
    printf("loaded_oneshot: %u\n", loaded_oneshot);
    printf("loaded_boolean: %u\n", loaded_boolean);
    printf("cfl_initialize_s_engine: module initialized\n");
}


// ============================================================================
// S-ENGINE VALIDATION (Phase 2b)
// ============================================================================

void cfl_s_engine_module_check(cfl_runtime_handle_t *handle) {
    s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_module_ptr;
    
    // Phase 2b: Resolve all function references
    uint8_t err = s_expr_module_validate(mod);
    
    if (err == S_EXPR_MOD_OK) {
        printf("cfl_s_engine_module_check: validation passed\n");
        return;
    }
    
    printf("ERROR: %s", s_expr_module_error_str(err));
    if (s_expr_module_get_error_name(mod)) {
        printf(" - '%s' (index %d)", 
               s_expr_module_get_error_name(mod),
               s_expr_module_get_error_index(mod));
    }
    printf("\n");
    
    EXCEPTION("S-Engine module validation failed");
}


// ============================================================================
// S-ENGINE CLEANUP
// ============================================================================

void cfl_deinitialize_s_engine(cfl_runtime_handle_t *handle) {
    if (handle->s_expr_module_ptr) {
        s_expr_module_t* mod = (s_expr_module_t*)handle->s_expr_module_ptr;
        s_expr_module_free(mod);
        cfl_heap_free_pointer(handle->heap, mod);
        handle->s_expr_module_ptr = NULL;
    }
}


// ============================================================================
// NODE FUNCTIONS (stubs - implement as needed)
// ============================================================================

void cfl_s_expression_node_init_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
    json_decoder_init_from_runtime(handle, node_index);
    json_print_node_data_runtime(handle, node_index);
    printf("cfl_s_expression_node_init_one_shot_fn\n");
    exit(0);
}

void cfl_s_expression_node_term_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
    (void)handle;
    (void)node_index;
    printf("cfl_s_expression_node_term_one_shot_fn\n");
    exit(0);
}

unsigned cfl_s_expression_node_main_main_fn(
    void *handle, 
    unsigned bool_function_index, 
    unsigned node_index, 
    unsigned event_type, 
    unsigned event_id, 
    void *event_data
) {
    (void)handle;
    (void)bool_function_index;
    (void)node_index;
    (void)event_type;
    (void)event_id;
    (void)event_data;
    printf("cfl_s_expression_node_main_main_fn\n");
    exit(0);
    return CFL_CONTINUE;
}