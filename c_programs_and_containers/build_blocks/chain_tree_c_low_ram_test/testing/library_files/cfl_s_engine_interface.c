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
#include "cfl_exception.h"


static void* s_expr_malloc(void* ctx, size_t size) {
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)ctx;
    return cfl_heap_malloc_pointer(runtime_handle->heap, size);
}

static void s_expr_free(void* ctx, void* ptr) {
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)ctx;
    cfl_heap_free_pointer(runtime_handle->heap, ptr);
}

static s_expr_allocator_t s_expr_allocator = {
    .malloc = s_expr_malloc,
    .free = s_expr_free,
    .ctx = NULL,  // Set at runtime
};

// ============================================================================
// DEBUG FUNCTION
// ============================================================================

static void fn_debug(s_expr_tree_instance_t* inst, const char* message) {
    uint16_t node_id = inst->ct_node_id;
    printf("  [node %u DBG] %s\n", node_id, message);
}

// ============================================================================
// S-ENGINE INITIALIZATION (Phase 1)
// ============================================================================

void cfl_initialize_s_engine(cfl_runtime_handle_t *handle, 
                             const s_expr_module_def_t* const* registry,
                             int registry_count) {
    
    if (!handle) {
        EXCEPTION("ERROR: Null handle");
        return;
    }
    printf("----*******************************>cfl_initialize_s_engine: registry_count=%d\n", registry_count);
    if (registry_count <= 0 || !registry) {
        EXCEPTION("ERROR: Empty or null registry");
        return;
    }
    
    // Set allocator context before use
    s_expr_allocator.ctx = handle;
    
    // Allocate array of module pointers
    s_expr_module_t** modules = (s_expr_module_t**)cfl_heap_malloc_pointer(
        handle->heap, sizeof(s_expr_module_t*) * registry_count
    );
    
    if (!modules) {
        EXCEPTION("ERROR: Module array allocation failed");
        return;
    }
    
    // Initialize each module
    for (int i = 0; i < registry_count; i++) {
        // Allocate module struct
        s_expr_module_t* mod = (s_expr_module_t*)cfl_heap_malloc_pointer(
            handle->heap, sizeof(s_expr_module_t)
        );
        
        if (!mod) {
            printf("ERROR: Module allocation failed for index %d\n", i);
            EXCEPTION("ERROR: Module allocation failed for index");
            continue;
        }
        
        // Phase 1: Initialize module structure
        uint8_t err = s_expr_module_init(mod, registry[i], s_expr_allocator);
        
        if (err != S_EXPR_ERR_OK) {
            printf("ERROR: code=%u (module %d, hash=0x%llX)\n", 
                   err, i, (unsigned long long)registry[i]->name_hash);
            cfl_heap_free_pointer(handle->heap, mod);
            EXCEPTION("S-Engine module initialization failed");
            continue;
        }
        
        // Set debug function
        s_expr_module_set_debug(mod, fn_debug);
        
        // Store in array
        modules[i] = mod;
    }
    
    // Store in handle
    handle->s_expr_modules = modules;
    handle->s_expr_module_count = registry_count;
    
    // Phase 2a: Load function tables
    cfl_load_boolean_s_functions(handle);
    cfl_load_oneshot_s_functions(handle);
    cfl_load_main_s_functions(handle);
}

// ============================================================================
// S-ENGINE CLEANUP
// ============================================================================

void cfl_deinitialize_s_engine(cfl_runtime_handle_t *handle) {
    if (!handle) {
        return;
    }
    
    if (handle->s_expr_modules) {
        for (int i = 0; i < handle->s_expr_module_count; i++) {
            if (handle->s_expr_modules[i]) {
                s_expr_module_free(handle->s_expr_modules[i]);
                cfl_heap_free_pointer(handle->heap, handle->s_expr_modules[i]);
                handle->s_expr_modules[i] = NULL;
            }
        }
        cfl_heap_free_pointer(handle->heap, handle->s_expr_modules);
        handle->s_expr_modules = NULL;
        handle->s_expr_module_count = 0;
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// ============================================================================
// Find module by name (computes hash, compares)
// ============================================================================

s_expr_module_t* cfl_find_module(cfl_runtime_handle_t* handle, const char* module_name) {
    if (!handle || !handle->s_expr_modules || !module_name) {
        return NULL;
    }
    
    s_expr_hash_t name_hash = s_expr_hash(module_name);
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        if (modules[i]->def->name_hash == name_hash) {
            return modules[i];
        }
    }
    printf("cfl_find_module: module not found 0x%llX\n", (unsigned long long)name_hash);
    EXCEPTION("cfl_find_module: module not found\n");
    return NULL;
}

// ============================================================================
// Find module by hash directly
// ============================================================================

s_expr_module_t* cfl_find_module_by_hash(cfl_runtime_handle_t* handle, s_expr_hash_t name_hash) {
    if (!handle || !handle->s_expr_modules) {
        return NULL;
    }
    
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        if (modules[i]->def->name_hash == name_hash) {
            return modules[i];
        }
    }
    printf("cfl_find_module_by_hash: module not found 0x%llX\n", (unsigned long long)name_hash);
    EXCEPTION("cfl_find_module_by_hash: module not found\n");
    return NULL;
}

// ============================================================================
// Get module by index
// ============================================================================

s_expr_module_t* cfl_get_module(cfl_runtime_handle_t* handle, int index) {
    if (!handle || !handle->s_expr_modules) {
        EXCEPTION("cfl_get_module: invalid handle");
        return NULL;
    }
    
    if (index < 0 || index >= handle->s_expr_module_count) {
        printf("cfl_get_module: invalid index %d\n", index);
        EXCEPTION("cfl_get_module: invalid index\n");
        return NULL;
    }
    
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    return modules[index];
}

// ============================================================================
// Get module count
// ============================================================================

int cfl_get_module_count(cfl_runtime_handle_t* handle) {
    if (!handle) {
        EXCEPTION("cfl_get_module_count: invalid handle");
        return -1;
    }
    if (handle->s_expr_module_count == 0) {
        EXCEPTION("cfl_get_module_count: no modules");
        return -1;
    }
    return handle->s_expr_module_count;
}

// ============================================================================
// Module validation - check all functions are registered
// ============================================================================

void cfl_s_engine_module_check(cfl_runtime_handle_t* handle) {
    if (!handle || !handle->s_expr_modules) {
        EXCEPTION("cfl_s_engine_module_check: invalid handle");
        return;
    }
    
    int total_missing = 0;
    
    s_expr_module_t** modules = (s_expr_module_t**)handle->s_expr_modules;
    
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        s_expr_module_t* mod = modules[i];
        
        if (!mod) {
            printf("ERROR: Module %d is NULL\n", i);
            total_missing++;
            continue;
        }
        
        printf("Checking module %d (hash=0x%llX)...\n", i, (unsigned long long)mod->def->name_hash);
        
        int module_missing = 0;
        
        // Check all main functions
        for (uint16_t j = 0; j < mod->def->main_count; j++) {
            if (!mod->main_fns || !mod->main_fns[j]) {
                s_expr_hash_t fn_hash = mod->def->main_hashes[j];
                printf("  MISSING MAIN: hash=0x%llX (index=%u)\n", (unsigned long long)fn_hash, j);
                module_missing++;
            }
        }
        
        // Check all oneshot functions
        for (uint16_t j = 0; j < mod->def->oneshot_count; j++) {
            if (!mod->oneshot_fns || !mod->oneshot_fns[j]) {
                s_expr_hash_t fn_hash = mod->def->oneshot_hashes[j];
                printf("  MISSING ONESHOT: hash=0x%llX (index=%u)\n", (unsigned long long)fn_hash, j);
                module_missing++;
            }
        }
        
        // Check all predicate functions
        for (uint16_t j = 0; j < mod->def->pred_count; j++) {
            if (!mod->pred_fns || !mod->pred_fns[j]) {
                s_expr_hash_t fn_hash = mod->def->pred_hashes[j];
                printf("  MISSING PRED: hash=0x%llX (index=%u)\n", (unsigned long long)fn_hash, j);
                module_missing++;
            }
        }
        
        if (module_missing > 0) {
            printf("  Module %d: %d missing functions\n", i, module_missing);
            total_missing += module_missing;
        } else {
           ;
        }
    }
    
    if (total_missing > 0) {
        printf("\n========================================\n");
        printf("S-Engine validation FAILED\n");
        printf("Total missing functions: %d\n", total_missing);
        printf("========================================\n");
        EXCEPTION("S-Engine validation failed");
    }
    
   
}
// ============================================================================
// NODE FUNCTIONS
// ============================================================================

void cfl_s_expression_node_init_one_shot_fn(cfl_runtime_handle_t* handle, uint16_t node_index) {
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_smart_arena_alloc(
        handle, node_index, sizeof(s_expr_tree_instance_t*)
    );
    if (!tree_inst_ptr) {
        EXCEPTION("failed to allocate tree instance");
        return;
    }
    
    json_decoder_init_from_runtime(handle, node_index);
    
    const char* module_name;
    const char* tree_name;
    json_extract_string_runtime(handle, "node_dict.column_data.module_name", &module_name);
    json_extract_string_runtime(handle, "node_dict.column_data.tree_name", &tree_name);

    // Find the module by name (computes hash internally)
    s_expr_module_t* mod = cfl_find_module(handle, module_name);
    if (!mod) {
        printf("module not found: %s\n", module_name);
        EXCEPTION("module not found");
        return;
    }
    
    // Compute tree name hash
    s_expr_hash_t tree_hash = s_expr_hash(tree_name);
    
    // Find tree index by hash
    uint16_t tree_index = UINT16_MAX;
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
        if (mod->def->trees[i].name_hash == tree_hash) {
            tree_index = i;
            break;
        }
    }
    
    if (tree_index == UINT16_MAX) {
        printf("tree not found: %s (hash=0x%llX) in module (hash=0x%llX)\n", 
               tree_name, (unsigned long long)tree_hash, (unsigned long long)mod->def->name_hash);
        EXCEPTION("tree not found");
        return;
    }
    
    // Create tree instance
    s_expr_tree_instance_t* tree_inst = s_expr_tree_create(
        mod,
        tree_index,
        node_index       // ct_node_id
    );
    
    if (!tree_inst) {
        printf("failed to create tree instance: %s\n", tree_name);
        EXCEPTION("failed to create tree instance");
        return;
    }
    
    // Set runtime handle as user context
    s_expr_tree_set_user_ctx(tree_inst, handle);
    
    *tree_inst_ptr = tree_inst;
    
    printf("tree instance created: %s (hash=0x%llX)\n", tree_name, (unsigned long long)tree_hash);
}

void cfl_s_expression_node_term_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(
        handle->arena_system, node_index
    );
    s_expr_tree_instance_t* tree_inst = *tree_inst_ptr;
    if (tree_inst) {
        // Send terminate events to all active nodes
        s_expr_tree_terminate(tree_inst);
        
        // Free the instance
        s_expr_tree_free(tree_inst);
    }
}

unsigned cfl_s_expression_node_main_main_fn(
    void *handle, 
    unsigned bool_function_index, 
    unsigned node_index, 
    unsigned event_type, 
    unsigned event_id, 
    void *event_data
) {
    (void)bool_function_index;
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(
        runtime_handle->arena_system, node_index
    );
    s_expr_tree_instance_t* tree_inst = *tree_inst_ptr;
    if (!tree_inst) {
        EXCEPTION("failed to locate tree instance");
    }
    
    unsigned result = s_expr_tree_tick(tree_inst, event_id, event_data);
    
    switch(result) {
        case SE_CONTINUE:
            return CFL_CONTINUE;
            
        case SE_TERMINATE:
            return CFL_TERMINATE;
            
        case SE_RESET:
            return CFL_RESET;
            
        case SE_DISABLE:
            return CFL_DISABLE;
            
        case SE_HALT:
            return CFL_HALT;

        case SE_SKIP_CONTINUE:
            return CFL_SKIP_CONTINUE;

        case SE_FUNCTION_HALT:
            return CFL_CONTINUE;

        case SE_FUNCTION_RESET:
            // Keep EVER_INIT flags, clear INITIALIZED
            s_expr_tree_reset(tree_inst);
            return CFL_CONTINUE;

        case SE_FUNCTION_TERMINATE:
            return CFL_DISABLE;
        
        default:
            EXCEPTION("cfl_s_expression_node_main_main_fn: invalid result");
            return CFL_TERMINATE_SYSTEM;
    }
}

// ============================================================================
// LINK FUNCTIONS
// ============================================================================

void cfl_s_expression_link_init_one_shot_fn(cfl_runtime_handle_t* handle, uint16_t node_index) {
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_smart_arena_alloc(
        handle, node_index, sizeof(s_expr_tree_instance_t*)
    );
    if (!tree_inst_ptr) {
        EXCEPTION("failed to allocate tree instance");
        return;
    }
    
    json_decoder_init_from_runtime(handle, node_index);
    
    const char* module_name;
    const char* tree_name;
    json_extract_string_runtime(handle, "node_dict.column_data.module_name", &module_name);
    json_extract_string_runtime(handle, "node_dict.column_data.tree_name", &tree_name);

    // Find the module by name (computes hash internally)
    s_expr_module_t* mod = cfl_find_module(handle, module_name);
    if (!mod) {
        printf("module not found: %s\n", module_name);
        EXCEPTION("module not found");
        return;
    }
    
    // Compute tree name hash
    s_expr_hash_t tree_hash = s_expr_hash(tree_name);
    
    // Find tree index by hash
    uint16_t tree_index = UINT16_MAX;
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
        if (mod->def->trees[i].name_hash == tree_hash) {
            tree_index = i;
            break;
        }
    }
    
    if (tree_index == UINT16_MAX) {
        printf("tree not found: %s (hash=0x%llX) in module (hash=0x%llX)\n", 
               tree_name, (unsigned long long)tree_hash, (unsigned long long)mod->def->name_hash);
        EXCEPTION("tree not found");
        return;
    }
    
    // Create tree instance
    s_expr_tree_instance_t* tree_inst = s_expr_tree_create(
        mod,
        tree_index,
        node_index       // ct_node_id
    );
    
    if (!tree_inst) {
        printf("failed to create tree instance: %s\n", tree_name);
        EXCEPTION("failed to create tree instance");
        return;
    }
    
    // Set runtime handle as user context
    s_expr_tree_set_user_ctx(tree_inst, handle);
    
    *tree_inst_ptr = tree_inst;
    
    
}

void cfl_s_expression_link_term_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(
        handle->arena_system, node_index
    );
    s_expr_tree_instance_t* tree_inst = *tree_inst_ptr;
    if (tree_inst) {
        // Send terminate events to all active nodes
        s_expr_tree_terminate(tree_inst);
        
        // Free the instance
        s_expr_tree_free(tree_inst);
    }
}

unsigned cfl_s_expression_link_main_main_fn(
    void *handle, 
    unsigned bool_function_index, 
    unsigned node_index, 
    unsigned event_type, 
    unsigned event_id, 
    void *event_data
) {
    (void)bool_function_index;
    (void)event_type;
    cfl_runtime_handle_t *runtime_handle = (cfl_runtime_handle_t *)handle;
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(
        runtime_handle->arena_system, node_index
    );
    s_expr_tree_instance_t* tree_inst = *tree_inst_ptr;
    if (!tree_inst) {
        EXCEPTION("failed to locate tree instance");
    }
    
    unsigned result = s_expr_tree_tick(tree_inst, event_id, event_data);
    
    switch(result) {
        case SE_CONTINUE:
            return CFL_CONTINUE;

        case SE_TERMINATE:
            return CFL_TERMINATE;

        case SE_RESET:
            return CFL_RESET;

        case SE_DISABLE:
            return CFL_DISABLE;
            
        case SE_HALT:
            return CFL_HALT;

        case SE_SKIP_CONTINUE:
            return CFL_SKIP_CONTINUE;

        case SE_FUNCTION_HALT:
            return CFL_CONTINUE;

        case SE_FUNCTION_RESET:
            // Keep EVER_INIT flags, clear INITIALIZED
            s_expr_tree_reset(tree_inst);
            return CFL_CONTINUE;

        case SE_FUNCTION_TERMINATE:
            return CFL_DISABLE;
        
        default:
            EXCEPTION("cfl_s_expression_link_main_main_fn: invalid result");
            return CFL_TERMINATE_SYSTEM;
    }
}