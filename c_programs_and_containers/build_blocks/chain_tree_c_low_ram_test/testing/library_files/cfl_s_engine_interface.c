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

void cfl_initialize_s_engine(cfl_runtime_handle_t *handle, 
                             const s_expr_module_def_t* const* registry,
                             int registry_count) {
    
    if (!handle) {
        EXCEPTION("ERROR: Null handle");
    }
    
    if (registry_count <= 0 || !registry) {
        EXCEPTION("ERROR: Empty or null registry");
    }
    
    // Allocate array of module pointers
    s_expr_module_t** modules = (s_expr_module_t**)cfl_heap_malloc_pointer(
        handle->heap, sizeof(s_expr_module_t*) * registry_count
    );
    
    if (!modules) {
        EXCEPTION("ERROR: Module array allocation failed");
    }
    
    // Initialize each module
    for (int i = 0; i < registry_count; i++) {
        // Allocate module struct
        s_expr_module_t* mod = (s_expr_module_t*)cfl_heap_malloc_pointer(
            handle->heap, sizeof(s_expr_module_t)
        );
        
        if (!mod) {
            printf("ERROR: Module allocation failed for index %d\n", i);
            EXCEPTION("ERROR: Module allocation failed for index ");
        }
        
        // Phase 1: Initialize module structure
        uint8_t err = s_expr_module_init(mod, registry[i], s_expr_allocator, handle);
        
        if (err != S_EXPR_MOD_OK) {
            printf("ERROR: %s (module %d: %s)\n", 
                   s_expr_module_error_str(err), i, registry[i]->name);
            cfl_heap_free_pointer(handle->heap, mod);
            EXCEPTION("S-Engine module initialization failed");
        }
        
        // Set debug function
        s_expr_module_set_debug(mod, fn_debug);
        
        // Store in array
        modules[i] = mod;
    }
    
    // Store in handle
    handle->s_expr_modules = modules;
    handle->s_expr_module_count = registry_count;
    
    // Phase 2a: Load function tables (shared across all modules)
    cfl_load_oneshot_s_functions(handle);
    cfl_load_boolean_s_functions(handle);
    cfl_load_main_s_functions(handle);
}


// ============================================================================
// S-ENGINE VALIDATION (Phase 2b)
// ============================================================================

void cfl_s_engine_module_check(cfl_runtime_handle_t *handle) {
    if (!handle || !handle->s_expr_modules) {
        EXCEPTION("cfl_s_engine_module_check: invalid handle");
    }
    
    int failed_count = 0;
    int pool_warning_count = 0;
    
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        s_expr_module_t* mod = handle->s_expr_modules[i];
        
        if (!mod) {
            printf("ERROR: Module %d is NULL\n", i);
            failed_count++;
            continue;
        }
        
        // Check pool table
        if (!mod->pool_table || mod->pool_count == 0) {
            printf("WARNING: Module %d (%s) has no pool table set\n",
                   i, mod->def->name);
            pool_warning_count++;
        } else {
           //printf("cfl_s_engine_module_check: module %d (%s) pool table OK (%d pools)\n",
             //      i, mod->def->name, mod->pool_count);
        }
        
        // Validate function resolution
        uint8_t err = s_expr_module_validate(mod);
        
        if (err == S_EXPR_MOD_OK) {
           // printf("cfl_s_engine_module_check: module %d (%s) functions OK\n",
            //       i, mod->def->name);
            continue;
        }
        
        failed_count++;
        printf("ERROR: Module %d (%s): %s", i, mod->def->name, s_expr_module_error_str(err));
        if (s_expr_module_get_error_name(mod)) {
            printf(" - '%s' (index %d)", 
                   s_expr_module_get_error_name(mod),
                   s_expr_module_get_error_index(mod));
        }
        printf("\n");
    }
    
    if (failed_count > 0) {
        printf("S-Engine validation failed: %d of %d modules\n", 
               failed_count, handle->s_expr_module_count);
        EXCEPTION("S-Engine validation failed");
    }
    
   // printf("cfl_s_engine_module_check: all %d modules validated", 
    //       handle->s_expr_module_count);
    if (pool_warning_count > 0) {
        EXCEPTION("cfl_s_engine_module_check: some modules have no pool tables");
    }
    
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
// POOL TABLE REGISTRATION
// ============================================================================

void cfl_set_pool_table(cfl_runtime_handle_t *handle, 
                        const char* module_name,
                        void** pool_table, 
                        uint16_t pool_count) {
    if (!handle || !handle->s_expr_modules || !module_name) {
        EXCEPTION("cfl_set_pool_table: invalid parameters");
    }
    
    if (!pool_table || pool_count == 0) {
        EXCEPTION("cfl_set_pool_table: invalid pool table");
    }
    
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        const char* name = s_expr_module_get_name(handle->s_expr_modules[i]);
        if (name && strcmp(name, module_name) == 0) {
            s_expr_module_set_pool_table(handle->s_expr_modules[i], pool_table, pool_count);
            
            return;
        }
    }
    
    printf("cfl_set_pool_table: module '%s' not found\n", module_name);
    EXCEPTION("cfl_set_pool_table: module not found");
}


// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

s_expr_module_t* cfl_find_module(cfl_runtime_handle_t *handle, const char* module_name) {
    if (!handle || !handle->s_expr_modules || !module_name) {
        return NULL;
    }
    
    for (int i = 0; i < handle->s_expr_module_count; i++) {
        const char* name = s_expr_module_get_name(handle->s_expr_modules[i]);
        if (name && strcmp(name, module_name) == 0) {
            return handle->s_expr_modules[i];
        }
    }
    
    EXCEPTION("cfl_find_module: module not found");
    return NULL;
}

s_expr_module_t* cfl_get_module(cfl_runtime_handle_t *handle, int index) {
    if (!handle || !handle->s_expr_modules) {
         EXCEPTION("cfl_get_module: invalid handle");
         return NULL;
    }
    
    if (index < 0 || index >= handle->s_expr_module_count) {
        EXCEPTION("cfl_get_module: invalid index");
        return NULL;
    }
    
    return handle->s_expr_modules[index];
}

int cfl_get_module_count(cfl_runtime_handle_t *handle) {
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
// NODE FUNCTIONS (stubs - implement as needed)
// ============================================================================

void cfl_s_expression_node_init_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_smart_arena_alloc(handle, node_index, sizeof(s_expr_tree_instance_t*));
    if (!tree_inst_ptr) {
        EXCEPTION("failed to allocate tree instance");
    }
    
    json_decoder_init_from_runtime(handle, node_index);
    
    
    
    const char *module_name;
    const char *tree_name;
    json_extract_string_runtime(handle, "node_dict.column_data.module_name", &module_name);
    json_extract_string_runtime(handle, "node_dict.column_data.tree_name", &tree_name);

 
    // Find the module
    s_expr_module_t* mod = cfl_find_module(handle, module_name);
    if (!mod) {
        printf("module not found: %s\n", module_name);
        EXCEPTION("module not found");
    }
    
    // Find tree index by name
    uint16_t tree_index = UINT16_MAX;
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
    
        if (strcmp(mod->def->trees[i].name, tree_name) == 0) {
            tree_index = i;
            break;
        }
    }
    
    if (tree_index == UINT16_MAX) {
        printf("tree not found: %s in module %s\n", tree_name, module_name);
        EXCEPTION("tree not found");
    }
    
    
    
    // Create tree instance
    s_expr_tree_instance_t* tree_inst = s_expr_tree_create(
        mod,
        tree_index,
        handle,          // runtime handle
        node_index       // ct_node_id
    );
    *tree_inst_ptr = tree_inst;
    if (!tree_inst) {
        printf("failed to create tree instance: %s\n", tree_name);
        EXCEPTION("failed to create tree instance");
    }
    
    printf("tree instance created: %s\n", tree_name);
    // Store tree instance in node's private data
   
}

void cfl_s_expression_node_term_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
      
        s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(handle->arena_system, node_index);
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
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
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
            return CFL_CONTINUE;

        case SE_SKIP_CONTINUE:
            return CFL_SKIP_CONTINUE;

        case SE_FUNCTION_HALT:
            return CFL_CONTINUE;

        case SE_FUNCTION_RESET:
        
            // keep never initialize flags;
             s_expr_tree_reset(tree_inst);
            return CFL_CONTINUE;


        case SE_FUNCTION_TERMINATE:
          
            // reset never initialize flags
            //s_expr_tree_full_terminate(tree_inst);
           
            return CFL_DISABLE;
        
        default:
            EXCEPTION("cfl_s_expression_node_main_main_fn: invalid result");
            return CFL_TERMINATE_SYSTEM;
    }
   
    EXCEPTION("cfl_s_expression_node_main_main_fn: invalid result");
    return CFL_TERMINATE_SYSTEM;
}

#if 0
ttypedef enum {
    SE_CONTINUE           = 0,
    SE_HALT               = 1,
    SE_TERMINATE          = 2,
    SE_RESET              = 3,
    SE_DISABLE            = 4,
    SE_FUNCTION_TERMINATE = 5,
    SE_SKIP_CONTINUE      = 6,  // Skip remaining siblings, return CONTINUE to parent
    SE_FUNCTION_HALT      = 7,  // Function-level halt, propagates up
    SE_FUNCTION_RESET     = 8,  // Function-level reset, propagates up
} s_expr_result_t;
#endif




void cfl_s_expression_link_init_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_smart_arena_alloc(handle, node_index, sizeof(s_expr_tree_instance_t*));
    if (!tree_inst_ptr) {
        EXCEPTION("failed to allocate tree instance");
    }
    
    json_decoder_init_from_runtime(handle, node_index);
    
    
    
    
    const char *module_name;
    const char *tree_name;
    json_extract_string_runtime(handle, "node_dict.module_name", &module_name);
    json_extract_string_runtime(handle, "node_dict.tree_name", &tree_name);

 
    // Find the module
    s_expr_module_t* mod = cfl_find_module(handle, module_name);
    if (!mod) {
        printf("module not found: %s\n", module_name);
        EXCEPTION("module not found");
    }
    
    // Find tree index by name
    uint16_t tree_index = UINT16_MAX;
    for (uint16_t i = 0; i < mod->def->tree_count; i++) {
    
        if (strcmp(mod->def->trees[i].name, tree_name) == 0) {
            tree_index = i;
            break;
        }
    }
    
    if (tree_index == UINT16_MAX) {
        printf("tree not found: %s in module %s\n", tree_name, module_name);
        EXCEPTION("tree not found");
    }
    
    
    
    // Create tree instance
    s_expr_tree_instance_t* tree_inst = s_expr_tree_create(
        mod,
        tree_index,
        handle,          // runtime handle
        node_index       // ct_node_id
    );
    *tree_inst_ptr = tree_inst;
    if (!tree_inst) {
        printf("failed to create tree instance: %s\n", tree_name);
        EXCEPTION("failed to create tree instance");
    }
    
    printf("tree instance created: %s\n", tree_name);
    // Store tree instance in node's private data
   
}

void cfl_s_expression_link_term_one_shot_fn(cfl_runtime_handle_t *handle, uint16_t node_index) {
      
        s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(handle->arena_system, node_index);
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
    s_expr_tree_instance_t** tree_inst_ptr = (s_expr_tree_instance_t**)cfl_heap_arena_get_node_ptr(runtime_handle->arena_system, node_index);
    s_expr_tree_instance_t* tree_inst = *tree_inst_ptr;
    if (!tree_inst) {
        EXCEPTION("failed to locate tree instance");
    }
    
    unsigned result = s_expr_tree_tick(tree_inst, event_id, event_data);
    //printf("cfl_s_expression_link_main_main_fn: result: %d\n", result);
    
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
            return CFL_HALT;

        case SE_FUNCTION_HALT:
            return CFL_HALT;

        case SE_FUNCTION_RESET:
        
            // keep never initialize flags;
             s_expr_tree_reset(tree_inst);
            return CFL_HALT;


        case SE_FUNCTION_TERMINATE:
             return CFL_DISABLE;
            // reset never initialize flags
            //s_expr_tree_full_terminate(tree_inst);
           
            return CFL_DISABLE;
        
        default:
            EXCEPTION("cfl_s_expression_node_main_main_fn: invalid result");
            return CFL_TERMINATE_SYSTEM;
    }
   
    EXCEPTION("cfl_s_expression_node_main_main_fn: invalid result");
    return CFL_TERMINATE_SYSTEM;
}

#if 0
ttypedef enum {
    SE_CONTINUE           = 0,
    SE_HALT               = 1,
    SE_TERMINATE          = 2,
    SE_RESET              = 3,
    SE_DISABLE            = 4,
    SE_FUNCTION_TERMINATE = 5,
    SE_SKIP_CONTINUE      = 6,  // Skip remaining siblings, return CONTINUE to parent
    SE_FUNCTION_HALT      = 7,  // Function-level halt, propagates up
    SE_FUNCTION_RESET     = 8,  // Function-level reset, propagates up
} s_expr_result_t;
#endif