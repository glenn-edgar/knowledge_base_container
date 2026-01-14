// ============================================================================
// s_expr_dsl_test_main.c
// Test application for S-Expression Engine
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "s_engine_types.h"
#include "s_engine_init.h"
#include "s_engine_eval.h"
#include "s_expr_dsl_test.h"
#include "s_expr_dsl_test_bin_32.h"
#include "s_expr_dsl_test_records.h"

// Forward declare generated registration
void s_expr_dsl_test_register_all(s_expr_module_t* module);

// ============================================================================
// SIMPLE ALLOCATOR
// ============================================================================

static void* simple_malloc(void* ctx, size_t size) {
    (void)ctx;
    return malloc(size);
}

static void simple_free(void* ctx, void* ptr) {
    (void)ctx;
    free(ptr);
}

// ============================================================================
// DEBUG CALLBACK
// ============================================================================

static void debug_callback(s_expr_tree_instance_t* inst, const char* msg) {
    (void)inst;
    printf("[DEBUG] %s\n", msg);
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    printf("=== S-Expression Engine Test ===\n\n");
    
    (void)argc;
    (void)argv;
    
    // Setup allocator
    s_expr_allocator_t alloc = {
        .malloc = simple_malloc,
        .free = simple_free,
        .ctx = NULL
    };
    
    // Initialize engine from ROM binary
    s_engine_handle_t engine;
    uint8_t err = s_engine_init_from_rom(
        &engine,
        s_expr_dsl_test_module_bin_32,
        S_EXPR_DSL_TEST_MODULE_BIN_32_SIZE,
        alloc,
        NULL  // user context
    );
    
    if (err != S_EXPR_ERR_OK) {
        printf("ERROR: Failed to init engine: %s\n", s_engine_error_str(&engine));
        return 1;
    }
    
    printf("Module loaded successfully\n");
    printf("  Trees: %d\n", engine.module.def->tree_count);
    printf("  Records: %d\n", engine.module.def->record_count);
    printf("  Strings: %d\n", engine.module.def->string_count);
    printf("  Oneshot: %d\n", engine.module.def->oneshot_count);
    printf("  Main: %d\n", engine.module.def->main_count);
    printf("  Pred: %d\n", engine.module.def->pred_count);
    printf("\n");
    
    // Register built-in functions
    s_engine_register_builtins(&engine);
    
    // Register user functions
    s_expr_dsl_test_register_all(&engine.module);
    
    // Set debug callback
    s_expr_module_set_debug(&engine.module, debug_callback);
    
    // Validate all functions resolved
    err = s_engine_validate(&engine);
    if (err != S_EXPR_ERR_OK) {
        printf("ERROR: Validation failed: %s\n", s_expr_error_str(err));
        printf("  Missing hash: 0x%08X\n", engine.module.error_hash);
        s_engine_free(&engine);
        return 1;
    }
    
    printf("All functions registered successfully\n\n");
    
    // Create tree instance
    printf("=== Creating tree: TEST_ALL_CALL_TYPES ===\n");
    s_expr_tree_instance_t* tree = s_engine_create_tree_by_hash(
        &engine,
        TEST_ALL_CALL_TYPES_HASH,
        0
    );
    
    if (!tree) {
        printf("ERROR: Failed to create tree\n");
        s_engine_free(&engine);
        return 1;
    }
    
    printf("Tree created: node_count=%d, pointer_count=%d\n",
           tree->node_count, tree->pointer_count);
    
    // Get blackboard
    test_blackboard_t* bb = (test_blackboard_t*)s_expr_tree_get_blackboard(tree);
    if (bb) {
        printf("Blackboard allocated: size=%d\n", tree->blackboard_size);
        bb->enabled = true;
        bb->state = 0;
        bb->counter = 0;
    }
    
    printf("\n=== Running tree ticks ===\n");
    
    // Run a few ticks
    for (int i = 0; i < 5; i++) {
        printf("\n--- Tick %d ---\n", i);
        
        double delta_time = 0.016;  // ~60fps
        s_expr_result_t result = s_expr_tree_tick(tree, 0, &delta_time);
        
        printf("Tick result: %d\n", result);
        
        if (result == SE_FUNCTION_TERMINATE) {
            printf("Tree terminated\n");
            break;
        }
        
        if (bb) {
            printf("Blackboard: state=%d counter=%d enabled=%d\n",
                   bb->state, bb->counter, bb->enabled);
        }
    }
    
    printf("\n=== Testing other trees ===\n");
    
    // Test composable predicates tree
    printf("\n--- TEST_COMPOSABLE_PREDICATES ---\n");
    s_expr_tree_instance_t* pred_tree = s_engine_create_tree_by_hash(
        &engine,
        TEST_COMPOSABLE_PREDICATES_HASH,   // <-- FIXED
        0
    );
    
    if (pred_tree) {
        double dt = 0.016;
        s_expr_result_t result = s_expr_tree_tick(pred_tree, 0, &dt);
        printf("Result: %d\n", result);
    }
    
    // Test pipeline and delays tree
    printf("\n--- TEST_PIPELINE_AND_DELAYS ---\n");
    s_expr_tree_instance_t* pt_tree = s_engine_create_tree_by_hash(
        &engine,
        TEST_PIPELINE_AND_DELAYS_HASH,     // <-- FIXED
        0
    );
    
    if (pt_tree) {
        for (int i = 0; i < 10; i++) {
            double dt = 0.016;
            s_expr_result_t result = s_expr_tree_tick(pt_tree, 0, &dt);
            printf("Tick %d: result=%d\n", i, result);
            if (result == SE_FUNCTION_TERMINATE) break;
        }
    }
    
    // Cleanup
    printf("\n=== Cleanup ===\n");
    s_engine_free(&engine);
    printf("Done.\n");
    
    return 0;
}