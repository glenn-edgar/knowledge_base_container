#define _GNU_SOURCE
#include <time.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "s_engine_loader.h"
#include "s_engine_init.h"
#include "s_engine_builtins.h"
#include "s_engine_node.h"

#include "state_machine_test.h"
#include "state_machine_test_bin_32.h"
#include "state_machine_test_records.h"

extern void state_machine_test_register_all(s_expr_module_t* module); // loading user functions

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
// DEBUG/ERROR CALLBACKS
// ============================================================================

static int g_test_errors = 0;

static void debug_callback(s_expr_tree_instance_t* inst, const char* msg) {
    (void)inst;
    printf("  [DEBUG] %s\n", msg);
}

static void error_callback(s_expr_tree_instance_t* inst, uint8_t error_code, const char* msg) {
    (void)inst;
    printf("  [ERROR %d] %s\n", error_code, msg);
    g_test_errors++;
}

// ============================================================================
// RESULT HELPERS
// ============================================================================

static const char* result_to_str(s_expr_result_t r) {
    switch (r) {
        case SE_CONTINUE:           return "CONTINUE";
        case SE_HALT:               return "HALT";
        case SE_TERMINATE:          return "TERMINATE";
        case SE_RESET:              return "RESET";
        case SE_DISABLE:            return "DISABLE";
        case SE_FUNCTION_TERMINATE: return "FUNCTION_TERMINATE";
        case SE_SKIP_CONTINUE:      return "SKIP_CONTINUE";
        case SE_FUNCTION_HALT:      return "FUNCTION_HALT";
        case SE_FUNCTION_RESET:     return "FUNCTION_RESET";
        case SE_PIPELINE_TERMINATE: return "PIPELINE_TERMINATE";
        case SE_PIPELINE_RESET_CONTINUE: return "PIPELINE_RESET_CONTINUE";
        case SE_PIPELINE_RESET_HALT: return "PIPELINE_RESET_HALT";
        default:                    return "UNKNOWN";
    }
}

// Linux monotonic time
static double linux_get_time(void* ctx) {
    (void)ctx;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

// ============================================================================
// ENGINE LOADING
// ============================================================================

static bool load_from_rom(s_engine_handle_t* engine, s_expr_allocator_t* alloc, 
                          const uint8_t* binary_data, size_t binary_size) {
    printf("=== Initializing Engine from ROM ===\n");
    
    memset(engine, 0, sizeof(s_engine_handle_t));
    
    uint8_t err = s_engine_init_from_rom(
        engine,
        binary_data,
        binary_size,
        *alloc,
        NULL
    );
    
    if (err != S_EXPR_ERR_OK) {
        printf("❌ FATAL: Failed to init engine: %s\n", s_engine_error_str(engine));
        return false;
    }
    
    printf("✅ Module loaded successfully\n");
    printf("   Trees:    %d\n", engine->module.def->tree_count);
    printf("   Records:  %d\n", engine->module.def->record_count);
    printf("   Strings:  %d\n", engine->module.def->string_count);
    printf("   Oneshot:  %d\n", engine->module.def->oneshot_count);
    printf("   Main:     %d\n", engine->module.def->main_count);
    printf("   Pred:     %d\n", engine->module.def->pred_count);

    // Register functions
    printf("\n=== Registering Functions ===\n");
    
    s_engine_register_builtins(engine);
    printf("✅ Built-in functions registered\n");
    
   // loading user functions
   state_machine_test_register_all(&engine->module);
   printf("✅ User functions registered\n");
    
    s_expr_module_set_debug(&engine->module, debug_callback);
    s_expr_module_set_error(&engine->module, error_callback);
    printf("✅ Debug/error callbacks set\n");
    
    printf("\n=== Validating Function Resolution ===\n");
    
    err = s_engine_validate(engine);
    if (err != S_EXPR_ERR_OK) {
        printf("❌ FATAL: Validation failed: %s\n", s_expr_error_str(err));
        printf("   Missing hash: 0x%08X at index %d\n", 
               engine->module.error_hash, engine->module.error_index);
        s_engine_free(engine);
        return false;
    }
    
    printf("✅ All functions resolved successfully\n");
    return true;
}

static bool load_from_file(s_engine_handle_t* engine, s_expr_allocator_t* alloc, 
                           const char* filepath) {
    printf("=== Initializing Engine from File ===\n");
    
    memset(engine, 0, sizeof(s_engine_handle_t));
    
    uint8_t err = s_engine_init_from_file(
        engine,
        filepath,
        *alloc,
        NULL
    );
    
    if (err != S_EXPR_ERR_OK) {
        printf("❌ FATAL: Failed to init engine: %s\n", s_engine_error_str(engine));
        return false;
    }
    
    printf("✅ Module loaded successfully\n");
    printf("   Trees:    %d\n", engine->module.def->tree_count);
    printf("   Records:  %d\n", engine->module.def->record_count);
    printf("   Strings:  %d\n", engine->module.def->string_count);
    printf("   Oneshot:  %d\n", engine->module.def->oneshot_count);
    printf("   Main:     %d\n", engine->module.def->main_count);
    printf("   Pred:     %d\n", engine->module.def->pred_count);

    // Register functions
    printf("\n=== Registering Functions ===\n");
    
    s_engine_register_builtins(engine);
    printf("✅ Built-in functions registered\n");
    
    // No user functions needed for this test - uses only builtins
    printf("✅ No user functions required\n");
    
    s_expr_module_set_debug(&engine->module, debug_callback);
    s_expr_module_set_error(&engine->module, error_callback);
    printf("✅ Debug/error callbacks set\n");
    // loading user functions
    state_machine_test_register_all(&engine->module);
    printf("\n=== Validating Function Resolution ===\n");
    
    err = s_engine_validate(engine);
    if (err != S_EXPR_ERR_OK) {
        printf("❌ FATAL: Validation failed: %s\n", s_expr_error_str(err));
        printf("   Missing hash: 0x%08X at index %d\n", 
               engine->module.error_hash, engine->module.error_index);
        s_engine_free(engine);
        return false;
    }
    
    printf("✅ All functions resolved successfully\n");
    return true;
}

// ============================================================================
// STATE MACHINE TEST
// Runs the state machine through multiple ticks until termination
// ============================================================================

static void test_state_machine(s_engine_handle_t* engine) {
    printf("\n╔════════════════════════════════════════╗\n");
    printf("║    STATE MACHINE TEST                  ║\n");
    printf("╚════════════════════════════════════════╝\n");
    
    printf("\nTesting state machine with tick loop...\n");
    
    g_test_errors = 0;
    
    // Create tree
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        STATE_MACHINE_TEST_HASH,
        0
    );
    
    if (!tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", STATE_MACHINE_TEST_HASH);
        return;
    }
    
    // Get blackboard pointer to observe state changes
    state_machine_blackboard_t* bb = (state_machine_blackboard_t*)s_expr_tree_get_blackboard(tree);
    
    printf("\n  Initial state: %d\n", bb ? bb->state : -1);
    
    // Tick loop - run until TERMINATE or max ticks
    int tick_count = 0;
    int max_ticks = 500;  // Safety limit
    s_expr_result_t result;
    
    printf("\n  Running tick loop...\n");
    
    do {
        result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        tick_count++;
        
        // Print state changes periodically
        if (tick_count == 1 || tick_count % 100 == 0 || result == SE_TERMINATE) {
            printf("    Tick %3d: state=%d, result=%s\n", 
                   tick_count, bb ? bb->state : -1, result_to_str(result));
        }
        
    } while (result != SE_TERMINATE && tick_count < max_ticks);
    
    printf("\n  Final state: %d\n", bb ? bb->state : -1);
    printf("  Total ticks: %d\n", tick_count);
    printf("  Final result: %s\n", result_to_str(result));
    
    if (result == SE_TERMINATE) {
        printf("\n  ✅ PASSED - State machine terminated normally\n");
    } else if (tick_count >= max_ticks) {
        printf("\n  ❌ FAILED - Max ticks exceeded without termination\n");
    } else {
        printf("\n  ❌ FAILED - Unexpected result\n");
    }
    
    s_expr_tree_free(tree);
}


// ============================================================================
// RUN ALL STATE MACHINE TESTS
// ============================================================================

static void run_state_machine_tests(s_engine_handle_t* engine) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║           STATE MACHINE TEST SUITE                             ║\n");
    printf("╚════════════════════════════════════════════════════════════════╝\n");
    
    test_state_machine(engine);
   
    
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║           ALL STATE MACHINE TESTS COMPLETE                     ║\n");
    printf("╚════════════════════════════════════════════════════════════════╝\n");
}

// ============================================================================
// MAIN FUNCTION
// ============================================================================

int main(int argc, char* argv[]) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║           S-EXPRESSION ENGINE STATE MACHINE TEST               ║\n");
    printf("╚════════════════════════════════════════════════════════════════╝\n\n");
    
    (void)argc;
    (void)argv;
    
    // Setup allocator
    s_expr_allocator_t alloc = {
        .malloc = simple_malloc,
        .free = simple_free,
        .ctx = NULL,
        .get_time = linux_get_time
    };
    
    s_engine_handle_t engine;
    bool result;
    
    // ========================================================================
    // TEST 1: Load from ROM
    // ========================================================================
    
    printf("\n=== Loading module from ROM ===\n\n");
    result = load_from_rom(&engine, &alloc, state_machine_test_module_bin_32, STATE_MACHINE_TEST_MODULE_BIN_32_SIZE);
    if (!result) {
        printf("❌ FATAL: Failed to load module from ROM\n");
        return 1;
    }
    
    run_state_machine_tests(&engine);
    s_engine_free(&engine);
    
    // ========================================================================
    // TEST 2: Load from File (optional)
    // ========================================================================
    
    printf("\n\n=== Loading module from file ===\n\n");
    result = load_from_file(&engine, &alloc, "state_machine_test_32.bin");
    if (!result) {
        printf("⚠️  WARNING: Could not load from file (may not exist)\n");
        printf("   This is OK if running without the binary file.\n");
    } else {
        run_state_machine_tests(&engine);
        s_engine_free(&engine);
    }
    
    printf("\n✅ All tests completed!\n\n");
    return 0;
}