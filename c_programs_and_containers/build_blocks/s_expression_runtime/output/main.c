#define _GNU_SOURCE
#include <time.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>


#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "s_engine_loader.h"
#include "s_engine_init.h"
#include "s_engine_builtins.h"

#include "s_expr_dsl_test.h"
#include "s_expr_dsl_test_records.h"
#include "s_expr_dsl_test_bin_32.h"
#include "s_expr_dsl_test_user_functions.h"

// Forward declaration for generated registration function
extern void s_expr_dsl_test_register_all(s_expr_module_t* module);

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
    printf("  [DEBUG] %s\n", msg);
}

// ============================================================================
// TEST RESULT TRACKING
// ============================================================================

typedef struct {
    int passed;
    int failed;
    int skipped;
} test_stats_t;

static test_stats_t g_stats = {0, 0, 0};

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
        default:                    return "UNKNOWN";
    }
}
// Linux monotonic time
static double linux_get_time(void* ctx) {
    (void)ctx;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static bool load_from_rom(s_engine_handle_t* engine, s_expr_allocator_t* alloc, const uint8_t* binary_data, size_t binary_size) {
    // ========================================================================
    // INITIALIZE ENGINE
    // ========================================================================
    
    printf("=== Initializing Engine ===\n");
    

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

    // ========================================================================
    // REGISTER FUNCTIONS
    // ========================================================================
    
    printf("\n=== Registering Functions ===\n");
    
    s_engine_register_builtins(engine);
    printf("✅ Built-in functions registered\n");
    
    s_expr_dsl_test_register_all(&engine->module);
    printf("✅ User functions registered\n");
    
    s_expr_module_set_debug(&engine->module, debug_callback);
    printf("✅ Debug callback set\n");
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


static bool load_from_file(s_engine_handle_t* engine, s_expr_allocator_t* alloc, const char* filepath) {
    // ========================================================================
    // INITIALIZE ENGINE
    // ========================================================================
    
    printf("=== Initializing Engine ===\n");
    

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

    // ========================================================================
    // REGISTER FUNCTIONS
    // ========================================================================
    
    printf("\n=== Registering Functions ===\n");
    
    s_engine_register_builtins(engine);
    printf("✅ Built-in functions registered\n");
    
    s_expr_dsl_test_register_all(&engine->module);
    printf("✅ User functions registered\n");
    
    s_expr_module_set_debug(&engine->module, debug_callback);
    printf("✅ Debug callback set\n");
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
// MAIN FUNCTION
// ============================================================================


static void return_code_tests(s_engine_handle_t* engine);
static void test_parameter_types(s_engine_handle_t* engine);
static void test_all_call_types(s_engine_handle_t* engine);
static void test_composable_predicates(s_engine_handle_t* engine);

int main(int argc, char* argv[]) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║           S-EXPRESSION ENGINE TEST SUITE                       ║\n");
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
    printf("\n\nLoading module from ROM...\n\n");
    bool result = load_from_rom(&engine, &alloc, s_expr_dsl_test_module_bin_32, S_EXPR_DSL_TEST_MODULE_BIN_32_SIZE);
    if (!result) {
        printf("❌ FATAL: Failed to load module\n");
        return 1;
    }
    s_engine_free(&engine);

    printf("\n\nLoading module from file...\n\n");
    result = load_from_file(&engine, &alloc, "s_expr_dsl_test_32.bin");
    if (!result) {
        printf("❌ FATAL: Failed to load module\n");
        return 1;
    }

    return_code_tests(&engine);
    test_parameter_types(&engine);
    test_all_call_types(&engine);
    test_composable_predicates(&engine);
    s_engine_free(&engine);
    // ========================================================================
    // INITIALIZE ENGINE
    // ========================================================================
    
   
   return 0;
}


static void return_code_tests(s_engine_handle_t* engine) {

    s_expr_tree_instance_t* continue_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_1_HASH,
        0
    );
    if (!continue_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_1_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    s_expr_result_t last_result = s_expr_tree_tick(continue_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_CONTINUE) {
        printf("  ❌ FAILED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(continue_tree);

    s_expr_tree_instance_t* terminate_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_2_HASH,
        0
    );
    if (!terminate_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_2_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(terminate_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_TERMINATE) {
        printf("  ❌ FAILED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(terminate_tree);

    s_expr_tree_instance_t* reset_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_3_HASH,
        0
    );
    if (!reset_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_3_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(reset_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_RESET) {
        printf("  ❌ FAILED: Expected SE_RESET, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(reset_tree);

    s_expr_tree_instance_t* disable_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_4_HASH,
        0
    );
    if (!disable_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_4_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(disable_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_CONTINUE) {
        printf("  ❌ FAILED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_HALT, got %s\n", result_to_str(last_result));
    s_expr_tree_free(disable_tree);

    s_expr_tree_instance_t* halt_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_5_HASH,
        0
    );
    if (!halt_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_5_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(halt_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_HALT) {
        printf("  ❌ FAILED: Expected SE_HALT, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_HALT, got %s\n", result_to_str(last_result));
    s_expr_tree_free(halt_tree);

    s_expr_tree_instance_t* skip_continue_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_6_HASH,
        0
    );
    if (!skip_continue_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_6_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(halt_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_SKIP_CONTINUE) {
        printf("  ❌ FAILED: Expected SE_SKIP_CONTINUE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_SKIP_CONTINUE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(skip_continue_tree);

    s_expr_tree_instance_t* function_halt_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_7_HASH,
        0
    );
    if (!function_halt_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_5_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(function_halt_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_FUNCTION_HALT) {
        printf("  ❌ FAILED: Expected SE_FUNCTION_HALT, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_FUNCTION_HALT, got %s\n", result_to_str(last_result));
    s_expr_tree_free(function_halt_tree);

    s_expr_tree_instance_t* function_reset_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_8_HASH,
        0
    );
    if (!function_reset_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_5_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(function_reset_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_FUNCTION_RESET) {
        printf("  ❌ FAILED: Expected SE_HALT, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_FUNCTION_RESET, got %s\n", result_to_str(last_result));
    s_expr_tree_free(function_reset_tree);

  

    s_expr_tree_instance_t* function_terminate_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_RESULT_CODES_9_HASH,
        0
    );
    if (!function_terminate_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_RESULT_CODES_5_HASH);
        exit(1);
    }
    
    // Dump params to see node_index values

    last_result = s_expr_tree_tick(function_reset_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_FUNCTION_TERMINATE) {
        printf("  ❌ FAILED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(function_terminate_tree);
}
    

static void test_parameter_types(s_engine_handle_t* engine) {
    s_expr_tree_instance_t* test_params_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_ALL_PARAM_TYPES_HASH,
        0
    );
    if (!test_params_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_PARAMS_HASH);
        exit(1);
    }
    s_expr_result_t last_result = s_expr_tree_tick(test_params_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_CONTINUE) {
        printf("  ❌ FAILED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(test_params_tree);
}


static void test_all_call_types(s_engine_handle_t* engine) {
    s_expr_tree_instance_t* test_all_call_types_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_ALL_CALL_TYPES_HASH,
        0
    );
    if (!test_all_call_types_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_ALL_CALL_TYPES_HASH);
        exit(1);
    }
    for (int i = 0; i < 10; i++) {
        s_expr_result_t last_result = s_expr_tree_tick(test_all_call_types_tree, SE_EVENT_TICK, NULL);
        if (last_result == SE_CONTINUE) {
            break;
        }
        printf("  ✅ PASSED: Expected SE_CONTINUE, got %s\n", result_to_str(last_result));
    }
    s_expr_result_t result = s_expr_tree_tick(test_all_call_types_tree, 42,NULL);
    if (result != SE_CONTINUE) {
        printf("  ❌ FAILED: Expected SE_CONTINUE, got %s\n", result_to_str(result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_CONTINUE, got %s\n", result_to_str(result));
    
    s_expr_tree_free(test_all_call_types_tree);
}

static void test_composable_predicates(s_engine_handle_t* engine) {
    s_expr_tree_instance_t* test_composable_predicates_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_COMPOSABLE_PREDICATES_HASH,
        0
    );
    if (!test_composable_predicates_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_COMPOSABLE_PREDICATES_HASH);
        exit(1);
    }
    s_expr_result_t last_result = s_expr_tree_tick(test_composable_predicates_tree, SE_EVENT_TICK, NULL);
    printf("last_result: %d\n", last_result);
    if ((last_result != SE_CONTINUE) && (last_result != SE_HALT)) {
        printf("  ❌ FAILED: Expected SE_CONTINUE or SE_HALT, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_CONTINUE or SE_HALT, got %s\n", result_to_str(last_result));
    s_expr_tree_free(test_composable_predicates_tree);
}