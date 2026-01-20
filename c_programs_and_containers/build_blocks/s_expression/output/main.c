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
    clock_gettime(CLOCK_REALTIME, &ts);
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
static void test_pipeline_and_delays(s_engine_handle_t* engine);
static void test_conditionals(s_engine_handle_t* engine);
static void test_state_machine(s_engine_handle_t* engine);
static void test_dispatch(s_engine_handle_t* engine);
static void test_basic_lists(s_engine_handle_t* engine);
static void test_dictionary_basic(s_engine_handle_t* engine);
static void test_dictionary_with_actions(s_engine_handle_t* engine);
static void test_array_access(s_engine_handle_t* engine);
static void test_tuple_basic(s_engine_handle_t* engine);
static void test_named_state_machine(s_engine_handle_t* engine);
static void test_dict_event_dispatch(s_engine_handle_t* engine);
static void test_complex_structures(s_engine_handle_t* engine);
static void test_alist_style(s_engine_handle_t* engine);
static void test_plist_style(s_engine_handle_t* engine);
static void test_trigger_on_change(s_engine_handle_t* engine);
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
    test_pipeline_and_delays(&engine);
    test_conditionals(&engine);
    test_state_machine(&engine);
    test_dispatch(&engine);
    test_basic_lists(&engine);
    test_dictionary_basic(&engine);
    test_dictionary_with_actions(&engine);
    test_array_access(&engine);
    test_tuple_basic(&engine);
    test_named_state_machine(&engine);
    test_dict_event_dispatch(&engine);
    test_complex_structures(&engine);
    test_alist_style(&engine);
    test_plist_style(&engine);
    test_trigger_on_change(&engine);
/**
    test_parameter_types(&engine);
    test_all_call_types(&engine);
    test_composable_predicates(&engine);
    
   
    
    test_dispatch(&engine);
    test_predicate_helpers(&engine);
    test_nested_fields(&engine);
    test_pointer_slots(&engine);
    test_complex_nesting(&engine);
    test_basic_lists(&engine);
    test_dictionary_basic(&engine);
    test_dictionary_with_actions(&engine);
    */
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

    s_expr_result_t last_result = s_expr_node_tick(continue_tree, SE_EVENT_TICK, NULL);
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

    last_result =s_expr_node_tick(terminate_tree, SE_EVENT_TICK, NULL);
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

    last_result = s_expr_node_tick(reset_tree, SE_EVENT_TICK, NULL);
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

    last_result =s_expr_node_tick(disable_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_FUNCTION_TERMINATE) {
        printf("  ❌ FAILED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
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

    last_result =s_expr_node_tick(halt_tree, SE_EVENT_TICK, NULL);
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

    last_result =s_expr_node_tick(halt_tree, SE_EVENT_TICK, NULL);
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

    last_result =s_expr_node_tick(function_halt_tree, SE_EVENT_TICK, NULL);
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

    last_result =s_expr_node_tick(function_reset_tree, SE_EVENT_TICK, NULL);
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

    last_result =s_expr_node_tick(function_reset_tree, SE_EVENT_TICK, NULL);
    if (last_result != SE_FUNCTION_TERMINATE) {
        printf("  ❌ FAILED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(function_terminate_tree);
}

static void test_pipeline_and_delays(s_engine_handle_t* engine) {
    printf("\n=== Test Pipeline and Delays ===\n");
    
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_PIPELINE_AND_DELAYS_HASH,
        0
    );
    if (!tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_PIPELINE_AND_DELAYS_HASH);
        exit(1);
    }
    
    s_expr_result_t result;
    
    // Step 1: Pass the tick_delay(100)
    printf("Phase 1: Tick delay (100 ticks)\n");
    for (int i = 0; i < 110; i++) {
        result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        if (result != SE_HALT) {
            printf("  tick %d: result=%d\n", i, result);
        }
    }
    printf("  After 110 ticks: result=%d\n", result);
    
    // Step 2: Pass the time_delay(1.5)
    printf("Phase 2: Time delay (1.5 seconds)\n");
    result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  Before sleep: result=%d\n", result);
    
    usleep(1600000);  // 1.6 seconds
    
    result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  After sleep: result=%d\n", result);
    
    // Step 3: Pass the wait_event(42, 3)
    printf("Phase 3: Wait for event 42 (3 times)\n");
    for (int i = 0; i < 3; i++) {
        result = s_expr_node_tick(tree, 42, NULL);  
       
    }
    
    // Step 4: Pass the wait_event_once(99)
    printf("Phase 4: Wait for event 99 (once)\n");
    result = s_expr_node_tick(tree, 99, NULL);  
    
    printf("  event 99: result=%d\n", result);
    
    // Step 5: Final tick to complete pipeline
    printf("Phase 5: Final tick\n");
    result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  Final result: %d\n", result);
    
    // Expected: SE_FUNCTION_TERMINATE (5) or SE_DISABLE (4)
    if (result == SE_FUNCTION_TERMINATE ) {
        printf("✅ PASSED: Pipeline completed correctly\n");
    } else {
        printf("❌ FAILED: Expected SE_FUNCTION_TERMINATE  but got %s\n", result_to_str(result));
        exit(1);
    }
    
    s_expr_tree_free(tree);
}


static void test_conditionals(s_engine_handle_t* engine) {
    printf("\n=== Test Conditionals ===\n");
    s_expr_tree_instance_t* test_conditionals_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_CONDITIONALS_HASH,
        0
    );
    if (!test_conditionals_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_CONDITIONALS_HASH);
        exit(1);
    }
    s_expr_result_t last_result = s_expr_node_tick(test_conditionals_tree, SE_EVENT_TICK, NULL);
    last_result = s_expr_node_tick(test_conditionals_tree, SE_EVENT_TICK, NULL);
    printf("last_result: %d\n", last_result);
    if (last_result != SE_FUNCTION_TERMINATE) {
        printf("  ❌ FAILED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
        exit(1);
    }
    printf("  ✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
    s_expr_tree_free(test_conditionals_tree);
}
static void test_state_machine(s_engine_handle_t* engine) {
    printf("\n=== Test State Machine ===\n");
    s_expr_tree_instance_t* test_state_machine_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_STATE_MACHINE_HASH,
        0
    );
    if (!test_state_machine_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_STATE_MACHINE_HASH);
        exit(1);
    }
    s_expr_result_t last_result;
    for (int i = 0; i <  200 ; i++) {
        last_result = s_expr_node_tick(test_state_machine_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d %d\n", i, last_result);
        if (last_result == SE_FUNCTION_TERMINATE) {
            printf("  ✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
            s_expr_tree_free(test_state_machine_tree);
            return;
        }
    }
    printf("  ❌ FAILED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
    exit(1);
    
}

#define EVT_TIMER 100
#define EVT_BUTTON 101
#define EVT_SENSOR 102
static void test_dispatch(s_engine_handle_t* engine) {
    printf("\n=== Test Dispatch ===\n");
    
    s_expr_result_t result;
    s_expr_tree_instance_t* tree;
    
    // Test 1: Field dispatch IDLE
    printf("\nTest: field_dispatch IDLE\n");
    tree = s_expr_tree_create_by_hash(&engine->module, TEST_FIELD_DISPATCH_IDLE_HASH, 0);
    if (tree) {
        result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        printf("Result: %d (expected %d)\n", result, SE_FUNCTION_TERMINATE);
        s_expr_tree_free(tree);
    }
    
    // Test 2: Field dispatch START
    printf("\nTest: field_dispatch START\n");
    tree = s_expr_tree_create_by_hash(&engine->module, TEST_FIELD_DISPATCH_START_HASH, 0);
    if (tree) {
        result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        printf("Result: %d (expected %d)\n", result, SE_FUNCTION_TERMINATE);
        s_expr_tree_free(tree);
    }
    
    // Test 3: Field dispatch STOP
    printf("\nTest: field_dispatch STOP\n");
    tree = s_expr_tree_create_by_hash(&engine->module, TEST_FIELD_DISPATCH_STOP_HASH, 0);
    if (tree) {
        result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        printf("Result: %d (expected %d)\n", result, SE_FUNCTION_TERMINATE);
        s_expr_tree_free(tree);
    }
    
    // Test 4: Event dispatch
    printf("\nTest: event_dispatch\n");
    tree = s_expr_tree_create_by_hash(&engine->module, TEST_EVENT_DISPATCH_HASH, 0);
    if (tree) {
        printf("Sending EVT_TIMER (100):\n");
        s_expr_node_tick(tree, EVT_TIMER, NULL);
        
        printf("Sending EVT_BUTTON (101):\n");
        s_expr_node_tick(tree, EVT_BUTTON, NULL);
        
        printf("Sending EVT_SENSOR (102):\n");
        result = s_expr_node_tick(tree, EVT_SENSOR, NULL);
        
        printf("Final result: %d\n", result);
        s_expr_tree_free(tree);
    }
    
    printf("\n=== Dispatch Tests Complete ===\n");
}



static void test_basic_lists(s_engine_handle_t* engine) {
    printf("\n=== Test Basic Lists ===\n");
    s_expr_tree_instance_t* test_basic_lists_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_BASIC_LISTS_HASH,
        0
    );
    if (!test_basic_lists_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_BASIC_LISTS_HASH);
        exit(1);
    }
    s_expr_result_t last_result = s_expr_node_tick(test_basic_lists_tree, SE_EVENT_TICK, NULL);
    printf("last_result: %d\n", last_result);
    s_expr_tree_free(test_basic_lists_tree);
}

static void test_dictionary_basic(s_engine_handle_t* engine) {
    printf("\n=== Test Dictionary Basic ===\n");
    s_expr_tree_instance_t* test_dictionary_basic_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_DICTIONARY_BASIC_HASH,
        0
    );
    if (!test_dictionary_basic_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_DICTIONARY_BASIC_HASH);
        exit(1);
    }
    
    for (int i = 0; i < 10; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_dictionary_basic_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_dictionary_basic_tree);
}

static void test_dictionary_with_actions(s_engine_handle_t* engine) {
    printf("\n=== Test Dictionary With Actions ===\n");
    s_expr_tree_instance_t* test_dictionary_with_actions_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_DICTIONARY_WITH_ACTIONS_HASH,
        0
    );
    if (!test_dictionary_with_actions_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_DICTIONARY_WITH_ACTIONS_HASH);
        exit(1);
    }
    for (int i = 0; i < 10; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_dictionary_with_actions_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_dictionary_with_actions_tree);
}


static void test_array_access(s_engine_handle_t* engine) {
    printf("\n=== Test Array Access ===\n");
    s_expr_tree_instance_t* test_array_access_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_ARRAY_BASIC_HASH,
        0
    );
    if (!test_array_access_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_ARRAY_BASIC_HASH);
        exit(1);
    }
    for (int i = 0; i < 1; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_array_access_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_array_access_tree);
}


static void test_tuple_basic(s_engine_handle_t* engine) {
    printf("\n=== Test Tuple Basic ===\n");
    s_expr_tree_instance_t* test_tuple_basic_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_TUPLE_BASIC_HASH,
        0
    );
    if (!test_tuple_basic_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_TUPLE_BASIC_HASH);
        exit(1);
    }
    for (int i = 0; i < 1; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_tuple_basic_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_tuple_basic_tree);
}

static void test_named_state_machine(s_engine_handle_t* engine) {
    printf("\n=== Test Named State Machine ===\n");
    s_expr_tree_instance_t* test_named_state_machine_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_NAMED_STATE_MACHINE_HASH,
        0
    );
    if (!test_named_state_machine_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_NAMED_STATE_MACHINE_HASH);
        exit(1);
    }
    for (int i = 0; i < 100; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_named_state_machine_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d %d\n", i, last_result);
        if (last_result == SE_FUNCTION_TERMINATE) {
            printf("  ✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(last_result));
            s_expr_tree_free(test_named_state_machine_tree);
            return;
        }
    }
    s_expr_tree_free(test_named_state_machine_tree);
}

static void test_dict_event_dispatch(s_engine_handle_t* engine) {
    printf("\n=== Test Dict Event Dispatch ===\n");
    
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_DICT_EVENT_DISPATCH_HASH,
        0
    );
    if (!tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_DICT_EVENT_DISPATCH_HASH);
        exit(1);
    }
    
    // Compute event hashes
    uint32_t TIMER_TICK_HASH = s_expr_hash("TIMER_TICK");
    uint32_t BUTTON_PRESS_HASH = s_expr_hash("BUTTON_PRESS");
    uint32_t SENSOR_TRIGGER_HASH = s_expr_hash("SENSOR_TRIGGER");
    uint32_t SHUTDOWN_HASH = s_expr_hash("SHUTDOWN");
    uint32_t RESET_HASH = s_expr_hash("RESET");
    uint32_t UNKNOWN_EVENT_HASH = s_expr_hash("UNKNOWN_EVENT");
    
    printf("Event hashes:\n");
    printf("  TIMER_TICK:     0x%08X\n", TIMER_TICK_HASH);
    printf("  BUTTON_PRESS:   0x%08X\n", BUTTON_PRESS_HASH);
    printf("  SENSOR_TRIGGER: 0x%08X\n", SENSOR_TRIGGER_HASH);
    printf("  SHUTDOWN:       0x%08X\n", SHUTDOWN_HASH);
    printf("  RESET:          0x%08X\n", RESET_HASH);
    printf("\n");
    
    s_expr_result_t result;
    uint32_t event_hash;
    
    // Test 1: Send TIMER_TICK events
    printf("--- Sending TIMER_TICK events ---\n");
    for (int i = 0; i < 3; i++) {
        event_hash = TIMER_TICK_HASH;
        result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
        printf("  tick %d: result=%d (%s)\n", i, result, result_to_str(result));
    }
    
    // Test 2: Send BUTTON_PRESS event
    printf("\n--- Sending BUTTON_PRESS event ---\n");
    event_hash = BUTTON_PRESS_HASH;
    result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
    printf("  result=%d (%s)\n", result, result_to_str(result));
    
    // Test 3: Send SENSOR_TRIGGER event
    printf("\n--- Sending SENSOR_TRIGGER event ---\n");
    event_hash = SENSOR_TRIGGER_HASH;
    result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
    printf("  result=%d (%s)\n", result, result_to_str(result));
    
    // Test 4: Send unknown event (should be ignored/no-op)
    printf("\n--- Sending UNKNOWN_EVENT (should be no-op) ---\n");
    event_hash = UNKNOWN_EVENT_HASH;
    result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
    printf("  result=%d (%s)\n", result, result_to_str(result));
    
    // Test 5: Send more TIMER_TICK to verify counter increments
    printf("\n--- Sending more TIMER_TICK events ---\n");
    for (int i = 0; i < 2; i++) {
        event_hash = TIMER_TICK_HASH;
        result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
        printf("  tick %d: result=%d (%s)\n", i, result, result_to_str(result));
    }
    
    // Test 6: Send RESET event
    printf("\n--- Sending RESET event ---\n");
    event_hash = RESET_HASH;
    result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
    printf("  result=%d (%s)\n", result, result_to_str(result));
    
    // Test 7: Send SHUTDOWN event - should terminate
    printf("\n--- Sending SHUTDOWN event ---\n");
    event_hash = SHUTDOWN_HASH;
    result = s_expr_node_tick(tree, SE_EVENT_USER, &event_hash);
    printf("  result=%d (%s)\n", result, result_to_str(result));
    
    if (result == SE_FUNCTION_TERMINATE) {
        printf("\n  ✅ PASSED: SHUTDOWN returned SE_FUNCTION_TERMINATE as expected\n");
    } else {
        printf("\n  ❌ FAILED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(result));
    }
    
    s_expr_tree_free(tree);
}

static void test_complex_structures(s_engine_handle_t* engine) {
    printf("\n=== Test Complex Structures ===\n");
    s_expr_tree_instance_t* test_complex_structures_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_COMPLEX_STRUCTURES_HASH,
        0
    );
    if (!test_complex_structures_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_COMPLEX_STRUCTURES_HASH);
        exit(1);
    }
    for (int i = 0; i < 1; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_complex_structures_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_complex_structures_tree);
}

static void test_alist_style(s_engine_handle_t* engine) {
    printf("\n=== Test Alist Style ===\n");
    s_expr_tree_instance_t* test_alist_style_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_ALIST_STYLE_HASH,
        0
    );

    if (!test_alist_style_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_ALIST_STYLE_HASH);
        exit(1);
    }
    for (int i = 0; i < 1; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_alist_style_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_alist_style_tree);
}

static void test_plist_style(s_engine_handle_t* engine) {
    printf("\n=== Test Plist Style ===\n");
    s_expr_tree_instance_t* test_plist_style_tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_PLIST_STYLE_HASH,
        0
    );
    if (!test_plist_style_tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", TEST_PLIST_STYLE_HASH);
        exit(1);
    }
    for (int i = 0; i < 1; i++) {
        s_expr_result_t last_result = s_expr_node_tick(test_plist_style_tree, SE_EVENT_TICK, NULL);
        printf("last_result: %d\n", last_result);
    }
    s_expr_tree_free(test_plist_style_tree);
}


#define EVENT_BIT0_RISE     (1U << 0)
#define EVENT_BIT0_FALL     (1U << 1)
#define EVENT_BITS12_RISE   (1U << 2)
#define EVENT_BITS12_FALL   (1U << 3)
#define EVENT_BITS34_RISE   (1U << 4)
#define EVENT_BITS34_FALL   (1U << 5)
#define EVENT_BIT5_CLEAR    (1U << 6)
#define EVENT_BIT5_SET      (1U << 7)

void reset_trigger_events(void);
uint32_t get_trigger_events(void);

static void test_trigger_on_change(s_engine_handle_t* engine) {
    
    printf("\n=== Test Trigger On Change ===\n");
    
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        TEST_TRIGGER_ON_CHANGE_HASH,
        0
    );
    if (!tree) {
        printf("  ❌ FAILED: Could not create tree\n");
        exit(1);
    }
    
    // Bitmap for predicates to read
    uint32_t bitmap = 0;
    tree->user_ctx = &bitmap;
    
    
    int test_pass = 1;
    
    // -------------------------------------------------------------------------
    // Initial tick - all triggers start with their initial state
    // Trigger 4 starts with initial_state=1 (NOT bit5, bit5=0 means pred=true)
    // -------------------------------------------------------------------------
    printf("\n--- Initial tick (bitmap=0x%08X) ---\n", bitmap);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    // No transitions expected on first tick (state matches initial)
    
    // -------------------------------------------------------------------------
    // Test 1: Set bit 0 -> should trigger ON_BIT0_RISE
    // -------------------------------------------------------------------------
    printf("\n--- Set bit 0 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap | 0x01);
    bitmap |= (1U << 0);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BIT0_RISE)) {
        printf("  ❌ Expected BIT0_RISE\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 2: Clear bit 0 -> should trigger ON_BIT0_FALL
    // -------------------------------------------------------------------------
    printf("\n--- Clear bit 0 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap & ~0x01);
    bitmap &= ~(1U << 0);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BIT0_FALL)) {
        printf("  ❌ Expected BIT0_FALL\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 3: Set bit 1 only -> AND should not trigger (need both 1 and 2)
    // -------------------------------------------------------------------------
    printf("\n--- Set bit 1 only (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap | 0x02);
    bitmap |= (1U << 1);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (get_trigger_events() & EVENT_BITS12_RISE) {
        printf("  ❌ Unexpected BITS12_RISE (only bit1 set)\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 4: Set bit 2 -> now AND is true, should trigger ON_BITS_12_RISE
    // -------------------------------------------------------------------------
    printf("\n--- Set bit 2 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap | 0x04);
    bitmap |= (1U << 2);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BITS12_RISE)) {
        printf("  ❌ Expected BITS12_RISE\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 5: Clear bit 1 -> AND becomes false, should trigger ON_BITS_12_FALL
    // -------------------------------------------------------------------------
    printf("\n--- Clear bit 1 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap & ~0x02);
    bitmap &= ~(1U << 1);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BITS12_FALL)) {
        printf("  ❌ Expected BITS12_FALL\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 6: Set bit 3 -> OR becomes true, should trigger ON_BITS_34_RISE
    // -------------------------------------------------------------------------
    printf("\n--- Set bit 3 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap | 0x08);
    bitmap |= (1U << 3);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BITS34_RISE)) {
        printf("  ❌ Expected BITS34_RISE\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 7: Set bit 4 also -> OR still true, no new trigger
    // -------------------------------------------------------------------------
    printf("\n--- Set bit 4 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap | 0x10);
    bitmap |= (1U << 4);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (get_trigger_events() & (EVENT_BITS34_RISE | EVENT_BITS34_FALL)) {
        printf("  ❌ Unexpected BITS34 event (OR still true)\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 8: Clear bit 3 -> OR still true (bit 4 set), no trigger
    // -------------------------------------------------------------------------
    printf("\n--- Clear bit 3 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap & ~0x08);
    bitmap &= ~(1U << 3);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (get_trigger_events() & (EVENT_BITS34_RISE | EVENT_BITS34_FALL)) {
        printf("  ❌ Unexpected BITS34 event (OR still true via bit4)\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 9: Clear bit 4 -> OR now false, should trigger ON_BITS_34_FALL
    // -------------------------------------------------------------------------
    printf("\n--- Clear bit 4 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap & ~0x10);
    bitmap &= ~(1U << 4);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BITS34_FALL)) {
        printf("  ❌ Expected BITS34_FALL\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 10: Set bit 5 -> NOT bit5 becomes false, should trigger ON_BIT5_SET
    // (initial_state=1, so NOT bit5 starts true when bit5=0)
    // -------------------------------------------------------------------------
    printf("\n--- Set bit 5 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap | 0x20);
    bitmap |= (1U << 5);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BIT5_SET)) {
        printf("  ❌ Expected BIT5_SET (NOT became false)\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Test 11: Clear bit 5 -> NOT bit5 becomes true, should trigger ON_BIT5_CLEAR
    // -------------------------------------------------------------------------
    printf("\n--- Clear bit 5 (bitmap=0x%08X -> 0x%08X) ---\n", bitmap, bitmap & ~0x20);
    bitmap &= ~(1U << 5);
    reset_trigger_events();
    s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
    printf("  events fired: 0x%02X\n", get_trigger_events());
    if (!(get_trigger_events() & EVENT_BIT5_CLEAR)) {
        printf("  ❌ Expected BIT5_CLEAR (NOT became true)\n");
        test_pass = 0;
    }
    
    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------
    if (test_pass) {
        printf("\n  ✅ PASSED: All edge triggers working correctly\n");
    } else {
        printf("\n  ❌ FAILED: Some edge triggers failed\n");
    }
    
    s_expr_tree_free(tree);
}