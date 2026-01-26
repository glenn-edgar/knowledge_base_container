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
#include "demo_test.h"  // s_hashes
#include "demo_test_records.h"
#include "demo_test_bin_32.h"
#include "demo_test_user_functions.h"

// Forward declaration for generated registration function
extern void demo_test_register_all(s_expr_module_t* module);
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
    
    demo_test_register_all(&engine->module);
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
    
    demo_test_register_all(&engine->module);
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


static void test_state_machine(s_engine_handle_t* engine);
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
    bool result = load_from_rom(&engine, &alloc, demo_test_module_bin_32, DEMO_TEST_MODULE_BIN_32_SIZE);
    if (!result) {
        printf("❌ FATAL: Failed to load module\n");
        return 1;
    }
    s_engine_free(&engine);

    printf("\n\nLoading module from file...\n\n");
    result = load_from_file(&engine, &alloc, "demo_test_32.bin");
    if (!result) {
        printf("❌ FATAL: Failed to load module\n");
        return 1;
    }

    test_state_machine(&engine);

    s_engine_free(&engine);

    // ========================================================================
    // INITIALIZE ENGINE
    // ========================================================================
    
   
   return 0;
}



static void test_state_machine(s_engine_handle_t* engine) {
    printf("\n=== State Machine Test ===\n");
    
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        STATE_MACHINE_TEST_HASH,
        0
    );
    if (!tree) {
        printf("  ❌ FAILED: Could not create tree (hash=0x%08X)\n", STATE_MACHINE_TEST_HASH);
        exit(1);
    }
    
    s_expr_result_t result;
    
    // Step 1: Pass the tick_delay(100)
    for (int i = 0; i < 1000; i++) {
        tree->tick_type = SE_EVENT_TICK;
        result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        //printf("last_result: %d\n", result);
        if (result == SE_TERMINATE ){
            printf("test is done %d ticks\n", i);
            s_expr_tree_free(tree);
            return;
        } 
        
    
    }
   
    printf("test did not finish\n");
    exit(1);
    s_expr_tree_free(tree);
}


