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
#include "s_engine_init.h"
#include "s_engine_node.h"
#include "se_lua53_bridge.h"

#include "state_machine_test.h"
#include "state_machine_test_bin_32.h"
#include "state_machine_test_records.h"

// ============================================================================
// USER HANDLE — opaque context passed through the engine
// ============================================================================

typedef struct {
    lua_State*  lua_state;      // Lua 5.3 runtime
    // Add additional user fields here as needed
} user_handle_t;

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

static void register_user_functions(s_engine_handle_t* engine);

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
// RESULT HELPERS
// ============================================================================

static const char* result_to_str(s_expr_result_t r) {
    switch (r) {
        case SE_CONTINUE:               return "CONTINUE";
        case SE_HALT:                   return "HALT";
        case SE_TERMINATE:              return "TERMINATE";
        case SE_RESET:                  return "RESET";
        case SE_DISABLE:                return "DISABLE";
        case SE_SKIP_CONTINUE:          return "SKIP_CONTINUE";
        case SE_FUNCTION_CONTINUE:      return "FUNCTION_CONTINUE";
        case SE_FUNCTION_HALT:          return "FUNCTION_HALT";
        case SE_FUNCTION_TERMINATE:     return "FUNCTION_TERMINATE";
        case SE_FUNCTION_RESET:         return "FUNCTION_RESET";
        case SE_FUNCTION_DISABLE:       return "FUNCTION_DISABLE";
        case SE_FUNCTION_SKIP_CONTINUE: return "FUNCTION_SKIP_CONTINUE";
        case SE_PIPELINE_CONTINUE:      return "PIPELINE_CONTINUE";
        case SE_PIPELINE_HALT:          return "PIPELINE_HALT";
        case SE_PIPELINE_TERMINATE:     return "PIPELINE_TERMINATE";
        case SE_PIPELINE_RESET:         return "PIPELINE_RESET";
        case SE_PIPELINE_DISABLE:       return "PIPELINE_DISABLE";
        case SE_PIPELINE_SKIP_CONTINUE: return "PIPELINE_SKIP_CONTINUE";
        default:                        return "UNKNOWN";
    }
}

static const char* result_scope_str(s_expr_result_t r) {
    if (r >= SE_PIPELINE_CONTINUE) return "PIPELINE";
    if (r >= SE_FUNCTION_CONTINUE) return "FUNCTION";
    return "LOCAL";
}

static s_expr_result_t result_base(s_expr_result_t r) {
    if (r >= SE_PIPELINE_CONTINUE) return r - SE_PIPELINE_CONTINUE;
    if (r >= SE_FUNCTION_CONTINUE) return r - SE_FUNCTION_CONTINUE;
    return r;
}

static bool result_stops_tick_loop(s_expr_result_t r) {
    s_expr_result_t base = result_base(r);
    return base == SE_DISABLE || base == SE_TERMINATE;
}

// ============================================================================
// UTC REALTIME TIMESTAMP
// ============================================================================

static double utc_realtime(void* ctx) {
    (void)ctx;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

// ============================================================================
// USER FUNCTION REGISTRATION
// ============================================================================

extern void state_machine_test_register_all(s_expr_module_t* module);

static void register_user_functions(s_engine_handle_t* engine) {
    state_machine_test_register_all(&engine->module);
}

// ============================================================================
// STATE MACHINE TEST
// ============================================================================

static void test_state_machine(s_engine_handle_t* engine) {
    printf("\n=== STATE MACHINE TEST ===\n\n");
    
    s_expr_tree_instance_t* tree = s_expr_tree_create_by_hash(
        &engine->module,
        STATE_MACHINE_TEST_HASH,
        0
    );
    
    if (!tree) {
        printf("FAILED: Could not create tree\n");
        return;
    }
    
    for (int i = 0; i < 500; i++) {
        s_expr_result_t result = s_expr_node_tick(tree, SE_EVENT_TICK, NULL);
        printf("Tick %3d: %s\n", i + 1, result_to_str(result));
        if (result == SE_FUNCTION_TERMINATE) {
            printf("✅ PASSED: Expected SE_FUNCTION_TERMINATE, got %s\n", result_to_str(result));
            break;
        }
    }
    
    s_expr_tree_free(tree);
}

// ============================================================================
// RUN ALL TESTS
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
// Add before main():

static void register_lua_trampolines(s_engine_handle_t* engine) {
    static s_expr_fn_entry_t lua_oneshot_entries[] = {
        { 0x5839B05B, (void*)se_lua_oneshot_trampoline },  // CFL_DISABLE_CHILDREN
        { 0xD42E3453, (void*)se_lua_oneshot_trampoline },  // CFL_ENABLE_CHILD
    };
    static const s_expr_fn_table_t lua_oneshot_table = {
        .entries = lua_oneshot_entries,
        .count = 2
    };
    s_expr_module_register_oneshot(&engine->module, &lua_oneshot_table);
}
// ============================================================================
// MAIN FUNCTION
// ============================================================================

int main(int argc, char* argv[]) {
    printf("\n");
    printf("╔════════════════════════════════════════════════════════════════╗\n");
    printf("║           S-EXPRESSION ENGINE STATE MACHINE TEST               ║\n");
    printf("║           (with Lua 5.3 integration)                           ║\n");
    printf("╚════════════════════════════════════════════════════════════════╝\n\n");
    
    (void)argc;
    (void)argv;
    
    // Suppress unused-function warnings for helpers used in extended tests
    (void)result_scope_str;
    (void)result_base;
    (void)result_stops_tick_loop;
    
    // ========================================================================
    // Step 1: Create Lua state
    // ========================================================================
    
    printf("=== Step 1: Initializing Lua 5.3 ===\n");
    
    lua_State* L = luaL_newstate();
    if (!L) {
        printf("❌ FATAL: Failed to create Lua state\n");
        return 1;
    }
    luaL_openlibs(L);
    printf("  ✅ Lua 5.3 state created\n");
    
    // ========================================================================
    // Step 2: Initialize the Lua bridge
    //         Creates registry tables, inst metatable, se_bridge global
    // ========================================================================
    
    printf("=== Step 2: Initializing Lua bridge ===\n");
    
    se_lua_bridge_init(L);
    printf("  ✅ Bridge initialized (registry + metatable)\n");
    
    // ========================================================================
    // Step 3: Load Lua scripts that register their functions
    //         Scripts call: se_bridge.register(hash, "oneshot", function(...) end)
    //         This must happen BEFORE tree execution but AFTER bridge init
    // ========================================================================
    
    printf("=== Step 3: Loading Lua function scripts ===\n");
    
    if (luaL_dofile(L, "app_lua_functions.lua") != LUA_OK) {
        printf("  ⚠️  Lua script load: %s\n", lua_tostring(L, -1));
        printf("     (This is OK if no Lua functions are used yet)\n");
        lua_pop(L, 1);
    } else {
        printf("  ✅ Lua functions registered\n");
    }
    
    // ========================================================================
    // Create user handle with Lua state
    // ========================================================================
    
    user_handle_t user_handle = {
        .lua_state = L,
    };
    
    // ========================================================================
    // Create allocator (user_handle passed as ctx)
    // ========================================================================
    
    s_expr_allocator_t alloc = {
        .malloc   = simple_malloc,
        .free     = simple_free,
        .ctx      = &user_handle,
        .get_time = utc_realtime
    };
    
    s_engine_handle_t engine;
    
    // ========================================================================
    // Step 4: C-native function registration
    // Step 5: Lua trampoline registration
    //
    // Both are handled via user_fns[] callbacks during module load.
    // Add a second callback for Lua trampoline registration when ready:
    //   extern void my_module_register_lua(s_engine_handle_t* engine);
    //   s_engine_user_register_fn user_fns[] = {
    //       register_user_functions,       // Step 4: C-native
    //       register_lua_trampolines,      // Step 5: Lua trampolines
    //   };
    // ========================================================================
    
    s_engine_user_register_fn user_fns[] = {
        register_user_functions,        // Step 4: C-native functions
        register_lua_trampolines,    // Step 5: Lua trampolines (add when ready)
    };
    size_t user_fn_count = sizeof(user_fns) / sizeof(user_fns[0]);
    
    // ========================================================================
    // Step 6: Load binary module, create tree instances, run
    // ========================================================================
    
    // --- TEST 1: Load from ROM ---
    
    printf("\n=== Step 6: Loading module from ROM ===\n\n");
    
    bool loaded = s_engine_load_from_rom(
        &engine,
        &alloc,
        state_machine_test_module_bin_32,
        STATE_MACHINE_TEST_MODULE_BIN_32_SIZE,
        debug_callback,
        user_fn_count,
        user_fns
    );
    
    if (!loaded) {
        printf("❌ FATAL: Failed to load module from ROM\n");
        lua_close(L);
        return 1;
    }
    
    printf("✅ Engine loaded from ROM (Lua bridge active)\n");
    run_state_machine_tests(&engine);
    s_engine_free(&engine);
    
    // --- TEST 2: Load from File (optional) ---
    
    printf("\n\n=== Loading module from file ===\n\n");
    
    loaded = s_engine_load_from_file(
        &engine,
        &alloc,
        "state_machine_test_32.bin",
        debug_callback,
        user_fn_count,
        user_fns
    );
    
    if (!loaded) {
        printf("⚠️  WARNING: Could not load from file (may not exist)\n");
        printf("   This is OK if running without the binary file.\n");
    } else {
        printf("✅ Engine loaded from file (Lua bridge active)\n");
        run_state_machine_tests(&engine);
        s_engine_free(&engine);
    }
    
    // ========================================================================
    // Cleanup
    // ========================================================================
    
    printf("\n=== Cleaning up ===\n");
    lua_close(L);
    printf("  ✅ Lua state destroyed\n");
    
    printf("\n✅ All tests completed!\n\n");
    return 0;
}
