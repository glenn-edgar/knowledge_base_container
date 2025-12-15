// ============================================================================
// test_main_bin.c
// S-Node Engine Test Program (Binary File Loading)
// Usage: ./test_snode_bin <module.bin>
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "s_engine_binary.h"
#include "s_engine_file.h"

// ============================================================================
// TEST STATE (same as test_main.c)
// ============================================================================

typedef struct {
    bool ready;
    bool calibrated;
    bool has_power;
    bool has_fault;
    bool has_warning;
    bool has_timeout;
    bool has_override;
    
    int counter;
    int led_state;
    int alarm_state;
    int delay_remaining;
    
    int log_count;
    char last_log[256];
    
    int debug_count;
    char last_debug[256];
} test_state_t;

static test_state_t g_state;

// ============================================================================
// ALLOCATOR
// ============================================================================

static void* test_malloc(void* handle, uint16_t ct_node_id, size_t size) {
    (void)handle;
    (void)ct_node_id;
    void* ptr = malloc(size);
    if (ptr) {
        printf("  [ALLOC] node=%d size=%zu ptr=%p\n", ct_node_id, size, ptr);
    }
    return ptr;
}

static void test_free(void* handle, uint16_t ct_node_id, void* ptr) {
    (void)handle;
    (void)ct_node_id;
    printf("  [FREE]  node=%d ptr=%p\n", ct_node_id, ptr);
    free(ptr);
}

static s_allocator_t g_allocator = {
    .malloc = test_malloc,
    .free = test_free,
};

// ============================================================================
// ONESHOT FUNCTIONS (same as test_main.c)
// ============================================================================

static void fn_led_on(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    g_state.led_state = 1;
    printf("    LED_ON\n");
}

static void fn_led_off(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    g_state.led_state = 0;
    printf("    LED_OFF\n");
}

static void fn_log(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (params && params[0].type == PARAM_STRING) {
        const char* msg = module_get_string(mod, params[0].str_index);
        strncpy(g_state.last_log, msg, sizeof(g_state.last_log) - 1);
        printf("    LOG: \"%s\"\n", msg);
    } else {
        printf("    LOG: (no message)\n");
    }
    g_state.log_count++;
}

static void fn_alarm_on(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    g_state.alarm_state = 1;
    printf("    ALARM_ON\n");
}

static void fn_increment_counter(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    g_state.counter++;
    printf("    INCREMENT_COUNTER -> %d\n", g_state.counter);
}

// ============================================================================
// BOOLEAN FUNCTIONS (same as test_main.c)
// ============================================================================

static bool fn_is_ready(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    IS_READY -> %s\n", g_state.ready ? "true" : "false");
    return g_state.ready;
}

static bool fn_is_calibrated(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    IS_CALIBRATED -> %s\n", g_state.calibrated ? "true" : "false");
    return g_state.calibrated;
}

static bool fn_has_power(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    HAS_POWER -> %s\n", g_state.has_power ? "true" : "false");
    return g_state.has_power;
}

static bool fn_has_fault(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    HAS_FAULT -> %s\n", g_state.has_fault ? "true" : "false");
    return g_state.has_fault;
}

static bool fn_has_warning(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    HAS_WARNING -> %s\n", g_state.has_warning ? "true" : "false");
    return g_state.has_warning;
}

static bool fn_has_timeout(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    HAS_TIMEOUT -> %s\n", g_state.has_timeout ? "true" : "false");
    return g_state.has_timeout;
}

static bool fn_has_override(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    HAS_OVERRIDE -> %s\n", g_state.has_override ? "true" : "false");
    return g_state.has_override;
}

// ============================================================================
// MAIN FUNCTIONS (same as test_main.c)
// ============================================================================

static uint8_t fn_delay(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)event_id; (void)event_data;
    
    int32_t ms = params[0].i32;
    
    if (!(state->flags & NODE_FLAG_INITIALIZED)) {
        state->user_data = (uint16_t)(ms / 100);
        printf("    DELAY(%d) starting, ticks=%d\n", ms, state->user_data);
    }
    
    if (state->user_data > 0) {
        state->user_data--;
        printf("    DELAY waiting, ticks remaining=%d\n", state->user_data);
        return CFL_HALT;
    }
    
    printf("    DELAY complete\n");
    return CFL_CONTINUE;
}

static uint8_t fn_init_system(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    INIT_SYSTEM\n");
    g_state.ready = true;
    g_state.calibrated = true;
    return CFL_CONTINUE;
}

static uint8_t fn_start_motor(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    START_MOTOR\n");
    return CFL_CONTINUE;
}

static uint8_t fn_stop_motor(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data; (void)params;
    printf("    STOP_MOTOR\n");
    return CFL_CONTINUE;
}

static uint8_t fn_test_params(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)state; (void)event_id; (void)event_data;
    
    printf("    TEST_PARAMS: %d params\n", node->param_count);
    
    for (int i = 0; i < node->param_count; i++) {
        switch (params[i].type) {
            case PARAM_INT32:
                printf("      [%d] i32 = %d\n", i, params[i].i32);
                break;
            case PARAM_UINT32:
                printf("      [%d] u32 = 0x%08X\n", i, params[i].u32);
                break;
            case PARAM_FLOAT32:
                printf("      [%d] f32 = %f\n", i, (double)params[i].f32);
                break;
            case PARAM_STRING:
                printf("      [%d] str = \"%s\"\n", i, module_get_string(mod, params[i].str_index));
                break;
        }
    }
    
    return CFL_CONTINUE;
}

// ============================================================================
// DEBUG FUNCTION
// ============================================================================

static void fn_debug(module_runtime_t* mod, const char* message) {
    (void)mod;
    strncpy(g_state.last_debug, message, sizeof(g_state.last_debug) - 1);
    g_state.debug_count++;
    printf("    [DEBUG] %s\n", message);
}

// ============================================================================
// FUNCTION REGISTRY
// ============================================================================

static const fn_entry_t oneshot_entries[] = {
    { "LED_ON",            (void*)fn_led_on },
    { "LED_OFF",           (void*)fn_led_off },
    { "LOG",               (void*)fn_log },
    { "ALARM_ON",          (void*)fn_alarm_on },
    { "INCREMENT_COUNTER", (void*)fn_increment_counter },
};

static const fn_entry_t boolean_entries[] = {
    { "IS_READY",      (void*)fn_is_ready },
    { "IS_CALIBRATED", (void*)fn_is_calibrated },
    { "HAS_POWER",     (void*)fn_has_power },
    { "HAS_FAULT",     (void*)fn_has_fault },
    { "HAS_WARNING",   (void*)fn_has_warning },
    { "HAS_TIMEOUT",   (void*)fn_has_timeout },
    { "HAS_OVERRIDE",  (void*)fn_has_override },
};

static const fn_entry_t main_entries[] = {
    { "DELAY",        (void*)fn_delay },
    { "INIT_SYSTEM",  (void*)fn_init_system },
    { "START_MOTOR",  (void*)fn_start_motor },
    { "STOP_MOTOR",   (void*)fn_stop_motor },
    { "TEST_PARAMS",  (void*)fn_test_params },
};

static const module_registry_t g_registry = {
    .oneshot = { oneshot_entries, sizeof(oneshot_entries) / sizeof(oneshot_entries[0]) },
    .boolean = { boolean_entries, sizeof(boolean_entries) / sizeof(boolean_entries[0]) },
    .main    = { main_entries,    sizeof(main_entries) / sizeof(main_entries[0]) },
    .debug   = fn_debug,
};

// ============================================================================
// TEST HELPERS
// ============================================================================

static void reset_state(void) {
    memset(&g_state, 0, sizeof(g_state));
    g_state.has_power = true;
}

static const char* cfl_name(uint8_t code) {
    switch (code) {
        case CFL_CONTINUE:           return "CONTINUE";
        case CFL_HALT:               return "HALT";
        case CFL_TERMINATE:          return "TERMINATE";
        case CFL_RESET:              return "RESET";
        case CFL_DISABLE:            return "DISABLE";
        case CFL_FUNCTION_TERMINATE: return "FUNCTION_TERMINATE";
        default:                     return "UNKNOWN";
    }
}

static void print_separator(void) {
    printf("\n========================================\n");
}

// ============================================================================
// SIMPLIFIED TESTS (subset to verify binary loading works)
// ============================================================================

static int test_simple_pipeline(module_runtime_t* mod) {
    print_separator();
    printf("TEST: simple_pipeline (from .bin)\n");
    print_separator();
    
    reset_state();
    if (!module_select_tree_by_name(mod, "simple_pipeline")) {
        printf("FAIL: Could not select tree 'simple_pipeline'\n");
        return 1;
    }
    
    printf("\nTick 1:\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (g_state.led_state != 1) {
        printf("FAIL: LED should be ON\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_if_else(module_runtime_t* mod) {
    print_separator();
    printf("TEST: if_else_test (from .bin)\n");
    print_separator();
    
    reset_state();
    g_state.ready = false;
    if (!module_select_tree_by_name(mod, "if_else_test")) {
        printf("FAIL: Could not select tree 'if_else_test'\n");
        return 1;
    }
    
    printf("\nTick 1 (ready=false):\n");
    module_tick(mod, 0, NULL);
    
    if (strcmp(g_state.last_log, "initializing") != 0) {
        printf("FAIL: Expected 'initializing', got '%s'\n", g_state.last_log);
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_cond(module_runtime_t* mod) {
    print_separator();
    printf("TEST: cond_test (from .bin)\n");
    print_separator();
    
    reset_state();
    g_state.has_fault = true;
    if (!module_select_tree_by_name(mod, "cond_test")) {
        printf("FAIL: Could not select tree 'cond_test'\n");
        return 1;
    }
    
    printf("\nTick 1 (has_fault=true):\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (result != CFL_TERMINATE) {
        printf("FAIL: Expected TERMINATE\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_param_types(module_runtime_t* mod) {
    print_separator();
    printf("TEST: param_types_test (from .bin)\n");
    print_separator();
    
    reset_state();
    if (!module_select_tree_by_name(mod, "param_types_test")) {
        printf("FAIL: Could not select tree 'param_types_test'\n");
        return 1;
    }
    
    printf("\nTick 1:\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    printf("\nPASS (visual inspection)\n");
    return 0;
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s <module.bin>\n", argv[0]);
        return 1;
    }
    
    const char* bin_path = argv[1];
    
    printf("\n");
    printf("S-NODE ENGINE BINARY LOADER TEST\n");
    printf("=================================\n");
    printf("Loading: %s\n\n", bin_path);
    
    // Load and parse binary file
    bin_load_result_t load_result = bin_file_load(
        bin_path,
        &g_allocator,
        NULL,   // handle
        0       // ct_node_id for loading
    ,   0       // max_size (no limit)
    );
    
    if (load_result.error_code != BIN_OK) {
        printf("FATAL: Failed to load module: %s\n", load_result.error_msg);
        bin_file_free_all(&load_result, &g_allocator, NULL, 0);
        return 1;
    }
    
    printf("Module loaded successfully:\n");
    printf("  Name: %s\n", load_result.module->def.name);
    printf("  Trees: %d\n", load_result.module->def.tree_count);
    printf("  Max nodes: %d\n", load_result.module->def.max_node_count);
    printf("  Oneshot functions: %d\n", load_result.module->def.oneshot_count);
    printf("  Boolean functions: %d\n", load_result.module->def.boolean_count);
    printf("  Main functions: %d\n", load_result.module->def.main_count);
    printf("  Strings: %d\n", load_result.module->def.string_count);
    printf("  Binary size: %u bytes\n", load_result.buffer->size);
    
    // List trees
    printf("\nTrees:\n");
    for (uint16_t i = 0; i < load_result.module->def.tree_count; i++) {
        printf("  [%d] %s (%d nodes)\n", 
               i,
               load_result.module->def.trees[i].name,
               load_result.module->def.trees[i].node_count);
    }
    
    // Create module runtime
    printf("\nCreating module runtime...\n");
    module_runtime_t* mod = module_create(
        &load_result.module->def,
        &g_registry,
        &g_allocator,
        NULL,   // handle
        1       // ct_node_id for runtime
    );
    
    if (!mod) {
        printf("FATAL: Failed to create module runtime\n");
        bin_file_free_all(&load_result, &g_allocator, NULL, 0);
        return 1;
    }
    
    // Run tests
    int failures = 0;
    
    failures += test_simple_pipeline(mod);
    failures += test_if_else(mod);
    failures += test_cond(mod);
    failures += test_param_types(mod);
    
    // Summary
    print_separator();
    printf("TEST SUMMARY (BINARY LOADER)\n");
    print_separator();
    
    if (failures == 0) {
        printf("\nALL TESTS PASSED\n");
    } else {
        printf("\nFAILURES: %d\n", failures);
    }
    
    // Cleanup
    printf("\nDestroying module runtime...\n");
    module_destroy(mod);
    
    printf("\nFreeing loaded binary...\n");
    bin_file_free_all(&load_result, &g_allocator, NULL, 0);
    
    printf("\nDone.\n");
    return failures;
}