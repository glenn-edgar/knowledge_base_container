// ============================================================================
// test_main.c
// S-Node Engine Test Program
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "s_engine_types.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "test_comprehensive.h"  // Generated header

// ============================================================================
// TEST STATE
// ============================================================================

typedef struct {
    // Simulated hardware/system state
    bool ready;
    bool calibrated;
    bool has_power;
    bool has_fault;
    bool has_warning;
    bool has_timeout;
    bool has_override;
    
    // Counters for verification
    int counter;
    int led_state;
    int alarm_state;
    int delay_remaining;
    
    // Logging
    int log_count;
    char last_log[256];
    
    // Debug trace
    int debug_count;
    char last_debug[256];
} test_state_t;

static test_state_t g_state;

// ============================================================================
// ALLOCATOR (simple malloc wrapper for testing)
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
// ONESHOT FUNCTIONS
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
// BOOLEAN FUNCTIONS
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
// MAIN FUNCTIONS
// ============================================================================

static uint8_t fn_delay(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data, const param_t* params
) {
    (void)mod; (void)node; (void)event_id; (void)event_data;
    
    int32_t ms = params[0].i32;
    
    // Use state->user_data to track remaining time
    if (!(state->flags & NODE_FLAG_INITIALIZED)) {
        state->user_data = (uint16_t)(ms / 100);  // Simplified: 100ms per tick
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
    g_state.has_power = true;  // Default to having power
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
// TEST CASES
// ============================================================================

static int test_simple_pipeline(module_runtime_t* mod) {
    print_separator();
    printf("TEST: simple_pipeline\n");
    printf("Expected: LED_ON, DELAY, LED_OFF, DELAY, CONTINUE\n");
    print_separator();
    
    reset_state();
    module_select_tree_by_name(mod, "simple_pipeline");
    
    printf("\nTick 1:\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    // Should HALT on first DELAY
    if (result != CFL_HALT) {
        printf("FAIL: Expected HALT during delay\n");
        return 1;
    }
    
    if (g_state.led_state != 1) {
        printf("FAIL: LED should be ON\n");
        return 1;
    }
    
    // Continue ticking until complete
    for (int i = 0; i < 20 && result == CFL_HALT; i++) {
        printf("\nTick %d:\n", i + 2);
        result = module_tick(mod, 0, NULL);
        printf("  Result: %s\n", cfl_name(result));
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_if_then(module_runtime_t* mod) {
    print_separator();
    printf("TEST: if_then_test\n");
    printf("Expected: If ready, log message\n");
    print_separator();
    
    // Test with ready=false
    reset_state();
    g_state.ready = false;
    module_select_tree_by_name(mod, "if_then_test");
    
    printf("\nTick 1 (ready=false):\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (g_state.log_count != 0) {
        printf("FAIL: Should not log when not ready\n");
        return 1;
    }
    
    // Test with ready=true
    reset_state();
    g_state.ready = true;
    module_select_tree_by_name(mod, "if_then_test");
    
    printf("\nTick 2 (ready=true):\n");
    result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (g_state.log_count != 1) {
        printf("FAIL: Should log when ready\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_if_else(module_runtime_t* mod) {
    print_separator();
    printf("TEST: if_else_test\n");
    printf("Expected: If ready -> start, else -> init\n");
    print_separator();
    
    // Test else branch
    reset_state();
    g_state.ready = false;
    module_select_tree_by_name(mod, "if_else_test");
    
    printf("\nTick 1 (ready=false, expect init):\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (strcmp(g_state.last_log, "initializing") != 0) {
        printf("FAIL: Expected 'initializing', got '%s'\n", g_state.last_log);
        return 1;
    }
    
    // Test then branch
    reset_state();
    g_state.ready = true;
    module_select_tree_by_name(mod, "if_else_test");
    
    printf("\nTick 2 (ready=true, expect start):\n");
    result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (strcmp(g_state.last_log, "starting") != 0) {
        printf("FAIL: Expected 'starting', got '%s'\n", g_state.last_log);
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_bool_and(module_runtime_t* mod) {
    print_separator();
    printf("TEST: bool_and_test\n");
    printf("Expected: All three conditions must be true\n");
    print_separator();
    
    // All true
    reset_state();
    g_state.ready = true;
    g_state.calibrated = true;
    g_state.has_power = true;
    module_select_tree_by_name(mod, "bool_and_test");
    
    printf("\nTick 1 (all true):\n");
    module_tick(mod, 0, NULL);
    
    if (g_state.log_count != 1) {
        printf("FAIL: Should log when all true\n");
        return 1;
    }
    
    // One false
    reset_state();
    g_state.ready = true;
    g_state.calibrated = false;  // This one false
    g_state.has_power = true;
    module_select_tree_by_name(mod, "bool_and_test");
    
    printf("\nTick 2 (one false):\n");
    module_tick(mod, 0, NULL);
    
    if (g_state.log_count != 0) {
        printf("FAIL: Should not log when one false\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_bool_or(module_runtime_t* mod) {
    print_separator();
    printf("TEST: bool_or_test\n");
    printf("Expected: Any error condition triggers\n");
    print_separator();
    
    // All false - no action
    reset_state();
    module_select_tree_by_name(mod, "bool_or_test");
    
    printf("\nTick 1 (all false):\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (result != CFL_CONTINUE) {
        printf("FAIL: Should CONTINUE when no errors\n");
        return 1;
    }
    
    // One true - should trigger
    reset_state();
    g_state.has_warning = true;
    module_select_tree_by_name(mod, "bool_or_test");
    
    printf("\nTick 2 (has_warning=true):\n");
    result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (result != CFL_TERMINATE) {
        printf("FAIL: Should TERMINATE on error\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_bool_not(module_runtime_t* mod) {
    print_separator();
    printf("TEST: bool_not_test\n");
    printf("Expected: NOT inverts condition\n");
    print_separator();
    
    // ready=true, NOT(ready)=false -> no log
    reset_state();
    g_state.ready = true;
    module_select_tree_by_name(mod, "bool_not_test");
    
    printf("\nTick 1 (ready=true, NOT should be false):\n");
    module_tick(mod, 0, NULL);
    
    if (g_state.log_count != 0) {
        printf("FAIL: Should not log when ready (NOT is false)\n");
        return 1;
    }
    
    // ready=false, NOT(ready)=true -> log
    reset_state();
    g_state.ready = false;
    module_select_tree_by_name(mod, "bool_not_test");
    
    printf("\nTick 2 (ready=false, NOT should be true):\n");
    module_tick(mod, 0, NULL);
    
    if (g_state.log_count != 1) {
        printf("FAIL: Should log when not ready (NOT is true)\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_cond(module_runtime_t* mod) {
    print_separator();
    printf("TEST: cond_test\n");
    printf("Expected: First matching clause executes\n");
    print_separator();
    
    // No conditions match - default
    reset_state();
    module_select_tree_by_name(mod, "cond_test");
    
    printf("\nTick 1 (no conditions, expect default -> CONTINUE):\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (result != CFL_CONTINUE) {
        printf("FAIL: Default should CONTINUE\n");
        return 1;
    }
    
    // Fault condition - should TERMINATE
    reset_state();
    g_state.has_fault = true;
    module_select_tree_by_name(mod, "cond_test");
    
    printf("\nTick 2 (has_fault=true, expect TERMINATE):\n");
    result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (result != CFL_TERMINATE) {
        printf("FAIL: Fault should TERMINATE\n");
        return 1;
    }
    
    // Warning condition - should HALT
    reset_state();
    g_state.has_warning = true;
    module_select_tree_by_name(mod, "cond_test");
    
    printf("\nTick 3 (has_warning=true, expect HALT):\n");
    result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (result != CFL_HALT) {
        printf("FAIL: Warning should HALT\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_oneshot(module_runtime_t* mod) {
    print_separator();
    printf("TEST: oneshot_test\n");
    printf("Expected: Counter increments once per node, not per tick\n");
    print_separator();
    
    reset_state();
    module_select_tree_by_name(mod, "oneshot_test");
    
    printf("\nTick 1:\n");
    module_tick(mod, 0, NULL);
    printf("  Counter: %d\n", g_state.counter);
    
    if (g_state.counter != 3) {
        printf("FAIL: Expected counter=3 (three oneshots), got %d\n", g_state.counter);
        return 1;
    }
    
    // Second tick - oneshots should not fire again
    printf("\nTick 2 (oneshots should not fire again):\n");
    module_tick(mod, 0, NULL);
    printf("  Counter: %d\n", g_state.counter);
    
    if (g_state.counter != 3) {
        printf("FAIL: Counter should still be 3, got %d\n", g_state.counter);
        return 1;
    }
    
    // Reset and tick again - should fire
    printf("\nAfter reset:\n");
    module_reset(mod);
    module_tick(mod, 0, NULL);
    printf("  Counter: %d\n", g_state.counter);
    
    if (g_state.counter != 6) {
        printf("FAIL: Counter should be 6 after reset, got %d\n", g_state.counter);
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_debug(module_runtime_t* mod) {
    print_separator();
    printf("TEST: debug_test\n");
    printf("Expected: Debug callback fires, then child executes\n");
    print_separator();
    
    reset_state();
    module_select_tree_by_name(mod, "debug_test");
    
    printf("\nTick 1:\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (g_state.debug_count != 1) {
        printf("FAIL: Debug should fire once\n");
        return 1;
    }
    
    if (strcmp(g_state.last_debug, "entering debug_test tree") != 0) {
        printf("FAIL: Wrong debug message\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

static int test_param_types(module_runtime_t* mod) {
    print_separator();
    printf("TEST: param_types_test\n");
    printf("Expected: All param types received correctly\n");
    print_separator();
    
    reset_state();
    module_select_tree_by_name(mod, "param_types_test");
    
    printf("\nTick 1:\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    printf("\nPASS (visual inspection of params above)\n");
    return 0;
}

static int test_deep_nest(module_runtime_t* mod) {
    print_separator();
    printf("TEST: deep_nest_test\n");
    printf("Expected: Deeply nested structures evaluate correctly\n");
    print_separator();
    
    // Set up conditions for path through nested structure
    reset_state();
    g_state.ready = true;
    g_state.has_power = true;
    g_state.has_fault = false;
    g_state.calibrated = true;
    module_select_tree_by_name(mod, "deep_nest_test");
    
    printf("\nTick 1 (ready, power, !fault, calibrated):\n");
    uint8_t result = module_tick(mod, 0, NULL);
    printf("  Result: %s\n", cfl_name(result));
    
    if (strcmp(g_state.last_log, "calibrated path") != 0) {
        printf("FAIL: Expected 'calibrated path'\n");
        return 1;
    }
    
    printf("\nPASS\n");
    return 0;
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    (void)argc; (void)argv;
    
    printf("\n");
    printf("S-NODE ENGINE TEST SUITE\n");
    printf("========================\n");
    
    // Create module
    printf("\nCreating module...\n");
    module_runtime_t* mod = module_create(
        &test_comprehensive_module,
        &g_registry,
        &g_allocator,
        NULL,   // handle
        1       // ct_node_id
    );
    
    if (!mod) {
        printf("FATAL: Failed to create module\n");
        return 1;
    }
    
    printf("Module created: %s\n", mod->def->name);
    printf("Trees: %d\n", mod->def->tree_count);
    printf("Max nodes: %d\n", mod->def->max_node_count);
    
    // Run tests
    int failures = 0;
    
    failures += test_simple_pipeline(mod);
    failures += test_if_then(mod);
    failures += test_if_else(mod);
    failures += test_bool_and(mod);
    failures += test_bool_or(mod);
    failures += test_bool_not(mod);
    failures += test_cond(mod);
    failures += test_oneshot(mod);
    failures += test_debug(mod);
    failures += test_param_types(mod);
    failures += test_deep_nest(mod);
    
    // Summary
    print_separator();
    printf("TEST SUMMARY\n");
    print_separator();
    
    if (failures == 0) {
        printf("\nALL TESTS PASSED\n");
    } else {
        printf("\nFAILURES: %d\n", failures);
    }
    
    // Cleanup
    printf("\nDestroying module...\n");
    module_destroy(mod);
    
    printf("\nDone.\n");
    return failures;
}