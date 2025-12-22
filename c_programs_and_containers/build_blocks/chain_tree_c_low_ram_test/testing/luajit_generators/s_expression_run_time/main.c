// ============================================================================
// main.c
// Test harness for test_comprehensive.lua module
// 
// Build:
//   1. Generate header: luajit compile.lua test_comprehensive.lua --header=test_module.h
//   2. Compile: gcc -o test_runner main.c s_engine_module.c s_engine_eval.c
//   3. Run: ./test_runner
//
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// Include generated module header FIRST (defines MODULE_IS_64BIT, types)
// Generate with: luajit compile.lua test_comprehensive.lua --header=test_comprehensive.h
#include "test_comprehensive.h"

// Then include engine headers
#include "s_engine_module.h"
#include "s_engine_eval.h"

// ============================================================================
// TEST STATE
// ============================================================================

typedef struct {
    // Simulated hardware state
    bool led_on;
    int counter;
    
    // Simulated conditions
    bool is_ready;
    bool is_calibrated;
    bool has_power;
    bool has_fault;
    bool has_warning;
    bool has_timeout;
    bool has_override;
    
    // Delay tracking
    int delay_remaining;
    int delay_target;
    
    // Test tracking
    int tick_count;
    int oneshot_calls;
    int boolean_calls;
    int main_calls;
} test_state_t;

static test_state_t g_state;

// ============================================================================
// HELPER: Print param value
// ============================================================================

static void print_param(const param_t* p) {
    switch (p->type) {
        case PARAM_INT:
            printf("int(%d)", (int)p->i);
            break;
        case PARAM_UINT:
            printf("uint(0x%X)", (unsigned)p->u);
            break;
        case PARAM_FLOAT:
            printf("float(%f)", (double)p->f);
            break;
        case PARAM_STRING:
            printf("str[%d]", p->str_index);
            break;
        case PARAM_MAIN:
            printf("main_ref[%d]", p->func_idx);
            break;
        case PARAM_ONESHOT:
            printf("oneshot_ref[%d]", p->func_idx);
            break;
        case PARAM_PRED:
            printf("pred_ref[%d]", p->func_idx);
            break;
        case PARAM_OPEN:
            printf("{data");
            break;
        case PARAM_OPEN_CALL:
            printf("{call");
            break;
        case PARAM_CLOSE:
            printf("}");
            break;
        default:
            printf("?(%d)", p->type);
    }
}

static void print_params(module_runtime_t* mod, const param_t* params, uint8_t count) {
    printf("(");
    for (uint8_t i = 0; i < count; i++) {
        if (i > 0) printf(", ");
        print_param(&params[i]);
        
        // For strings, also print the actual string
        if (params[i].type == PARAM_STRING) {
            const char* s = module_get_string(mod, params[i].str_index);
            if (s) printf("=\"%s\"", s);
        }
    }
    printf(")");
}

// ============================================================================
// ONESHOT FUNCTIONS (@)
// ============================================================================

static void fn_led_on(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [@] LED_ON\n");
    g_state.led_on = true;
    g_state.oneshot_calls++;
}

static void fn_led_off(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [@] LED_OFF\n");
    g_state.led_on = false;
    g_state.oneshot_calls++;
}

static void fn_log(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    const char* msg = "";
    if (param_count > 0 && params[0].type == PARAM_STRING) {
        msg = module_get_string(mod, params[0].str_index);
    }
    
    printf("  [@] LOG: \"%s\"\n", msg ? msg : "(null)");
    g_state.oneshot_calls++;
}

static void fn_alarm_on(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [@] ALARM_ON\n");
    g_state.oneshot_calls++;
}

static void fn_increment_counter(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    g_state.counter++;
    printf("  [@] INCREMENT_COUNTER -> %d\n", g_state.counter);
    g_state.oneshot_calls++;
}

static void fn_cleanup(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [@] CLEANUP\n");
    g_state.oneshot_calls++;
}

// ============================================================================
// BOOLEAN FUNCTIONS (?)
// ============================================================================

static bool fn_is_ready(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] IS_READY -> %s\n", g_state.is_ready ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.is_ready;
}

static bool fn_is_calibrated(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] IS_CALIBRATED -> %s\n", g_state.is_calibrated ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.is_calibrated;
}

static bool fn_has_power(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] HAS_POWER -> %s\n", g_state.has_power ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.has_power;
}

static bool fn_has_fault(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] HAS_FAULT -> %s\n", g_state.has_fault ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.has_fault;
}

static bool fn_has_warning(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] HAS_WARNING -> %s\n", g_state.has_warning ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.has_warning;
}

static bool fn_has_timeout(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] HAS_TIMEOUT -> %s\n", g_state.has_timeout ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.has_timeout;
}

static bool fn_has_override(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] HAS_OVERRIDE -> %s\n", g_state.has_override ? "true" : "false");
    g_state.boolean_calls++;
    return g_state.has_override;
}

static bool fn_is_valid(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] IS_VALID -> true\n");
    g_state.boolean_calls++;
    return true;
}

// ============================================================================
// MAIN FUNCTIONS (!)
// ============================================================================

static cfl_code_t fn_delay(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)event_id; (void)event_data;
    
    // Get delay value from first param
    int delay_ms = 500;
    if (param_count > 0 && params[0].type == PARAM_INT) {
        delay_ms = (int)params[0].i;
    }
    
    // Initialize on first call
    if (state->state == 0) {
        g_state.delay_target = delay_ms;
        g_state.delay_remaining = delay_ms;
        state->state = 1;
        printf("  [!] DELAY(%d) starting\n", delay_ms);
    }
    
    // Simulate tick (decrement by 100ms per tick)
    g_state.delay_remaining -= 100;
    
    if (g_state.delay_remaining <= 0) {
        printf("  [!] DELAY(%d) complete\n", delay_ms);
        state->state = 0;
        g_state.main_calls++;
        return CFL_CONTINUE;
    }
    
    printf("  [!] DELAY(%d) remaining: %d\n", delay_ms, g_state.delay_remaining);
    return CFL_HALT;
}

static cfl_code_t fn_start_motor(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [!] START_MOTOR\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_stop_motor(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [!] STOP_MOTOR\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_init_system(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [!] INIT_SYSTEM\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_test_params(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    printf("  [!] TEST_PARAMS ");
    print_params(mod, params, param_count);
    printf("\n");
    
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_process_array(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    printf("  [!] PROCESS_ARRAY ");
    print_params(mod, params, param_count);
    printf("\n");
    
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_eval(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    printf("  [!] EVAL ");
    print_params(mod, params, param_count);
    printf("\n");
    
    // If first param is PARAM_OPEN_CALL, evaluate it
    if (param_count > 0 && params[0].type == PARAM_OPEN_CALL) {
        printf("      -> Evaluating S-expression...\n");
        // In real code: eval_sexpr(mod, node, state, params, 0);
    }
    
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_eval_nested(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    printf("  [!] EVAL_NESTED ");
    print_params(mod, params, param_count);
    printf("\n");
    
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_register_callbacks(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    printf("  [!] REGISTER_CALLBACKS ");
    print_params(mod, params, param_count);
    printf("\n");
    
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_filter(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    printf("  [!] FILTER ");
    print_params(mod, params, param_count);
    printf("\n");
    
    g_state.main_calls++;
    return CFL_CONTINUE;
}

// Arithmetic functions for S-expr tests
static cfl_code_t fn_add(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    
    ct_int_t sum = 0;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == PARAM_INT) {
            sum += params[i].i;
        }
    }
    
    printf("  [!] ADD -> %d\n", (int)sum);
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_sub(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    
    ct_int_t result = 0;
    if (param_count > 0 && params[0].type == PARAM_INT) {
        result = params[0].i;
        for (uint8_t i = 1; i < param_count; i++) {
            if (params[i].type == PARAM_INT) {
                result -= params[i].i;
            }
        }
    }
    
    printf("  [!] SUB -> %d\n", (int)result);
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_mul(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    
    ct_int_t product = 1;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == PARAM_INT) {
            product *= params[i].i;
        }
    }
    
    printf("  [!] MUL -> %d\n", (int)product);
    g_state.main_calls++;
    return CFL_CONTINUE;
}

// Placeholder functions for function refs
static cfl_code_t fn_on_success(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("  [!] ON_SUCCESS\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_on_failure(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("  [!] ON_FAILURE\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_transform(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("  [!] TRANSFORM\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

static cfl_code_t fn_square(
    module_runtime_t* mod, const node_t* node, node_state_t* state,
    uint16_t event_id, void* event_data,
    const param_t* params, uint8_t param_count
) {
    (void)mod; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    printf("  [!] SQUARE\n");
    g_state.main_calls++;
    return CFL_CONTINUE;
}

// ============================================================================
// DEBUG FUNCTION
// ============================================================================

static void fn_debug(module_runtime_t* mod, const char* message) {
    (void)mod;
    printf("  [DBG] %s\n", message);
}

// ============================================================================
// ALLOCATOR
// ============================================================================

static void* test_malloc(void* handle, uint16_t ct_node_id, size_t size) {
    (void)handle; (void)ct_node_id;
    return malloc(size);
}

static void test_free(void* handle, uint16_t ct_node_id, void* ptr) {
    (void)handle; (void)ct_node_id;
    free(ptr);
}

// ============================================================================
// FUNCTION TABLES
// ============================================================================

static const fn_entry_t oneshot_entries[] = {
    { "LED_ON",            (void*)fn_led_on },
    { "LED_OFF",           (void*)fn_led_off },
    { "LOG",               (void*)fn_log },
    { "ALARM_ON",          (void*)fn_alarm_on },
    { "INCREMENT_COUNTER", (void*)fn_increment_counter },
    { "CLEANUP",           (void*)fn_cleanup },
};

static const fn_entry_t boolean_entries[] = {
    { "IS_READY",      (void*)fn_is_ready },
    { "IS_CALIBRATED", (void*)fn_is_calibrated },
    { "HAS_POWER",     (void*)fn_has_power },
    { "HAS_FAULT",     (void*)fn_has_fault },
    { "HAS_WARNING",   (void*)fn_has_warning },
    { "HAS_TIMEOUT",   (void*)fn_has_timeout },
    { "HAS_OVERRIDE",  (void*)fn_has_override },
    { "IS_VALID",      (void*)fn_is_valid },
};

static const fn_entry_t main_entries[] = {
    { "DELAY",              (void*)fn_delay },
    { "START_MOTOR",        (void*)fn_start_motor },
    { "STOP_MOTOR",         (void*)fn_stop_motor },
    { "INIT_SYSTEM",        (void*)fn_init_system },
    { "TEST_PARAMS",        (void*)fn_test_params },
    { "PROCESS_ARRAY",      (void*)fn_process_array },
    { "EVAL",               (void*)fn_eval },
    { "EVAL_NESTED",        (void*)fn_eval_nested },
    { "REGISTER_CALLBACKS", (void*)fn_register_callbacks },
    { "FILTER",             (void*)fn_filter },
    { "ADD",                (void*)fn_add },
    { "SUB",                (void*)fn_sub },
    { "MUL",                (void*)fn_mul },
    { "ON_SUCCESS",         (void*)fn_on_success },
    { "ON_FAILURE",         (void*)fn_on_failure },
    { "TRANSFORM",          (void*)fn_transform },
    { "SQUARE",             (void*)fn_square },
};

// ============================================================================
// TEST RUNNER
// ============================================================================

static void reset_state(void) {
    memset(&g_state, 0, sizeof(g_state));
    g_state.has_power = true;  // Default: power is on
}

static void run_tree_test(
    module_runtime_t* mod,
    const char* tree_name,
    int max_ticks
) {
    printf("\n");
    printf("============================================================\n");
    printf("TREE: %s\n", tree_name);
    printf("============================================================\n");
    
    if (!module_select_tree_by_name(mod, tree_name)) {
        printf("ERROR: Tree not found: %s\n", tree_name);
        return;
    }
    
    reset_state();
    g_state.is_ready = true;
    g_state.is_calibrated = true;
    
    for (int tick = 0; tick < max_ticks; tick++) {
        printf("\n--- Tick %d ---\n", tick + 1);
        
        cfl_code_t result = module_tick(mod, 0, NULL);
        
        printf("Result: ");
        switch (result) {
            case CFL_CONTINUE:  printf("CONTINUE\n"); break;
            case CFL_HALT:      printf("HALT\n"); break;
            case CFL_TERMINATE: printf("TERMINATE\n"); break;
            case CFL_RESET:     printf("RESET\n"); break;
            case CFL_DISABLE:   printf("DISABLE\n"); break;
            default:            printf("? (%d)\n", result); break;
        }
        
        // Stop on terminal states
        if (result == CFL_TERMINATE || result == CFL_CONTINUE) {
            break;
        }
    }
    
    printf("\nStats: oneshot=%d, boolean=%d, main=%d\n",
           g_state.oneshot_calls, g_state.boolean_calls, g_state.main_calls);
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    (void)argc; (void)argv;
    
    printf("ChainTree S-Expression Engine Test Runner\n");
    printf("Version 2.2\n");
    printf("\n");
    
    // Set up allocator
    s_allocator_t alloc = {
        .malloc = test_malloc,
        .free = test_free
    };
    
    // Set up registry
    module_registry_t registry = {
        .oneshot = {
            .entries = oneshot_entries,
            .count = sizeof(oneshot_entries) / sizeof(oneshot_entries[0])
        },
        .boolean = {
            .entries = boolean_entries,
            .count = sizeof(boolean_entries) / sizeof(boolean_entries[0])
        },
        .main = {
            .entries = main_entries,
            .count = sizeof(main_entries) / sizeof(main_entries[0])
        },
        .debug = fn_debug
    };
    
    // Create module
    // NOTE: test_comprehensive_module is defined in test_comprehensive.h
    // Generate with: luajit compile.lua test_comprehensive.lua --header=test_comprehensive.h
    
    module_runtime_t* mod = module_create(
        &test_comprehensive_module,
        &registry,
        &alloc,
        NULL,
        0
    );
    
    if (!mod) {
        printf("ERROR: Failed to create module\n");
        return 1;
    }
    
    // Check for function resolution errors
    if (module_get_error(mod) != MOD_OK) {
        printf("ERROR: %s", module_error_str(module_get_error(mod)));
        if (module_get_error_name(mod)) {
            printf(" - '%s' (index %d)", 
                   module_get_error_name(mod),
                   module_get_error_index(mod));
        }
        printf("\n");
        module_destroy(mod);
        return 1;
    }
    
    printf("Module: %s\n", module_get_name(mod));
    printf("Trees: %d\n", module_tree_count(mod));
    printf("64-bit: %s\n", module_is_64bit(mod) ? "yes" : "no");
    
    // List all trees
    printf("\nAvailable trees:\n");
    for (uint16_t i = 0; i < module_tree_count(mod); i++) {
        printf("  [%d] %s\n", i, module_tree_name(mod, i));
    }
    
    // Run tests
    run_tree_test(mod, "simple_pipeline", 10);
    run_tree_test(mod, "if_then_test", 5);
    run_tree_test(mod, "if_else_test", 5);
    run_tree_test(mod, "bool_and_test", 5);
    run_tree_test(mod, "bool_or_test", 5);
    run_tree_test(mod, "bool_not_test", 5);
    run_tree_test(mod, "nested_bool_test", 5);
    run_tree_test(mod, "cond_test", 5);
    run_tree_test(mod, "oneshot_test", 5);
    run_tree_test(mod, "main_return_test", 15);  // More ticks for delay
    run_tree_test(mod, "param_types_test", 5);
    run_tree_test(mod, "deep_nest_test", 5);
    run_tree_test(mod, "data_list_test", 5);
    run_tree_test(mod, "sexpr_test", 5);
    run_tree_test(mod, "func_ref_test", 5);
    
    // Cleanup
    module_destroy(mod);
    
    printf("\n============================================================\n");
    printf("All tests completed.\n");
    printf("============================================================\n");
    
    return 0;
}