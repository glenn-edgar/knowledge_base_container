// ============================================================================
// main.c
// Test harness for S-Expression Engine v2.5
// Demonstrates two-tier architecture with multiple simultaneous tree instances
// 
// Build:
//   gcc -o test_runner main.c s_engine_module.c s_engine_eval.c
//   ./test_runner
//
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// Include generated module header FIRST
#include "test_comprehensive.h"

// Then include engine headers
#include "s_engine_module.h"
#include "s_engine_eval.h"

// ============================================================================
// TEST STATE (per tree instance)
// ============================================================================

typedef struct {
    // Instance identifier
    uint16_t ct_node_id;
    const char* name;
    
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
    bool is_valid;
    
    // Delay tracking
    int delay_remaining;
    int delay_target;
    
    // Stats
    int oneshot_calls;
    int boolean_calls;
    int main_calls;
} test_state_t;

// Multiple states for multiple instances
#define MAX_TEST_INSTANCES 4
static test_state_t g_states[MAX_TEST_INSTANCES];

static test_state_t* get_state_for_instance(s_expr_tree_instance_t* inst) {
    uint16_t id = s_expr_tree_get_ct_node_id(inst);
    if (id < MAX_TEST_INSTANCES) {
        return &g_states[id];
    }
    return &g_states[0];
}

// ============================================================================
// HELPER: Print param value
// ============================================================================

static void print_param(s_expr_tree_instance_t* inst, const s_expr_param_t* p) {
    switch (p->type) {
        case S_EXPR_PARAM_INT:
            printf("int(%d)", (int)p->i);
            break;
        case S_EXPR_PARAM_UINT:
            printf("uint(0x%X)", (unsigned)p->u);
            break;
        case S_EXPR_PARAM_FLOAT:
            printf("float(%f)", (double)p->f);
            break;
        case S_EXPR_PARAM_STRING:
            printf("str[%d]", p->str_index);
            break;
        case S_EXPR_PARAM_MAIN:
            printf("main_ref[%d]", p->func_idx);
            break;
        case S_EXPR_PARAM_ONESHOT:
            printf("oneshot_ref[%d]", p->func_idx);
            break;
        case S_EXPR_PARAM_PRED:
            printf("pred_ref[%d]", p->func_idx);
            break;
        case S_EXPR_PARAM_OPEN:
            printf("{data");
            break;
        case S_EXPR_PARAM_OPEN_CALL:
            printf("{call");
            break;
        case S_EXPR_PARAM_CLOSE:
            printf("}");
            break;
        default:
            printf("?(%d)", p->type);
    }
    
    if (p->type == S_EXPR_PARAM_STRING) {
        const char* s = s_expr_tree_get_string(inst, p->str_index);
        if (s) printf("=\"%s\"", s);
    }
}

static void print_params(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint8_t count) {
    printf("(");
    for (uint8_t i = 0; i < count; i++) {
        if (i > 0) printf(", ");
        print_param(inst, &params[i]);
    }
    printf(")");
}

// ============================================================================
// ONESHOT FUNCTIONS (@)
// ============================================================================

static void fn_led_on(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s @] LED_ON\n", ts->name);
    ts->led_on = true;
    ts->oneshot_calls++;
}

static void fn_led_off(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s @] LED_OFF\n", ts->name);
    ts->led_on = false;
    ts->oneshot_calls++;
}

static void fn_log(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    const char* msg = "";
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_STRING) {
        msg = s_expr_tree_get_string(inst, params[0].str_index);
    }
    
    printf("  [%s @] LOG: \"%s\"\n", ts->name, msg ? msg : "(null)");
    ts->oneshot_calls++;
}

static void fn_alarm_on(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s @] ALARM_ON\n", ts->name);
    ts->oneshot_calls++;
}

static void fn_increment_counter(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    ts->counter++;
    printf("  [%s @] INCREMENT_COUNTER -> %d\n", ts->name, ts->counter);
    ts->oneshot_calls++;
}

static void fn_cleanup(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s @] CLEANUP\n", ts->name);
    ts->oneshot_calls++;
}

// ============================================================================
// BOOLEAN FUNCTIONS (?)
// ============================================================================

static bool fn_is_ready(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] IS_READY -> %s\n", ts->name, ts->is_ready ? "true" : "false");
    ts->boolean_calls++;
    return ts->is_ready;
}

static bool fn_is_calibrated(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] IS_CALIBRATED -> %s\n", ts->name, ts->is_calibrated ? "true" : "false");
    ts->boolean_calls++;
    return ts->is_calibrated;
}

static bool fn_has_power(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] HAS_POWER -> %s\n", ts->name, ts->has_power ? "true" : "false");
    ts->boolean_calls++;
    return ts->has_power;
}

static bool fn_has_fault(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] HAS_FAULT -> %s\n", ts->name, ts->has_fault ? "true" : "false");
    ts->boolean_calls++;
    return ts->has_fault;
}

static bool fn_has_warning(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] HAS_WARNING -> %s\n", ts->name, ts->has_warning ? "true" : "false");
    ts->boolean_calls++;
    return ts->has_warning;
}

static bool fn_has_timeout(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] HAS_TIMEOUT -> %s\n", ts->name, ts->has_timeout ? "true" : "false");
    ts->boolean_calls++;
    return ts->has_timeout;
}

static bool fn_has_override(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] HAS_OVERRIDE -> %s\n", ts->name, ts->has_override ? "true" : "false");
    ts->boolean_calls++;
    return ts->has_override;
}

static bool fn_is_valid(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s ?] IS_VALID -> %s\n", ts->name, ts->is_valid ? "true" : "false");
    ts->boolean_calls++;
    return ts->is_valid;
}

// ============================================================================
// MAIN FUNCTIONS (!)
// ============================================================================

static s_expr_result_t fn_delay(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    
    int delay_ms = 500;
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_INT) {
        delay_ms = (int)params[0].i;
    }
    
    if (state->state == 0) {
        ts->delay_target = delay_ms;
        ts->delay_remaining = delay_ms;
        state->state = 1;
        printf("  [%s !] DELAY(%d) starting\n", ts->name, delay_ms);
    }
    
    ts->delay_remaining -= 100;
    
    if (ts->delay_remaining <= 0) {
        printf("  [%s !] DELAY(%d) complete\n", ts->name, delay_ms);
        state->state = 0;
        ts->main_calls++;
        return SE_CONTINUE;
    }
    
    printf("  [%s !] DELAY(%d) remaining: %d\n", ts->name, delay_ms, ts->delay_remaining);
    return SE_HALT;
}

static s_expr_result_t fn_start_motor(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] START_MOTOR\n", ts->name);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_stop_motor(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] STOP_MOTOR\n", ts->name);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_init_system(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] INIT_SYSTEM\n", ts->name);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_test_params(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] TEST_PARAMS ", ts->name);
    print_params(inst, params, param_count);
    printf("\n");
    
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_process_array(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] PROCESS_ARRAY ", ts->name);
    print_params(inst, params, param_count);
    printf("\n");
    
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_eval(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] EVAL ", ts->name);
    print_params(inst, params, param_count);
    printf("\n");
    
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_eval_nested(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] EVAL_NESTED ", ts->name);
    print_params(inst, params, param_count);
    printf("\n");
    
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_register_callbacks(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] REGISTER_CALLBACKS ", ts->name);
    print_params(inst, params, param_count);
    printf("\n");
    
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_filter(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] FILTER ", ts->name);
    print_params(inst, params, param_count);
    printf("\n");
    
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_add(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    ct_int_t sum = 0;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_INT) {
            sum += params[i].i;
        }
    }
    
    printf("  [%s !] ADD -> %d\n", ts->name, (int)sum);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_sub(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    ct_int_t result = 0;
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_INT) {
        result = params[0].i;
        for (uint8_t i = 1; i < param_count; i++) {
            if (params[i].type == S_EXPR_PARAM_INT) {
                result -= params[i].i;
            }
        }
    }
    
    printf("  [%s !] SUB -> %d\n", ts->name, (int)result);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_mul(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    test_state_t* ts = get_state_for_instance(inst);
    ct_int_t product = 1;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_INT) {
            product *= params[i].i;
        }
    }
    
    printf("  [%s !] MUL -> %d\n", ts->name, (int)product);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_on_success(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] ON_SUCCESS\n", ts->name);
    ts->main_calls++;
    return SE_CONTINUE;
}

static s_expr_result_t fn_on_failure(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s !] ON_FAILURE\n", ts->name);
    ts->main_calls++;
    return SE_CONTINUE;
}

// ============================================================================
// DEBUG FUNCTION
// ============================================================================

static void fn_debug(s_expr_tree_instance_t* inst, const char* message) {
    test_state_t* ts = get_state_for_instance(inst);
    printf("  [%s DBG] %s\n", ts->name, message);
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

static const s_expr_fn_entry_t oneshot_entries[] = {
    { "LED_ON",            (void*)fn_led_on },
    { "LED_OFF",           (void*)fn_led_off },
    { "LOG",               (void*)fn_log },
    { "ALARM_ON",          (void*)fn_alarm_on },
    { "INCREMENT_COUNTER", (void*)fn_increment_counter },
    { "CLEANUP",           (void*)fn_cleanup },
};

static const s_expr_fn_entry_t boolean_entries[] = {
    { "IS_READY",      (void*)fn_is_ready },
    { "IS_CALIBRATED", (void*)fn_is_calibrated },
    { "HAS_POWER",     (void*)fn_has_power },
    { "HAS_FAULT",     (void*)fn_has_fault },
    { "HAS_WARNING",   (void*)fn_has_warning },
    { "HAS_TIMEOUT",   (void*)fn_has_timeout },
    { "HAS_OVERRIDE",  (void*)fn_has_override },
    { "IS_VALID",      (void*)fn_is_valid },
};

static const s_expr_fn_entry_t main_entries[] = {
    { "DELAY",              (void*)fn_delay },
    { "START_MOTOR",        (void*)fn_start_motor },
    { "INIT_SYSTEM",        (void*)fn_init_system },
    { "STOP_MOTOR",         (void*)fn_stop_motor },
    { "TEST_PARAMS",        (void*)fn_test_params },
    { "PROCESS_ARRAY",      (void*)fn_process_array },
    { "EVAL",               (void*)fn_eval },
    { "ADD",                (void*)fn_add },
    { "EVAL_NESTED",        (void*)fn_eval_nested },
    { "MUL",                (void*)fn_mul },
    { "SUB",                (void*)fn_sub },
    { "REGISTER_CALLBACKS", (void*)fn_register_callbacks },
    { "ON_SUCCESS",         (void*)fn_on_success },
    { "ON_FAILURE",         (void*)fn_on_failure },
    { "FILTER",             (void*)fn_filter },
};

// ============================================================================
// TEST STATE MANAGEMENT
// ============================================================================

static void init_test_state(test_state_t* ts, uint16_t id, const char* name) {
    memset(ts, 0, sizeof(test_state_t));
    ts->ct_node_id = id;
    ts->name = name;
    ts->has_power = true;
    ts->is_ready = true;
    ts->is_calibrated = true;
    ts->is_valid = true;
}

static const char* se_result_str(s_expr_result_t code) {
    switch (code) {
        case SE_CONTINUE:  return "CONTINUE";
        case SE_HALT:      return "HALT";
        case SE_TERMINATE: return "TERMINATE";
        case SE_RESET:     return "RESET";
        case SE_DISABLE:   return "DISABLE";
        default:            return "UNKNOWN";
    }
}

// ============================================================================
// SINGLE TREE TEST
// ============================================================================

static void run_single_tree_test(
    s_expr_module_t* mod,
    const char* tree_name,
    uint16_t ct_node_id,
    int max_ticks
) {
    printf("\n");
    printf("============================================================\n");
    printf("TREE: %s (node %d)\n", tree_name, ct_node_id);
    printf("============================================================\n");
    
    // Initialize test state
    init_test_state(&g_states[ct_node_id], ct_node_id, tree_name);
    
    // Create tree instance
    s_expr_tree_instance_t* inst = s_expr_tree_create(mod, tree_name, NULL, ct_node_id);
    if (!inst) {
        printf("ERROR: Failed to create tree instance: %s\n", tree_name);
        return;
    }
    
    printf("Tree: %s, Nodes: %d\n", 
           s_expr_tree_get_name(inst), 
           s_expr_tree_node_count(inst));
    
    for (int tick = 0; tick < max_ticks; tick++) {
        printf("\n--- Tick %d ---\n", tick + 1);
        
        s_expr_result_t result = s_expr_tree_tick(inst, 0, NULL);
        
        printf("Result: %s\n", se_result_str(result));
        
        if (result == SE_TERMINATE || result == SE_CONTINUE) {
            break;
        }
    }
    
    test_state_t* ts = &g_states[ct_node_id];
    printf("\nStats: oneshot=%d, boolean=%d, main=%d\n",
           ts->oneshot_calls, ts->boolean_calls, ts->main_calls);
    
    // Cleanup
    s_expr_tree_destroy(inst);
}

// ============================================================================
// MULTI-INSTANCE TEST (demonstrates simultaneous execution)
// ============================================================================

static void run_multi_instance_test(s_expr_module_t* mod) {
    printf("\n");
    printf("============================================================\n");
    printf("MULTI-INSTANCE TEST: Two trees running simultaneously\n");
    printf("============================================================\n");
    
    // Create two instances of the same tree
    init_test_state(&g_states[0], 0, "inst_A");
    init_test_state(&g_states[1], 1, "inst_B");
    
    s_expr_tree_instance_t* inst_a = s_expr_tree_create(
        mod, "simple_pipeline_2", NULL, 0
    );
    s_expr_tree_instance_t* inst_b = s_expr_tree_create(
        mod, "simple_pipeline_2", NULL, 1
    );
    
    if (!inst_a || !inst_b) {
        printf("ERROR: Failed to create instances\n");
        if (inst_a) s_expr_tree_destroy(inst_a);
        if (inst_b) s_expr_tree_destroy(inst_b);
        return;
    }
    
    printf("Created two instances of 'simple_pipeline_2'\n");
    printf("Each has independent node states\n\n");
    
    // Run alternating ticks
    for (int tick = 0; tick < 6; tick++) {
        printf("--- Tick %d ---\n", tick + 1);
        
        s_expr_result_t result_a = s_expr_tree_tick(inst_a, 0, NULL);
        s_expr_result_t result_b = s_expr_tree_tick(inst_b, 0, NULL);
        
        printf("  inst_A: %s, inst_B: %s\n", 
               se_result_str(result_a), se_result_str(result_b));
        
        if (result_a == SE_CONTINUE && result_b == SE_CONTINUE) {
            break;
        }
    }
    
    printf("\nFinal stats:\n");
    printf("  inst_A: oneshot=%d, main=%d\n", 
           g_states[0].oneshot_calls, g_states[0].main_calls);
    printf("  inst_B: oneshot=%d, main=%d\n", 
           g_states[1].oneshot_calls, g_states[1].main_calls);
    
    s_expr_tree_destroy(inst_a);
    s_expr_tree_destroy(inst_b);
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    (void)argc; (void)argv;
    
    printf("ChainTree S-Expression Engine v2.5\n");
    printf("Two-tier Architecture: Shared Module + Tree Instances\n");
    printf("\n");
    
    // ========================================================================
    // STEP 1: Set up allocator (just function pointers, no state)
    // ========================================================================
    
    s_expr_allocator_t alloc = {
        .malloc = test_malloc,
        .free = test_free
    };
    
    // Simulated ChainTree runtime handle (in real system, provided by runtime)
    void* runtime_handle = NULL;
    
    // ========================================================================
    // STEP 2: Set up function tables (can be from different sources)
    // ========================================================================
    
    // System functions (could come from a system library)
    s_expr_fn_table_t system_oneshot = {
        .entries = oneshot_entries,
        .count = sizeof(oneshot_entries) / sizeof(oneshot_entries[0])
    };
    
    s_expr_fn_table_t system_boolean = {
        .entries = boolean_entries,
        .count = sizeof(boolean_entries) / sizeof(boolean_entries[0])
    };
    
    s_expr_fn_table_t system_main = {
        .entries = main_entries,
        .count = sizeof(main_entries) / sizeof(main_entries[0])
    };
    
    // ========================================================================
    // STEP 3: Initialize module using three-step process
    // ========================================================================
    
    printf("Creating module structure...\n");
    
    // Step 1: Create module structure
    s_expr_module_t* mod = s_expr_module_create(
        &test_comprehensive_module,
        &alloc,
        runtime_handle,
        0  // ct_node_id (0 for system-level)
    );
    
    if (!mod) {
        printf("ERROR: Module allocation failed\n");
        return 1;
    }
    
    // Check for create errors (allocation, 64-bit mismatch)
    if (s_expr_module_get_error(mod) != S_EXPR_MOD_OK) {
        printf("ERROR: %s\n", s_expr_module_error_str(s_expr_module_get_error(mod)));
        s_expr_module_deinit(mod);
        return 1;
    }
    
    // Step 2: Load functions (can call multiple times with different tables)
    printf("Loading functions...\n");
    
    uint16_t loaded_oneshot = s_expr_module_load_oneshot(mod, &system_oneshot);
    uint16_t loaded_boolean = s_expr_module_load_boolean(mod, &system_boolean);
    uint16_t loaded_main = s_expr_module_load_main(mod, &system_main);
    s_expr_module_set_debug(mod, fn_debug);
    
    printf("  Loaded: %d oneshot, %d boolean, %d main\n", 
           loaded_oneshot, loaded_boolean, loaded_main);
    
    // Step 3: Validate all functions resolved
    printf("Validating...\n");
    
    uint8_t err = s_expr_module_validate(mod);
    if (err != S_EXPR_MOD_OK) {
        printf("ERROR: %s", s_expr_module_error_str(err));
        if (s_expr_module_get_error_name(mod)) {
            printf(" - '%s' (index %d)", 
                   s_expr_module_get_error_name(mod),
                   s_expr_module_get_error_index(mod));
        }
        printf("\n");
        s_expr_module_deinit(mod);
        return 1;
    }
    
    printf("Module validated successfully!\n\n");
    
    printf("Module: %s\n", s_expr_module_get_name(mod));
    printf("Trees: %d\n", s_expr_module_tree_count(mod));
    printf("64-bit: %s\n", s_expr_module_is_64bit(mod) ? "yes" : "no");
    
    // List all trees
    printf("\nAvailable trees:\n");
    for (uint16_t i = 0; i < s_expr_module_tree_count(mod); i++) {
        printf("  [%d] %s\n", i, s_expr_module_tree_name(mod, i));
    }
    
    // ========================================================================
    // STEP 4: Run tests (create tree instances as needed)
    // ========================================================================
    
    // Single tree tests
    run_single_tree_test(mod, "simple_pipeline_2", 0, 15);
    
    // Multi-instance test (demonstrates simultaneous execution)
    run_multi_instance_test(mod);
    
    // ========================================================================
    // STEP 5: Cleanup shared module
    // ========================================================================
    
    s_expr_module_deinit(mod);
    
    printf("\n============================================================\n");
    printf("All tests completed.\n");
    printf("============================================================\n");
    
    return 0;
}