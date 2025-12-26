// ============================================================================
// main.c
// Test harness for S-Expression Engine v2.7
// Demonstrates two-tier architecture with slotted blackboards
// 
// Build:
//   gcc -o test_runner main.c s_engine_module.c s_engine_eval.c test_comprehensive_pools.c
//   ./test_runner
//
// ============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// Include generated module header FIRST (defines types)
#include "test_comprehensive.h"

// Then include engine headers
#include "s_engine_module.h"
#include "s_engine_eval.h"

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
        case S_EXPR_PARAM_SLOT:
            printf("slot(pool=%d,idx=%d)", p->slot.pool_id, p->slot.slot_index);
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
// SLOT ACCESS HELPERS
// ============================================================================

static motor_state_t* get_motor_slot(const s_expr_param_t* p) {
    if (p->type != S_EXPR_PARAM_SLOT) return NULL;
    if (p->slot.pool_id != POOL_MOTOR_STATE) return NULL;
    return &motor_state_pool[p->slot.slot_index];
}

static led_state_t* get_led_slot(const s_expr_param_t* p) {
    if (p->type != S_EXPR_PARAM_SLOT) return NULL;
    if (p->slot.pool_id != POOL_LED_STATE) return NULL;
    return &led_state_pool[p->slot.slot_index];
}

static system_state_t* get_system_slot(const s_expr_param_t* p) {
    if (p->type != S_EXPR_PARAM_SLOT) return NULL;
    if (p->slot.pool_id != POOL_SYSTEM_STATE) return NULL;
    return &system_state_pool[p->slot.slot_index];
}

static alarm_state_t* get_alarm_slot(const s_expr_param_t* p) {
    if (p->type != S_EXPR_PARAM_SLOT) return NULL;
    if (p->slot.pool_id != POOL_ALARM_STATE) return NULL;
    return &alarm_state_pool[p->slot.slot_index];
}

static counter_state_t* get_counter_slot(const s_expr_param_t* p) {
    if (p->type != S_EXPR_PARAM_SLOT) return NULL;
    if (p->slot.pool_id != POOL_COUNTER_STATE) return NULL;
    return &counter_state_pool[p->slot.slot_index];
}

static const char* get_slot_name(const s_expr_param_t* p) {
    if (p->type != S_EXPR_PARAM_SLOT) return "?";
    
    switch (p->slot.pool_id) {
        case POOL_MOTOR_STATE:
            return (p->slot.slot_index == 0) ? "motor_main" : "motor_aux";
        case POOL_LED_STATE:
            return (p->slot.slot_index == 0) ? "led_status" : "led_alarm";
        case POOL_SYSTEM_STATE:
            return "sys_main";
        case POOL_ALARM_STATE:
            return "alarm_main";
        case POOL_COUNTER_STATE:
            return (p->slot.slot_index == 0) ? "counter_a" : "counter_b";
        default:
            return "unknown";
    }
}

// ============================================================================
// ONESHOT FUNCTIONS (@)
// ============================================================================

static void fn_led_on(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        led_state_t* led = get_led_slot(&params[0]);
        if (led) {
            led->on = true;
            printf("  [@] LED_ON slot=%s -> on=true\n", get_slot_name(&params[0]));
            return;
        }
    }
    printf("  [@] LED_ON (no slot)\n");
}

static void fn_led_off(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        led_state_t* led = get_led_slot(&params[0]);
        if (led) {
            led->on = false;
            printf("  [@] LED_OFF slot=%s -> on=false\n", get_slot_name(&params[0]));
            return;
        }
    }
    printf("  [@] LED_OFF (no slot)\n");
}

static void fn_log(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_id; (void)event_data;
    
    const char* msg = "";
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_STRING) {
        msg = s_expr_tree_get_string(inst, params[0].str_index);
    }
    
    printf("  [@] LOG: \"%s\"\n", msg ? msg : "(null)");
}

static void fn_alarm_on(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        alarm_state_t* alarm = get_alarm_slot(&params[0]);
        if (alarm) {
            alarm->active = true;
            alarm->level = 1;
            printf("  [@] ALARM_ON slot=%s -> active=true\n", get_slot_name(&params[0]));
            return;
        }
    }
    printf("  [@] ALARM_ON (no slot)\n");
}

static void fn_increment_counter(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        counter_state_t* counter = get_counter_slot(&params[0]);
        if (counter) {
            counter->count++;
            printf("  [@] INCREMENT_COUNTER slot=%s -> count=%u\n", 
                   get_slot_name(&params[0]), (unsigned)counter->count);
            return;
        }
    }
    printf("  [@] INCREMENT_COUNTER (no slot)\n");
}

static void fn_cleanup(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [@] CLEANUP\n");
}

// ============================================================================
// BOOLEAN FUNCTIONS (?)
// ============================================================================

static bool fn_is_ready(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] IS_READY slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->ready ? "true" : "false");
            return sys->ready;
        }
    }
    printf("  [?] IS_READY (no slot) -> false\n");
    return false;
}

static bool fn_is_calibrated(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] IS_CALIBRATED slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->calibrated ? "true" : "false");
            return sys->calibrated;
        }
    }
    printf("  [?] IS_CALIBRATED (no slot) -> false\n");
    return false;
}

static bool fn_has_power(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] HAS_POWER slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->has_power ? "true" : "false");
            return sys->has_power;
        }
    }
    printf("  [?] HAS_POWER (no slot) -> false\n");
    return false;
}

static bool fn_has_fault(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] HAS_FAULT slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->has_fault ? "true" : "false");
            return sys->has_fault;
        }
    }
    printf("  [?] HAS_FAULT (no slot) -> false\n");
    return false;
}

static bool fn_has_warning(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] HAS_WARNING slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->has_warning ? "true" : "false");
            return sys->has_warning;
        }
    }
    printf("  [?] HAS_WARNING (no slot) -> false\n");
    return false;
}

static bool fn_has_timeout(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] HAS_TIMEOUT slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->has_timeout ? "true" : "false");
            return sys->has_timeout;
        }
    }
    printf("  [?] HAS_TIMEOUT (no slot) -> false\n");
    return false;
}

static bool fn_has_override(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            printf("  [?] HAS_OVERRIDE slot=%s -> %s\n", 
                   get_slot_name(&params[0]), sys->has_override ? "true" : "false");
            return sys->has_override;
        }
    }
    printf("  [?] HAS_OVERRIDE (no slot) -> false\n");
    return false;
}

static bool fn_is_valid(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    (void)params; (void)param_count;
    
    printf("  [?] IS_VALID -> true\n");
    return true;
}

static bool fn_is_running(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_id; (void)event_data;
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        motor_state_t* motor = get_motor_slot(&params[0]);
        if (motor) {
            printf("  [?] IS_RUNNING slot=%s -> %s\n", 
                   get_slot_name(&params[0]), motor->running ? "true" : "false");
            return motor->running;
        }
    }
    printf("  [?] IS_RUNNING (no slot) -> false\n");
    return false;
}

// ============================================================================
// MAIN FUNCTIONS (!)
// ============================================================================

static s_expr_result_t fn_delay(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        printf("  [!] DELAY: init event\n");
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        printf("  [!] DELAY: terminate event\n");
        return SE_CONTINUE;
    }
    
    int delay_ms = 500;
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_INT) {
        delay_ms = (int)params[0].i;
    }
    
    if (state->state == 0) {
        state->user_data = (uint16_t)(delay_ms / 100);
        state->state = 1;
        printf("  [!] DELAY(%d) starting, ticks=%d\n", delay_ms, state->user_data);
    }
    
    if (state->user_data > 0) {
        state->user_data--;
        printf("  [!] DELAY(%d) remaining ticks=%d\n", delay_ms, state->user_data);
        return SE_HALT;
    }
    
    printf("  [!] DELAY(%d) complete\n", delay_ms);
    state->state = 0;
    return SE_CONTINUE;
}

static s_expr_result_t fn_start_motor(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        printf("  [!] START_MOTOR: init event\n");
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        printf("  [!] START_MOTOR: terminate event\n");
        return SE_CONTINUE;
    }
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        motor_state_t* motor = get_motor_slot(&params[0]);
        if (motor) {
            motor->running = true;
            motor->speed = 100;
            printf("  [!] START_MOTOR slot=%s -> running=true\n", get_slot_name(&params[0]));
            return SE_CONTINUE;
        }
    }
    printf("  [!] START_MOTOR (no slot)\n");
    return SE_CONTINUE;
}

static s_expr_result_t fn_stop_motor(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        printf("  [!] STOP_MOTOR: init event\n");
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        printf("  [!] STOP_MOTOR: terminate event\n");
        return SE_CONTINUE;
    }
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        motor_state_t* motor = get_motor_slot(&params[0]);
        if (motor) {
            motor->running = false;
            motor->speed = 0;
            printf("  [!] STOP_MOTOR slot=%s -> running=false\n", get_slot_name(&params[0]));
            return SE_CONTINUE;
        }
    }
    printf("  [!] STOP_MOTOR (no slot)\n");
    return SE_CONTINUE;
}

static s_expr_result_t fn_init_system(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT) {
        printf("  [!] INIT_SYSTEM: init event\n");
        return SE_CONTINUE;
    }
    if (event_id == S_EXPR_EVENT_TERMINATE) {
        printf("  [!] INIT_SYSTEM: terminate event\n");
        return SE_CONTINUE;
    }
    
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_SLOT) {
        system_state_t* sys = get_system_slot(&params[0]);
        if (sys) {
            sys->ready = true;
            sys->calibrated = true;
            sys->has_power = true;
            printf("  [!] INIT_SYSTEM slot=%s -> initialized\n", get_slot_name(&params[0]));
            return SE_CONTINUE;
        }
    }
    printf("  [!] INIT_SYSTEM (no slot)\n");
    return SE_CONTINUE;
}

static s_expr_result_t fn_test_params(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] TEST_PARAMS ");
    print_params(inst, params, param_count);
    printf("\n");
    
    return SE_CONTINUE;
}

static s_expr_result_t fn_process_array(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] PROCESS_ARRAY ");
    print_params(inst, params, param_count);
    printf("\n");
    
    return SE_CONTINUE;
}

static s_expr_result_t fn_eval(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] EVAL ");
    print_params(inst, params, param_count);
    printf("\n");
    
    return SE_CONTINUE;
}

static s_expr_result_t fn_eval_nested(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] EVAL_NESTED ");
    print_params(inst, params, param_count);
    printf("\n");
    
    return SE_CONTINUE;
}

static s_expr_result_t fn_register_callbacks(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] REGISTER_CALLBACKS ");
    print_params(inst, params, param_count);
    printf("\n");
    
    return SE_CONTINUE;
}

static s_expr_result_t fn_filter(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] FILTER ");
    print_params(inst, params, param_count);
    printf("\n");
    
    return SE_CONTINUE;
}

static s_expr_result_t fn_add(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    ct_int_t sum = 0;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_INT) {
            sum += params[i].i;
        }
    }
    
    printf("  [!] ADD -> %d\n", (int)sum);
    return SE_CONTINUE;
}

static s_expr_result_t fn_sub(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    ct_int_t result = 0;
    if (param_count > 0 && params[0].type == S_EXPR_PARAM_INT) {
        result = params[0].i;
        for (uint8_t i = 1; i < param_count; i++) {
            if (params[i].type == S_EXPR_PARAM_INT) {
                result -= params[i].i;
            }
        }
    }
    
    printf("  [!] SUB -> %d\n", (int)result);
    return SE_CONTINUE;
}

static s_expr_result_t fn_mul(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    ct_int_t product = 1;
    for (uint8_t i = 0; i < param_count; i++) {
        if (params[i].type == S_EXPR_PARAM_INT) {
            product *= params[i].i;
        }
    }
    
    printf("  [!] MUL -> %d\n", (int)product);
    return SE_CONTINUE;
}

static s_expr_result_t fn_on_success(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] ON_SUCCESS\n");
    return SE_CONTINUE;
}

static s_expr_result_t fn_on_failure(
    s_expr_tree_instance_t* inst, const s_expr_node_t* node, s_expr_node_state_t* state,
    uint16_t event_id, void* event_data,
    const s_expr_param_t* params, uint8_t param_count
) {
    (void)inst; (void)node; (void)state; (void)event_data;
    (void)params; (void)param_count;
    
    if (event_id == S_EXPR_EVENT_INIT || event_id == S_EXPR_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    printf("  [!] ON_FAILURE\n");
    return SE_CONTINUE;
}

// ============================================================================
// DEBUG FUNCTION
// ============================================================================

static void fn_debug(s_expr_tree_instance_t* inst, const char* message) {
    (void)inst;
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
    { "IS_RUNNING",    (void*)fn_is_running },
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

static const s_expr_fn_table_t oneshot_table = {
    .entries = oneshot_entries,
    .count = sizeof(oneshot_entries) / sizeof(oneshot_entries[0])
};

static const s_expr_fn_table_t boolean_table = {
    .entries = boolean_entries,
    .count = sizeof(boolean_entries) / sizeof(boolean_entries[0])
};

static const s_expr_fn_table_t main_table = {
    .entries = main_entries,
    .count = sizeof(main_entries) / sizeof(main_entries[0])
};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

static const char* se_result_str(s_expr_result_t code) {
    switch (code) {
        case SE_CONTINUE:           return "CONTINUE";
        case SE_HALT:               return "HALT";
        case SE_TERMINATE:          return "TERMINATE";
        case SE_RESET:              return "RESET";
        case SE_DISABLE:            return "DISABLE";
        case SE_FUNCTION_TERMINATE: return "FUNCTION_TERMINATE";
        default:                    return "UNKNOWN";
    }
}

static int find_tree_index(const s_expr_module_def_t* def, const char* name) {
    if (!def || !name) return -1;
    for (uint16_t i = 0; i < def->tree_count; i++) {
        if (def->trees[i].name && strcmp(def->trees[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

// ============================================================================
// SINGLE TREE TEST
// ============================================================================

static void run_single_tree_test(
    s_expr_module_t* mod,
    const char* tree_name,
    int max_ticks
) {
    printf("\n");
    printf("============================================================\n");
    printf("TREE: %s\n", tree_name);
    printf("============================================================\n");
    
    int tree_idx = find_tree_index(mod->def, tree_name);
    if (tree_idx < 0) {
        printf("ERROR: Tree not found: %s\n", tree_name);
        return;
    }
    
    s_expr_tree_instance_t* inst = s_expr_tree_create(mod, (uint16_t)tree_idx, NULL, 0);
    if (!inst) {
        printf("ERROR: Failed to create tree instance: %s\n", tree_name);
        return;
    }
    
    printf("Tree: %s, Nodes: %d\n", 
           s_expr_tree_get_name(inst), 
           s_expr_tree_get_node_count(inst));
    
    for (int tick = 0; tick < max_ticks; tick++) {
        printf("\n--- Tick %d ---\n", tick + 1);
        
        s_expr_result_t result = s_expr_tree_tick(inst, 0, NULL);
        
        printf("Result: %s\n", se_result_str(result));
        
        if (result == SE_TERMINATE || result == SE_CONTINUE) {
            break;
        }
    }
    
    s_expr_tree_free(inst);
}

// ============================================================================
// POOL STATE DUMP
// ============================================================================

static void dump_pool_state(void) {
    printf("\n--- Pool State ---\n");
    
    printf("Motor Pool:\n");
    printf("  motor_main: running=%d, speed=%d\n", 
           motor_state_pool[0].running, motor_state_pool[0].speed);
    printf("  motor_aux:  running=%d, speed=%d\n", 
           motor_state_pool[1].running, motor_state_pool[1].speed);
    
    printf("LED Pool:\n");
    printf("  led_status: on=%d\n", led_state_pool[0].on);
    printf("  led_alarm:  on=%d\n", led_state_pool[1].on);
    
    printf("System Pool:\n");
    printf("  sys_main: ready=%d, calibrated=%d, power=%d, fault=%d\n",
           system_state_pool[0].ready,
           system_state_pool[0].calibrated,
           system_state_pool[0].has_power,
           system_state_pool[0].has_fault);
    
    printf("Counter Pool:\n");
    printf("  counter_a: count=%u\n", (unsigned)counter_state_pool[0].count);
    printf("  counter_b: count=%u\n", (unsigned)counter_state_pool[1].count);
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    (void)argc; (void)argv;
    
    printf("ChainTree S-Expression Engine v2.7\n");
    printf("Two-tier Architecture with Slotted Blackboards\n");
    printf("Lifecycle Events: INIT/TERMINATE\n");
    printf("Incremental Function Loading\n");
    printf("\n");
    
    // ========================================================================
    // STEP 1: Initialize pools
    // ========================================================================
    
    printf("Initializing pools...\n");
    test_comprehensive_pools_init();
    
    system_state_pool[0].ready = true;
    system_state_pool[0].calibrated = true;
    system_state_pool[0].has_power = true;
    
    // ========================================================================
    // STEP 2: Set up allocator
    // ========================================================================
    
    s_expr_allocator_t alloc = {
        .malloc = test_malloc,
        .free = test_free
    };
    
    // ========================================================================
    // STEP 3: Initialize module (Phase 1 - no function resolution yet)
    // ========================================================================
    
    printf("Creating module...\n");
    
    s_expr_module_t mod;
    uint8_t err = s_expr_module_init(&mod, &test_comprehensive_module, alloc, NULL);
    
    if (err != S_EXPR_MOD_OK) {
        printf("ERROR: %s\n", s_expr_module_error_str(err));
        return 1;
    }
    
    // Set debug function
    s_expr_module_set_debug(&mod, fn_debug);
    
    // Set pool table
    s_expr_module_set_pool_table(&mod, test_comprehensive_pool_table, TEST_COMPREHENSIVE_POOL_COUNT);
    
    // ========================================================================
    // STEP 4: Load function tables (Phase 2a - can be called multiple times)
    // ========================================================================
    
    printf("Loading functions...\n");
    
    uint16_t loaded_oneshot = s_expr_module_load_oneshot(&mod, &oneshot_table);
    uint16_t loaded_boolean = s_expr_module_load_boolean(&mod, &boolean_table);
    uint16_t loaded_main = s_expr_module_load_main(&mod, &main_table);
    
    printf("  Loaded: %u oneshot, %u boolean, %u main\n",
           loaded_oneshot, loaded_boolean, loaded_main);
    
    // ========================================================================
    // STEP 5: Validate and resolve functions (Phase 2b)
    // ========================================================================
    
    printf("Validating module...\n");
    
    err = s_expr_module_validate(&mod);
    if (err != S_EXPR_MOD_OK) {
        printf("ERROR: %s", s_expr_module_error_str(err));
        if (mod.error_name) {
            printf(" - '%s' (index %d)", mod.error_name, mod.error_index);
        }
        printf("\n");
        s_expr_module_free(&mod);
        return 1;
    }
    
    printf("Module validated successfully!\n\n");
    
    printf("Module: %s\n", s_expr_module_get_name(&mod));
    printf("Trees: %d\n", s_expr_module_tree_count(&mod));
    printf("Pools: %d\n", s_expr_module_get_pool_count(&mod));
    
    printf("\nAvailable trees:\n");
    for (uint16_t i = 0; i < s_expr_module_tree_count(&mod); i++) {
        printf("  [%d] %s\n", i, s_expr_module_tree_name(&mod, i));
    }
    
    // ========================================================================
    // STEP 6: Run tests
    // ========================================================================
    
    run_single_tree_test(&mod, "simple_pipeline_2", 15);
    dump_pool_state();
    
    test_comprehensive_pools_init();
    system_state_pool[0].ready = true;
    system_state_pool[0].calibrated = true;
    system_state_pool[0].has_power = true;
    
    run_single_tree_test(&mod, "if_else_test_9", 5);
    dump_pool_state();
    
    test_comprehensive_pools_init();
    system_state_pool[0].ready = true;
    system_state_pool[0].calibrated = true;
    system_state_pool[0].has_power = true;
    
    run_single_tree_test(&mod, "multi_slot_test_104", 5);
    dump_pool_state();
    
    test_comprehensive_pools_init();
    system_state_pool[0].ready = true;
    
    run_single_tree_test(&mod, "cross_pool_test_110", 5);
    dump_pool_state();
    
    // ========================================================================
    // STEP 7: Cleanup
    // ========================================================================
    
    s_expr_module_free(&mod);
    
    printf("\n============================================================\n");
    printf("All tests completed.\n");
    printf("============================================================\n");
    
    return 0;
}