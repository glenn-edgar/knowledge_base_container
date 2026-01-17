// ============================================================================
// s_engine_builtins.c
// Built-in S-Expression Engine Functions Implementation - Version 5.2
//
// VERSION 5.2 CHANGES:
//   - Added dictionary navigation helpers
//   - Added se_string_dispatch (string-based dispatch with hash lookup)
//   - Added se_hash_dispatch (dispatch on pre-computed hash)
//   - Added se_named_state_machine (state machine with string states)
//   - Added se_named_event_dispatch (event dispatch with string names)
//   - Added field comparison predicates
//   - Added field operation oneshots
// ============================================================================

#include "s_engine_builtins.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"
#include "s_engine_types.h"
#include "cfl_exception.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

// Predicates
static bool se_pred_and(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_or(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_not(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_nor(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_nand(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_pred_xor(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_true(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_false(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_check_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_check_named_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// Field comparison predicates
static bool se_field_eq(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_ne(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_gt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_ge(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_lt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_le(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_in_range(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// Main functions
static s_expr_result_t se_pipeline(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_tick_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_time_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_wait_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_nop(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_if_then_else(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_trigger_on_change(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_state_machine(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_state_actions(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_field_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_event_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// NEW v5.2: Dictionary-based dispatch functions
static s_expr_result_t se_string_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_hash_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_named_state_machine(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_named_event_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// Result code functions
static s_expr_result_t se_return_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_disable(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_skip_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// Oneshots
static void se_log(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_log_int(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_log_float(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_log_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_set_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_set_field_float(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_inc_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_dec_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// ============================================================================
// FUNCTION TABLES
// ============================================================================

static s_expr_fn_entry_t builtin_oneshot_entries[] = {
    { SE_LOG_HASH, (void*)se_log },
    { SE_LOG_INT_HASH, (void*)se_log_int },
    { SE_LOG_FLOAT_HASH, (void*)se_log_float },
    { SE_LOG_FIELD_HASH, (void*)se_log_field },
    { SE_SET_FIELD_HASH, (void*)se_set_field },
    { SE_SET_FIELD_FLOAT_HASH, (void*)se_set_field_float },
    { SE_INC_FIELD_HASH, (void*)se_inc_field },
    { SE_DEC_FIELD_HASH, (void*)se_dec_field },
};

static s_expr_fn_entry_t builtin_main_entries[] = {
    { SE_PIPELINE_HASH, (void*)se_pipeline },
    { SE_TICK_DELAY_HASH, (void*)se_tick_delay },
    { SE_TIME_DELAY_HASH, (void*)se_time_delay },
    { SE_WAIT_EVENT_HASH, (void*)se_wait_event },
    { SE_NOP_HASH, (void*)se_nop },
    { SE_IF_THEN_ELSE_HASH, (void*)se_if_then_else },
    { SE_TRIGGER_ON_CHANGE_HASH, (void*)se_trigger_on_change },
    { SE_STATE_MACHINE_HASH, (void*)se_state_machine },
    { SE_STATE_ACTIONS_HASH, (void*)se_state_actions },
    { SE_FIELD_DISPATCH_HASH, (void*)se_field_dispatch },
    { SE_EVENT_DISPATCH_HASH, (void*)se_event_dispatch },
    { SE_DISPATCH_HASH, (void*)se_dispatch },
    // NEW v5.2: Dictionary-based dispatch
    { SE_STRING_DISPATCH_HASH, (void*)se_string_dispatch },
    { SE_HASH_DISPATCH_HASH, (void*)se_hash_dispatch },
    { SE_NAMED_STATE_MACHINE_HASH, (void*)se_named_state_machine },
    { SE_NAMED_EVENT_DISPATCH_HASH, (void*)se_named_event_dispatch },
    // Result code functions
    { SE_RETURN_CONTINUE_HASH, (void*)se_return_continue },
    { SE_RETURN_HALT_HASH, (void*)se_return_halt },
    { SE_RETURN_TERMINATE_HASH, (void*)se_return_terminate },
    { SE_RETURN_RESET_HASH, (void*)se_return_reset },
    { SE_RETURN_DISABLE_HASH, (void*)se_return_disable },
    { SE_RETURN_SKIP_CONTINUE_HASH, (void*)se_return_skip_continue },
    { SE_RETURN_FUNCTION_HALT_HASH, (void*)se_return_function_halt },
    { SE_RETURN_FUNCTION_RESET_HASH, (void*)se_return_function_reset },
    { SE_RETURN_FUNCTION_TERMINATE_HASH, (void*)se_return_function_terminate },
};

static s_expr_fn_entry_t builtin_pred_entries[] = {
    { SE_PRED_AND_HASH, (void*)se_pred_and },
    { SE_PRED_OR_HASH, (void*)se_pred_or },
    { SE_PRED_NOT_HASH, (void*)se_pred_not },
    { SE_PRED_NOR_HASH, (void*)se_pred_nor },
    { SE_PRED_NAND_HASH, (void*)se_pred_nand },
    { SE_PRED_XOR_HASH, (void*)se_pred_xor },
    { SE_TRUE_HASH, (void*)se_true },
    { SE_FALSE_HASH, (void*)se_false },
    { SE_CHECK_EVENT_HASH, (void*)se_check_event },
    { SE_CHECK_NAMED_EVENT_HASH, (void*)se_check_named_event },
    // Field comparison predicates
    { SE_FIELD_EQ_HASH, (void*)se_field_eq },
    { SE_FIELD_NE_HASH, (void*)se_field_ne },
    { SE_FIELD_GT_HASH, (void*)se_field_gt },
    { SE_FIELD_GE_HASH, (void*)se_field_ge },
    { SE_FIELD_LT_HASH, (void*)se_field_lt },
    { SE_FIELD_LE_HASH, (void*)se_field_le },
    { SE_FIELD_IN_RANGE_HASH, (void*)se_field_in_range },
};

static const s_expr_fn_table_t builtin_oneshot_table = {
    .entries = builtin_oneshot_entries,
    .count = sizeof(builtin_oneshot_entries) / sizeof(builtin_oneshot_entries[0])
};

static const s_expr_fn_table_t builtin_main_table = {
    .entries = builtin_main_entries,
    .count = sizeof(builtin_main_entries) / sizeof(builtin_main_entries[0])
};

static const s_expr_fn_table_t builtin_pred_table = {
    .entries = builtin_pred_entries,
    .count = sizeof(builtin_pred_entries) / sizeof(builtin_pred_entries[0])
};

// ============================================================================
// TABLE ACCESSORS
// ============================================================================

const s_expr_fn_table_t* s_engine_builtin_oneshot_table(void) {
    return &builtin_oneshot_table;
}

const s_expr_fn_table_t* s_engine_builtin_main_table(void) {
    return &builtin_main_table;
}

const s_expr_fn_table_t* s_engine_builtin_pred_table(void) {
    return &builtin_pred_table;
}

// ============================================================================
// FNV-1a HASH IMPLEMENTATION
// ============================================================================

uint32_t s_expr_fnv1a_hash(const char* str) {
    uint32_t hash = 0x811c9dc5;  // FNV offset basis
    
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= 0x01000193;  // FNV prime
    }
    
    return hash;
}

// ============================================================================
// DICTIONARY NAVIGATION HELPERS
// ============================================================================

// Find a key in a dictionary by hash value
// dict_param should point to OPEN_DICT
// Returns pointer to the content after OPEN_KEY (first value in key), or NULL
const s_expr_param_t* s_expr_dict_find_key(
    const s_expr_param_t* dict_param,
    uint32_t key_hash
) {
    uint8_t opcode = dict_param->type & S_EXPR_OPCODE_MASK;
    
    // Verify this is OPEN_DICT
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        return NULL;
    }
    
    // Get dict bounds from brace_idx
    uint16_t dict_size = dict_param->brace_idx;
    const s_expr_param_t* dict_end = dict_param + dict_size;
    const s_expr_param_t* p = dict_param + 1;  // Skip OPEN_DICT
    
    // Scan through dictionary looking for OPEN_KEY with matching hash
    while (p < dict_end) {
        opcode = p->type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN_KEY) {
            // Check if hash matches (stored in u32 field)
            if (p->uint_val == key_hash) {
                // Found! Return pointer to first content param
                return p + 1;
            }
            // Skip to CLOSE_KEY using brace_idx
            // CLOSE_KEY is at p + brace_idx, but we need to get brace_idx from CLOSE_KEY
            // Actually, OPEN_KEY doesn't store brace_idx, the hash is in u32
            // We need to skip to CLOSE_KEY
            uint16_t skip = 1;
            while (p + skip < dict_end) {
                uint8_t skip_opcode = p[skip].type & S_EXPR_OPCODE_MASK;
                if (skip_opcode == S_EXPR_PARAM_CLOSE_KEY) {
                    break;
                }
                skip++;
            }
            p += skip + 1;  // Move past CLOSE_KEY
        } else if (opcode == S_EXPR_PARAM_CLOSE_DICT) {
            break;  // End of dict
        } else {
            p++;
        }
    }
    
    return NULL;  // Key not found
}

// Get the contents of a dictionary key (between OPEN_KEY and CLOSE_KEY)
// key_param should point to OPEN_KEY
const s_expr_param_t* s_expr_key_contents(
    const s_expr_param_t* key_param,
    uint16_t* content_count
) {
    uint8_t opcode = key_param->type & S_EXPR_OPCODE_MASK;
    
    if (opcode != S_EXPR_PARAM_OPEN_KEY) {
        *content_count = 0;
        return NULL;
    }
    
    // Content starts after OPEN_KEY
    const s_expr_param_t* content = key_param + 1;
    
    // Find CLOSE_KEY to get content count
    uint16_t count = 0;
    const s_expr_param_t* p = content;
    
    while (true) {
        opcode = p->type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_CLOSE_KEY) {
            break;
        }
        count++;
        p++;
        
        // Safety limit
        if (count > 10000) {
            *content_count = 0;
            return NULL;
        }
    }
    
    *content_count = count;
    return content;
}

// ============================================================================
// UNIFIED BODY EXECUTION HELPER
// ============================================================================

static s_expr_result_t s_expr_execute_body(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count
) {
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_oneshot(&params[i])) {
            s_expr_invoke_oneshot(inst, params, i);
        }
        else if (s_expr_param_is_main(&params[i])) {
            s_expr_result_t r = s_expr_invoke_main(inst, params, i);
            if (r != SE_CONTINUE && r != SE_DISABLE) {
                return r;
            }
        }
        i = s_expr_skip_param(params, i);
    }
    
    return s_expr_find_result(params, param_count);
}

// ============================================================================
// PREDICATE IMPLEMENTATIONS
// ============================================================================

static bool se_pred_and(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            if (!s_expr_invoke_pred(inst, params, i)) {
                return false;
            }
        }
        i = s_expr_skip_param(params, i);
    }
    return true;
}

static bool se_pred_or(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            if (s_expr_invoke_pred(inst, params, i)) {
                return true;
            }
        }
        i = s_expr_skip_param(params, i);
    }
    return false;
}

static bool se_pred_not(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            return !s_expr_invoke_pred(inst, params, i);
        }
        i = s_expr_skip_param(params, i);
    }
    return true;
}

static bool se_pred_nor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    return !se_pred_or(inst, params, param_count, event_type, event_id, event_data);
}

static bool se_pred_nand(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    return !se_pred_and(inst, params, param_count, event_type, event_id, event_data);
}

static bool se_pred_xor(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    int true_count = 0;
    
    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            if (s_expr_invoke_pred(inst, params, i)) {
                true_count++;
                if (true_count > 1) return false;
            }
        }
        i = s_expr_skip_param(params, i);
    }
    
    return (true_count == 1);
}

static bool se_true(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return true;
}

static bool se_false(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return false;
}

static bool se_check_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)event_type; (void)event_data;
    
    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t opcode = params[i].type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            if ((uint16_t)params[i].int_val == event_id) {
                return true;
            }
        }
    }
    return false;
}

// SE_CHECK_NAMED_EVENT - check event by string hash
// params: [str] - event name to match
static bool se_check_named_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_data;
    
    if (param_count < 1) return false;
    
    // Get event name and compute hash
    const char* event_name = s_expr_get_string(inst, &params[0]);
    if (!event_name) return false;
    
    uint32_t name_hash = s_expr_fnv1a_hash(event_name);
    
    // Compare with event_id (which should be a hash)
    return (name_hash == (uint32_t)event_id);
}

// ============================================================================
// FIELD COMPARISON PREDICATES
// ============================================================================

// SE_FIELD_EQ - field equals value
// params: [field_ref] [int]
static bool se_field_eq(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return false;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    int32_t compare_val = (int32_t)params[1].int_val;
    return (*field_ptr == compare_val);
}

// SE_FIELD_NE - field not equals value
static bool se_field_ne(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    return !se_field_eq(inst, params, param_count, event_type, event_id, event_data);
}

// SE_FIELD_GT - field greater than value
static bool se_field_gt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return false;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    int32_t compare_val = (int32_t)params[1].int_val;
    return (*field_ptr > compare_val);
}

// SE_FIELD_GE - field greater than or equal
static bool se_field_ge(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return false;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    int32_t compare_val = (int32_t)params[1].int_val;
    return (*field_ptr >= compare_val);
}

// SE_FIELD_LT - field less than value
static bool se_field_lt(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return false;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    int32_t compare_val = (int32_t)params[1].int_val;
    return (*field_ptr < compare_val);
}

// SE_FIELD_LE - field less than or equal
static bool se_field_le(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return false;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    int32_t compare_val = (int32_t)params[1].int_val;
    return (*field_ptr <= compare_val);
}

// SE_FIELD_IN_RANGE - field in [min, max] inclusive
// params: [field_ref] [min] [max]
static bool se_field_in_range(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 3) return false;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return false;
    
    int32_t min_val = (int32_t)params[1].int_val;
    int32_t max_val = (int32_t)params[2].int_val;
    
    return (*field_ptr >= min_val && *field_ptr <= max_val);
}

// ============================================================================
// MAIN FUNCTION IMPLEMENTATIONS
// ============================================================================

static s_expr_result_t se_pipeline(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    return s_expr_execute_body(inst, params, param_count);
}

static s_expr_result_t se_tick_delay(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        uint32_t ticks = (param_count > 0) ? (uint32_t)params[0].uint_val : 0;
        ticks++;
        s_expr_set_u64(inst, (uint64_t)ticks);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    uint64_t remaining = s_expr_get_u64(inst);
    
    if (remaining > 0) {
        remaining--;
        s_expr_set_u64(inst, remaining);
        return SE_HALT;
    }
    
    return SE_DISABLE;
}

static s_expr_result_t se_time_delay(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    s_expr_module_t* mod = inst->module;
    
    if (event_type == SE_EVENT_INIT) {
        double seconds = (param_count > 0) ? (double)params[0].float_val : 0.0;
        
        if (seconds <= 0.0) {
            return SE_CONTINUE;
        }
        
        double now = 0.0;
        if (mod && mod->alloc.get_time) {
            now = mod->alloc.get_time(mod->alloc.ctx);
        }
        
        double target_time = now + seconds;
        s_expr_set_f64(inst, target_time);
        
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (event_id != SE_EVENT_TICK) {
        return SE_HALT;
    }
    
    double target_time = s_expr_get_f64(inst);
    
    double now = 0.0;
    if (mod && mod->alloc.get_time) {
        now = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (now >= target_time) {
        return SE_DISABLE;
    }
    
    return SE_HALT;
}

static s_expr_result_t se_wait_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT) {
        uint32_t target_event = (param_count > 0) ? (uint32_t)params[0].int_val : 0;
        uint32_t count = (param_count > 1) ? (uint32_t)params[1].int_val : 1;
        
        uint64_t state = ((uint64_t)target_event << 32) | count;
        s_expr_set_u64(inst, state);
        
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    uint64_t state = s_expr_get_u64(inst);
    uint32_t target_event = (uint32_t)(state >> 32);
    uint32_t remaining = (uint32_t)(state & 0xFFFFFFFF);
    
    if (remaining <= 0) {
        return SE_DISABLE;
    }
    
    if (event_id == target_event) {
        remaining--;
        state = ((uint64_t)target_event << 32) | remaining;
        s_expr_set_u64(inst, state);
        
        if (remaining == 0) {
            return SE_DISABLE;
        }
    }
    
    return SE_HALT;
}

static s_expr_result_t se_nop(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_DISABLE;
}

static s_expr_result_t se_if_then_else(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count == 0) return SE_CONTINUE;
    
    uint16_t idx = 0;
    
    bool condition = false;
    if (s_expr_param_is_predicate(&params[idx])) {
        condition = s_expr_invoke_pred(inst, params, idx);
        idx = s_expr_skip_param(params, idx);
    }
    
    if (idx >= param_count) return SE_CONTINUE;
    
    uint16_t then_idx = idx;
    uint16_t then_end = s_expr_skip_param(params, idx);
    idx = then_end;
    
    uint16_t else_idx = idx;
    uint16_t else_end = (idx < param_count) ? s_expr_skip_param(params, idx) : idx;
    bool has_else = (idx < param_count);
    
    if (condition) {
        return s_expr_execute_body(inst, &params[then_idx], then_end - then_idx);
    } else if (has_else) {
        return s_expr_execute_body(inst, &params[else_idx], else_end - else_idx);
    }
    
    return SE_CONTINUE;
}

static s_expr_result_t se_trigger_on_change(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (param_count < 2) return SE_CONTINUE;
    
    const uint16_t init_idx = 0;
    const uint16_t pred_idx = 1;
    const uint16_t rising_idx = s_expr_skip_param(params, pred_idx);
    const uint16_t falling_idx = s_expr_skip_param(params, rising_idx);
    const bool has_falling = (falling_idx < param_count);
    
    if (event_type == SE_EVENT_INIT) {
        uint8_t type0 = params[init_idx].type & S_EXPR_OPCODE_MASK;
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("se_trigger_on_change: param[0] must be INT or UINT");
            return SE_CONTINUE;
        }
        
        if (!s_expr_param_is_predicate(&params[pred_idx])) {
            EXCEPTION("se_trigger_on_change: param[1] must be predicate");
            return SE_CONTINUE;
        }
        
        int32_t initial_state = (int32_t)params[init_idx].int_val;
        s_expr_set_state(inst, initial_state ? 1 : 0);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    bool current = s_expr_invoke_pred(inst, params, pred_idx);
    uint8_t prev = s_expr_get_state(inst);
    
    bool rising = (prev == 0 && current);
    bool falling = (prev != 0 && !current);
    
    s_expr_set_state(inst, current ? 1 : 0);
    
    if (rising) {
        uint16_t rising_end = s_expr_skip_param(params, rising_idx);
        s_expr_restart_actions(inst, &params[rising_idx], rising_end - rising_idx);
        return s_expr_execute_body(inst, &params[rising_idx], rising_end - rising_idx);
    } 
    else if (falling && has_falling) {
        uint16_t falling_end = s_expr_skip_param(params, falling_idx);
        s_expr_restart_actions(inst, &params[falling_idx], falling_end - falling_idx);
        return s_expr_execute_body(inst, &params[falling_idx], falling_end - falling_idx);
    }
    
    return SE_CONTINUE;
}

static s_expr_result_t se_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("se_state_machine: first param must be field_ref");
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    int32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!state_ptr) return SE_CONTINUE;
    
    int32_t state = *state_ptr;
    if (state < 0) return SE_CONTINUE;
    
    uint16_t idx = s_expr_skip_param(params, 0);
    int32_t state_idx = 0;
    uint16_t action_idx = 0;
    
    while (idx < param_count) {
        if (state_idx == state) {
            action_idx = idx;
            break;
        }
        idx = s_expr_skip_param(params, idx);
        state_idx++;
    }
    
    if (action_idx == 0) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
            s_expr_set_user_flags(inst, 0);
        }
        return SE_CONTINUE;
    }
    
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        s_expr_enable_actions(inst, &params[action_idx],
            s_expr_skip_param(params, action_idx) - action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    uint16_t action_end = s_expr_skip_param(params, action_idx);
    return s_expr_execute_body(inst, &params[action_idx], action_end - action_idx);
}

static s_expr_result_t se_state_actions(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    return s_expr_execute_body(inst, params, param_count);
}

static s_expr_result_t se_field_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx], 
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("se_field_dispatch: missing field parameter");
            return SE_CONTINUE;
        }
        uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_FIELD) {
            EXCEPTION("se_field_dispatch: first param must be field_ref");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) return SE_CONTINUE;
    
    int32_t val = *val_ptr;
    
    uint16_t idx = s_expr_skip_param(params, 0);
    uint16_t action_idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_val = (int32_t)params[idx].int_val;
            uint16_t this_action_idx = idx + 1;
            
            if (case_val == val && this_action_idx < param_count) {
                action_idx = this_action_idx;
                break;
            }
            
            idx = s_expr_skip_param(params, idx);
            idx = s_expr_skip_param(params, idx);
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    if (action_idx == 0) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
            s_expr_set_user_flags(inst, 0);
        }
        return SE_CONTINUE;
    }
    
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        s_expr_enable_actions(inst, &params[action_idx],
            s_expr_skip_param(params, action_idx) - action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    uint16_t action_end = s_expr_skip_param(params, action_idx);
    return s_expr_execute_body(inst, &params[action_idx], action_end - action_idx);
}

static s_expr_result_t se_event_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    uint16_t idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_event = (int32_t)params[idx].int_val;
            uint16_t action_idx = idx + 1;
            
            if (case_event == (int32_t)event_id && action_idx < param_count) {
                uint16_t action_end = s_expr_skip_param(params, action_idx);
                return s_expr_execute_body(inst, &params[action_idx], action_end - action_idx);
            }
            
            idx = s_expr_skip_param(params, idx);
            idx = s_expr_skip_param(params, idx);
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    return SE_CONTINUE;
}

static s_expr_result_t se_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 1) return SE_CONTINUE;
    
    int32_t val = params[0].int_val;
    
    uint16_t idx = s_expr_skip_param(params, 0);
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_OPEN) {
            uint16_t case_count;
            const s_expr_param_t* case_params = s_expr_brace_contents(params, idx, &case_count);
            
            if (case_count >= 2) {
                int32_t case_val = case_params[0].int_val;
                if (case_val == val) {
                    uint16_t body_start = s_expr_skip_param(case_params, 0);
                    return s_expr_execute_body(inst, &case_params[body_start], case_count - body_start);
                }
            }
        }
        idx = s_expr_skip_param(params, idx);
    }
    
    return SE_CONTINUE;
}

// ============================================================================
// NEW v5.2: DICTIONARY-BASED DISPATCH FUNCTIONS
// ============================================================================

// SE_STRING_DISPATCH - dispatch based on string field value (computes hash)
// params: [field_ref] [OPEN_DICT cases...]
// Field contains a string pointer, we hash it and look up in dict
static s_expr_result_t se_string_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("se_string_dispatch: need field_ref and dict");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // Get string from field and compute hash
    const char** str_ptr = S_EXPR_GET_FIELD(inst, &params[0], const char*);
    if (!str_ptr || !*str_ptr) return SE_CONTINUE;
    
    uint32_t str_hash = s_expr_fnv1a_hash(*str_ptr);
    
    // Find dictionary (should be second param)
    uint16_t dict_idx = s_expr_skip_param(params, 0);
    if (dict_idx >= param_count) return SE_CONTINUE;
    
    uint8_t opcode = params[dict_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_string_dispatch: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up key in dictionary
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[dict_idx], str_hash);
    
    if (!key_content) {
        // No match - check for DEFAULT key
        uint32_t default_hash = s_expr_fnv1a_hash("DEFAULT");
        key_content = s_expr_dict_find_key(&params[dict_idx], default_hash);
        
        if (!key_content) {
            // No default either - terminate previous if any
            if (prev_action_idx > 0) {
                s_expr_restart_actions(inst, &params[prev_action_idx],
                    s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
                s_expr_set_user_flags(inst, 0);
            }
            return SE_CONTINUE;
        }
    }
    
    // Calculate action index (offset from params start)
    uint16_t action_idx = (uint16_t)(key_content - params);
    
    // Get content count
    uint16_t content_count;
    s_expr_key_contents(key_content - 1, &content_count);
    
    // Handle state transition
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        s_expr_enable_actions(inst, key_content, content_count);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    return s_expr_execute_body(inst, key_content, content_count);
}

// SE_HASH_DISPATCH - dispatch based on pre-computed hash in field
// params: [field_ref] [OPEN_DICT cases...]
// Field already contains a hash value
static s_expr_result_t se_hash_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("se_hash_dispatch: need field_ref and dict");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // Get hash value from field
    uint32_t* hash_ptr = S_EXPR_GET_FIELD(inst, &params[0], uint32_t);
    if (!hash_ptr) return SE_CONTINUE;
    
    uint32_t hash_val = *hash_ptr;
    printf("hash_val: %u\n", hash_val);
    // Find dictionary
    uint16_t dict_idx = s_expr_skip_param(params, 0);
    if (dict_idx >= param_count) return SE_CONTINUE;
    
    uint8_t opcode = params[dict_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_hash_dispatch: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up key in dictionary
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[dict_idx], hash_val);
    
    if (!key_content) {
        // No match - check for DEFAULT
        uint32_t default_hash = s_expr_fnv1a_hash("DEFAULT");
        key_content = s_expr_dict_find_key(&params[dict_idx], default_hash);
        
        if (!key_content) {
            if (prev_action_idx > 0) {
                s_expr_restart_actions(inst, &params[prev_action_idx],
                    s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
                s_expr_set_user_flags(inst, 0);
            }
            return SE_CONTINUE;
        }
    }
    
    uint16_t action_idx = (uint16_t)(key_content - params);
    uint16_t content_count;
    s_expr_key_contents(key_content - 1, &content_count);
    
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        s_expr_enable_actions(inst, key_content, content_count);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    return s_expr_execute_body(inst, key_content, content_count);
}

// SE_NAMED_STATE_MACHINE - state machine with string state names
// params: [field_ref] [OPEN_DICT states...]
// Field contains state hash
static s_expr_result_t se_named_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("se_named_state_machine: need field_ref and dict");
            return SE_CONTINUE;
        }
        uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
        if (opcode != S_EXPR_PARAM_FIELD) {
            EXCEPTION("se_named_state_machine: first param must be field_ref");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // Get state hash from field
    uint32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], uint32_t);
    if (!state_ptr) return SE_CONTINUE;
    
    uint32_t state_hash = *state_ptr;
    
    // Find dictionary
    uint16_t dict_idx = s_expr_skip_param(params, 0);
    if (dict_idx >= param_count) return SE_CONTINUE;
    
    uint8_t opcode = params[dict_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_named_state_machine: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up state in dictionary
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[dict_idx], state_hash);
    
    if (!key_content) {
        // State not found - terminate previous
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
            s_expr_set_user_flags(inst, 0);
        }
        return SE_CONTINUE;
    }
    
    uint16_t action_idx = (uint16_t)(key_content - params);
    uint16_t content_count;
    s_expr_key_contents(key_content - 1, &content_count);
    
    // Handle state transition
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0) {
            s_expr_restart_actions(inst, &params[prev_action_idx],
                s_expr_skip_param(params, prev_action_idx) - prev_action_idx);
        }
        s_expr_enable_actions(inst, key_content, content_count);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    return s_expr_execute_body(inst, key_content, content_count);
}

// SE_NAMED_EVENT_DISPATCH - event dispatch with string event names
// params: [OPEN_DICT events...]
// event_id is a hash of the event name
static s_expr_result_t se_named_event_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_data;
    
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }
    
    if (param_count < 1) return SE_CONTINUE;
    
    // First param should be OPEN_DICT
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_named_event_dispatch: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up event by hash
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[0], (uint32_t)event_id);
    
    if (!key_content) {
        return SE_CONTINUE;  // No handler for this event
    }
    
    uint16_t content_count;
    s_expr_key_contents(key_content - 1, &content_count);
    
    return s_expr_execute_body(inst, key_content, content_count);
}

// ============================================================================
// ONESHOT IMPLEMENTATIONS
// ============================================================================

static void se_log(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
   
    if (param_count < 1) {
        EXCEPTION("SE_LOG: param_count < 1");
        return;
    }
    
    const char* msg = s_expr_get_string(inst, &params[0]);
    if (!msg) return;
    
    double timestamp = 0.0;
    s_expr_module_t* mod = inst->module;
    if (mod && mod->alloc.get_time) {
        timestamp = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (mod && mod->debug_fn) {
        char buf[256];
        snprintf(buf, sizeof(buf), "[%.6f] %s", timestamp, msg);
        mod->debug_fn(inst, buf);
    } else {
        #ifndef S_ENGINE_NO_STDIO
        printf("[SE_LOG %.6f] %s\n", timestamp, msg);
        #endif
    }
}

// SE_LOG_INT - log message with integer value
// params: [str_ptr] [int]
static void se_log_int(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    const char* msg = s_expr_get_string(inst, &params[0]);
    if (!msg) return;
    
    int32_t val = (int32_t)params[1].int_val;
    
    double timestamp = 0.0;
    s_expr_module_t* mod = inst->module;
    if (mod && mod->alloc.get_time) {
        timestamp = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (mod && mod->debug_fn) {
        char buf[256];
        snprintf(buf, sizeof(buf), "[%.6f] %s %d", timestamp, msg, val);
        mod->debug_fn(inst, buf);
    } else {
        #ifndef S_ENGINE_NO_STDIO
        printf("[SE_LOG %.6f] %s %d\n", timestamp, msg, val);
        #endif
    }
}

// SE_LOG_FLOAT - log message with float value
// params: [str_ptr] [float]
static void se_log_float(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    const char* msg = s_expr_get_string(inst, &params[0]);
    if (!msg) return;
    
    float val = params[1].float_val;
    
    double timestamp = 0.0;
    s_expr_module_t* mod = inst->module;
    if (mod && mod->alloc.get_time) {
        timestamp = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (mod && mod->debug_fn) {
        char buf[256];
        snprintf(buf, sizeof(buf), "[%.6f] %s %.6f", timestamp, msg, val);
        mod->debug_fn(inst, buf);
    } else {
        #ifndef S_ENGINE_NO_STDIO
        printf("[SE_LOG %.6f] %s %.6f\n", timestamp, msg, (double)val);
        #endif
    }
}

// SE_LOG_FIELD - log message with field value
// params: [str_ptr] [field_ref]
static void se_log_field(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    const char* msg = s_expr_get_string(inst, &params[0]);
    if (!msg) return;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[1], int32_t);
    if (!field_ptr) return;
    
    int32_t val = *field_ptr;
    
    double timestamp = 0.0;
    s_expr_module_t* mod = inst->module;
    if (mod && mod->alloc.get_time) {
        timestamp = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (mod && mod->debug_fn) {
        char buf[256];
        snprintf(buf, sizeof(buf), "[%.6f] %s %d", timestamp, msg, val);
        mod->debug_fn(inst, buf);
    } else {
        #ifndef S_ENGINE_NO_STDIO
        printf("[SE_LOG %.6f] %s %d\n", timestamp, msg, val);
        #endif
    }
}

// SE_SET_FIELD - set field to integer value
// params: [field_ref] [int]
static void se_set_field(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return;
    
    *field_ptr = (int32_t)params[1].int_val;
}

// SE_SET_FIELD_FLOAT - set field to float value
// params: [field_ref] [float]
static void se_set_field_float(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    float* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], float);
    if (!field_ptr) return;
    
    *field_ptr = params[1].float_val;
}

// SE_INC_FIELD - increment field by delta
// params: [field_ref] [delta]
static void se_inc_field(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return;
    
    int32_t delta = (int32_t)params[1].int_val;
    *field_ptr += delta;
}

// SE_DEC_FIELD - decrement field by delta
// params: [field_ref] [delta]
static void se_dec_field(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!field_ptr) return;
    
    int32_t delta = (int32_t)params[1].int_val;
    *field_ptr -= delta;
}

// ============================================================================
// RESULT CODE FUNCTION IMPLEMENTATIONS
// ============================================================================

static s_expr_result_t se_return_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_CONTINUE;
}

static s_expr_result_t se_return_halt(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_HALT;
}

static s_expr_result_t se_return_terminate(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_TERMINATE;
}

static s_expr_result_t se_return_reset(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_RESET;
}

static s_expr_result_t se_return_disable(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_DISABLE;
}

static s_expr_result_t se_return_skip_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_SKIP_CONTINUE;
}

static s_expr_result_t se_return_function_halt(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_FUNCTION_HALT;
}

static s_expr_result_t se_return_function_reset(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_FUNCTION_RESET;
}

static s_expr_result_t se_return_function_terminate(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_FUNCTION_TERMINATE;
}