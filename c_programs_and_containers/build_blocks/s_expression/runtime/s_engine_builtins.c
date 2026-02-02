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
#include "s_engine_node.h"
#include "s_engine_exception.h"
#include "s_engine_list_dictionary_support.h"
#include "s_engine_stack_functions.h"
#include "s_engine_event_queue.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
// ============================================================================
// FORWARD DECLARATIONS - Internal Helpers
// ============================================================================

// ============================================================================
// STATIC HELPERS for state machine nodes
// ============================================================================

static void terminate_action_at_index(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t phys_idx
) {
    uint8_t opcode = params[phys_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_CALL) {
        return;
    }
    
    const s_expr_param_t* func_param = &params[phys_idx + 1];
    uint8_t func_opcode = func_param->type & S_EXPR_OPCODE_MASK;
    
    if (func_opcode != S_EXPR_PARAM_MAIN) {
        return;  // Only MAIN nodes receive TERMINATE
    }
    
    uint16_t node_idx = func_param->node_index;
    if (node_idx >= inst->node_count) {
        return;
    }
    
    if (!(inst->node_states[node_idx].flags & S_EXPR_NODE_FLAG_INITIALIZED)) {
        return;  // Never initialized, no cleanup needed
    }
    
    uint16_t func_idx = func_param->func_index;
    if (func_idx >= inst->module->def->main_count) {
        return;
    }
    
    s_expr_main_fn_t fn = inst->module->main_fns[func_idx];
    if (!fn) {
        return;
    }
    
    // Calculate args
    uint16_t close_idx = phys_idx + params[phys_idx].brace_idx;
    uint16_t arg_count = (close_idx > phys_idx + 2) ? (close_idx - phys_idx - 2) : 0;
    const s_expr_param_t* args = (arg_count > 0) ? &params[phys_idx + 2] : NULL;
    
    // Save/restore context
    uint16_t saved_node = inst->current_node_index;
    bool saved_in_ptr = inst->in_pointer_call;
    uint8_t saved_ptr_base = inst->pointer_base;
    
    inst->current_node_index = node_idx;
    if (func_param->type & S_EXPR_FLAG_POINTER) {
        inst->in_pointer_call = true;
        inst->pointer_base = func_param->index_to_pointer;
    }
    
    // Send TERMINATE event
    fn(inst, args, arg_count, SE_EVENT_TERMINATE, 0, NULL);
    
    inst->current_node_index = saved_node;
    inst->in_pointer_call = saved_in_ptr;
    inst->pointer_base = saved_ptr_base;
    
    // Clear node state
    inst->node_states[node_idx].flags = 0;
    inst->node_states[node_idx].state = 0;
    inst->node_states[node_idx].user_data = 0;
}

static void reset_action_at_index(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t phys_idx
) {
    s_expr_reset_recursive_at(inst, params, phys_idx);
}


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

// Field comparison predicates
static bool se_field_eq(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_ne(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_gt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_ge(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_lt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_le(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static bool se_field_in_range(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

//static s_expr_result_t se_pipeline(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_tick_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_time_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_wait_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_nop(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_if_then_else(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_trigger_on_change(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_state_machine(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
//static s_expr_result_t se_state_actions(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_function_interface(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_field_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_event_dispatch(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_sequence(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_fork(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_fork_join(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_chain_flow(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_for(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_while(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_cond(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
// Result code functions
static s_expr_result_t se_return_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_disable(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_skip_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

static s_expr_result_t se_return_function_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_disable(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_function_skip_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

static s_expr_result_t se_return_pipeline_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_pipeline_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_pipeline_reset(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_pipeline_disable(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_pipeline_skip_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data); 
static s_expr_result_t se_return_pipeline_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);


/// Oneshots
static void se_queue_event(s_expr_tree_instance_t* inst,const s_expr_param_t* params,uint16_t param_count,s_expr_event_type_t event_type,uint16_t event_id,void* event_data);
static void se_log(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_log_int(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_log_float(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_log_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_set_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_set_field_float(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_inc_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_dec_field(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static void se_set_hash(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
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
    { SE_SET_HASH_HASH, (void*)se_set_hash },// Function table entries

    { SE_STACK_ADD_HASH,         (void*)se_stack_add },         // SE_STACK_ADD
    { SE_STACK_SUB_HASH,         (void*)se_stack_sub },         // SE_STACK_SUB
    { SE_STACK_MUL_HASH,         (void*)se_stack_mul },         // SE_STACK_MUL
    { SE_STACK_DIV_HASH,         (void*)se_stack_div },         // SE_STACK_DIV
    { SE_STACK_MOD_HASH,         (void*)se_stack_mod },         // SE_STACK_MOD
    { SE_STACK_IDIV_HASH,        (void*)se_stack_idiv },        // SE_STACK_IDIV
    { SE_STACK_IMOD_HASH,        (void*)se_stack_imod },        // SE_STACK_IMOD
    
    // Unary arithmetic
    { SE_STACK_NEG_HASH,         (void*)se_stack_neg },         // SE_STACK_NEG
    { SE_STACK_ABS_HASH,         (void*)se_stack_abs },         // SE_STACK_ABS
    { SE_STACK_INC_HASH,         (void*)se_stack_inc },         // SE_STACK_INC
    { SE_STACK_DEC_HASH,         (void*)se_stack_dec },         // SE_STACK_DEC
    
    // Bitwise
    { SE_STACK_BAND_HASH,        (void*)se_stack_band },        // SE_STACK_BAND
    { SE_STACK_BOR_HASH,         (void*)se_stack_bor },         // SE_STACK_BOR
    { SE_STACK_BXOR_HASH,        (void*)se_stack_bxor },        // SE_STACK_BXOR
    { SE_STACK_SHL_HASH,         (void*)se_stack_shl },         // SE_STACK_SHL
    { SE_STACK_SHR_HASH,         (void*)se_stack_shr },         // SE_STACK_SHR
    { SE_STACK_SAR_HASH,         (void*)se_stack_sar },         // SE_STACK_SAR
    { SE_STACK_BNOT_HASH,        (void*)se_stack_bnot },        // SE_STACK_BNOT
    
    // Comparison
    { SE_STACK_EQ_HASH,          (void*)se_stack_eq },          // SE_STACK_EQ
    { SE_STACK_NE_HASH,          (void*)se_stack_ne },          // SE_STACK_NE
    { SE_STACK_LT_HASH,          (void*)se_stack_lt },          // SE_STACK_LT
    { SE_STACK_LE_HASH,          (void*)se_stack_le },          // SE_STACK_LE
    { SE_STACK_GT_HASH,          (void*)se_stack_gt },          // SE_STACK_GT
    { SE_STACK_GE_HASH,          (void*)se_stack_ge },          // SE_STACK_GE
    
    // Logical
    { SE_STACK_AND_HASH,         (void*)se_stack_and },         // SE_STACK_AND
    { SE_STACK_OR_HASH,          (void*)se_stack_or },          // SE_STACK_OR
    { SE_STACK_NOT_HASH,         (void*)se_stack_not },         // SE_STACK_NOT
    
    // Math functions
    { SE_STACK_SQRT_HASH,        (void*)se_stack_sqrt },        // SE_STACK_SQRT
    { SE_STACK_EXP_HASH,         (void*)se_stack_exp },         // SE_STACK_EXP
    { SE_STACK_LOG_HASH,         (void*)se_stack_log },         // SE_STACK_LOG
    { SE_STACK_LOG10_HASH,       (void*)se_stack_log10 },       // SE_STACK_LOG10
    { SE_STACK_SIN_HASH,         (void*)se_stack_sin },         // SE_STACK_SIN
    { SE_STACK_COS_HASH,         (void*)se_stack_cos },         // SE_STACK_COS
    { SE_STACK_TAN_HASH,         (void*)se_stack_tan },         // SE_STACK_TAN
    { SE_STACK_ASIN_HASH,        (void*)se_stack_asin },        // SE_STACK_ASIN
    { SE_STACK_ACOS_HASH,        (void*)se_stack_acos },        // SE_STACK_ACOS
    { SE_STACK_ATAN_HASH,        (void*)se_stack_atan },        // SE_STACK_ATAN
    { SE_STACK_FLOOR_HASH,       (void*)se_stack_floor },       // SE_STACK_FLOOR
    { SE_STACK_CEIL_HASH,        (void*)se_stack_ceil },        // SE_STACK_CEIL
    { SE_STACK_ROUND_HASH,       (void*)se_stack_round },       // SE_STACK_ROUND
    { SE_STACK_TRUNC_HASH,       (void*)se_stack_trunc },       // SE_STACK_TRUNC
    { SE_STACK_POW_HASH,         (void*)se_stack_pow },         // SE_STACK_POW
    { SE_STACK_ATAN2_HASH,       (void*)se_stack_atan2 },       // SE_STACK_ATAN2
    { SE_STACK_MIN_HASH,         (void*)se_stack_min },         // SE_STACK_MIN
    { SE_STACK_MAX_HASH,         (void*)se_stack_max },         // SE_STACK_MAX
    { SE_STACK_CLAMP_HASH,       (void*)se_stack_clamp },       // SE_STACK_CLAMP
    
    // Type conversion
    { SE_STACK_TOINT_HASH,       (void*)se_stack_to_int },      // SE_STACK_TOINT
    { SE_STACK_TOUINT_HASH,      (void*)se_stack_to_uint },     // SE_STACK_TOUINT
    { SE_STACK_TOFLOAT_HASH,     (void*)se_stack_to_float },    // SE_STACK_TOFLOAT
    
    // Constant/immediate
    { SE_STACK_PUSH_CONST_HASH,  (void*)se_stack_push_const },  // SE_STACK_PUSH_CONST
    { SE_STACK_ADDI_HASH,        (void*)se_stack_addi },        // SE_STACK_ADDI
    { SE_STACK_SUBI_HASH,        (void*)se_stack_subi },        // SE_STACK_SUBI
    { SE_STACK_MULI_HASH,        (void*)se_stack_muli },        // SE_STACK_MULI
    { SE_STACK_DIVI_HASH,        (void*)se_stack_divi },        // SE_STACK_DIVI
    { SE_STACK_MODI_HASH,        (void*)se_stack_modi },        // SE_STACK_MODI
    { SE_STACK_SHLI_HASH,        (void*)se_stack_shli },        // SE_STACK_SHLI
    { SE_STACK_SHRI_HASH,        (void*)se_stack_shri },        // SE_STACK_SHRI
    { SE_STACK_SARI_HASH,        (void*)se_stack_sari },        // SE_STACK_SARI
    { SE_STACK_BANDI_HASH,       (void*)se_stack_bandi },       // SE_STACK_BANDI
    { SE_STACK_BORI_HASH,        (void*)se_stack_bori },        // SE_STACK_BORI
    { SE_STACK_BXORI_HASH,       (void*)se_stack_bxori },       // SE_STACK_BXORI
    
    // Field operations
    { SE_STACK_LOAD_INT_HASH,    (void*)se_stack_load_int },    // SE_STACK_LOAD_INT
    { SE_STACK_LOAD_UINT_HASH,   (void*)se_stack_load_uint },   // SE_STACK_LOAD_UINT
    { SE_STACK_LOAD_FLOAT_HASH,  (void*)se_stack_load_float },  // SE_STACK_LOAD_FLOAT

    { SE_STACK_STORE_INT_HASH,   (void*)se_stack_store_int },   // SE_STACK_STORE_INT
    { SE_STACK_STORE_UINT_HASH,  (void*)se_stack_store_uint },  // SE_STACK_STORE_UINT
    { SE_STACK_STORE_FLOAT_HASH, (void*)se_stack_store_float }, // SE_STACK_STORE_FLOAT
    
    // Stack manipulation
    { SE_STACK_DROP_HASH,        (void*)se_stack_drop },     // SE_STACK_DROP
    { SE_STACK_DROP2_HASH,       (void*)se_stack_drop2 },    // SE_STACK_DROP2
    { SE_STACK_DROPN_HASH,       (void*)se_stack_dropn },    // SE_STACK_DROPN
    { SE_STACK_DUP_HASH,         (void*)se_stack_dup },      // SE_STACK_DUP
    { SE_STACK_DUP2_HASH,        (void*)se_stack_dup2 },     // SE_STACK_DUP2
    { SE_STACK_SWAP_HASH,        (void*)se_stack_swap },     // SE_STACK_SWAP
    { SE_STACK_OVER_HASH,        (void*)se_stack_over },     // SE_STACK_OVER
    { SE_STACK_ROT_HASH,         (void*)se_stack_rot },      // SE_STACK_ROT
    { SE_STACK_NROT_HASH,        (void*)se_stack_nrot },     // SE_STACK_NROT
    { SE_STACK_PICK_HASH,        (void*)se_stack_pick },     // SE_STACK_PICK
    { SE_STACK_ROLL_HASH,        (void*)se_stack_roll },     // SE_STACK_ROLL
    
    // Conditional
    { SE_STACK_SELECT_HASH,      (void*)se_stack_select },   // SE_STACK_SELECT
    
    // Hash
    { SE_STACK_PUSH_HASH_HASH,   (void*)se_stack_push_hash }, // SE_STACK_PUSH_HASH
    { SE_STACK_HASH_EQ_HASH,     (void*)se_stack_hash_eq },   // SE_STACK_HASH_EQ
    { SE_QUEUE_EVENT_HASH,       (void*)se_queue_event },     // SE_QUEUE_EVENT
};

static s_expr_fn_entry_t builtin_main_entries[] = {
    //{ SE_PIPELINE_HASH, (void*)se_pipeline },
    { SE_TICK_DELAY_HASH, (void*)se_tick_delay },
    { SE_TIME_DELAY_HASH, (void*)se_time_delay },
    { SE_WAIT_EVENT_HASH, (void*)se_wait_event },
    { SE_NOP_HASH, (void*)se_nop },
    { SE_IF_THEN_ELSE_HASH, (void*)se_if_then_else },
    { SE_TRIGGER_ON_CHANGE_HASH, (void*)se_trigger_on_change },
    { SE_STATE_MACHINE_HASH, (void*)se_state_machine },
    //{ SE_STATE_ACTIONS_HASH, (void*)se_state_actions },
    { SE_FIELD_DISPATCH_HASH, (void*)se_field_dispatch },
    { SE_EVENT_DISPATCH_HASH, (void*)se_event_dispatch },
    { SE_FUNCTION_INTERFACE_HASH, (void*)se_function_interface },
    { SE_SEQUENCE_HASH, (void*)se_sequence },
    { SE_FORK_HASH, (void*)se_fork },
    { SE_FORK_JOIN_HASH, (void*)se_fork_join },
    { SE_CHAIN_FLOW_HASH, (void*)se_chain_flow },
    { SE_FOR_HASH, (void*)se_for },
    { SE_WHILE_HASH, (void*)se_while },
    { SE_COND_HASH, (void*)se_cond },
    // NEW v5.2: Dictionary-based dispatch
    
    // Result code functions
    { SE_RETURN_CONTINUE_HASH, (void*)se_return_continue },
    { SE_RETURN_HALT_HASH, (void*)se_return_halt },
    { SE_RETURN_TERMINATE_HASH, (void*)se_return_terminate },
    { SE_RETURN_RESET_HASH, (void*)se_return_reset },
    { SE_RETURN_DISABLE_HASH, (void*)se_return_disable },
    { SE_RETURN_SKIP_CONTINUE_HASH, (void*)se_return_skip_continue },

    { SE_RETURN_FUNCTION_CONTINUE_HASH, (void*)se_return_function_continue },
    { SE_RETURN_FUNCTION_HALT_HASH, (void*)se_return_function_halt },
    { SE_RETURN_FUNCTION_RESET_HASH, (void*)se_return_function_reset },
    { SE_RETURN_FUNCTION_TERMINATE_HASH, (void*)se_return_function_terminate },
    { SE_RETURN_FUNCTION_DISABLE_HASH, (void*)se_return_function_disable },
    { SE_RETURN_FUNCTION_SKIP_CONTINUE_HASH, (void*)se_return_function_skip_continue },

    // Pipeline result code functions
    { SE_RETURN_PIPELINE_CONTINUE_HASH, (void*)se_return_pipeline_continue },
    { SE_RETURN_PIPELINE_TERMINATE_HASH, (void*)se_return_pipeline_terminate },
    { SE_RETURN_PIPELINE_RESET_HASH, (void*)se_return_pipeline_reset },
    { SE_RETURN_PIPELINE_DISABLE_HASH, (void*)se_return_pipeline_disable },
    { SE_RETURN_PIPELINE_SKIP_CONTINUE_HASH, (void*)se_return_pipeline_skip_continue },
    { SE_RETURN_PIPELINE_HALT_HASH, (void*)se_return_pipeline_halt },
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

//=============================================================
// DICTIONARY NAVIGATION HELPERS
// ============================================================================

// Find a key in a dictionary by hash value
// dict_param should point to OPEN_DICT
// Returns pointer to the content after OPEN_KEY (first value in key), or NULL

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
        return SE_PIPELINE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_PIPELINE_CONTINUE;
    }
    
    uint64_t remaining = s_expr_get_u64(inst);
   
    if (remaining > 0) {
        remaining--;
        s_expr_set_u64(inst, remaining);
        return SE_FUNCTION_HALT;
    }
    
    return SE_PIPELINE_DISABLE;
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
            return SE_PIPELINE_CONTINUE;
        }
        
        double now = 0.0;
        if (mod && mod->alloc.get_time) {
            now = mod->alloc.get_time(mod->alloc.ctx);
        }
        
        double target_time = now + seconds;
        s_expr_set_f64(inst, target_time);
        
        return SE_PIPELINE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_TERMINATE) {
        return SE_PIPELINE_CONTINUE;
    }
    
    if (event_id != SE_EVENT_TICK) {
        return SE_FUNCTION_HALT;
    }
    
    double target_time = s_expr_get_f64(inst);
    
    double now = 0.0;
    if (mod && mod->alloc.get_time) {
        now = mod->alloc.get_time(mod->alloc.ctx);
    }
    
    if (now >= target_time) {
        return SE_PIPELINE_DISABLE;
    }
    
    return SE_FUNCTION_HALT;
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
    (void)event_id; 
    (void)event_data;
    
    // Expected structure: (if_then_else predicate then_action [else_action])
    // Child 0: predicate
    // Child 1: then branch
    // Child 2: else branch (optional)
    
    uint16_t count = s_expr_child_count(params, param_count);
    if (count < 2) {
        EXCEPTION("se_if_then_else: need at least predicate and then branch");
        return SE_PIPELINE_CONTINUE;
    }
    
    bool has_else = (count >= 3);
    
    const uint16_t PRED_CHILD = 0;
    const uint16_t THEN_CHILD = 1;
    const uint16_t ELSE_CHILD = 2;
    
    // =========================================================================
    // TERMINATE EVENT - pass through to all children
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // INIT EVENT - pass through
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    
    // Evaluate predicate (child 0)
    uint16_t pred_phys_idx = s_expr_child_index(params, param_count, PRED_CHILD);
    bool condition = s_expr_invoke_pred(inst, params, pred_phys_idx);
    
    s_expr_result_t r;
    
    if (condition) {
        // Execute then branch (child 1)
        uint16_t phys_idx = s_expr_child_index(params, param_count, THEN_CHILD);
        r = s_expr_invoke_any(inst, params, phys_idx);
    } else if (has_else) {
        // Execute else branch (child 2)
        uint16_t phys_idx = s_expr_child_index(params, param_count, ELSE_CHILD);
        r = s_expr_invoke_any(inst, params, phys_idx);
    } else {
        // No else branch, condition false
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // RESULT HANDLING
    // =========================================================================
    
    // Non-PIPELINE codes (0-11): propagate to caller
    if (r < SE_PIPELINE_CONTINUE) {
        return r;
    }
    
    switch (r) {
        case SE_PIPELINE_CONTINUE:
        case SE_PIPELINE_HALT:
            return r;
            
        case SE_PIPELINE_RESET:
            // Recursive reset: terminate and reset all children
            s_expr_child_terminate(inst, params, param_count, THEN_CHILD);
            s_expr_child_reset(inst, params, param_count, THEN_CHILD);
            if (has_else) {
                s_expr_child_terminate(inst, params, param_count, ELSE_CHILD);
                s_expr_child_reset(inst, params, param_count, ELSE_CHILD);
            }
            return SE_PIPELINE_RESET;
            
        case SE_PIPELINE_DISABLE:
        case SE_PIPELINE_TERMINATE:
            s_expr_child_terminate(inst, params, param_count, THEN_CHILD);
            s_expr_child_reset(inst, params, param_count, THEN_CHILD);
            if (has_else) {
                s_expr_child_terminate(inst, params, param_count, ELSE_CHILD);
                s_expr_child_reset(inst, params, param_count, ELSE_CHILD);
            }
            return SE_PIPELINE_CONTINUE;
            
        case SE_PIPELINE_SKIP_CONTINUE:
            return SE_PIPELINE_CONTINUE;
            
        default:
            printf("se_if_then_else: unexpected result code %d\n", r);
            EXCEPTION("se_if_then_else: unexpected result code");
            return SE_PIPELINE_CONTINUE;
    }
}


static s_expr_result_t se_cond(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;

    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_PIPELINE_CONTINUE;
    }

    if (event_type == SE_EVENT_INIT) {
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_PIPELINE_CONTINUE;
    }

    // Parameter layout: [pred0] [action0] [pred1] [action1] ... [predN] [actionN]
    // Both predicates and actions are OPEN_CALL and counted as logical children.
    // Predicates are at even child indices: 0, 2, 4, ...
    // Actions are at odd child indices:    1, 3, 5, ...

    uint16_t active_child = s_expr_get_user_flags(inst);
    uint16_t matched_action = 0xFFFF;
    uint16_t child_index = 0;

    for (uint16_t i = 0; i < param_count; ) {
        if (s_expr_param_is_predicate(&params[i])) {
            bool result = s_expr_invoke_pred(inst, params, i);
            // Skip predicate
            i = s_expr_skip_param(params, i);
            child_index++;
            
            if (result && matched_action == 0xFFFF) {
                matched_action = child_index;  // action is next child
                break;
            }
            
            // Skip action
            i = s_expr_skip_param(params, i);
            child_index++;
        } else {
            i = s_expr_skip_param(params, i);
            child_index++;
        }
    }

    if (matched_action == 0xFFFF) {
        EXCEPTION("se_cond: no matching case (missing default)");
        return SE_PIPELINE_CONTINUE;
    }

    // Active child changed: terminate old, reset new
    if (matched_action != active_child) {
        if (active_child != 0xFFFF) {
            s_expr_child_terminate(inst, params, param_count, active_child);
            s_expr_child_reset_recursive(inst, params, param_count, active_child);
        }
        s_expr_child_terminate(inst, params, param_count, matched_action);
        s_expr_child_reset_recursive(inst, params, param_count, matched_action);
        s_expr_set_user_flags(inst, matched_action);
    }

    s_expr_result_t r = s_expr_child_invoke(inst, params, param_count, matched_action);

    // Non-PIPELINE codes (0-11): propagate to caller
    if (r < SE_PIPELINE_CONTINUE) {
        return r;
    }

    switch (r) {
        case SE_PIPELINE_CONTINUE:
        case SE_PIPELINE_HALT:
            return SE_PIPELINE_CONTINUE;
        case SE_PIPELINE_RESET:
            s_expr_child_terminate(inst, params, param_count, matched_action);
            s_expr_child_reset_recursive(inst, params, param_count, matched_action);
            return SE_PIPELINE_CONTINUE;
        case SE_PIPELINE_DISABLE:
        case SE_PIPELINE_TERMINATE:
        case SE_PIPELINE_SKIP_CONTINUE:
            return r;
        default:
            EXCEPTION("se_cond: unexpected result code");
            return SE_PIPELINE_CONTINUE;
    }
}


static s_expr_result_t se_trigger_on_change(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; 
    (void)event_data;
    
    // Expected structure: (trigger_on_change initial_state predicate rising_action [falling_action])
    // Child 0: initial state (INT 0 or 1)
    // Child 1: predicate
    // Child 2: rising action
    // Child 3: falling action (optional)
    
    uint16_t count = s_expr_child_count(params, param_count);
    if (count < 3) {
        EXCEPTION("se_trigger_on_change: need at least 3 children");
        return SE_PIPELINE_CONTINUE;
    }
    
    const uint16_t INIT_STATE_CHILD = 0;
    const uint16_t PRED_CHILD = 1;
    const uint16_t RISING_CHILD = 2;
    const uint16_t FALLING_CHILD = 3;
    bool has_falling = (count >= 4);
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        // Get initial state from child 0
        uint16_t init_phys_idx = s_expr_child_index(params, param_count, INIT_STATE_CHILD);
        uint8_t type0 = params[init_phys_idx].type & S_EXPR_OPCODE_MASK;
        
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("se_trigger_on_change: child 0 must be INT or UINT");
            return SE_PIPELINE_CONTINUE;
        }
        
        int32_t initial_state = (int32_t)params[init_phys_idx].int_val;
        s_expr_set_state(inst, initial_state ? 1 : 0);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    
    // Evaluate predicate and detect edges
    bool current = s_expr_child_invoke_pred(inst, params, param_count, PRED_CHILD);
    uint8_t prev = s_expr_get_state(inst);
    
    bool rising = (prev == 0 && current);
    bool falling = (prev != 0 && !current);
    
    s_expr_set_state(inst, current ? 1 : 0);
    
    if (rising) {
        // Terminate falling action (was running, now stopping)
        if (has_falling) {
            s_expr_child_terminate(inst, params, param_count, FALLING_CHILD);
            s_expr_child_reset(inst, params, param_count, FALLING_CHILD);
        }
        
        // Restart rising action: terminate, reset, invoke
        s_expr_child_terminate(inst, params, param_count, RISING_CHILD);
        s_expr_child_reset(inst, params, param_count, RISING_CHILD);
        
        uint16_t phys_idx = s_expr_child_index(params, param_count, RISING_CHILD);
        s_expr_result_t r = s_expr_invoke_any(inst, params, phys_idx);
        
        // Non-PIPELINE codes (0-11) - propagate to caller
        if (r < SE_PIPELINE_CONTINUE) {
            return r;
        }
        
        // PIPELINE codes (12-17) - handle internally
        switch (r) {
            case SE_PIPELINE_CONTINUE:
            case SE_PIPELINE_HALT:
                return SE_PIPELINE_CONTINUE;
                
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
            case SE_PIPELINE_RESET:
                s_expr_child_terminate(inst, params, param_count, RISING_CHILD);
                s_expr_child_reset(inst, params, param_count, RISING_CHILD);
                return SE_PIPELINE_CONTINUE;
                
            case SE_PIPELINE_SKIP_CONTINUE:
                return SE_PIPELINE_CONTINUE;
                
            default:
                return SE_PIPELINE_CONTINUE;
        }
    } 
    else if (falling && has_falling) {
        // Terminate rising action (was running, now stopping)
        s_expr_child_terminate(inst, params, param_count, RISING_CHILD);
        s_expr_child_reset(inst, params, param_count, RISING_CHILD);
        
        // Restart falling action: terminate, reset, invoke
        s_expr_child_terminate(inst, params, param_count, FALLING_CHILD);
        s_expr_child_reset(inst, params, param_count, FALLING_CHILD);
        
        uint16_t phys_idx = s_expr_child_index(params, param_count, FALLING_CHILD);
        s_expr_result_t r = s_expr_invoke_any(inst, params, phys_idx);
        
        // Non-PIPELINE codes (0-11) - propagate to caller
        if (r < SE_PIPELINE_CONTINUE) {
            return r;
        }
        
        // PIPELINE codes (12-17) - handle internally
        switch (r) {
            case SE_PIPELINE_CONTINUE:
            case SE_PIPELINE_HALT:
                return SE_PIPELINE_CONTINUE;
                
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
            case SE_PIPELINE_RESET:
                s_expr_child_terminate(inst, params, param_count, FALLING_CHILD);
                s_expr_child_reset(inst, params, param_count, FALLING_CHILD);
                return SE_PIPELINE_CONTINUE;
                
            case SE_PIPELINE_SKIP_CONTINUE:
                return SE_PIPELINE_CONTINUE;
                
            default:
                return SE_PIPELINE_CONTINUE;
        }
    }
    
    return SE_PIPELINE_CONTINUE;
}

// SE_FIELD_DISPATCH - dispatch based on integer field value
// params: [field_ref] [int, action] pairs (flat structure)
// Stateful: tracks branch changes, handles INIT/TERMINATE
// Crashes if no matching case (Erlang-style)
// SE_FIELD_DISPATCH - dispatch based on integer field value
// params: [field_ref] [int, action] pairs (flat structure)
// Stateful: tracks branch changes, handles INIT/TERMINATE
// Crashes if no matching case (Erlang-style)
static s_expr_result_t se_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_id);
    UNUSED(event_data);
    
    uint16_t prev_action_idx = s_expr_get_user_flags(inst);
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            terminate_action_at_index(inst, params, prev_action_idx);
        }
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 3) {
            EXCEPTION("se_state_machine: need field_ref and at least one case");
            return SE_PIPELINE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
   
    // Get integer value from field
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) {
        EXCEPTION("se_state_machine: field not found");
        return SE_PIPELINE_CONTINUE;
    }
    
    int32_t val = *val_ptr;
   
    // Search for matching case in flat [int, action] pairs
    uint16_t idx = s_expr_skip_param(params, 0);  // Skip field_ref
    uint16_t action_idx = 0;
    uint16_t default_idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_val = (int32_t)params[idx].int_val;
            uint16_t this_action_idx = idx + 1;
            
            if (this_action_idx < param_count) {
                if (case_val == val) {
                    action_idx = this_action_idx;
                    break;
                }
                
                if (case_val == -1) {
                    default_idx = this_action_idx;
                }
            }
            
            // Skip [int, action] pair
            idx = s_expr_skip_param(params, idx);      // Skip int
            idx = s_expr_skip_param(params, idx);      // Skip action
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    // =========================================================================
    // Use default if no exact match
    // =========================================================================
    if (action_idx == 0) {
        action_idx = default_idx;
    }
    
    // =========================================================================
    // No match and no default - crash (Erlang-style)
    // =========================================================================
    if (action_idx == 0) {
        EXCEPTION("se_state_machine: no matching case");
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // Handle branch change: terminate old, reset new
    // =========================================================================
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            terminate_action_at_index(inst, params, prev_action_idx);
            reset_action_at_index(inst, params, prev_action_idx);
        }
        
        reset_action_at_index(inst, params, action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    // =========================================================================
    // Invoke current action
    // =========================================================================
    s_expr_result_t r = s_expr_invoke_any(inst, params, action_idx);
    
    // -----------------------------------------------------------------
    // Non-PIPELINE codes (0-11) - propagate to caller
    // -----------------------------------------------------------------
    if (r == SE_FUNCTION_HALT){
        return SE_PIPELINE_HALT;
    }
    if (r < SE_PIPELINE_CONTINUE) {
        return r;
    }
    
    // -----------------------------------------------------------------
    // PIPELINE codes (12-17) - handle internally
    // -----------------------------------------------------------------
    switch (r) {
        case SE_PIPELINE_CONTINUE:
        case SE_PIPELINE_HALT:
            // Action still running
            return r;
            
        case SE_PIPELINE_DISABLE:
        case SE_PIPELINE_TERMINATE:
        case SE_PIPELINE_RESET:
            // Action completed - terminate and reset for next activation
            terminate_action_at_index(inst, params, action_idx);
            reset_action_at_index(inst, params, action_idx);
            return SE_PIPELINE_CONTINUE;
            
        case SE_PIPELINE_SKIP_CONTINUE:
            return SE_PIPELINE_CONTINUE;
            
        default:
            EXCEPTION("se_state_machine: unknown result code");
            return SE_PIPELINE_CONTINUE;
    }
}

// SE_FIELD_DISPATCH - dispatch based on integer field value
// params: [field_ref] [int, action] pairs (flat structure)
// Stateful: tracks branch changes, handles INIT/TERMINATE
// Crashes if no matching case (Erlang-style)
// Supports "default" case with value -1
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
    
    // =========================================================================
    // TERMINATE: Clean up active branch
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            terminate_action_at_index(inst, params, prev_action_idx);
        }
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // INIT: Validate and set sentinel
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 3) {
            EXCEPTION("se_field_dispatch: need field_ref and at least one case");
            return SE_PIPELINE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK: Dispatch based on field value
    // =========================================================================
    
    // Get integer value from field
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) {
        EXCEPTION("se_field_dispatch: field not found");
        return SE_PIPELINE_CONTINUE;
    }
    
    int32_t val = *val_ptr;
    
    // Search for matching case in flat [int, action] pairs
    // Also track default case (-1) as fallback
    uint16_t idx = s_expr_skip_param(params, 0);  // Skip field_ref
    uint16_t action_idx = 0;
    uint16_t default_idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_val = (int32_t)params[idx].int_val;
            uint16_t this_action_idx = idx + 1;
            
            if (this_action_idx < param_count) {
                if (case_val == val) {
                    action_idx = this_action_idx;
                    break;
                }
                
                if (case_val == -1) {
                    default_idx = this_action_idx;
                }
            }
            
            // Skip [int, action] pair
            idx = s_expr_skip_param(params, idx);      // Skip int
            idx = s_expr_skip_param(params, idx);      // Skip action
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    // =========================================================================
    // Use default if no exact match
    // =========================================================================
    if (action_idx == 0) {
        action_idx = default_idx;
    }
    
    // =========================================================================
    // No match and no default - crash (Erlang-style)
    // =========================================================================
    if (action_idx == 0) {
        EXCEPTION("se_field_dispatch: no matching case");
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // Handle branch change: terminate old, reset new
    // =========================================================================
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            terminate_action_at_index(inst, params, prev_action_idx);
            reset_action_at_index(inst, params, prev_action_idx);
        }
        
        reset_action_at_index(inst, params, action_idx);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    // =========================================================================
    // Invoke current action and handle pipeline reset
    // =========================================================================
    s_expr_result_t result = s_expr_invoke_any(inst, params, action_idx);
    
    if (result == SE_PIPELINE_RESET) {
        printf("se_field_dispatch: action_idx=%d, result=SE_PIPELINE_RESET\n", action_idx);
        
        terminate_action_at_index(inst, params, action_idx);
        reset_action_at_index(inst, params, action_idx);
        return SE_PIPELINE_CONTINUE;
    }
    
    return result;
}
// Helper to invoke and handle all PIPELINE result codes consistently
static s_expr_result_t invoke_and_handle_result(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t action_idx
) {
    s_expr_result_t r = s_expr_invoke_any(inst, params, action_idx);
    
    // -----------------------------------------------------------------
    // Non-PIPELINE codes (0-11) - propagate to caller
    // -----------------------------------------------------------------
    if (r < SE_PIPELINE_CONTINUE) {
        return r;
    }
    
    // -----------------------------------------------------------------
    // PIPELINE codes (12-17) - handle internally
    // -----------------------------------------------------------------
    switch (r) {
        case SE_PIPELINE_CONTINUE:
        case SE_PIPELINE_HALT:
            // Action still running
            return r;
            
        case SE_PIPELINE_DISABLE:
        case SE_PIPELINE_TERMINATE:
        case SE_PIPELINE_RESET:
            // Action completed - terminate and reset for next activation
            terminate_action_at_index(inst, params, action_idx);
            s_expr_reset_recursive_at(inst, params, action_idx);
            return SE_PIPELINE_CONTINUE;
            
        case SE_PIPELINE_SKIP_CONTINUE:
            return SE_PIPELINE_CONTINUE;
            
        default:
            EXCEPTION("se_event_dispatch: unknown result code");
            return SE_PIPELINE_CONTINUE;
    }
}

static s_expr_result_t se_event_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_data);
    
    // =========================================================================
    // INIT/TERMINATE EVENTS
    // =========================================================================
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE) {
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT - Dispatch based on event_id
    // =========================================================================
    uint16_t idx = 0;
    uint16_t default_action_idx = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_event = (int32_t)params[idx].int_val;
            uint16_t action_idx = idx + 1;
            
            if (action_idx < param_count) {
                // Exact match - invoke immediately
                if (case_event == (int32_t)event_id) {
                    return invoke_and_handle_result(inst, params, action_idx);
                }
                
                // Track default case (-1)
                if (case_event == -1) {
                    default_action_idx = action_idx;
                }
            }
            
            idx = s_expr_skip_param(params, idx);      // Skip int
            idx = s_expr_skip_param(params, idx);      // Skip action
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    // No exact match - try default
    if (default_action_idx > 0) {
        return invoke_and_handle_result(inst, params, default_action_idx);
    }
    
    // No match and no default - crash (Erlang-style)
    EXCEPTION("se_event_dispatch: no matching event handler");
    return SE_PIPELINE_CONTINUE;
}



//
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
    if (!msg){
        EXCEPTION("SE_LOG: msg not found");
        return;
    }
    
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

static s_expr_result_t se_return_function_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_FUNCTION_CONTINUE;
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

static s_expr_result_t se_return_function_disable(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_FUNCTION_DISABLE;
}

static s_expr_result_t se_return_function_skip_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_FUNCTION_SKIP_CONTINUE;
}

static s_expr_result_t se_return_pipeline_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_PIPELINE_CONTINUE;
}
static s_expr_result_t se_return_pipeline_terminate(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_TERMINATE;
}

static s_expr_result_t se_return_pipeline_reset(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_RESET;
}

static s_expr_result_t se_return_pipeline_halt(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_HALT;
}

static s_expr_result_t se_return_pipeline_disable(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_DISABLE;
}

static s_expr_result_t se_return_pipeline_skip_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    return SE_PIPELINE_SKIP_CONTINUE;
}
// SE_SET_HASH - set field to precomputed hash value
// params: [field_ref] [u32]
static void se_set_hash(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    if (param_count < 2) return;
    
    uint32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], uint32_t);
    if (!field_ptr) return;
    
    *field_ptr = (uint32_t)params[1].uint_val;
   
}

// In SE_QUEUE_EVENT implementation - store the field offset
static void se_queue_event(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_type; (void)event_id; (void)event_data;
    
    uint16_t ev_type = (uint16_t)params[0].int_val;
    uint16_t ev_id = (uint16_t)params[1].int_val;
    
    // Get field offset as event_data (cast to void* for storage)
    void* ev_data = NULL;
    if (param_count > 2 && S_EXPR_PARAM_IS_FIELD(params[2].type)) {
        ev_data = (void*)(uintptr_t)params[2].field_offset;
    }
    
    // Queue it
    s_expr_event_push(inst, ev_type, ev_id, ev_data);
}

// ============================================================================
// SE_SEQUENCE - Sequential Execution
// 
// Executes children one at a time in order. Advances to next child when
// current child completes. Sequence completes when all children finish.
//
// State: Current child index (0 to child_count-1)
//
// Child results:
//   PIPELINE_CONTINUE/HALT  -> Child running, pause sequence
//   PIPELINE_DISABLE/TERMINATE/RESET -> Child complete, advance to next
//   PIPELINE_SKIP_CONTINUE  -> Pause sequence this tick
//   Non-PIPELINE (0-11)     -> Propagate immediately to caller
// ============================================================================

static s_expr_result_t se_sequence(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_id);
    UNUSED(event_data);
    
    uint8_t state = s_expr_get_state(inst);
    uint16_t child_count = s_expr_child_count(params, param_count);
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (state < child_count) {
            if (s_expr_child_is_initialized(inst, params, param_count, state)) {
                s_expr_child_terminate(inst, params, param_count, state);
            }
        }
        s_expr_set_state(inst, 0);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    while (state < child_count) {
        // Skip non-callable parameters
        if (!s_expr_child_is_callable(params, param_count, state)) {
            state++;
            s_expr_set_state(inst, state);
            continue;
        }
        
        uint16_t phys_idx = s_expr_child_index(params, param_count, state);
        if (phys_idx == UINT16_MAX) {
            state++;
            s_expr_set_state(inst, state);
            continue;
        }
        
        uint8_t func_type = s_expr_child_func_type(params, param_count, state);
        
        // -----------------------------------------------------------------
        // ONESHOT - invoke and advance immediately
        // -----------------------------------------------------------------
        if (func_type == S_EXPR_PARAM_ONESHOT) {
            s_expr_invoke_any(inst, params, phys_idx);
            state++;
            s_expr_set_state(inst, state);
            continue;
        }
        
        // -----------------------------------------------------------------
        // PRED - invoke and advance immediately
        // -----------------------------------------------------------------
        if (func_type == S_EXPR_PARAM_PRED) {
            s_expr_invoke_any(inst, params, phys_idx);
            state++;
            s_expr_set_state(inst, state);
            continue;
        }
        
        // -----------------------------------------------------------------
        // MAIN - invoke and check result
        // -----------------------------------------------------------------
        s_expr_result_t r = s_expr_invoke_any(inst, params, phys_idx);
        
        // APPLICATION codes (0-5) - propagate immediately
        if (r <= SE_SKIP_CONTINUE) {
            return r;
        }
        
        // FUNCTION codes (6-11) - propagate, except HALT which converts
        if (r >= SE_FUNCTION_CONTINUE && r <= SE_FUNCTION_SKIP_CONTINUE) {
            if (r == SE_FUNCTION_HALT) {
                return SE_PIPELINE_HALT;
            }
            return r;
        }
        
        // PIPELINE codes (12-17) - handle internally
        switch (r) {
            case SE_PIPELINE_CONTINUE:
            case SE_PIPELINE_HALT:
                // Child still running - pause, resume next tick
                return SE_PIPELINE_CONTINUE;
                
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
            case SE_PIPELINE_RESET:
                // Child complete - terminate and advance
                s_expr_child_terminate(inst, params, param_count, state);
                state++;
                s_expr_set_state(inst, state);
                continue;
                
            case SE_PIPELINE_SKIP_CONTINUE:
                return SE_PIPELINE_CONTINUE;
                
            default:
                EXCEPTION("se_sequence: unknown result code");
                return SE_PIPELINE_CONTINUE;
        }
    }
    
    // All children complete
    return SE_PIPELINE_DISABLE;
}
#define FORK_STATE_INIT      0
#define FORK_STATE_RUNNING   1
#define FORK_STATE_COMPLETE  2



static s_expr_result_t se_function_interface(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_id);
    UNUSED(event_data);
    
    uint16_t child_count = s_expr_child_count(params, param_count);
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, FORK_STATE_RUNNING);
        s_expr_set_user_flags(inst, 0);
        
        for (uint16_t i = 0; i < child_count; i++) {
            if (s_expr_child_is_callable(params, param_count, i)) {
                s_expr_child_reset(inst, params, param_count, i);
            }
        }
        return SE_FUNCTION_CONTINUE;
    }
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        s_expr_set_state(inst, FORK_STATE_COMPLETE);
        return SE_FUNCTION_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    uint8_t state = s_expr_get_state(inst);
    
    if (state != FORK_STATE_RUNNING) {
        return SE_FUNCTION_DISABLE;
    }
    
    uint16_t active_count = 0;
    //printf("se_function_interface: child_count=%d\n", child_count);
    for (uint16_t i = 0; i < child_count; i++) {
        //printf("se_function_interface: child %d is_callable=%d is_active=%d\n", i, s_expr_child_is_callable(params, param_count, i), s_expr_child_is_active(inst, params, param_count, i));
        if (!s_expr_child_is_callable(params, param_count, i)) {
            continue;
        }
        
        if (!s_expr_child_is_active(inst, params, param_count, i)) {
            continue;
        }
        
        // Use raw invoke to get actual return code
        uint16_t phys_idx = s_expr_child_index(params, param_count, i);
        s_expr_result_t r = s_expr_invoke_any(inst, params, phys_idx);
        //printf("se_function_interface: child %d result=%d\n", i, r);
        // -----------------------------------------------------------------
        // Non-PIPELINE codes (0-11) - immediate exit, propagate to caller
        // -----------------------------------------------------------------
        
        if (r < SE_PIPELINE_CONTINUE) {
            //printf("se_function_interface: child %d result=%d propagating to caller\n", i, r);
            return r;
        }
        
        // -----------------------------------------------------------------
        // PIPELINE codes (12-17) - handle internally
        // -----------------------------------------------------------------
        //printf("se_function_interface: child %d result=%d\n", i, r);
        switch (r) {
            case SE_PIPELINE_CONTINUE:
            case SE_PIPELINE_HALT:
                active_count++;
                break;
                
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
                s_expr_child_terminate(inst, params, param_count, i);
                break;
                
            case SE_PIPELINE_RESET:
                s_expr_child_terminate(inst, params, param_count, i);
                s_expr_child_reset(inst, params, param_count, i);
                active_count++;
                break;
                
            case SE_PIPELINE_SKIP_CONTINUE:
                active_count++;
                goto tick_complete;
                
            default:
                active_count++;
                break;
        }
    }
    
tick_complete:
    if (active_count == 0) {
        s_expr_set_state(inst, FORK_STATE_COMPLETE);
        return SE_FUNCTION_DISABLE;
    }
    //printf("se_function_interface: active_count=%d\n", active_count);
    return SE_FUNCTION_CONTINUE;
}

// ============================================================================
// se_fork.c
// Fork Composite - Executes all children in parallel each tick
// Only handles PIPELINE codes internally; others pass to outer node
// ============================================================================



s_expr_result_t se_fork(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t*   params,
    uint16_t                param_count,
    s_expr_event_type_t     event_type,
    uint16_t                event_id,
    void*                   event_data
) {
    UNUSED(event_id);
    UNUSED(event_data);
    
    uint16_t child_count = s_expr_child_count(params, param_count);
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, FORK_STATE_RUNNING);
        s_expr_set_user_flags(inst, 0);
        
        for (uint16_t i = 0; i < child_count; i++) {
            if (s_expr_child_is_callable(params, param_count, i)) {
                s_expr_child_reset(inst, params, param_count, i);
            }
        }
        
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        s_expr_set_state(inst, FORK_STATE_COMPLETE);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    uint8_t state = s_expr_get_state(inst);
    
    if (state != FORK_STATE_RUNNING) {
        return SE_DISABLE;
    }
    
    uint16_t active_count = 0;
    //printf("se_fork: child_count=%d\n", child_count);
    for (uint16_t i = 0; i < child_count; i++) {
        //printf("se_fork: child %d is_callable=%d is_active=%d\n", i, s_expr_child_is_callable(params, param_count, i), s_expr_child_is_active(inst, params, param_count, i));
        if (!s_expr_child_is_callable(params, param_count, i)) {
            continue;
        }
        
        if (!s_expr_child_is_active(inst, params, param_count, i)) {
            continue;
        }
        
        s_expr_result_t r = s_expr_child_invoke(inst, params, param_count, i);
        //printf("se_fork: child %d result=%d\n", i, r);
        // -----------------------------------------------------------------
        // Regular (0-5) and FUNCTION (6-11) codes - pass to outer node
        // -----------------------------------------------------------------
        if (r == SE_FUNCTION_HALT) {
            return SE_PIPELINE_HALT;
        }
        if (r < SE_PIPELINE_CONTINUE) {
            return r;
        }
        
        // -----------------------------------------------------------------
        // PIPELINE codes (12-17) - handle internally
        // -----------------------------------------------------------------
        switch (r) {
            case SE_PIPELINE_CONTINUE:
            case SE_PIPELINE_HALT:
                active_count++;
                break;
                
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
                s_expr_child_terminate(inst, params, param_count, i);
                break;
                
            case SE_PIPELINE_RESET:
                s_expr_child_terminate(inst, params, param_count, i);
                s_expr_child_reset(inst, params, param_count, i);
                active_count++;
                break;
                
            case SE_PIPELINE_SKIP_CONTINUE:
                active_count++;
                goto tick_complete;
                
            default:
                active_count++;
                break;
        }
    }
    
tick_complete:
    if (active_count == 0) {
        s_expr_set_state(inst, FORK_STATE_COMPLETE);
        return SE_PIPELINE_DISABLE;
    }
    
    return SE_PIPELINE_CONTINUE;
}
// ============================================================================
// SE_FORK_JOIN
// Execute all children in parallel, return SE_FUNCTION_HALT until all complete
// Returns SE_FUNCTION_HALT while working, SE_PIPELINE_DISABLE when all complete
// Fatal codes propagate immediately
// ============================================================================

static s_expr_result_t se_fork_join(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_id);
    UNUSED(event_data);
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    uint16_t count = s_expr_child_count(params, param_count);
    
    for (uint16_t i = 0; i < count; i++) {
        if (!s_expr_child_is_callable(params, param_count, i)) {
            continue;
        }
        
        uint16_t phys_idx = s_expr_child_index(params, param_count, i);
        if (phys_idx == UINT16_MAX) {
            continue;
        }
        
        uint8_t func_type = s_expr_child_func_type(params, param_count, i);
        
        // -----------------------------------------------------------------
        // ONESHOT - fire once, skip if already initialized
        // -----------------------------------------------------------------
        if (func_type == S_EXPR_PARAM_ONESHOT) {
            if (!s_expr_child_is_initialized(inst, params, param_count, i)) {
                s_expr_invoke_any(inst, params, phys_idx);
            }
            continue;
        }
        
        // -----------------------------------------------------------------
        // PRED - evaluate once, skip if already initialized
        // -----------------------------------------------------------------
        if (func_type == S_EXPR_PARAM_PRED) {
            if (!s_expr_child_is_initialized(inst, params, param_count, i)) {
                s_expr_invoke_any(inst, params, phys_idx);
            }
            continue;
        }
        
        // -----------------------------------------------------------------
        // MAIN - only invoke if still active
        // -----------------------------------------------------------------
        if (!s_expr_child_is_active(inst, params, param_count, i)) {
            continue;
        }
        
        s_expr_result_t r = s_expr_invoke_any(inst, params, phys_idx);
        
        // Non-PIPELINE codes (0-11) - immediate exit, propagate to caller
        if (r < SE_PIPELINE_CONTINUE) {
            return r;
        }
        
        // PIPELINE codes (12-17) - handle internally
        switch (r) {
            case SE_PIPELINE_CONTINUE:
            case SE_PIPELINE_HALT:
                // Child still running
                break;
                
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
                // Child complete - mark inactive
                terminate_action_at_index(inst, params, phys_idx);
                break;
                
            case SE_PIPELINE_RESET:
                // Child wants to restart
                terminate_action_at_index(inst, params, phys_idx);
                s_expr_reset_recursive_at(inst, params, phys_idx);
                break;
                
            case SE_PIPELINE_SKIP_CONTINUE:
                goto check_completion;
                
            default:
                EXCEPTION("se_fork_join: unknown result code");
                break;
        }
    }
    
check_completion:
    // Count active MAIN children
    uint16_t active_main_count = 0;
    for (uint16_t i = 0; i < count; i++) {
        if (!s_expr_child_is_callable(params, param_count, i)) {
            continue;
        }
        
        uint8_t func_type = s_expr_child_func_type(params, param_count, i);
        
        if (func_type != S_EXPR_PARAM_MAIN) {
            continue;
        }
        
        if (s_expr_child_is_active(inst, params, param_count, i)) {
            active_main_count++;
        }
    }
    
    if (active_main_count == 0) {
        return SE_PIPELINE_DISABLE;
    }
    
    return SE_PIPELINE_HALT;
}
// ============================================================================
// SE_CHAIN_FLOW
// Pipeline variant that processes events through children like ChainTree walker
// SE_FUNCTION_RESET: reset child, continue to next
// SE_FUNCTION_TERMINATE: terminate child, continue to next
// SE_CONTINUE: continue to next child
// All other results: return immediately
// ============================================================================

static s_expr_result_t se_chain_flow(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    UNUSED(event_id);
    UNUSED(event_data);
    
    // =========================================================================
    // TERMINATE EVENT
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // INIT EVENT
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        return SE_PIPELINE_CONTINUE;
    }
    
    // =========================================================================
    // TICK EVENT
    // =========================================================================
    uint16_t count = s_expr_child_count(params, param_count);
    uint16_t active_count = 0;
    //printf("se_chain_flow: count=%d\n", count);

    //printf("se_chain_flow: child 0 is_callable=%d is_active=%d\n", s_expr_child_is_callable(params, param_count, 0), s_expr_child_is_active(inst, params, param_count, 0));
    for (uint16_t i = 0; i < count; i++) {
        if (!s_expr_child_is_callable(params, param_count, i)) {
            continue;
        }
        
        if (!s_expr_child_is_active(inst, params, param_count, i)) {
            continue;
        }
        
        uint16_t phys_idx = s_expr_child_index(params, param_count, i);
        if (phys_idx == UINT16_MAX) {
            continue;
        }
        
        s_expr_result_t r = s_expr_invoke_any(inst, params, phys_idx);
        //printf("se_chain_flow: child %d result=%d\n", i, r);
        
        // -----------------------------------------------------------------
        // Non-PIPELINE codes (0-11) - immediate exit, propagate to caller
        // -----------------------------------------------------------------
        if (r == SE_FUNCTION_HALT) {
            return SE_PIPELINE_CONTINUE;
        }
        if (r < SE_PIPELINE_CONTINUE) {
            printf("se_chain_flow: child %d result=%d propagating to caller\n", i, r);
            return r;
        }
        
        // -----------------------------------------------------------------
        // PIPELINE codes (12-17) - handle internally
        // -----------------------------------------------------------------
        switch (r) {
            case SE_PIPELINE_CONTINUE:
                active_count++;
                continue;
                
            case SE_PIPELINE_HALT:
                
                return SE_PIPELINE_CONTINUE;
                
            case SE_PIPELINE_DISABLE:
               s_expr_child_terminate(inst, params, param_count, i);
               continue;


            case SE_PIPELINE_TERMINATE:
                s_expr_children_terminate_all(inst, params, param_count);
                return SE_PIPELINE_TERMINATE;
                
            case SE_PIPELINE_RESET:
                s_expr_children_terminate_all(inst, params, param_count);
                s_expr_children_reset_all(inst, params, param_count);
                return SE_PIPELINE_CONTINUE;
            
                
            case SE_PIPELINE_SKIP_CONTINUE:
                active_count++;
                goto tick_complete;
                
            default:
                active_count++;
                continue;
        }
    }
    
tick_complete:
    if (active_count == 0) {
        return SE_PIPELINE_DISABLE;
    }
    
    return SE_PIPELINE_CONTINUE;
}
// ============================================================================
// SE_FOR
// Pipeline variant that executes children N times
// First parameter: iteration count (int/uint or slot reference)
// Resets all children before each iteration
// Returns SE_PIPELINE_DISABLE when all iterations complete
// ============================================================================

// ============================================================================
// SE_FOR
// Pipeline variant that executes children N times
// First parameter: iteration count (int/uint or field reference)
// Resets all children before each iteration
// Returns SE_PIPELINE_DISABLE when all iterations complete
// ============================================================================

static s_expr_result_t se_for(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; 
    (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        s_expr_set_state(inst, 0);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
        return SE_CONTINUE;
    }
    
    // Get iteration count from first parameter
    if (param_count < 1) {
        EXCEPTION("se_for: missing count parameter");
        return SE_TERMINATE;
    }
    
    int32_t count = 0;
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    
    if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
        count = params[0].int_val;
    } else {
        // Try field reference
        int32_t* field_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
        if (field_ptr) {
            count = *field_ptr;
        } else {
            EXCEPTION("se_for: invalid count parameter");
            return SE_TERMINATE;
        }
    }
    
    if (count <= 0) {
        return SE_PIPELINE_DISABLE;
    }
    
    uint8_t iteration = s_expr_get_state(inst);
    uint16_t child_count = s_expr_child_count(params + 1, param_count - 1);
    
    // Loop until we need to wait or all iterations complete
    while (iteration < (uint8_t)count) {
        
        // Invoke all active children
        for (uint16_t i = 0; i < child_count; i++) {
            bool callable = s_expr_child_is_callable(params + 1, param_count - 1, i);
            bool active = s_expr_child_is_active(inst, params + 1, param_count - 1, i);
            
            if (!callable || !active) {
                continue;
            }
            
            s_expr_result_t result = s_expr_child_invoke(inst, params + 1, param_count - 1, i);
            
            // Propagate fatal/halt results
            switch (result) {
                case SE_CONTINUE:
                    continue;
                case SE_TERMINATE:
                case SE_RESET:
                case SE_FUNCTION_TERMINATE:
                case SE_FUNCTION_RESET:
                case SE_HALT:
                case SE_FUNCTION_HALT:
                    s_expr_set_state(inst, iteration);
                    return result;
                default:
                    continue;
            }
        }
        
        // Recount active children AFTER all have run
        uint16_t active_count = 0;
        for (uint16_t i = 0; i < child_count; i++) {
            if (s_expr_child_is_callable(params + 1, param_count - 1, i) &&
                s_expr_child_is_active(inst, params + 1, param_count - 1, i)) {
                active_count++;
            }
        }
        
        // Children still working - wait
        if (active_count > 0) {
            s_expr_set_state(inst, iteration);
            return SE_CONTINUE;
        }
        
        // All children complete this iteration - advance
        iteration++;
        s_expr_set_state(inst, iteration);
        
        // Check if all iterations complete
        if (iteration >= (uint8_t)count) {
            return SE_PIPELINE_DISABLE;
        }
        
        // Reset all children for next iteration
        for (uint16_t i = 0; i < child_count; i++) {
            if (s_expr_child_is_callable(params + 1, param_count - 1, i)) {
                s_expr_child_reset(inst, params + 1, param_count - 1, i);
            }
        }
        
        // Continue while loop for next iteration (same tick)
    }
    
    // All iterations complete
    return SE_PIPELINE_DISABLE;
}
// ============================================================================
// SE_WHILE
// Pipeline variant that loops while predicate returns true
// First child: boolean predicate (receives all events)
//   - SE_CONTINUE = true, continue loop
//   - SE_DISABLE/SE_PIPELINE_DISABLE = false, exit loop
// Remaining children: loop body
// Resets body children before each iteration
// Returns SE_PIPELINE_DISABLE when predicate returns false
// ============================================================================

static s_expr_result_t se_while(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; 
    (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    uint16_t child_count = s_expr_child_count(params, param_count);
    
    if (child_count < 1) {
        EXCEPTION("se_while: missing predicate");
        return SE_TERMINATE;
    }
    
    if (!s_expr_child_is_callable(params, param_count, 0)) {
        EXCEPTION("se_while: predicate not callable");
        return SE_TERMINATE;
    }
    
    // Loop until predicate fails or we need to yield
    while (1) {
        // Invoke predicate
        bool pred_result = s_expr_child_invoke_pred(inst, params, param_count, 0);
        if (!pred_result) {
            s_expr_children_terminate_all(inst, params, param_count);
            
            return SE_PIPELINE_DISABLE;
        }
        
        // Execute body children (skip predicate at index 0)
        bool any_still_running = false;
        
        for (uint16_t i = 1; i < child_count; i++) {
            if (!s_expr_child_is_callable(params, param_count, i)) {
                continue;
            }
            
            if (!s_expr_child_is_active(inst, params, param_count, i)) {
                // Already completed this iteration
                continue;
            }
            
            s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, i);
            
            if (result == SE_PIPELINE_DISABLE || result == SE_DISABLE) {
                // This child completed, check next
                continue;
            }
            
            if (result == SE_CONTINUE) {
                // Child still running - need to yield
                any_still_running = true;
                continue;
            }
            
            // Fatal or halt - propagate
            return result;
        }
        
        if (any_still_running) {
            // Yield, come back next tick
            return SE_FUNCTION_HALT;
        }
       
        // All body children complete - reset for next iteration
        s_expr_child_reset(inst, params, param_count, 0);
        
        for (uint16_t i = 1; i < child_count; i++) {
            if (s_expr_child_is_callable(params, param_count, i)) {
                s_expr_child_reset(inst, params, param_count, i);
            }
        }
        // Loop back to check predicate again
    }
}


