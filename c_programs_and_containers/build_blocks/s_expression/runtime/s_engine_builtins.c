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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
// ============================================================================
// FORWARD DECLARATIONS - Internal Helpers
// ============================================================================

static void terminate_action_at_index(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t phys_idx
);

static void reset_action_at_index(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t phys_idx
);
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

//static s_expr_result_t se_pipeline(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_tick_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_time_delay(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_wait_event(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_nop(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_if_then_else(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_trigger_on_change(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_state_machine(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
//static s_expr_result_t se_state_actions(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
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
static s_expr_result_t se_return_pipeline_terminate(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_pipeline_reset_continue(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_return_pipeline_reset_halt(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);

// Sequence functions
static s_expr_result_t se_sequence(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_fork(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_fork_join(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_chain_flow(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_for(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
static s_expr_result_t se_while(s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count, s_expr_event_type_t event_type, uint16_t event_id, void* event_data);
// Oneshots
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
    { SE_DISPATCH_HASH, (void*)se_dispatch },
    { SE_SEQUENCE_HASH, (void*)se_sequence },
    { SE_FORK_HASH, (void*)se_fork },
    { SE_FORK_JOIN_HASH, (void*)se_fork_join },
    { SE_CHAIN_FLOW_HASH, (void*)se_chain_flow },
    { SE_FOR_HASH, (void*)se_for },
    { SE_WHILE_HASH, (void*)se_while },
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
    { SE_RETURN_PIPELINE_TERMINATE_HASH, (void*)se_return_pipeline_terminate },
    { SE_RETURN_PIPELINE_RESET_CONTINUE_HASH, (void*)se_return_pipeline_reset_continue },
    { SE_RETURN_PIPELINE_RESET_HALT_HASH, (void*)se_return_pipeline_reset_halt },
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
#if 0
uint32_t s_expr_fnv1a_hash(const char* str) {
    uint32_t hash = 0x811c9dc5;  // FNV offset basis
    
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= 0x01000193;  // FNV prime
    }
    
    return hash;
}
#endif
// ============================================================================
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
    (void)event_id; 
    (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    // Expected structure: (if_then_else predicate then_action [else_action])
    // Child 0: predicate
    // Child 1: then branch
    // Child 2: else branch (optional)
    
    uint16_t count = s_expr_child_count(params, param_count);
    if (count < 2) {
        EXCEPTION("se_if_then_else: need at least predicate and then branch");
        return SE_CONTINUE;  // Need at least predicate and then branch
    }
    
    // Evaluate predicate (child 0)
    bool condition = s_expr_child_invoke_pred(inst, params, param_count, 0);
    //printf("se_if_then_else: condition=%d\n", condition);
    if (condition) {
        // Execute then branch (child 1)
        s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, 1);
        //printf("se_if_then_else: then branch result=%d\n", result);
        return result;
    } else if (count >= 3) {
        // Execute else branch (child 2)
        s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, 2);
        //printf("se_if_then_else: else branch result=%d\n", result);
        return result;
    }
    //printf("se_if_then_else: returning SE_CONTINUE\n");
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
        return SE_CONTINUE;
    }
    
    const uint16_t INIT_STATE_CHILD = 0;
    const uint16_t PRED_CHILD = 1;
    const uint16_t RISING_CHILD = 2;
    const uint16_t FALLING_CHILD = 3;
    bool has_falling = (count >= 4);
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        // Get initial state from child 0
        uint16_t init_phys_idx = s_expr_child_index(params, param_count, INIT_STATE_CHILD);
        uint8_t type0 = params[init_phys_idx].type & S_EXPR_OPCODE_MASK;
        
        if (type0 != S_EXPR_PARAM_INT && type0 != S_EXPR_PARAM_UINT) {
            EXCEPTION("se_trigger_on_change: child 0 must be INT or UINT");
            return SE_CONTINUE;
        }
        
        int32_t initial_state = (int32_t)params[init_phys_idx].int_val;
        s_expr_set_state(inst, initial_state ? 1 : 0);
        return SE_CONTINUE;
    }
    
    // TICK: evaluate predicate and detect edges
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
        return s_expr_child_invoke(inst, params, param_count, RISING_CHILD);
    } 
    else if (falling && has_falling) {
        // Terminate rising action (was running, now stopping)
        s_expr_child_terminate(inst, params, param_count, RISING_CHILD);
        s_expr_child_reset(inst, params, param_count, RISING_CHILD);
        
        // Restart falling action: terminate, reset, invoke
        s_expr_child_terminate(inst, params, param_count, FALLING_CHILD);
        s_expr_child_reset(inst, params, param_count, FALLING_CHILD);
        return s_expr_child_invoke(inst, params, param_count, FALLING_CHILD);
    }
    
    return SE_CONTINUE;
}

// SE_FIELD_DISPATCH - dispatch based on integer field value
// params: [field_ref] [int, action] pairs (flat structure)
// Stateful: tracks branch changes, handles INIT/TERMINATE
// Crashes if no matching case (Erlang-style)
// SE_FIELD_DISPATCH - dispatch based on integer field value
// params: [field_ref] [int, action] pairs (flat structure)
// Stateful: tracks branch changes, handles INIT/TERMINATE
// Crashes if no matching case (Erlang-style)
// Supports "default" case with value -1
static s_expr_result_t se_state_machine(
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
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK: Dispatch based on field value
    // =========================================================================
    
    // Get integer value from field
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) {
        EXCEPTION("se_field_dispatch: field not found");
        return SE_CONTINUE;
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
    // Invoke current action
    // =========================================================================
    return s_expr_invoke_any(inst, params, action_idx);
}


// SE_FIELD_DISPATCH - dispatch based on integer field value
// params: [field_ref] [int, action] pairs (flat structure)
// Stateful: tracks branch changes, handles INIT/TERMINATE
// Crashes if no matching case (Erlang-style)
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
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0xFFFF);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK: Dispatch based on field value
    // =========================================================================
    
    // Get integer value from field
    int32_t* val_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!val_ptr) {
        EXCEPTION("se_field_dispatch: field not found");
        return SE_CONTINUE;
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
    // Invoke current action
    // =========================================================================
    return s_expr_invoke_any(inst, params, action_idx);
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
    uint16_t default_action_idx = 0;
    uint16_t default_action_count = 0;
    
    while (idx < param_count) {
        uint8_t opcode = params[idx].type & S_EXPR_OPCODE_MASK;
        
        if (opcode == S_EXPR_PARAM_INT || opcode == S_EXPR_PARAM_UINT) {
            int32_t case_event = (int32_t)params[idx].int_val;
            uint16_t action_idx = idx + 1;
            
            if (action_idx < param_count) {
                uint16_t action_count = s_expr_skip_param(params, action_idx) - action_idx;
                
                // Exact match - invoke immediately
                if (case_event == (int32_t)event_id) {
                    s_expr_result_t result = s_expr_child_invoke(inst, &params[action_idx], action_count, 0);
                    
                    if (result == SE_FUNCTION_RESET) {
                        s_expr_children_reset_all(inst, &params[action_idx], action_count);
                        return SE_CONTINUE;
                    }
                    
                    return result;
                }
                
                // Track default case
                if (case_event == -1) {
                    default_action_idx = action_idx;
                    default_action_count = action_count;
                }
            }
            
            idx = s_expr_skip_param(params, idx);
            idx = s_expr_skip_param(params, idx);
        } else {
            idx = s_expr_skip_param(params, idx);
        }
    }
    
    // No exact match - try default
    if (default_action_idx > 0) {
        s_expr_result_t result = s_expr_child_invoke(inst, &params[default_action_idx], default_action_count, 0);
        
        if (result == SE_FUNCTION_RESET) {
            s_expr_children_reset_all(inst, &params[default_action_idx], default_action_count);
            return SE_CONTINUE;
        }
        
        return result;
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
                    uint16_t body_count = case_count - body_start;
                    
                    uint16_t child_count = s_expr_child_count(&case_params[body_start], body_count);
                    s_expr_result_t result = SE_CONTINUE;
                    
                    for (uint16_t i = 0; i < child_count; i++) {
                        result = s_expr_child_invoke(inst, &case_params[body_start], body_count, i);
                        if (result != SE_CONTINUE) {
                            break;
                        }
                    }
                    return result;
                }
            }
        }
        idx = s_expr_skip_param(params, idx);
    }
    
    return SE_CONTINUE;
}

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
    
    // =========================================================================
    // TERMINATE: Clean up active branch
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            const s_expr_param_t* prev_content = &params[prev_action_idx];
            uint16_t prev_count;
            s_expr_key_contents(prev_content - 1, &prev_count);
            s_expr_children_terminate_all(inst, prev_content, prev_count);
        }
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // INIT: Validate and set sentinel
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("se_string_dispatch: need field_ref and dict");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0xFFFF);  // Sentinel: no previous action
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK: Dispatch based on string hash
    // =========================================================================
    
    // Get string from field and compute hash
    const char** str_ptr = S_EXPR_GET_FIELD(inst, &params[0], const char*);
    if (!str_ptr || !*str_ptr) {
        EXCEPTION("se_string_dispatch: field not found or null");
        return SE_CONTINUE;
    }
    
    uint32_t str_hash = s_expr_fnv1a_hash(*str_ptr);
    
    // Find dictionary (skip field_ref)
    uint16_t dict_idx = s_expr_skip_param(params, 0);
    if (dict_idx >= param_count) {
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[dict_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_string_dispatch: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up key in dictionary
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[dict_idx], str_hash);
    
    if (!key_content) {
        // No match - check for DEFAULT
        uint32_t default_hash = s_expr_fnv1a_hash("DEFAULT");
        key_content = s_expr_dict_find_key(&params[dict_idx], default_hash);
        
        if (!key_content) {
            // No match and no default - terminate previous if any
            if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
                const s_expr_param_t* prev_content = &params[prev_action_idx];
                uint16_t prev_count;
                s_expr_key_contents(prev_content - 1, &prev_count);
                s_expr_children_terminate_all(inst, prev_content, prev_count);
                s_expr_set_user_flags(inst, 0xFFFF);
            }
            return SE_CONTINUE;
        }
    }
    
    uint16_t action_idx = (uint16_t)(key_content - params);
    uint16_t content_count;
    s_expr_key_contents(key_content - 1, &content_count);
    
    // =========================================================================
    // Handle branch change: terminate old, reset new
    // =========================================================================
    if (action_idx != prev_action_idx) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            const s_expr_param_t* prev_content = &params[prev_action_idx];
            uint16_t prev_count;
            s_expr_key_contents(prev_content - 1, &prev_count);
            s_expr_children_terminate_all(inst, prev_content, prev_count);
        }
        
        s_expr_children_reset_all(inst, key_content, content_count);
        s_expr_set_user_flags(inst, action_idx);
    }
    
    // =========================================================================
    // Execute children in selected branch
    // =========================================================================
    uint16_t child_count = s_expr_child_count(key_content, content_count);
    s_expr_result_t result = SE_CONTINUE;
    
    for (uint16_t i = 0; i < child_count; i++) {
        result = s_expr_child_invoke(inst, key_content, content_count, i);
        if (result != SE_CONTINUE) {
            break;
        }
    }
    
    return result;
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
    
    // =========================================================================
    // TERMINATE: Clean up active branch
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            const s_expr_param_t* prev_content = &params[prev_action_idx];
            uint16_t prev_count;
            s_expr_key_contents(prev_content - 1, &prev_count);
            s_expr_children_terminate_all(inst, prev_content, prev_count);
        }
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // INIT: Validate and set sentinel
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        if (param_count < 2) {
            EXCEPTION("se_hash_dispatch: need field_ref and dict");
            return SE_CONTINUE;
        }
        s_expr_set_user_flags(inst, 0xFFFF);  // Sentinel: no previous action
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK: Dispatch based on hash value
    // =========================================================================
    
    // Get hash value from field
    uint32_t* hash_ptr = S_EXPR_GET_FIELD(inst, &params[0], uint32_t);
    if (!hash_ptr) {
        EXCEPTION("se_hash_dispatch: field not found");
        return SE_CONTINUE;
    }
    
    uint32_t hash_val = *hash_ptr;
    printf("hash_val: %x\n", hash_val);
    
    // Find dictionary (skip field_ref)
    uint16_t dict_idx = s_expr_skip_param(params, 0);
    if (dict_idx >= param_count) {
        return SE_CONTINUE;
    }
    
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
            // No match and no default - terminate previous if any
            if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
                const s_expr_param_t* prev_content = &params[prev_action_idx];
                uint16_t prev_count;
                s_expr_key_contents(prev_content - 1, &prev_count);
                s_expr_children_terminate_all(inst, prev_content, prev_count);
                s_expr_set_user_flags(inst, 0xFFFF);
            }
            return SE_CONTINUE;
        }
    }
    
    uint16_t action_idx = (uint16_t)(key_content - params);
    uint16_t content_count;
    s_expr_key_contents(key_content - 1, &content_count);
    
    // =========================================================================
    // Handle branch change: terminate old, reset new so it can re-init
    // =========================================================================
    if (action_idx != prev_action_idx) {
        // Terminate previous branch children
        if (prev_action_idx > 0 && prev_action_idx != 0xFFFF) {
            const s_expr_param_t* prev_content = &params[prev_action_idx];
            uint16_t prev_count;
            s_expr_key_contents(prev_content - 1, &prev_count);
            s_expr_children_terminate_all(inst, prev_content, prev_count);
        }
        
        // Reset NEW branch children so they will receive INIT on invoke
        s_expr_children_reset_all(inst, key_content, content_count);
        
        s_expr_set_user_flags(inst, action_idx);
    }
    
    // =========================================================================
    // Execute children in selected branch
    // =========================================================================
    uint16_t child_count = s_expr_child_count(key_content, content_count);
    s_expr_result_t result = SE_CONTINUE;
    
    for (uint16_t i = 0; i < child_count; i++) {
        result = s_expr_child_invoke(inst, key_content, content_count, i);
        
        // Stop on non-continue results (except DISABLE which child_invoke handles)
        if (result != SE_CONTINUE) {
            break;
        }
    }
    
    printf("result: %d\n", result);
    return result;
}
// Terminate action at physical index


/// ============================================================================
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
    uint8_t opcode = params[phys_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_CALL) {
        return;
    }
    
    const s_expr_param_t* func_param = &params[phys_idx + 1];
    uint16_t node_idx = func_param->node_index;
    
    if (node_idx >= inst->node_count) {
        return;
    }
    
    s_expr_node_state_t* state = &inst->node_states[node_idx];
    uint8_t ever_init = state->flags & S_EXPR_NODE_FLAG_EVER_INIT;
    state->flags = S_EXPR_NODE_FLAG_ACTIVE | ever_init;
    state->state = 0;
    state->user_data = 0;
}

// ============================================================================
// se_named_state_machine
// params: [field_ref] [OPEN_DICT states...]
// Field contains state hash
// user_flags stores physical index of current action (0 = none)
// ============================================================================

static s_expr_result_t se_named_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Structure:
    // Child 0: field_ref (state hash variable) - not callable
    // Child 1: OPEN_DICT containing state->action mappings
    //   Key hashes are state names, values are actions
    
    uint16_t prev_action_phys_idx = s_expr_get_user_flags(inst);
    
    // =========================================================================
    // TERMINATE: Clean up current action
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_phys_idx > 0 && prev_action_phys_idx < param_count) {
            terminate_action_at_index(inst, params, prev_action_phys_idx);
        }
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // INIT: Validate structure
    // =========================================================================
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
    
    // =========================================================================
    // TICK: Get state hash from field and dispatch
    // =========================================================================
    
    // Get state hash from blackboard field
    uint32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], uint32_t);
    if (!state_ptr) {
        return SE_CONTINUE;
    }
    uint32_t state_hash = *state_ptr;
    
    // Find dictionary (child 1)
    uint16_t dict_phys_idx = s_expr_child_index(params, param_count, 1);
    if (dict_phys_idx == UINT16_MAX || dict_phys_idx >= param_count) {
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[dict_phys_idx].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_named_state_machine: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up state in dictionary
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[dict_phys_idx], state_hash);
    
    if (!key_content) {
        // State not found - terminate previous action if any
        if (prev_action_phys_idx > 0) {
            terminate_action_at_index(inst, params, prev_action_phys_idx);
            s_expr_set_user_flags(inst, 0);
        }
        return SE_CONTINUE;
    }
    
    // Calculate physical index of action within full params array
    uint16_t action_phys_idx = (uint16_t)(key_content - params);
    
    // Handle state transition
    if (action_phys_idx != prev_action_phys_idx) {
        // Terminate previous action
        if (prev_action_phys_idx > 0) {
            terminate_action_at_index(inst, params, prev_action_phys_idx);
        }
        // Reset new action so it gets INIT on first invoke
        reset_action_at_index(inst, params, action_phys_idx);
        s_expr_set_user_flags(inst, action_phys_idx);
    }
    
    // Invoke current state action
    // key_content points to first param after OPEN_KEY
    // We need to invoke the action at that position
    return s_expr_invoke_any(inst, params, action_phys_idx);
}

// SE_NAMED_EVENT_DISPATCH - event dispatch with string event names
// params: [OPEN_DICT events...]
// event_data points to uint32_t hash of the event name
// Handlers run fresh each time (terminate, reset, invoke)
static s_expr_result_t se_named_event_dispatch(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id;
    
    // Structure:
    // Child 0: OPEN_DICT containing event_hash -> action mappings
    //   Key hashes are event names, values are handler actions
    
    if (event_type == SE_EVENT_TERMINATE) {
        // Terminate all handlers in dictionary
        if (param_count < 1) return SE_CONTINUE;
        
        uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
        if (opcode == S_EXPR_PARAM_OPEN_DICT) {
            uint16_t dict_end = params[0].brace_idx;
            // Walk dictionary and terminate any initialized actions
            for (uint16_t idx = 1; idx < dict_end; ) {
                uint8_t op = params[idx].type & S_EXPR_OPCODE_MASK;
                if (op == S_EXPR_PARAM_OPEN_KEY) {
                    // Skip key hash, find action inside
                    uint16_t key_end = idx + params[idx].brace_idx;
                    for (uint16_t action_idx = idx + 1; action_idx < key_end; ) {
                        uint8_t action_op = params[action_idx].type & S_EXPR_OPCODE_MASK;
                        if (action_op == S_EXPR_PARAM_OPEN_CALL) {
                            terminate_action_at_index(inst, params, action_idx);
                        }
                        action_idx = s_expr_skip_param(params, action_idx);
                    }
                    idx = key_end + 1;
                } else {
                    idx = s_expr_skip_param(params, idx);
                }
            }
        }
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    // TICK: Look up event by hash from event_data
    if (param_count < 1) return SE_CONTINUE;
    
    // Get hash from event_data
    if (!event_data) {
        return SE_CONTINUE;  // No event hash provided
    }
    uint32_t event_hash = *(uint32_t*)event_data;
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_OPEN_DICT) {
        EXCEPTION("se_named_event_dispatch: expected OPEN_DICT");
        return SE_CONTINUE;
    }
    
    // Look up event by hash
    const s_expr_param_t* key_content = s_expr_dict_find_key(&params[0], event_hash);
    
    if (!key_content) {
        return SE_CONTINUE;  // No handler for this event
    }
    
    // Get action physical index
    uint16_t action_phys_idx = (uint16_t)(key_content - params);
    
    // Event handlers run fresh each time: terminate, reset, invoke
    terminate_action_at_index(inst, params, action_phys_idx);
    reset_action_at_index(inst, params, action_phys_idx);
    
    return s_expr_invoke_any(inst, params, action_phys_idx);
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
    return SE_PIPELINE_DISABLE;
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

static s_expr_result_t se_return_pipeline_terminate(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_TERMINATE;
}

static s_expr_result_t se_return_pipeline_reset_continue(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_RESET_CONTINUE;
}

static s_expr_result_t se_return_pipeline_reset_halt(
    s_expr_tree_instance_t* inst, const s_expr_param_t* params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void* event_data
) {
    (void)inst; (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
   
    return SE_PIPELINE_RESET_HALT;
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
// ============================================================================
// SE_SEQUENCE
// Execute children one at a time, advancing when each completes (SE_DISABLE)
// Returns SE_HALT while working, SE_DISABLE when all complete
// Fatal codes (TERMINATE, RESET, FUNCTION_TERMINATE, FUNCTION_RESET) propagate
// ============================================================================

static s_expr_result_t se_sequence(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    uint8_t state = s_expr_get_state(inst);
    uint16_t child_count = s_expr_child_count(params, param_count);
    
    // =========================================================================
    // TERMINATE: Clean up current child
    // =========================================================================
    if (event_type == SE_EVENT_TERMINATE) {
        if (state < child_count) {
            if (s_expr_child_is_initialized(inst, params, param_count, state)) {
                s_expr_child_terminate(inst, params, param_count, state);
            }
        }
        s_expr_set_state(inst, 0);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // INIT: Start at first child
    // =========================================================================
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_state(inst, 0);
        return SE_CONTINUE;
    }
    
    // =========================================================================
    // TICK: Execute current child, advance on SE_DISABLE
    // =========================================================================
    while (state < child_count) {
        // Skip non-callable children
        if (!s_expr_child_is_callable(params, param_count, state)) {
            state++;
            s_expr_set_state(inst, state);
            continue;
        }
        
        // Invoke current child
        uint16_t phys_idx = s_expr_child_index(params, param_count, state);
        if (phys_idx == UINT16_MAX) {
            state++;
            s_expr_set_state(inst, state);
            continue;
        }
        
        s_expr_result_t result = s_expr_invoke_any(inst, params, phys_idx);
       // printf("se_sequence: child %d result=%d\n", state, result);
        switch (result) {
            case SE_CONTINUE:
            case SE_DISABLE:
            case SE_PIPELINE_DISABLE:
            case SE_PIPELINE_TERMINATE:
            case SE_PIPELINE_RESET_CONTINUE:
            case SE_PIPELINE_RESET_HALT:
                // Child complete - terminate it and advance
                s_expr_child_terminate(inst, params, param_count, state);
                s_expr_child_reset(inst, params, param_count, state);
                state++;
                s_expr_set_state(inst, state);
                continue;  // Try next child same tick
            
            default:
                return result;
                // Fatal - propagate
        
          
        }
    }
    
    // All children complete
    return SE_DISABLE;
}

static s_expr_result_t se_fork(
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
    
    uint16_t count = s_expr_child_count(params, param_count);
    
    // Invoke all active children
    for (uint16_t i = 0; i < count; i++) {
        bool callable = s_expr_child_is_callable(params, param_count, i);
        bool active = s_expr_child_is_active(inst, params, param_count, i);
 
        if (!callable || !active) {
            continue;
        }
        
        s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, i);
      
        // Propagate fatal results immediately
        switch (result) {
            case SE_TERMINATE:
            case SE_RESET:
            case SE_FUNCTION_TERMINATE:
            case SE_FUNCTION_RESET:
                return result;
            default:
                printf("se_fork: child %d result=%d\n", i, result);
                break;
        }
    }
    
    // Recount active children AFTER all have run
    uint16_t active_count = 0;
    for (uint16_t i = 0; i < count; i++) {
        
       
        if (s_expr_child_is_callable(params, param_count, i) &&
            s_expr_child_is_active(inst, params, param_count, i)) {
            active_count++;
        }
    }
    
    if (active_count == 0) {
        return SE_PIPELINE_DISABLE;
    }
    
    return SE_CONTINUE;
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
    (void)event_id; 
    (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    uint16_t count = s_expr_child_count(params, param_count);
    
    // Invoke all active children
    for (uint16_t i = 0; i < count; i++) {
        bool callable = s_expr_child_is_callable(params, param_count, i);
        bool active = s_expr_child_is_active(inst, params, param_count, i);
        
        if (!callable || !active) {
            continue;
        }
        
        s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, i);
        
        // Propagate fatal results immediately
        switch (result) {
            case SE_TERMINATE:
            case SE_RESET:
            case SE_FUNCTION_TERMINATE:
            case SE_FUNCTION_RESET:
                return result;
            default:
                break;
        }
    }
    
    // Recount active children AFTER all have run
    uint16_t active_count = 0;
    for (uint16_t i = 0; i < count; i++) {
        if (s_expr_child_is_callable(params, param_count, i) &&
            s_expr_child_is_active(inst, params, param_count, i)) {
            active_count++;
        }
    }
    
    if (active_count == 0) {
        return SE_CONTINUE;  // Changed from SE_PIPELINE_DISABLE
    }
    
    return SE_FUNCTION_HALT;
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
    (void)event_id; 
    (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    uint16_t count = s_expr_child_count(params, param_count);
    uint16_t active_count = 0;
    
    for (uint16_t i = 0; i < count; i++) {
        bool callable = s_expr_child_is_callable(params, param_count, i);
        bool active = s_expr_child_is_active(inst, params, param_count, i);
        
        if (!callable) {
            continue;
        }
        
        if (!active) {
            continue;
        }
        
        active_count++;
        
        // Use s_expr_invoke_any directly to see actual result
        uint16_t phys_idx = s_expr_child_index(params, param_count, i);
        if (phys_idx == UINT16_MAX) {
            continue;
        }
        
        s_expr_result_t result = s_expr_invoke_any(inst, params, phys_idx);
        
        switch (result) {
            case SE_FUNCTION_RESET:
                s_expr_child_terminate(inst, params, param_count, i);
                s_expr_child_reset(inst, params, param_count, i);
                continue;
            
            case SE_FUNCTION_TERMINATE:
                s_expr_child_terminate(inst, params, param_count, i);
                continue;
            
            case SE_CONTINUE:
                continue;
            
            case SE_DISABLE:
              
                return SE_PIPELINE_DISABLE;
            
            default:
                
                return result;
        }
    }
    
    if (active_count == 0) {
        return SE_PIPELINE_DISABLE;
    }
    
    return SE_CONTINUE;
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



#if 0

// obsolete functions.
// SE_STATE_ACTIONS - container that executes all children in sequence
// Used to group multiple actions for a single state
static s_expr_result_t se_state_actions(
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
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    // TICK: invoke all children in sequence
    uint16_t count = s_expr_child_count(params, param_count);
    
    for (uint16_t i = 0; i < count; i++) {
        if (!s_expr_child_is_callable(params, param_count, i)) {
            continue;
        }
        
        if (!s_expr_child_is_active(inst, params, param_count, i)) {
            continue;
        }
        
        s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, i);
        
        if (result != SE_CONTINUE) {
            return result;
        }
    }
    
    return SE_CONTINUE;
}

// SE_STATE_MACHINE - state machine with integer state index
// params: [field_ref] [action0] [action1] [action2] ...
// Field contains state index (0, 1, 2, ...)
// user_flags stores physical index of current action (0 = none)
static s_expr_result_t se_state_machine(
    s_expr_tree_instance_t* inst,
    const s_expr_param_t* params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void* event_data
) {
    (void)event_id; (void)event_data;
    
    // Structure:
    // Child 0: field_ref (state index variable) - not callable
    // Child 1+: actions indexed by state value
    //   state=0 -> child 1
    //   state=1 -> child 2
    //   etc.
    
    if (param_count < 1){
        EXCEPTION("se_state_machine: need at least one parameter");
        return SE_CONTINUE;
    }
    
    uint8_t opcode = params[0].type & S_EXPR_OPCODE_MASK;
    if (opcode != S_EXPR_PARAM_FIELD) {
        EXCEPTION("se_state_machine: first param must be field_ref");
        return SE_CONTINUE;
    }
    
    uint16_t prev_action_phys_idx = s_expr_get_user_flags(inst);
    
    if (event_type == SE_EVENT_TERMINATE) {
        if (prev_action_phys_idx > 0) {
            terminate_action_at_index(inst, params, prev_action_phys_idx);
        }
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        s_expr_set_user_flags(inst, 0);
        return SE_CONTINUE;
    }
    
    // TICK: Get state index from field
    int32_t* state_ptr = S_EXPR_GET_FIELD(inst, &params[0], int32_t);
    if (!state_ptr){
        EXCEPTION("se_state_machine: field not found");
        return SE_CONTINUE;
    }
    
    int32_t state = *state_ptr;
    if (state < 0){
        EXCEPTION("se_state_machine: state is negative");
        return SE_CONTINUE;
    }
    
    // Find action for this state
    // state 0 -> logical child 1, state 1 -> logical child 2, etc.
    uint16_t action_logical_idx = (uint16_t)(state + 1);
    uint16_t action_phys_idx = s_expr_child_index(params, param_count, action_logical_idx);
    
    if (action_phys_idx == UINT16_MAX) {
        EXCEPTION("se_state_machine: state out of range");
        return SE_CONTINUE;

    }
    
    // Handle state transition
    if (action_phys_idx != prev_action_phys_idx) {
        // Terminate previous action
        if (prev_action_phys_idx > 0) {
            terminate_action_at_index(inst, params, prev_action_phys_idx);
            reset_action_at_index(inst, params, prev_action_phys_idx);
        }
        // Reset new action (invoke will handle INIT)
        reset_action_at_index(inst, params, action_phys_idx);
        s_expr_set_user_flags(inst, action_phys_idx);
    }
    
    // Invoke current state action
    return s_expr_invoke_any(inst, params, action_phys_idx);
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
    (void)event_id; 
    (void)event_data;
    
    if (event_type == SE_EVENT_TERMINATE) {
        s_expr_children_terminate_all(inst, params, param_count);
        return SE_CONTINUE;
    }
    
    if (event_type == SE_EVENT_INIT) {
        return SE_CONTINUE;
    }
    
    uint16_t count = s_expr_child_count(params, param_count);
    uint16_t active_count = 0;
    
    for (uint16_t i = 0; i < count; i++) {
        bool callable = s_expr_child_is_callable(params, param_count, i);
        bool active = s_expr_child_is_active(inst, params, param_count, i);
        
        if (!callable) {
            continue;
        }
        
        if (!active) {
            continue;
        }
        
        active_count++;
        
        s_expr_result_t result = s_expr_child_invoke(inst, params, param_count, i);
        
        // SE_PIPELINE_DISABLE is the signal from se_return_disable()
        // Convert it to SE_DISABLE for our parent
        if (result == SE_PIPELINE_DISABLE) {
            return SE_DISABLE;
        }
        
        if (result != SE_CONTINUE) {
            return result;
        }
    }
    
    if (active_count == 0) {
        return SE_DISABLE;
    }
    
    return SE_CONTINUE;
}
#endif