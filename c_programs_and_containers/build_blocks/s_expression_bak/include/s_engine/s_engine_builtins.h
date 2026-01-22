// ============================================================================
// s_engine_builtins.h
// Built-in S-Expression Engine Functions - Version 5.2
// ============================================================================

#ifndef S_ENGINE_BUILTINS_H
#define S_ENGINE_BUILTINS_H

#include "s_engine_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// BUILTIN FUNCTION TABLES
// Returns NULL if no functions of that type
// ============================================================================

const s_expr_fn_table_t* s_engine_builtin_oneshot_table(void);
const s_expr_fn_table_t* s_engine_builtin_main_table(void);
const s_expr_fn_table_t* s_engine_builtin_pred_table(void);

// ============================================================================
// DICTIONARY NAVIGATION HELPERS
// ============================================================================

// Find a key in a dictionary by hash value
// Returns pointer to OPEN_KEY param, or NULL if not found
// dict_param should point to OPEN_DICT
const s_expr_param_t* s_expr_dict_find_key(
    const s_expr_param_t* dict_param,
    uint32_t key_hash
);

// Get the contents of a dictionary key (between OPEN_KEY and CLOSE_KEY)
// key_param should point to OPEN_KEY
// Returns pointer to first content param, sets content_count
const s_expr_param_t* s_expr_key_contents(
    const s_expr_param_t* key_param,
    uint16_t* content_count
);

// Compute FNV-1a 32-bit hash (matches Lua implementation)
uint32_t s_expr_fnv1a_hash(const char* str);

// ============================================================================
// BUILTIN FUNCTION HASHES (uppercase names)
// ============================================================================

// Predicates
#define SE_PRED_AND_HASH          0x7C0DF5F3
#define SE_PRED_OR_HASH           0x0CF6212F
#define SE_PRED_NOT_HASH          0x217DEB8F
#define SE_PRED_NOR_HASH          0x1F7DE869
#define SE_PRED_NAND_HASH         0x067DC775
#define SE_PRED_XOR_HASH          0xB713B6A3
#define SE_TRUE_HASH              0x0C125BC2
#define SE_FALSE_HASH             0x77C35775
#define SE_CHECK_EVENT_HASH       0x80659F81
#define SE_CHECK_NAMED_EVENT_HASH 0x542BD82B
#define SE_LESS_THAN_INT_HASH     0xBFE88BCD
#define SE_GREATER_EQUAL_INT_HASH 0xBB057075

// Main functions
#define SE_PIPELINE_HASH              0x4D6E2B18
#define SE_TICK_DELAY_HASH            0x0C3460EB
#define SE_TIME_DELAY_HASH            0xA60CE767
#define SE_WAIT_EVENT_HASH            0xAD4917EC
#define SE_NOP_HASH                   0x080C2B37
#define SE_IF_THEN_ELSE_HASH          0x1E860193
#define SE_TRIGGER_ON_CHANGE_HASH     0x8374277F
#define SE_STATE_MACHINE_HASH         0x5EEDA8E9
#define SE_STATE_ACTIONS_HASH         0x25308B8F
#define SE_FIELD_DISPATCH_HASH        0xA1C11B35
#define SE_EVENT_DISPATCH_HASH        0xF3EDFC75
#define SE_DISPATCH_HASH              0xE67DDA18
#define SE_SEQUENCE_HASH              0xEC3EE7BF
#define SE_FORK_HASH                  0x0A24332A
#define SE_FORK_JOIN_HASH             0xE404E1CF
#define SE_CHAIN_FLOW_HASH            0xFFC1FAA4  
#define SE_FOR_HASH                   0xA11A2225
#define SE_WHILE_HASH                 0xA08B6DD3  
// NEW v5.2: Dictionary-based dispatch functions
#define SE_STRING_DISPATCH_HASH       0x6A3A4922
#define SE_HASH_DISPATCH_HASH         0xDE8E6F0D
#define SE_NAMED_STATE_MACHINE_HASH   0x876B8A33
#define SE_NAMED_EVENT_DISPATCH_HASH  0x5E99A787

// Result code functions
#define SE_RETURN_CONTINUE_HASH           0xB4243714
#define SE_RETURN_TERMINATE_HASH          0xDFE64C74
#define SE_RETURN_RESET_HASH              0x70EAA030
#define SE_RETURN_DISABLE_HASH            0x02C11A13
#define SE_RETURN_HALT_HASH               0x056FB9EA
#define SE_RETURN_SKIP_CONTINUE_HASH      0xEAE5524E
#define SE_RETURN_FUNCTION_HALT_HASH      0x891F0675
#define SE_RETURN_FUNCTION_RESET_HASH     0xF6027E85
#define SE_RETURN_FUNCTION_TERMINATE_HASH 0x0A5B8A85

// Oneshots
#define SE_LOG_HASH                   0xCEBBEFA4
#define SE_LOG_INT_HASH               0x2442CEA2
#define SE_LOG_FLOAT_HASH             0xA8949A19
#define SE_LOG_FIELD_HASH             0xBA2925AB
#define SE_SET_HASH_HASH              0xEF5AD4AB

// Field operations (oneshots)
#define SE_SET_FIELD_HASH             0xFFF84A15
#define SE_SET_FIELD_FLOAT_HASH       0x42345454
#define SE_INC_FIELD_HASH             0x09391555
#define SE_DEC_FIELD_HASH             0xC3053EA5

// Field comparison predicates
#define SE_FIELD_EQ_HASH              0xD9FBED0D
#define SE_FIELD_NE_HASH              0xF5E4BFD2
#define SE_FIELD_GT_HASH              0x10F63374
#define SE_FIELD_GE_HASH              0x01F61BD7
#define SE_FIELD_LT_HASH              0xD2EA98E7
#define SE_FIELD_LE_HASH              0xC1EA7E24
#define SE_FIELD_IN_RANGE_HASH        0x7BC1968E

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_BUILTINS_H