// ============================================================================
// s_engine_builtins.h
// Built-in S-Expression Engine Functions
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
// BUILTIN FUNCTION HASHES (uppercase names)
// ============================================================================

// Predicates
#define SE_PRED_AND_HASH      0x7C0DF5F3
#define SE_PRED_OR_HASH       0x0CF6212F
#define SE_PRED_NOT_HASH      0x217DEB8F
#define SE_PRED_NOR_HASH      0x1F7DE869
#define SE_PRED_NAND_HASH     0x067DC775
#define SE_PRED_XOR_HASH      0xB713B6A3
#define SE_TRUE_HASH          0x0C125BC2
#define SE_FALSE_HASH         0x77C35775
#define SE_CHECK_EVENT_HASH   0x80659F81

// Main functions
#define SE_PIPELINE_HASH          0x4D6E2B18
#define SE_TICK_DELAY_HASH        0x0C3460EB
#define SE_TIME_DELAY_HASH        0xA60CE767
#define SE_WAIT_EVENT_HASH        0xAD4917EC
#define SE_NOP_HASH               0x080C2B37
#define SE_IF_THEN_ELSE_HASH      0x1E860193
#define SE_TRIGGER_ON_CHANGE_HASH 0x8374277F
#define SE_STATE_MACHINE_HASH     0x5EEDA8E9
#define SE_STATE_ACTIONS_HASH     0x25308B8F
#define SE_FIELD_DISPATCH_HASH    0xA1C11B35
#define SE_EVENT_DISPATCH_HASH    0xF3EDFC75
#define SE_DISPATCH_HASH          0xE67DDA18

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
#define SE_LOG_HASH               0xCEBBEFA4

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_BUILTINS_H