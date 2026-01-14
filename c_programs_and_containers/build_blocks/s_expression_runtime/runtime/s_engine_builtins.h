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
// BUILTIN FUNCTION HASHES (for reference)
// ============================================================================

// Predicates
#define SE_PRED_AND_HASH      0xA0B17002
#define SE_PRED_OR_HASH       0x28413C60
#define SE_PRED_NOT_HASH      0x2D165022
#define SE_PRED_NOR_HASH      0x2B964DC6
#define SE_PRED_NAND_HASH     0xCA1E2562
#define SE_PRED_XOR_HASH      0x13A71924
#define SE_TRUE_HASH          0xF1B76220
#define SE_FALSE_HASH         0x11057CC6
#define SE_CHECK_EVENT_HASH   0x84401D18

// Main functions
#define SE_PIPELINE_HASH          0xA5C6D765
#define SE_TICK_DELAY_HASH        0x7993CB40
#define SE_TIME_DELAY_HASH        0xBCB79496
#define SE_WAIT_EVENT_HASH        0x9D1FAA68
#define SE_NOP_HASH               0x5E060980
#define SE_IF_THEN_ELSE_HASH      0xA6284BB4
#define SE_TRIGGER_ON_CHANGE_HASH 0x3CB1927D
#define SE_STATE_MACHINE_HASH     0x089EE5C4
#define SE_STATE_ACTIONS_HASH     0x07F1C252
#define SE_FIELD_DISPATCH_HASH    0x05A99203
#define SE_EVENT_DISPATCH_HASH    0x6F3FBC5A
#define SE_DISPATCH_HASH          0x12345678  // TODO: compute actual hash

// Oneshots
#define SE_LOG_HASH               0xF8B9F88C

#ifdef __cplusplus
}
#endif

#endif // S_ENGINE_BUILTINS_H