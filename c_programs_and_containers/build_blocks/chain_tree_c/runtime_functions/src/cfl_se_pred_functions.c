/*
 * cfl_se_pred_functions.c - ChainTree bridge predicate functions for S-Engine
 *
 * Predicates that read ChainTree runtime bitmask. The CFL_S_BIT_* functions
 * mix bit-index parameters (read from runtime->bitmask) with nested
 * s-engine predicates in a single expression — the builtin se_pred_*
 * functions only compose predicates, they can't read bitmask bits.
 */

#include "cfl_se_pred_functions.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

/* ====================================================================
 * CFL_READ_BIT: Read a single bit from runtime bitmask
 * Params: [0] = bit index (INT/UINT, 0-63)
 * ==================================================================== */

static bool cfl_read_bit_pred(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_READ_BIT: requires 1 parameter"); return false; }
    uint8_t t = params[0].type & S_EXPR_OPCODE_MASK;
    if (t != S_EXPR_PARAM_INT && t != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_READ_BIT: param[0] must be INT or UINT"); return false;
    }
    uint32_t bit = (uint32_t)s_expr_param_uint(&params[0]);
    if (bit >= 64) { EXCEPTION("CFL_READ_BIT: bit index out of range"); return false; }

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)s_expr_tree_get_user_ctx(inst);
    if (!rt) { EXCEPTION("CFL_READ_BIT: no runtime handle"); return false; }

    return (rt->bitmask & (1ULL << bit)) != 0;
}

/* ====================================================================
 * Shared bit-operation evaluator
 *
 * Each parameter is either:
 *   INT/UINT → bit index into runtime->bitmask
 *   PRED/OPEN_CALL → nested s-engine predicate
 * ==================================================================== */

typedef enum {
    BIT_OP_OR,
    BIT_OP_AND,
    BIT_OP_NOR,
    BIT_OP_NAND,
    BIT_OP_XOR
} bit_op_mode_t;

static inline bool eval_one(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t idx,
    uint64_t bitmask)
{
    uint8_t op = params[idx].type & S_EXPR_OPCODE_MASK;
    if (op == S_EXPR_PARAM_INT || op == S_EXPR_PARAM_UINT) {
        uint32_t bit = (uint32_t)s_expr_param_uint(&params[idx]);
        return (bitmask & (1ULL << bit)) != 0;
    }
    if (op == S_EXPR_PARAM_PRED || op == S_EXPR_PARAM_OPEN_CALL) {
        return s_expr_invoke_pred(inst, params, idx);
    }
    EXCEPTION("cfl_s_bit: invalid parameter type");
    return false;
}

static bool bit_eval(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    uint64_t bitmask, bit_op_mode_t mode)
{
    bool xor_acc = false;
    for (uint16_t i = 0; i < param_count; ) {
        bool v = eval_one(inst, params, i, bitmask);
        switch (mode) {
            case BIT_OP_OR:   if (v)  return true;  break;
            case BIT_OP_NOR:  if (v)  return false; break;
            case BIT_OP_AND:  if (!v) return false;  break;
            case BIT_OP_NAND: if (!v) return true;  break;
            case BIT_OP_XOR:  xor_acc ^= v;         break;
        }
        i = s_expr_skip_param(params, i);
    }
    switch (mode) {
        case BIT_OP_OR:   return false;
        case BIT_OP_NOR:  return true;
        case BIT_OP_AND:  return true;
        case BIT_OP_NAND: return false;
        case BIT_OP_XOR:  return xor_acc;
    }
    return false;
}

/* ====================================================================
 * CFL_S_BIT_OR / AND / NOR / NAND / XOR
 * ==================================================================== */

#define DEFINE_BIT_PRED(name, mode)                                         \
static bool name(                                                           \
    s_expr_tree_instance_t *inst,                                           \
    const s_expr_param_t *params, uint16_t param_count,                     \
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)    \
{                                                                           \
    (void)event_id; (void)event_data;                                       \
    if (event_type == SE_EVENT_INIT || event_type == SE_EVENT_TERMINATE)     \
        return true;                                                        \
    cfl_runtime_handle_t *rt =                                              \
        (cfl_runtime_handle_t *)s_expr_tree_get_user_ctx(inst);             \
    if (!rt) { EXCEPTION(#name ": no runtime handle"); return false; }      \
    return bit_eval(inst, params, param_count, rt->bitmask, mode);          \
}

DEFINE_BIT_PRED(cfl_s_bit_or_pred,   BIT_OP_OR)
DEFINE_BIT_PRED(cfl_s_bit_and_pred,  BIT_OP_AND)
DEFINE_BIT_PRED(cfl_s_bit_nor_pred,  BIT_OP_NOR)
DEFINE_BIT_PRED(cfl_s_bit_nand_pred, BIT_OP_NAND)
DEFINE_BIT_PRED(cfl_s_bit_xor_pred,  BIT_OP_XOR)

/* ====================================================================
 * Function table
 * ==================================================================== */

static const s_expr_fn_entry_named_t cfl_pred_entries_named[] = {
    { "CFL_READ_BIT",   (void *)cfl_read_bit_pred },
    { "CFL_S_BIT_OR",   (void *)cfl_s_bit_or_pred },
    { "CFL_S_BIT_AND",  (void *)cfl_s_bit_and_pred },
    { "CFL_S_BIT_NOR",  (void *)cfl_s_bit_nor_pred },
    { "CFL_S_BIT_NAND", (void *)cfl_s_bit_nand_pred },
    { "CFL_S_BIT_XOR",  (void *)cfl_s_bit_xor_pred },
};

static s_expr_fn_entry_t cfl_pred_entries[ARRAY_COUNT(cfl_pred_entries_named)];
static s_expr_fn_table_t cfl_pred_table;

const s_expr_fn_table_t *cfl_se_get_pred_table(void) {
    if (cfl_pred_table.count == 0) {
        s_expr_build_fn_table(
            cfl_pred_entries_named,
            cfl_pred_entries,
            ARRAY_COUNT(cfl_pred_entries_named)
        );
        cfl_pred_table.entries = cfl_pred_entries;
        cfl_pred_table.count = ARRAY_COUNT(cfl_pred_entries);
    }
    return &cfl_pred_table;
}
