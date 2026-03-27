/*
 * cfl_se_main_functions.c - ChainTree bridge main functions for S-Engine
 *
 * Only functions that require ChainTree runtime access belong here.
 * All pure s-engine composites (delays, dispatches, state machines, etc.)
 * are handled by the s-engine builtins layer.
 */

#include "cfl_se_main_functions.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include "s_engine_module.h"
#include "s_engine_eval.h"

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

/* ====================================================================
 * CFL_WAIT_CHILD_DISABLED: Wait until a ChainTree child node is disabled
 * Params: [0] = child_node_index (INT/UINT)
 *
 * Returns:
 *   SE_FUNCTION_HALT while child is enabled
 *   SE_DISABLE when child becomes disabled
 * ==================================================================== */

static s_expr_result_t cfl_wait_child_disabled_main(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params,
    uint16_t param_count,
    s_expr_event_type_t event_type,
    uint16_t event_id,
    void *event_data)
{
    (void)event_id; (void)event_data;

    if (event_type == SE_EVENT_INIT) {
        if (param_count < 1) {
            EXCEPTION("CFL_WAIT_CHILD_DISABLED: requires child_node_index");
            return SE_DISABLE;
        }
        uint8_t t = params[0].type & S_EXPR_OPCODE_MASK;
        if (t != S_EXPR_PARAM_INT && t != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_WAIT_CHILD_DISABLED: param[0] must be INT or UINT");
            return SE_DISABLE;
        }
        return SE_CONTINUE;
    }

    if (event_type == SE_EVENT_TERMINATE) {
        return SE_CONTINUE;
    }

    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)s_expr_tree_get_user_ctx(inst);
    if (!rt) {
        EXCEPTION("CFL_WAIT_CHILD_DISABLED: no runtime handle");
        return SE_TERMINATE;
    }

    uint16_t child_index = (uint16_t)s_expr_param_uint(&params[0]);

    if (cfl_child_is_enabled(rt, inst->ct_node_id, child_index)) {
        return SE_FUNCTION_HALT;
    }

    return SE_DISABLE;
}

/* ====================================================================
 * Function table
 * ==================================================================== */

static const s_expr_fn_entry_named_t cfl_main_entries_named[] = {
    { "CFL_WAIT_CHILD_DISABLED", (void *)cfl_wait_child_disabled_main },
};

static s_expr_fn_entry_t cfl_main_entries[ARRAY_COUNT(cfl_main_entries_named)];
static s_expr_fn_table_t cfl_main_table;

const s_expr_fn_table_t *cfl_se_get_main_table(void) {
    if (cfl_main_table.count == 0) {
        s_expr_build_fn_table(
            cfl_main_entries_named,
            cfl_main_entries,
            ARRAY_COUNT(cfl_main_entries_named)
        );
        cfl_main_table.entries = cfl_main_entries;
        cfl_main_table.count = ARRAY_COUNT(cfl_main_entries);
    }
    return &cfl_main_table;
}
