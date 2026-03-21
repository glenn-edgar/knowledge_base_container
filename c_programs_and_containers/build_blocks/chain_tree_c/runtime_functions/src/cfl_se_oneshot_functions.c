/*
 * cfl_se_oneshot_functions.c - ChainTree bridge oneshot functions for S-Engine
 *
 * S-engine oneshot functions that bridge back into ChainTree operations:
 * node enable/disable, CFL event queue, bitmask, JSON decode, constants.
 *
 * All functions retrieve cfl_runtime_handle_t* from inst->user_ctx.
 */

#include "cfl_se_oneshot_functions.h"
#include "cfl_runtime.h"
#include "cfl_engine.h"
#include "cfl_common_functions.h"
#include "s_engine_module.h"
#include "json_node_decoder.h"
#include <stdio.h>
#include <string.h>

#define ARRAY_COUNT(arr) (sizeof(arr) / sizeof((arr)[0]))

/* Helper: get runtime handle from tree instance user_ctx */
static cfl_runtime_handle_t *get_rt(s_expr_tree_instance_t *inst, const char *caller) {
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)s_expr_tree_get_user_ctx(inst);
    if (!rt) {
        EXCEPTION(caller);
    }
    return rt;
}

/* ====================================================================
 * CFL_LOG: Print timestamp, node ID, and string message
 * Params: [0] = STR_IDX
 * ==================================================================== */

static void cfl_log_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_LOG: requires 1 parameter"); return; }
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) {
        EXCEPTION("CFL_LOG: param[0] must be STR_IDX"); return;
    }

    const char *message = s_expr_get_string(inst, &params[0]);
    if (!message) { EXCEPTION("CFL_LOG: failed to get string"); return; }

    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_LOG: no runtime handle");
    if (!rt) return;

    double timestamp = cfl_timer_get_timestamp(rt->timer_handle);
    printf("Timestamp: %f, Node ID: %u, Message: %s\n",
           timestamp, inst->ct_node_id, message);
}

/* ====================================================================
 * CFL_ENABLE_CHILDREN / CFL_DISABLE_CHILDREN
 * Params: none
 * ==================================================================== */

static void cfl_enable_children_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_ENABLE_CHILDREN: no runtime handle");
    if (rt) cfl_enable_all_children(rt, inst->ct_node_id);
}

static void cfl_disable_children_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)params; (void)param_count;
    (void)event_type; (void)event_id; (void)event_data;
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_DISABLE_CHILDREN: no runtime handle");
    if (rt) cfl_disable_all_children(rt, inst->ct_node_id);
}

/* ====================================================================
 * CFL_ENABLE_CHILD / CFL_DISABLE_CHILD
 * Params: [0] = child_index (INT or UINT)
 * ==================================================================== */

static void cfl_enable_child_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_ENABLE_CHILD: requires 1 parameter"); return; }
    uint8_t t = params[0].type & S_EXPR_OPCODE_MASK;
    if (t != S_EXPR_PARAM_INT && t != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_ENABLE_CHILD: param[0] must be INT or UINT"); return;
    }
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_ENABLE_CHILD: no runtime handle");
    if (rt) cfl_enable_child(rt, inst->ct_node_id, (uint16_t)s_expr_param_uint(&params[0]));
}

static void cfl_disable_child_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_DISABLE_CHILD: requires 1 parameter"); return; }
    uint8_t t = params[0].type & S_EXPR_OPCODE_MASK;
    if (t != S_EXPR_PARAM_INT && t != S_EXPR_PARAM_UINT) {
        EXCEPTION("CFL_DISABLE_CHILD: param[0] must be INT or UINT"); return;
    }
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_DISABLE_CHILD: no runtime handle");
    if (rt) cfl_disable_child(rt, inst->ct_node_id, (uint16_t)s_expr_param_uint(&params[0]));
}

/* ====================================================================
 * CFL_INTERNAL_EVENT: Post event to ChainTree event queue
 * Params: [0] = event_type (INT/UINT), [1] = event_data (INT/UINT)
 * Skips on INIT and TERMINATE — only fires on TICK.
 * ==================================================================== */

static void cfl_internal_event_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_id; (void)event_data;
    if (event_type != SE_EVENT_TICK) return;
    if (param_count != 2) {
        EXCEPTION("CFL_INTERNAL_EVENT: requires 2 parameters"); return;
    }
    uint8_t t0 = params[0].type & S_EXPR_OPCODE_MASK;
    uint8_t t1 = params[1].type & S_EXPR_OPCODE_MASK;
    if ((t0 != S_EXPR_PARAM_INT && t0 != S_EXPR_PARAM_UINT) ||
        (t1 != S_EXPR_PARAM_INT && t1 != S_EXPR_PARAM_UINT)) {
        EXCEPTION("CFL_INTERNAL_EVENT: params must be INT or UINT"); return;
    }
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_INTERNAL_EVENT: no runtime handle");
    if (!rt) return;

    cfl_send_integer_event(
        rt->event_queue,
        CFL_EVENT_PRIORITY_LOW,
        inst->ct_node_id,
        (unsigned)s_expr_param_int(&params[0]),
        (cfl_int_t)s_expr_param_int(&params[1])
    );
}

/* ====================================================================
 * CFL_EXCEPTION: Raise exception with string hash
 * Params: [0] = STR_HASH
 * ==================================================================== */

static void cfl_exception_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)inst; (void)event_type; (void)event_id; (void)event_data;
    if (param_count != 1) { EXCEPTION("CFL_EXCEPTION: requires 1 parameter"); return; }
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_HASH) {
        EXCEPTION("CFL_EXCEPTION: param[0] must be STR_HASH"); return;
    }
    EXCEPTION("CFL_EXCEPTION triggered");
}

/* ====================================================================
 * CFL_SET_BITS / CFL_CLEAR_BITS: Modify ChainTree shadow bitmask
 * Params: [0..N] = bit indices (INT/UINT), each 0..63
 * ==================================================================== */

static void cfl_set_bits_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_SET_BITS: requires at least 1 parameter"); return; }
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_SET_BITS: no runtime handle");
    if (!rt) return;

    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t t = params[i].type & S_EXPR_OPCODE_MASK;
        if (t != S_EXPR_PARAM_INT && t != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_SET_BITS: param must be INT or UINT"); return;
        }
        uint32_t bit = (uint32_t)s_expr_param_uint(&params[i]);
        if (bit >= 64) { EXCEPTION("CFL_SET_BITS: bit index out of range"); return; }
        rt->shaddow_bitmask |= (1ULL << bit);
    }
}

static void cfl_clear_bits_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_CLEAR_BITS: requires at least 1 parameter"); return; }
    cfl_runtime_handle_t *rt = get_rt(inst, "CFL_CLEAR_BITS: no runtime handle");
    if (!rt) return;

    for (uint16_t i = 0; i < param_count; i++) {
        uint8_t t = params[i].type & S_EXPR_OPCODE_MASK;
        if (t != S_EXPR_PARAM_INT && t != S_EXPR_PARAM_UINT) {
            EXCEPTION("CFL_CLEAR_BITS: param must be INT or UINT"); return;
        }
        uint32_t bit = (uint32_t)s_expr_param_uint(&params[i]);
        if (bit >= 64) { EXCEPTION("CFL_CLEAR_BITS: bit index out of range"); return; }
        rt->shaddow_bitmask &= ~(1ULL << bit);
    }
}

/* ====================================================================
 * JSON READ functions
 * All take: [0] = FIELD ref, [1] = STR_IDX (json_path)
 * ==================================================================== */

/* Shared validation for JSON read functions */
static bool json_read_validate(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    const char *caller,
    void **out_bb, const char **out_path, cfl_runtime_handle_t **out_rt)
{
    if (param_count < 2) { EXCEPTION(caller); return false; }
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) { EXCEPTION(caller); return false; }
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_STR_IDX) { EXCEPTION(caller); return false; }

    *out_bb = s_expr_tree_get_blackboard(inst);
    if (!*out_bb) { EXCEPTION(caller); return false; }

    *out_path = s_expr_get_string(inst, &params[1]);
    if (!*out_path) { EXCEPTION(caller); return false; }

    *out_rt = get_rt(inst, caller);
    if (!*out_rt) return false;

    json_decoder_init_from_runtime(*out_rt, inst->ct_node_id);
    return true;
}

static void cfl_json_read_int_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    void *bb; const char *path; cfl_runtime_handle_t *rt;
    if (!json_read_validate(inst, params, param_count, "CFL_JSON_READ_INT", &bb, &path, &rt)) return;

    int32_t temp = 0;
    json_extract_int32_runtime(rt, path, &temp);

    void *field = (uint8_t *)bb + params[0].field_offset;
    switch (params[0].field_size) {
        case 1: *(int8_t *)field  = (int8_t)temp;  break;
        case 2: *(int16_t *)field = (int16_t)temp;  break;
        case 4: *(int32_t *)field = (int32_t)temp;  break;
        case 8: *(int64_t *)field = (int64_t)temp;  break;
        default: EXCEPTION("CFL_JSON_READ_INT: unsupported field_size"); return;
    }
}

static void cfl_json_read_uint_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    void *bb; const char *path; cfl_runtime_handle_t *rt;
    if (!json_read_validate(inst, params, param_count, "CFL_JSON_READ_UINT", &bb, &path, &rt)) return;

    int32_t temp = 0;
    json_extract_int32_runtime(rt, path, &temp);

    void *field = (uint8_t *)bb + params[0].field_offset;
    uint64_t value = (uint64_t)temp;
    switch (params[0].field_size) {
        case 1: *(uint8_t *)field  = (uint8_t)value;  break;
        case 2: *(uint16_t *)field = (uint16_t)value;  break;
        case 4: *(uint32_t *)field = (uint32_t)value;  break;
        case 8: *(uint64_t *)field = (uint64_t)value;  break;
        default: EXCEPTION("CFL_JSON_READ_UINT: unsupported field_size"); return;
    }
}

static void cfl_json_read_float_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    void *bb; const char *path; cfl_runtime_handle_t *rt;
    if (!json_read_validate(inst, params, param_count, "CFL_JSON_READ_FLOAT", &bb, &path, &rt)) return;

    float value = 0.0f;
    json_extract_float32_runtime(rt, path, &value);
    *(float *)((uint8_t *)bb + params[0].field_offset) = value;
}

static void cfl_json_read_bool_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    void *bb; const char *path; cfl_runtime_handle_t *rt;
    if (!json_read_validate(inst, params, param_count, "CFL_JSON_READ_BOOL", &bb, &path, &rt)) return;

    bool value = false;
    json_extract_bool_runtime(rt, path, &value);
    *(bool *)((uint8_t *)bb + params[0].field_offset) = value;
}

static void cfl_json_read_string_ptr_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    void *bb; const char *path; cfl_runtime_handle_t *rt;
    if (!json_read_validate(inst, params, param_count, "CFL_JSON_READ_STRING_PTR", &bb, &path, &rt)) return;

    const char *value = NULL;
    json_extract_string_runtime(rt, path, &value);
    /* Note: pointer into JSON decoder buffer — valid until next decode */
    *(char **)((uint8_t *)bb + params[0].field_offset) = (char *)value;
}

static void cfl_json_read_string_buf_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    void *bb; const char *path; cfl_runtime_handle_t *rt;
    if (!json_read_validate(inst, params, param_count, "CFL_JSON_READ_STRING_BUF", &bb, &path, &rt)) return;

    char *field = (char *)((uint8_t *)bb + params[0].field_offset);
    uint16_t buf_size = params[0].field_size;
    memset(field, 0, buf_size);

    const char *value = NULL;
    json_extract_string_runtime(rt, path, &value);

    if (value) {
        size_t copy_len = strlen(value);
        if (copy_len >= (size_t)buf_size)
            copy_len = (size_t)(buf_size - 1);
        memcpy(field, value, copy_len);
        field[copy_len] = '\0';
    }
}

/* ====================================================================
 * CFL_COPY_CONST: Copy ROM constant to blackboard field
 * Params: [0] = FIELD ref, [1] = CONST_REF
 * ==================================================================== */

static void cfl_copy_const_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 2) { EXCEPTION("CFL_COPY_CONST: requires 2 parameters"); return; }
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_FIELD) {
        EXCEPTION("CFL_COPY_CONST: param[0] must be FIELD"); return;
    }
    if ((params[1].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_CONST_REF) {
        EXCEPTION("CFL_COPY_CONST: param[1] must be CONST_REF"); return;
    }

    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) { EXCEPTION("CFL_COPY_CONST: no blackboard"); return; }

    const s_expr_module_def_t *def = s_expr_tree_get_module_def(inst);
    if (!def) { EXCEPTION("CFL_COPY_CONST: no module definition"); return; }

    uint16_t idx = params[1].const_index;
    if (idx >= def->const_count) { EXCEPTION("CFL_COPY_CONST: const_index out of range"); return; }

    const void *src = def->constants[idx];
    if (!src) { EXCEPTION("CFL_COPY_CONST: NULL constant"); return; }

    memcpy((uint8_t *)bb + params[0].field_offset, src, params[1].const_size);
}

/* ====================================================================
 * CFL_COPY_CONST_FULL: Copy ROM constant to entire blackboard
 * Params: [0] = CONST_REF
 * ==================================================================== */

static void cfl_copy_const_full_oneshot(
    s_expr_tree_instance_t *inst,
    const s_expr_param_t *params, uint16_t param_count,
    s_expr_event_type_t event_type, uint16_t event_id, void *event_data)
{
    (void)event_type; (void)event_id; (void)event_data;
    if (param_count < 1) { EXCEPTION("CFL_COPY_CONST_FULL: requires 1 parameter"); return; }
    if ((params[0].type & S_EXPR_OPCODE_MASK) != S_EXPR_PARAM_CONST_REF) {
        EXCEPTION("CFL_COPY_CONST_FULL: param[0] must be CONST_REF"); return;
    }

    void *bb = s_expr_tree_get_blackboard(inst);
    if (!bb) { EXCEPTION("CFL_COPY_CONST_FULL: no blackboard"); return; }

    const s_expr_module_def_t *def = s_expr_tree_get_module_def(inst);
    if (!def) { EXCEPTION("CFL_COPY_CONST_FULL: no module definition"); return; }

    uint16_t idx = params[0].const_index;
    if (idx >= def->const_count) { EXCEPTION("CFL_COPY_CONST_FULL: const_index out of range"); return; }

    const void *src = def->constants[idx];
    if (!src) { EXCEPTION("CFL_COPY_CONST_FULL: NULL constant"); return; }

    uint16_t bb_size = s_expr_tree_get_blackboard_size(inst);
    uint16_t const_size = params[0].const_size;
    if (const_size != bb_size) { EXCEPTION("CFL_COPY_CONST_FULL: size mismatch"); return; }

    memcpy(bb, src, const_size);
}

/* ====================================================================
 * Function table (named entries → hashed at init)
 * ==================================================================== */

static const s_expr_fn_entry_named_t cfl_oneshot_entries_named[] = {
    { "CFL_LOG",                  (void *)cfl_log_oneshot },
    { "CFL_ENABLE_CHILDREN",      (void *)cfl_enable_children_oneshot },
    { "CFL_DISABLE_CHILDREN",     (void *)cfl_disable_children_oneshot },
    { "CFL_ENABLE_CHILD",         (void *)cfl_enable_child_oneshot },
    { "CFL_DISABLE_CHILD",        (void *)cfl_disable_child_oneshot },
    { "CFL_INTERNAL_EVENT",       (void *)cfl_internal_event_oneshot },
    { "CFL_EXCEPTION",            (void *)cfl_exception_oneshot },
    { "CFL_SET_BITS",             (void *)cfl_set_bits_oneshot },
    { "CFL_CLEAR_BITS",           (void *)cfl_clear_bits_oneshot },
    { "CFL_JSON_READ_INT",        (void *)cfl_json_read_int_oneshot },
    { "CFL_JSON_READ_UINT",       (void *)cfl_json_read_uint_oneshot },
    { "CFL_JSON_READ_FLOAT",      (void *)cfl_json_read_float_oneshot },
    { "CFL_JSON_READ_BOOL",       (void *)cfl_json_read_bool_oneshot },
    { "CFL_JSON_READ_STRING_PTR", (void *)cfl_json_read_string_ptr_oneshot },
    { "CFL_JSON_READ_STRING_BUF", (void *)cfl_json_read_string_buf_oneshot },
    { "CFL_COPY_CONST",           (void *)cfl_copy_const_oneshot },
    { "CFL_COPY_CONST_FULL",      (void *)cfl_copy_const_full_oneshot },
};

static s_expr_fn_entry_t cfl_oneshot_entries[ARRAY_COUNT(cfl_oneshot_entries_named)];
static s_expr_fn_table_t cfl_oneshot_table;

const s_expr_fn_table_t *cfl_se_get_oneshot_table(void) {
    if (cfl_oneshot_table.count == 0) {
        s_expr_build_fn_table(
            cfl_oneshot_entries_named,
            cfl_oneshot_entries,
            ARRAY_COUNT(cfl_oneshot_entries_named)
        );
        cfl_oneshot_table.entries = cfl_oneshot_entries;
        cfl_oneshot_table.count = ARRAY_COUNT(cfl_oneshot_entries);
    }
    return &cfl_oneshot_table;
}
