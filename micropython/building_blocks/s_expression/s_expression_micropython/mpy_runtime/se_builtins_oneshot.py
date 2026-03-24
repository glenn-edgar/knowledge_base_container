# ==========================================================================
# se_builtins_oneshot.py
# Mirrors s_engine_builtins_oneshot.h
# ==========================================================================

import se_runtime

_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE


def se_log(inst, node):
    msg = se_runtime.param_str(node, 1) or "(nil)"
    print("[SE_LOG] " + str(msg))


def se_log_int(inst, node):
    fmt = se_runtime.param_str(node, 1) or "%d"
    v = se_runtime.field_get(inst, node, 2)
    v = int(v) if v is not None else 0
    print("[SE_LOG_INT] " + (fmt % v))


def se_log_float(inst, node):
    fmt = se_runtime.param_str(node, 1) or "%f"
    v = se_runtime.field_get(inst, node, 2)
    v = float(v) if v is not None else 0.0
    print("[SE_LOG_FLOAT] " + (fmt % v))


def se_log_field(inst, node):
    name = se_runtime.param_field_name(node, 1)
    v = inst["blackboard"].get(name)
    print("[SE_LOG_FIELD] " + str(name) + " = " + str(v))


def se_set_field(inst, node):
    params = node[_N_PARAMS]
    assert len(params) >= 2, "se_set_field: missing param 2"
    p = params[1]
    v = p[_P_VALUE]
    if isinstance(v, tuple) and len(v) == 2:
        val = v[0]  # (hash, str) tuple
    else:
        val = se_runtime.param_int(node, 2)
    se_runtime.field_set(inst, node, 1, val)


def se_set_field_float(inst, node):
    se_runtime.field_set(inst, node, 1, se_runtime.param_float(node, 2))


def se_inc_field(inst, node):
    v = se_runtime.field_get(inst, node, 1) or 0
    se_runtime.field_set(inst, node, 1, v + 1)


def se_dec_field(inst, node):
    v = se_runtime.field_get(inst, node, 1) or 0
    se_runtime.field_set(inst, node, 1, v - 1)


def se_set_hash(inst, node):
    params = node[_N_PARAMS]
    assert len(params) >= 2, "se_set_hash: missing param 2"
    p = params[1]
    v = p[_P_VALUE]
    h = v[0] if isinstance(v, tuple) and len(v) == 2 else v
    se_runtime.field_set(inst, node, 1, h)


def se_set_external_field(inst, node):
    se_runtime.field_set(inst, node, 1, se_runtime.param_int(node, 2))


def se_queue_event(inst, node):
    tt = se_runtime.param_int(node, 1)
    eid = se_runtime.param_int(node, 2)
    field_name = se_runtime.param_field_name(node, 3) if len(node[_N_PARAMS]) >= 3 else None
    se_runtime.event_push(inst, tt, eid, field_name)


def se_push_stack(inst, node):
    if inst["stack"]:
        import se_stack
        params = node[_N_PARAMS]
        assert len(params) >= 1, "se_push_stack: missing param 1"
        p = params[0]
        if p[_P_TYPE] == "field_ref":
            val = inst["blackboard"].get(p[_P_VALUE], 0)
        elif isinstance(p[_P_VALUE], tuple) and len(p[_P_VALUE]) == 2:
            val = p[_P_VALUE][0]  # (hash, str)
        else:
            val = se_runtime.param_int(node, 1)
        se_stack.push_int(inst["stack"], val)


builtins = {
    "se_log": se_log,
    "se_log_int": se_log_int,
    "se_log_float": se_log_float,
    "se_log_field": se_log_field,
    "se_set_field": se_set_field,
    "se_set_field_float": se_set_field_float,
    "se_inc_field": se_inc_field,
    "se_dec_field": se_dec_field,
    "se_set_hash": se_set_hash,
    "se_set_external_field": se_set_external_field,
    "se_queue_event": se_queue_event,
    "se_push_stack": se_push_stack,
}
