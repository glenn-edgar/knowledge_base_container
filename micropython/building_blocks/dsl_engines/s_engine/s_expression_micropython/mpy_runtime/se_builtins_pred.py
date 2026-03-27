# ==========================================================================
# se_builtins_pred.py
# Mirrors s_engine_builtins_pred.h
#
# All predicate functions: fn(inst, node) -> bool
# ==========================================================================

import se_runtime

_N_CHILDREN = se_runtime.N_CHILDREN
_N_NODE_INDEX = se_runtime.N_NODE_INDEX
_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE


def se_pred_and(inst, node):
    for child in node[_N_CHILDREN]:
        if not se_runtime.invoke_pred(inst, child):
            return False
    return True


def se_pred_or(inst, node):
    for child in node[_N_CHILDREN]:
        if se_runtime.invoke_pred(inst, child):
            return True
    return False


def se_pred_not(inst, node):
    children = node[_N_CHILDREN]
    assert len(children) >= 1, "se_pred_not: no child"
    return not se_runtime.invoke_pred(inst, children[0])


def se_pred_nor(inst, node):
    return not se_pred_or(inst, node)


def se_pred_nand(inst, node):
    return not se_pred_and(inst, node)


def se_pred_xor(inst, node):
    count = 0
    for child in node[_N_CHILDREN]:
        if se_runtime.invoke_pred(inst, child):
            count += 1
    return (count % 2) == 1


def se_true(inst, node):
    return True


def se_false(inst, node):
    return False


def se_check_event(inst, node):
    params = node[_N_PARAMS]
    if not params:
        return False
    p = params[0]
    v = p[_P_VALUE]
    check_id = v[0] if isinstance(v, tuple) else v
    return inst["current_event_id"] == check_id


def se_field_eq(inst, node):
    return se_runtime.field_get(inst, node, 1) == se_runtime.param_int(node, 2)


def se_field_ne(inst, node):
    return se_runtime.field_get(inst, node, 1) != se_runtime.param_int(node, 2)


def se_field_gt(inst, node):
    v = se_runtime.field_get(inst, node, 1)
    return (v if v is not None else 0) > se_runtime.param_int(node, 2)


def se_field_ge(inst, node):
    v = se_runtime.field_get(inst, node, 1)
    return (v if v is not None else 0) >= se_runtime.param_int(node, 2)


def se_field_lt(inst, node):
    v = se_runtime.field_get(inst, node, 1)
    return (v if v is not None else 0) < se_runtime.param_int(node, 2)


def se_field_le(inst, node):
    v = se_runtime.field_get(inst, node, 1)
    return (v if v is not None else 0) <= se_runtime.param_int(node, 2)


def se_field_in_range(inst, node):
    v = se_runtime.field_get(inst, node, 1)
    v = v if v is not None else 0
    low = se_runtime.param_int(node, 2)
    hi = se_runtime.param_int(node, 3)
    return low <= v <= hi


def se_field_increment_and_test(inst, node):
    counter = se_runtime.field_get(inst, node, 1) or 0
    increment = se_runtime.field_get(inst, node, 2) or 1
    limit = se_runtime.field_get(inst, node, 3) or 0
    counter += increment
    se_runtime.field_set(inst, node, 1, counter)
    return counter <= limit


def se_state_increment_and_test(inst, node):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    increment = se_runtime.param_int(node, 1)
    limit = se_runtime.param_int(node, 2)
    ns["user_data"] = (ns.get("user_data") or 0) + increment
    if ns["user_data"] > limit:
        ns["user_data"] = 0
        return False
    return True


builtins = {
    "se_pred_and": se_pred_and,
    "se_pred_or": se_pred_or,
    "se_pred_not": se_pred_not,
    "se_pred_nor": se_pred_nor,
    "se_pred_nand": se_pred_nand,
    "se_pred_xor": se_pred_xor,
    "se_true": se_true,
    "se_false": se_false,
    "se_check_event": se_check_event,
    "se_field_eq": se_field_eq,
    "se_field_ne": se_field_ne,
    "se_field_gt": se_field_gt,
    "se_field_ge": se_field_ge,
    "se_field_lt": se_field_lt,
    "se_field_le": se_field_le,
    "se_field_in_range": se_field_in_range,
    "se_field_increment_and_test": se_field_increment_and_test,
    "se_state_increment_and_test": se_state_increment_and_test,
}
