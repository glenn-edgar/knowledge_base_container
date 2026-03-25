# ==========================================================================
# ct_builtins_state.py
# CFL state machine builtin
#
# Two node_data formats:
#   Hand-written: (field_name, case0_val, case1_val, ..., -1)
#     Dispatches on blackboard[field_name] matching case values.
#
#   Generated (C DSL): ("column_data", ("initial_state_number", N, "state_names", (...)))
#     Dispatches by internal state index. Children map 1:1 to state_names.
#     State changes via CFL_CHANGE_STATE one-shot.
# ==========================================================================
import ct_runtime as rt

_SM_NO_ACTION = 0xFFFF


def _parse_sm_column_data(nd):
    """Parse generated state machine column_data format."""
    if not nd or len(nd) < 2:
        return None
    if nd[0] != "column_data":
        return None
    cd = nd[1]
    if not isinstance(cd, tuple) or len(cd) < 2:
        return None
    result = {}
    for i in range(0, len(cd) - 1, 2):
        result[cd[i]] = cd[i + 1]
    return result


def cfl_state_machine(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    children = node[rt.N_CHILDREN]
    nd = node[rt.N_NODE_DATA]

    # Detect format
    col_data = _parse_sm_column_data(nd)

    if col_data:
        # Generated format: index-based dispatch
        return _sm_indexed(inst, node, ns, children, col_data, event_id, event_data)
    else:
        # Hand-written format: field-value dispatch
        return _sm_field(inst, node, ns, children, nd, event_id, event_data)


def _sm_indexed(inst, node, ns, children, col_data, event_id, event_data):
    """State machine with index-based dispatch (generated from C DSL)."""
    if event_id == rt.CT_EVENT_INIT:
        initial = col_data.get("initial_state_number", 0)
        ns["state"] = initial  # current state index
        ns["user_data"] = _SM_NO_ACTION  # previously active child
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        prev = ns["user_data"]
        if prev != _SM_NO_ACTION and prev < len(children):
            rt.child_terminate(inst, node, prev)
        ns["user_data"] = _SM_NO_ACTION
        return rt.CT_CONTINUE

    action_idx = ns["state"]
    if action_idx >= len(children):
        action_idx = len(children) - 1

    prev = ns["user_data"]
    if action_idx != prev:
        if prev != _SM_NO_ACTION and prev < len(children):
            rt.child_terminate(inst, node, prev)
            rt.child_reset_recursive(inst, node, prev)
        rt.child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = action_idx

    r = rt.child_invoke(inst, node, action_idx, event_id, event_data)

    if r == rt.CT_TERMINATE or r == rt.CT_TERMINATE_SYSTEM:
        return r
    if r == rt.CT_CONTINUE or r == rt.CT_DISABLE:
        rt.child_terminate(inst, node, action_idx)
        rt.child_reset_recursive(inst, node, action_idx)
    return rt.CT_HALT


def _sm_field(inst, node, ns, children, nd, event_id, event_data):
    """State machine with field-value dispatch (hand-written modules)."""
    if event_id == rt.CT_EVENT_INIT:
        ns["user_data"] = _SM_NO_ACTION
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        prev = ns["user_data"]
        if prev != _SM_NO_ACTION and prev < len(children):
            rt.child_terminate(inst, node, prev)
        ns["user_data"] = _SM_NO_ACTION
        return rt.CT_CONTINUE

    field_name = nd[0]
    val = inst["blackboard"].get(field_name, 0)

    action_idx = None
    default_idx = None
    for i in range(1, len(nd)):
        case_val = nd[i]
        child_idx = i - 1
        if child_idx < len(children):
            if case_val == val:
                action_idx = child_idx
                break
            if case_val == -1:
                default_idx = child_idx

    if action_idx is None:
        action_idx = default_idx
    if action_idx is None:
        raise RuntimeError("cfl_state_machine: no case for %d" % val)

    prev = ns["user_data"]
    if action_idx != prev:
        if prev != _SM_NO_ACTION and prev < len(children):
            rt.child_terminate(inst, node, prev)
            rt.child_reset_recursive(inst, node, prev)
        rt.child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = action_idx

    r = rt.child_invoke(inst, node, action_idx, event_id, event_data)

    if r == rt.CT_TERMINATE or r == rt.CT_TERMINATE_SYSTEM:
        return r
    if r == rt.CT_CONTINUE or r == rt.CT_DISABLE:
        rt.child_terminate(inst, node, action_idx)
        rt.child_reset_recursive(inst, node, action_idx)
    return rt.CT_HALT


builtins = {
    "CFL_STATE_MACHINE_MAIN": cfl_state_machine,
    "CT_STATE_MACHINE": cfl_state_machine,
}
