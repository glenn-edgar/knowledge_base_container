# ==========================================================================
# se_builtins_dispatch.py
# Mirrors s_engine_builtins_dispatch.h
# ==========================================================================

import se_runtime

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE
_SE_PIPELINE_CONTINUE = se_runtime.SE_PIPELINE_CONTINUE
_SE_PIPELINE_HALT = se_runtime.SE_PIPELINE_HALT
_SE_PIPELINE_DISABLE = se_runtime.SE_PIPELINE_DISABLE
_SE_PIPELINE_TERMINATE = se_runtime.SE_PIPELINE_TERMINATE
_SE_PIPELINE_RESET = se_runtime.SE_PIPELINE_RESET
_SE_PIPELINE_SKIP_CONTINUE = se_runtime.SE_PIPELINE_SKIP_CONTINUE
_SE_FUNCTION_HALT = se_runtime.SE_FUNCTION_HALT

_N_NODE_INDEX = se_runtime.N_NODE_INDEX
_N_CHILDREN = se_runtime.N_CHILDREN
_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE

_NO_ACTION = 0xFFFF


def _invoke_and_handle(inst, node, action_idx):
    r = se_runtime.child_invoke(inst, node, action_idx,
                                inst["current_event_id"], inst["current_event_data"])
    if r < _SE_PIPELINE_CONTINUE:
        return r
    if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
        return r
    if r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE or r == _SE_PIPELINE_RESET:
        se_runtime.child_terminate(inst, node, action_idx)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        return _SE_PIPELINE_CONTINUE
    if r == _SE_PIPELINE_SKIP_CONTINUE:
        return _SE_PIPELINE_CONTINUE
    return _SE_PIPELINE_CONTINUE


def se_event_dispatch(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT or event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE

    children = node[_N_CHILDREN]
    params = node[_N_PARAMS]
    n = len(children)

    default_idx = None
    for i in range(min(len(params), n)):
        p = params[i]
        if p[_P_TYPE] == "int" or p[_P_TYPE] == "uint":
            case_val = p[_P_VALUE]
            if case_val == event_id:
                return _invoke_and_handle(inst, node, i)
            if case_val == -1:
                default_idx = i

    if default_idx is not None:
        return _invoke_and_handle(inst, node, default_idx)

    raise RuntimeError("se_event_dispatch: no matching event handler for %d" % event_id)


def se_state_machine(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    children = node[_N_CHILDREN]
    params = node[_N_PARAMS]

    if event_id == _SE_EVENT_TERMINATE:
        prev = ns.get("user_data", _NO_ACTION)
        if prev != _NO_ACTION and prev < len(children):
            se_runtime.child_terminate(inst, node, prev)
        ns["user_data"] = _NO_ACTION
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_INIT:
        ns["user_data"] = _NO_ACTION
        return _SE_PIPELINE_CONTINUE

    field_name = params[0][_P_VALUE] if params else None
    val = inst["blackboard"].get(field_name, 0) if field_name else 0
    if isinstance(val, str):
        try:
            val = int(val)
        except (ValueError, TypeError):
            val = 0

    action_idx = None
    default_idx = None
    for i in range(1, len(params)):
        p = params[i]
        if p[_P_TYPE] == "int" or p[_P_TYPE] == "uint":
            case_val = p[_P_VALUE]
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
        raise RuntimeError("se_state_machine: no matching case for %d" % val)

    prev = ns.get("user_data", _NO_ACTION)
    if action_idx != prev:
        if prev != _NO_ACTION and prev < len(children):
            se_runtime.child_terminate(inst, node, prev)
            se_runtime.child_reset_recursive(inst, node, prev)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = action_idx

    r = se_runtime.child_invoke(inst, node, action_idx, event_id, event_data)

    if r == _SE_FUNCTION_HALT:
        return _SE_PIPELINE_HALT
    if r < _SE_PIPELINE_CONTINUE:
        return r
    if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
        return r
    if r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE or r == _SE_PIPELINE_RESET:
        se_runtime.child_terminate(inst, node, action_idx)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        return _SE_PIPELINE_CONTINUE
    if r == _SE_PIPELINE_SKIP_CONTINUE:
        return _SE_PIPELINE_CONTINUE
    return _SE_PIPELINE_CONTINUE


def se_field_dispatch(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    children = node[_N_CHILDREN]
    params = node[_N_PARAMS]

    if event_id == _SE_EVENT_TERMINATE:
        prev = ns.get("user_data", _NO_ACTION)
        if prev != _NO_ACTION and prev < len(children):
            se_runtime.child_terminate(inst, node, prev)
        ns["user_data"] = _NO_ACTION
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_INIT:
        ns["user_data"] = _NO_ACTION
        return _SE_PIPELINE_CONTINUE

    field_name = params[0][_P_VALUE] if params else None
    val = inst["blackboard"].get(field_name, 0) if field_name else 0
    if isinstance(val, str):
        try:
            val = int(val)
        except (ValueError, TypeError):
            val = 0

    action_idx = None
    default_idx = None
    for i in range(1, len(params)):
        p = params[i]
        if p[_P_TYPE] == "int" or p[_P_TYPE] == "uint":
            case_val = p[_P_VALUE]
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
        raise RuntimeError("se_field_dispatch: no matching case for %d" % val)

    prev = ns.get("user_data", _NO_ACTION)
    if action_idx != prev:
        if prev != _NO_ACTION and prev < len(children):
            se_runtime.child_terminate(inst, node, prev)
            se_runtime.child_reset_recursive(inst, node, prev)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = action_idx

    r = se_runtime.child_invoke(inst, node, action_idx, event_id, event_data)

    if r == _SE_PIPELINE_RESET or r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE:
        se_runtime.child_terminate(inst, node, action_idx)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = _NO_ACTION
        return _SE_PIPELINE_CONTINUE

    return r


builtins = {
    "se_event_dispatch": se_event_dispatch,
    "se_state_machine": se_state_machine,
    "se_field_dispatch": se_field_dispatch,
}
