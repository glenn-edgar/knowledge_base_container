# ==========================================================================
# se_builtins_flow_control.py
# Mirrors s_engine_builtins_flow_control.h
# ==========================================================================

import se_runtime

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE
_SE_SKIP_CONTINUE = se_runtime.SE_SKIP_CONTINUE
_SE_PIPELINE_CONTINUE = se_runtime.SE_PIPELINE_CONTINUE
_SE_PIPELINE_HALT = se_runtime.SE_PIPELINE_HALT
_SE_PIPELINE_DISABLE = se_runtime.SE_PIPELINE_DISABLE
_SE_PIPELINE_TERMINATE = se_runtime.SE_PIPELINE_TERMINATE
_SE_PIPELINE_RESET = se_runtime.SE_PIPELINE_RESET
_SE_PIPELINE_SKIP_CONTINUE = se_runtime.SE_PIPELINE_SKIP_CONTINUE
_SE_FUNCTION_CONTINUE = se_runtime.SE_FUNCTION_CONTINUE
_SE_FUNCTION_HALT = se_runtime.SE_FUNCTION_HALT
_SE_FUNCTION_SKIP_CONTINUE = se_runtime.SE_FUNCTION_SKIP_CONTINUE
_SE_FUNCTION_DISABLE = se_runtime.SE_FUNCTION_DISABLE

_FLAG_ACTIVE = se_runtime.FLAG_ACTIVE
_FLAG_INITIALIZED = se_runtime.FLAG_INITIALIZED

_N_NODE_INDEX = se_runtime.N_NODE_INDEX
_N_CALL_TYPE = se_runtime.N_CALL_TYPE
_N_CHILDREN = se_runtime.N_CHILDREN
_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE

_FORK_RUNNING = 0
_FORK_COMPLETE = 1


# --------------------------------------------------------------------------
# SE_SEQUENCE
# --------------------------------------------------------------------------
def se_sequence(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    children = node[_N_CHILDREN]
    n = len(children)

    if event_id == _SE_EVENT_INIT:
        ns["state"] = 0
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        s = ns["state"]
        if s < n:
            cns = se_runtime.get_ns(inst, children[s][_N_NODE_INDEX])
            if cns["flags"] & _FLAG_INITIALIZED:
                se_runtime.child_terminate(inst, node, s)
        ns["state"] = 0
        return _SE_PIPELINE_CONTINUE

    # TICK
    while ns["state"] < n:
        s = ns["state"]
        child = children[s]
        ct = child[_N_CALL_TYPE]

        if ct == "o_call" or ct == "io_call" or ct == "p_call" or ct == "p_call_composite":
            se_runtime.child_invoke(inst, node, s, event_id, event_data)
            ns["state"] = s + 1
            continue

        r = se_runtime.child_invoke(inst, node, s, event_id, event_data)

        if r <= _SE_SKIP_CONTINUE:
            return r
        if _SE_FUNCTION_CONTINUE <= r <= _SE_FUNCTION_SKIP_CONTINUE:
            if r == _SE_FUNCTION_HALT:
                return _SE_PIPELINE_HALT
            return r
        if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
            return _SE_PIPELINE_CONTINUE
        if r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE or r == _SE_PIPELINE_RESET:
            se_runtime.child_terminate(inst, node, s)
            ns["state"] = s + 1
            continue
        if r == _SE_PIPELINE_SKIP_CONTINUE:
            return _SE_PIPELINE_CONTINUE
        return _SE_PIPELINE_CONTINUE

    return _SE_PIPELINE_DISABLE


# --------------------------------------------------------------------------
# SE_SEQUENCE_ONCE
# --------------------------------------------------------------------------
def se_sequence_once(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    children = node[_N_CHILDREN]
    n = len(children)

    if event_id == _SE_EVENT_INIT:
        ns["state"] = 0
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        for i in range(n):
            cns = se_runtime.get_ns(inst, children[i][_N_NODE_INDEX])
            if cns["flags"] & _FLAG_INITIALIZED:
                se_runtime.child_terminate(inst, node, i)
        ns["state"] = 0
        return _SE_PIPELINE_CONTINUE

    # TICK: fire all once
    for i in range(n):
        child = children[i]
        ct = child[_N_CALL_TYPE]
        if ct == "o_call" or ct == "io_call" or ct == "p_call" or ct == "p_call_composite":
            se_runtime.child_invoke(inst, node, i, event_id, event_data)
            continue
        r = se_runtime.child_invoke(inst, node, i, event_id, event_data)
        if r != _SE_PIPELINE_CONTINUE and r != _SE_PIPELINE_DISABLE:
            break

    for i in range(n):
        cns = se_runtime.get_ns(inst, children[i][_N_NODE_INDEX])
        if cns["flags"] & _FLAG_INITIALIZED:
            se_runtime.child_terminate(inst, node, i)

    return _SE_PIPELINE_DISABLE


# --------------------------------------------------------------------------
# SE_FUNCTION_INTERFACE
# --------------------------------------------------------------------------
def se_function_interface(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    children = node[_N_CHILDREN]
    n = len(children)

    if event_id == _SE_EVENT_INIT:
        ns["state"] = _FORK_RUNNING
        for i in range(n):
            se_runtime.child_reset(inst, node, i)
        return _SE_FUNCTION_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        ns["state"] = _FORK_COMPLETE
        return _SE_FUNCTION_CONTINUE

    if ns["state"] != _FORK_RUNNING:
        return _SE_FUNCTION_DISABLE

    active_count = 0
    skip = False

    for i in range(n):
        if skip:
            break
        child = children[i]
        cns = se_runtime.get_ns(inst, child[_N_NODE_INDEX])
        if not (cns["flags"] & _FLAG_ACTIVE):
            continue

        r = se_runtime.child_invoke(inst, node, i, event_id, event_data)

        if r < _SE_PIPELINE_CONTINUE:
            return r
        if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
            active_count += 1
        elif r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE:
            se_runtime.child_terminate(inst, node, i)
        elif r == _SE_PIPELINE_RESET:
            se_runtime.child_terminate(inst, node, i)
            se_runtime.child_reset(inst, node, i)
            active_count += 1
        elif r == _SE_PIPELINE_SKIP_CONTINUE:
            active_count += 1
            skip = True
        else:
            active_count += 1

    if active_count == 0:
        ns["state"] = _FORK_COMPLETE
        return _SE_FUNCTION_DISABLE
    return _SE_FUNCTION_CONTINUE


# --------------------------------------------------------------------------
# SE_FORK
# --------------------------------------------------------------------------
def se_fork(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])

    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        ns["state"] = _FORK_COMPLETE
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_INIT:
        ns["state"] = _FORK_RUNNING
        children = node[_N_CHILDREN]
        for i in range(len(children)):
            se_runtime.child_reset(inst, node, i)
        return _SE_PIPELINE_CONTINUE

    if ns["state"] != _FORK_RUNNING:
        return _SE_PIPELINE_DISABLE

    children = node[_N_CHILDREN]
    n = len(children)
    skip = False

    for i in range(n):
        if skip:
            break
        child = children[i]
        ct = child[_N_CALL_TYPE]
        cns = se_runtime.get_ns(inst, child[_N_NODE_INDEX])

        if ct == "o_call" or ct == "io_call":
            if not (cns["flags"] & _FLAG_INITIALIZED):
                se_runtime.child_invoke(inst, node, i, event_id, event_data)
            continue
        if ct == "p_call" or ct == "p_call_composite":
            if not (cns["flags"] & _FLAG_INITIALIZED):
                se_runtime.child_invoke(inst, node, i, event_id, event_data)
            continue

        if not (cns["flags"] & _FLAG_ACTIVE):
            continue

        r = se_runtime.child_invoke(inst, node, i, event_id, event_data)
        if r == _SE_FUNCTION_HALT:
            r = _SE_PIPELINE_HALT
        if r < _SE_PIPELINE_CONTINUE:
            return r
        if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
            pass
        elif r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE:
            se_runtime.child_terminate(inst, node, i)
        elif r == _SE_PIPELINE_RESET:
            se_runtime.child_terminate(inst, node, i)
            se_runtime.child_reset_recursive(inst, node, i)
        elif r == _SE_PIPELINE_SKIP_CONTINUE:
            skip = True

    active_main = 0
    for child in children:
        ct = child[_N_CALL_TYPE]
        if ct == "m_call" or ct == "pt_m_call":
            if se_runtime.get_ns(inst, child[_N_NODE_INDEX])["flags"] & _FLAG_ACTIVE:
                active_main += 1

    if active_main == 0:
        ns["state"] = _FORK_COMPLETE
        return _SE_PIPELINE_DISABLE
    return _SE_PIPELINE_CONTINUE


# --------------------------------------------------------------------------
# SE_FORK_JOIN
# --------------------------------------------------------------------------
def se_fork_join(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        return _SE_PIPELINE_CONTINUE

    children = node[_N_CHILDREN]
    n = len(children)
    skip = False

    for i in range(n):
        if skip:
            break
        child = children[i]
        ct = child[_N_CALL_TYPE]
        cns = se_runtime.get_ns(inst, child[_N_NODE_INDEX])

        if ct == "o_call" or ct == "io_call":
            if not (cns["flags"] & _FLAG_INITIALIZED):
                se_runtime.child_invoke(inst, node, i, event_id, event_data)
            continue
        if ct == "p_call" or ct == "p_call_composite":
            if not (cns["flags"] & _FLAG_INITIALIZED):
                se_runtime.child_invoke(inst, node, i, event_id, event_data)
            continue

        if not (cns["flags"] & _FLAG_ACTIVE):
            continue

        r = se_runtime.child_invoke(inst, node, i, event_id, event_data)
        if r == _SE_FUNCTION_HALT:
            r = _SE_PIPELINE_HALT
        if r < _SE_PIPELINE_CONTINUE:
            return r
        if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
            pass
        elif r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE:
            se_runtime.child_terminate(inst, node, i)
        elif r == _SE_PIPELINE_RESET:
            se_runtime.child_terminate(inst, node, i)
            se_runtime.child_reset_recursive(inst, node, i)
        elif r == _SE_PIPELINE_SKIP_CONTINUE:
            skip = True

    active_main = 0
    for child in children:
        ct = child[_N_CALL_TYPE]
        if ct == "m_call" or ct == "pt_m_call":
            if se_runtime.get_ns(inst, child[_N_NODE_INDEX])["flags"] & _FLAG_ACTIVE:
                active_main += 1

    if active_main == 0:
        return _SE_PIPELINE_DISABLE
    return _SE_FUNCTION_HALT


# --------------------------------------------------------------------------
# SE_CHAIN_FLOW
# --------------------------------------------------------------------------
def se_chain_flow(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        return _SE_PIPELINE_CONTINUE

    children = node[_N_CHILDREN]
    n = len(children)
    active_count = 0
    skip = False

    for i in range(n):
        if skip:
            break
        child = children[i]
        if not (se_runtime.get_ns(inst, child[_N_NODE_INDEX])["flags"] & _FLAG_ACTIVE):
            continue
        ct = child[_N_CALL_TYPE]

        if ct == "o_call" or ct == "io_call":
            se_runtime.child_invoke(inst, node, i, event_id, event_data)
            se_runtime.child_terminate(inst, node, i)
            continue
        if ct == "p_call" or ct == "p_call_composite":
            se_runtime.child_invoke(inst, node, i, event_id, event_data)
            se_runtime.child_terminate(inst, node, i)
            continue

        r = se_runtime.child_invoke(inst, node, i, event_id, event_data)

        if r == _SE_FUNCTION_HALT:
            return _SE_PIPELINE_HALT
        if r < _SE_PIPELINE_CONTINUE:
            return r
        if r == _SE_PIPELINE_CONTINUE:
            active_count += 1
        elif r == _SE_PIPELINE_HALT:
            return _SE_PIPELINE_CONTINUE
        elif r == _SE_PIPELINE_DISABLE:
            se_runtime.child_terminate(inst, node, i)
        elif r == _SE_PIPELINE_TERMINATE:
            se_runtime.children_terminate_all(inst, node)
            return _SE_PIPELINE_TERMINATE
        elif r == _SE_PIPELINE_RESET:
            se_runtime.children_terminate_all(inst, node)
            se_runtime.children_reset_all(inst, node)
            return _SE_PIPELINE_CONTINUE
        elif r == _SE_PIPELINE_SKIP_CONTINUE:
            active_count += 1
            skip = True
        else:
            active_count += 1

    if active_count == 0:
        return _SE_PIPELINE_DISABLE
    return _SE_PIPELINE_CONTINUE


# --------------------------------------------------------------------------
# SE_WHILE
# --------------------------------------------------------------------------
_WHILE_EVAL_PRED = 0
_WHILE_RUN_BODY = 1

def se_while(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])

    if event_id == _SE_EVENT_INIT:
        ns["state"] = _WHILE_EVAL_PRED
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        children = node[_N_CHILDREN]
        if len(children) >= 2:
            cns = se_runtime.get_ns(inst, children[1][_N_NODE_INDEX])
            if cns["flags"] & _FLAG_INITIALIZED:
                se_runtime.child_terminate(inst, node, 1)
        return _SE_PIPELINE_CONTINUE

    # TICK
    if ns["state"] == _WHILE_EVAL_PRED:
        if not se_runtime.child_invoke_pred(inst, node, 0):
            return _SE_PIPELINE_DISABLE
        se_runtime.child_reset_recursive(inst, node, 1)
        ns["state"] = _WHILE_RUN_BODY

    r = se_runtime.child_invoke(inst, node, 1, event_id, event_data)

    if r < _SE_PIPELINE_CONTINUE:
        return r
    if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT or r == _SE_PIPELINE_SKIP_CONTINUE:
        return _SE_FUNCTION_HALT
    if r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE or r == _SE_PIPELINE_RESET:
        se_runtime.child_terminate(inst, node, 1)
        se_runtime.child_reset_recursive(inst, node, 1)
        ns["state"] = _WHILE_EVAL_PRED
        return _SE_PIPELINE_HALT
    return _SE_PIPELINE_DISABLE


# --------------------------------------------------------------------------
# SE_IF_THEN_ELSE
# --------------------------------------------------------------------------
def se_if_then_else(inst, node, event_id, event_data):
    children = node[_N_CHILDREN]
    n = len(children)
    assert n >= 2, "se_if_then_else: need predicate and then branch"
    has_else = n >= 3

    if event_id == _SE_EVENT_INIT:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        return _SE_PIPELINE_CONTINUE

    condition = se_runtime.child_invoke_pred(inst, node, 0)

    if condition:
        r = se_runtime.child_invoke(inst, node, 1, event_id, event_data)
    elif has_else:
        r = se_runtime.child_invoke(inst, node, 2, event_id, event_data)
    else:
        return _SE_PIPELINE_CONTINUE

    if r < _SE_PIPELINE_CONTINUE:
        return r
    if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
        return r
    if r == _SE_PIPELINE_RESET:
        se_runtime.child_terminate(inst, node, 1)
        se_runtime.child_reset(inst, node, 1)
        if has_else:
            se_runtime.child_terminate(inst, node, 2)
            se_runtime.child_reset(inst, node, 2)
        return _SE_PIPELINE_RESET
    if r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE:
        se_runtime.child_terminate(inst, node, 1)
        se_runtime.child_reset(inst, node, 1)
        if has_else:
            se_runtime.child_terminate(inst, node, 2)
            se_runtime.child_reset(inst, node, 2)
        return _SE_PIPELINE_CONTINUE
    return _SE_PIPELINE_CONTINUE


# --------------------------------------------------------------------------
# SE_COND
# --------------------------------------------------------------------------
def se_cond(inst, node, event_id, event_data):
    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    children = node[_N_CHILDREN]
    n = len(children)
    NO_CHILD = 0xFFFF

    if event_id == _SE_EVENT_INIT:
        ns["user_data"] = NO_CHILD
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        ns["user_data"] = NO_CHILD
        return _SE_PIPELINE_CONTINUE

    matched_action = NO_CHILD
    i = 0
    while i < n:
        child = children[i]
        ct = child[_N_CALL_TYPE]
        if ct == "p_call" or ct == "p_call_composite":
            pred_result = se_runtime.child_invoke_pred(inst, node, i)
            if pred_result and matched_action == NO_CHILD:
                matched_action = i + 1
                break
            i += 2
        else:
            i += 1

    if matched_action == NO_CHILD:
        return _SE_PIPELINE_CONTINUE

    action_idx = matched_action
    active = ns["user_data"]

    if action_idx != active:
        if active != NO_CHILD:
            se_runtime.child_terminate(inst, node, active)
            se_runtime.child_reset_recursive(inst, node, active)
        se_runtime.child_terminate(inst, node, action_idx)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = action_idx

    r = se_runtime.child_invoke(inst, node, action_idx, event_id, event_data)

    if r < _SE_PIPELINE_CONTINUE:
        return r
    if r == _SE_PIPELINE_CONTINUE or r == _SE_PIPELINE_HALT:
        return _SE_PIPELINE_CONTINUE
    if r == _SE_PIPELINE_RESET:
        se_runtime.child_terminate(inst, node, action_idx)
        se_runtime.child_reset_recursive(inst, node, action_idx)
        return _SE_PIPELINE_CONTINUE
    return r


# --------------------------------------------------------------------------
# SE_TRIGGER_ON_CHANGE
# --------------------------------------------------------------------------
def se_trigger_on_change(inst, node, event_id, event_data):
    # Children layout (0-based):
    #   [0] = predicate (p_call or p_call_composite)
    #   [1] = rising action (m_call)
    #   [2] = falling action (m_call) -- optional
    # Initial state comes from node's own params[0] (int).
    children = node[_N_CHILDREN]
    n = len(children)
    assert n >= 2, "se_trigger_on_change: need at least 2 children"
    has_falling = n >= 3

    PRED_CHILD = 0
    RISING_CHILD = 1
    FALLING_CHILD = 2

    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_INIT:
        initial_state = 0
        params = node[_N_PARAMS]
        if params:
            p0 = params[0]
            if p0[_P_TYPE] == "int" or p0[_P_TYPE] == "uint":
                initial_state = 1 if p0[_P_VALUE] else 0
        se_runtime.set_state(inst, node, initial_state)
        return _SE_PIPELINE_CONTINUE

    # TICK
    current = se_runtime.child_invoke_pred(inst, node, PRED_CHILD)
    prev = se_runtime.get_state(inst, node)

    rising = (prev == 0 and current)
    falling = (prev != 0 and not current)

    se_runtime.set_state(inst, node, 1 if current else 0)

    def _invoke_and_handle(action_idx):
        se_runtime.child_terminate(inst, node, action_idx)
        se_runtime.child_reset(inst, node, action_idx)
        r = se_runtime.child_invoke(inst, node, action_idx, event_id, event_data)
        if r < _SE_PIPELINE_CONTINUE:
            return r
        if r == _SE_PIPELINE_DISABLE or r == _SE_PIPELINE_TERMINATE or r == _SE_PIPELINE_RESET:
            se_runtime.child_terminate(inst, node, action_idx)
            se_runtime.child_reset(inst, node, action_idx)
        return _SE_PIPELINE_CONTINUE

    if rising:
        if has_falling:
            se_runtime.child_terminate(inst, node, FALLING_CHILD)
            se_runtime.child_reset(inst, node, FALLING_CHILD)
        return _invoke_and_handle(RISING_CHILD)
    elif falling and has_falling:
        se_runtime.child_terminate(inst, node, RISING_CHILD)
        se_runtime.child_reset(inst, node, RISING_CHILD)
        return _invoke_and_handle(FALLING_CHILD)

    return _SE_PIPELINE_CONTINUE


builtins = {
    "se_sequence": se_sequence,
    "se_sequence_once": se_sequence_once,
    "se_function_interface": se_function_interface,
    "se_fork": se_fork,
    "se_fork_join": se_fork_join,
    "se_chain_flow": se_chain_flow,
    "se_while": se_while,
    "se_if_then_else": se_if_then_else,
    "se_cond": se_cond,
    "se_trigger_on_change": se_trigger_on_change,
}
