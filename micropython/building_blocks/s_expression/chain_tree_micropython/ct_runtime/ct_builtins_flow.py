# ==========================================================================
# ct_builtins_flow.py
# CFL composite flow control builtins
# ==========================================================================
import ct_runtime as rt

# ==========================================================================
# CFL_COLUMN_MAIN / CFL_SEQUENCE — execute children in order
# ==========================================================================
def cfl_sequence(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    children = node[rt.N_CHILDREN]
    n = len(children)

    if event_id == rt.CT_EVENT_INIT:
        ns["state"] = 0
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        s = ns["state"]
        if s < n:
            rt.child_terminate(inst, node, s)
        ns["state"] = 0
        return rt.CT_CONTINUE

    while ns["state"] < n:
        s = ns["state"]
        r = rt.child_invoke(inst, node, s, event_id, event_data)

        if r == rt.CT_HALT:
            return rt.CT_HALT
        if r == rt.CT_CONTINUE:
            ns["state"] = s + 1
            continue
        if r == rt.CT_TERMINATE:
            rt.child_terminate(inst, node, s)
            return rt.CT_TERMINATE
        if r == rt.CT_DISABLE:
            rt.child_terminate(inst, node, s)
            ns["state"] = s + 1
            continue
        if r == rt.CT_RESET:
            rt.children_terminate_all(inst, node)
            rt.children_reset_all(inst, node)
            ns["state"] = 0
            return rt.CT_HALT
        if r == rt.CT_SKIP_CONTINUE:
            ns["state"] = s + 1
            continue
        if r == rt.CT_TERMINATE_SYSTEM:
            return rt.CT_TERMINATE_SYSTEM
        ns["state"] = s + 1
        continue

    return rt.CT_CONTINUE


# ==========================================================================
# CFL_SELECTOR — try children until one succeeds
# ==========================================================================
def cfl_selector(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    children = node[rt.N_CHILDREN]
    n = len(children)

    if event_id == rt.CT_EVENT_INIT:
        ns["state"] = 0
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        s = ns["state"]
        if s < n:
            rt.child_terminate(inst, node, s)
        ns["state"] = 0
        return rt.CT_CONTINUE

    while ns["state"] < n:
        s = ns["state"]
        r = rt.child_invoke(inst, node, s, event_id, event_data)

        if r == rt.CT_CONTINUE:
            return rt.CT_CONTINUE
        if r == rt.CT_HALT:
            return rt.CT_HALT
        if r == rt.CT_DISABLE or r == rt.CT_TERMINATE or r == rt.CT_SKIP_CONTINUE:
            rt.child_terminate(inst, node, s)
            rt.child_reset(inst, node, s)
            ns["state"] = s + 1
            continue
        if r == rt.CT_TERMINATE_SYSTEM:
            return rt.CT_TERMINATE_SYSTEM
        ns["state"] = s + 1
        continue

    return rt.CT_DISABLE


# ==========================================================================
# CFL_PARALLEL — tick all children, AND completion
# ==========================================================================
def cfl_parallel(inst, node, event_id, event_data):
    children = node[rt.N_CHILDREN]
    n = len(children)

    if event_id == rt.CT_EVENT_INIT:
        for i in range(n):
            rt.child_reset(inst, node, i)
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        rt.children_terminate_all(inst, node)
        return rt.CT_CONTINUE

    active = 0
    for i in range(n):
        child = children[i]
        cns = inst["node_states"][child[rt.N_NODE_INDEX]]
        if not (cns["flags"] & rt.FLAG_ACTIVE):
            continue

        r = rt.child_invoke(inst, node, i, event_id, event_data)

        if r == rt.CT_TERMINATE_SYSTEM:
            return rt.CT_TERMINATE_SYSTEM
        if r == rt.CT_HALT:
            active += 1
        elif r == rt.CT_CONTINUE or r == rt.CT_DISABLE or r == rt.CT_TERMINATE:
            rt.child_terminate(inst, node, i)
        elif r == rt.CT_RESET:
            rt.child_terminate(inst, node, i)
            rt.child_reset(inst, node, i)
            active += 1
        else:
            active += 1

    if active == 0:
        return rt.CT_CONTINUE
    return rt.CT_HALT


# ==========================================================================
# CFL_FOR_MAIN — repeat child N times
# node_data[0] = iteration count
# ==========================================================================
def cfl_for_loop(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]

    if event_id == rt.CT_EVENT_INIT:
        ns["state"] = 0
        ns["user_data"] = node[rt.N_NODE_DATA][0]
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        rt.child_terminate(inst, node, 0)
        ns["state"] = 0
        return rt.CT_CONTINUE

    if ns["state"] >= ns["user_data"]:
        return rt.CT_CONTINUE

    r = rt.child_invoke(inst, node, 0, event_id, event_data)

    if r == rt.CT_HALT:
        return rt.CT_HALT
    if r == rt.CT_CONTINUE or r == rt.CT_DISABLE:
        rt.child_terminate(inst, node, 0)
        rt.child_reset(inst, node, 0)
        ns["state"] += 1
        if ns["state"] >= ns["user_data"]:
            return rt.CT_CONTINUE
        return rt.CT_HALT
    if r == rt.CT_TERMINATE or r == rt.CT_TERMINATE_SYSTEM:
        return r
    if r == rt.CT_RESET:
        rt.child_terminate(inst, node, 0)
        rt.child_reset(inst, node, 0)
        ns["state"] = 0
        return rt.CT_HALT

    return rt.CT_HALT


# ==========================================================================
# CFL_SUPERVISOR_MAIN — parallel with failure tracking (stub)
# ==========================================================================
def cfl_supervisor(inst, node, event_id, event_data):
    return cfl_parallel(inst, node, event_id, event_data)


# ==========================================================================
# CFL_COLUMN_MAIN — tick all children each cycle (like C engine)
#
# In C, the engine ticks every enabled node each cycle. The column
# function just checks if any children are still enabled.
# Here we tick all children and deactivate only on DISABLE/TERMINATE.
# CONTINUE means "still enabled" (leaf completed its one-shot action
# but the node stays enabled for the column's lifetime).
# HALT means "still running" (multi-tick operation).
# ==========================================================================
def cfl_column(inst, node, event_id, event_data):
    children = node[rt.N_CHILDREN]
    n = len(children)

    if event_id == rt.CT_EVENT_INIT:
        for i in range(n):
            rt.child_reset(inst, node, i)
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        rt.children_terminate_all(inst, node)
        return rt.CT_CONTINUE

    active = 0
    for i in range(n):
        child = children[i]
        cns = inst["node_states"][child[rt.N_NODE_INDEX]]
        if not (cns["flags"] & rt.FLAG_ACTIVE):
            continue

        r = rt.child_invoke(inst, node, i, event_id, event_data)

        if r == rt.CT_TERMINATE_SYSTEM:
            return rt.CT_TERMINATE_SYSTEM
        if r == rt.CT_HALT or r == rt.CT_CONTINUE:
            active += 1  # still enabled
        elif r == rt.CT_DISABLE or r == rt.CT_TERMINATE:
            rt.child_terminate(inst, node, i)
        elif r == rt.CT_RESET:
            rt.child_terminate(inst, node, i)
            rt.child_reset(inst, node, i)
            active += 1
        else:
            active += 1

    if active == 0:
        return rt.CT_DISABLE
    return rt.CT_HALT


builtins = {
    "CFL_COLUMN_MAIN": cfl_column,
    "CFL_SEQUENCE": cfl_sequence,
    "CFL_SELECTOR": cfl_selector,
    "CFL_PARALLEL": cfl_parallel,
    "CFL_FOR_MAIN": cfl_for_loop,
    "CFL_SUPERVISOR_MAIN": cfl_supervisor,
    # CT_ aliases for hand-written modules
    "CT_SEQUENCE": cfl_sequence,
    "CT_SELECTOR": cfl_selector,
    "CT_PARALLEL": cfl_parallel,
    "CT_FOR_LOOP": cfl_for_loop,
    "CT_SUPERVISOR": cfl_supervisor,
}
