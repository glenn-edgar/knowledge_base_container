# ==========================================================================
# ct_builtins_wait.py
# CFL wait and verify builtins
# ==========================================================================
import ct_runtime as rt


# ==========================================================================
# CFL_WAIT — halt indefinitely until externally terminated or reset
# ==========================================================================
def cfl_wait(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    return rt.CT_HALT


# ==========================================================================
# CFL_VERIFY — run pred each tick, succeed when true, optionally fail
# node_data contains verify config (pred is the guard/aux function)
# ==========================================================================
def cfl_verify(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    # Verify nodes use the guard predicate (aux_function) to check condition.
    # If guard passes, invoke_main runs this function -> HALT (keep checking).
    # If guard fails, invoke_main returns HALT before reaching here.
    # Verify succeeds when the guard returns true — but for now, just halt.
    return rt.CT_HALT


# ==========================================================================
# CFL_WAIT_FOR_EVENT — pred: check if event matches
# ==========================================================================
def cfl_wait_for_event(inst, node):
    # Stub: always returns False (event not received)
    # Full implementation would check inst["current_event_id"] against config
    return False


# ==========================================================================
# CFL_VERIFY_TIME_OUT — pred: check if time has elapsed
# ==========================================================================
def cfl_verify_time_out(inst, node):
    # Stub: always returns False (not timed out)
    # Full implementation would track elapsed ticks
    ns = rt.get_ns(inst, node[rt.N_NODE_INDEX])
    if not (ns["flags"] & rt.FLAG_EVER_INIT):
        ns["flags"] |= rt.FLAG_EVER_INIT
        ns["user_data"] = 0
    ns["user_data"] += 1
    nd = node[rt.N_NODE_DATA]
    # Try to find timeout value in node_data
    timeout = 50  # default
    if nd:
        for i in range(0, len(nd) - 1, 2):
            if nd[i] == "timeout" or nd[i] == "time_delay":
                timeout = nd[i + 1]
                break
        if len(nd) == 1:
            timeout = nd[0]
    return ns["user_data"] >= timeout


# ==========================================================================
# CFL_BOOL_FALSE — pred: always returns False
# ==========================================================================
def cfl_bool_false(inst, node):
    return False


builtins = {
    "CFL_WAIT": cfl_wait,
    "CFL_VERIFY": cfl_verify,
    "CFL_WAIT_FOR_EVENT": cfl_wait_for_event,
    "CFL_VERIFY_TIME_OUT": cfl_verify_time_out,
    "CFL_BOOL_FALSE": cfl_bool_false,
}
