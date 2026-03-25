# ==========================================================================
# ct_builtins_composite.py
# CFL advanced composite builtins: fork/join, while, sequence variants,
# event logger, watchdog
# ==========================================================================
import ct_runtime as rt


# ==========================================================================
# CFL_FORK_MAIN — parallel (same as CFL_PARALLEL)
# ==========================================================================
def cfl_fork_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_parallel
    return cfl_parallel(inst, node, event_id, event_data)


# ==========================================================================
# CFL_JOIN_MAIN — sequence (children run in order)
# ==========================================================================
def cfl_join_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# CFL_JOIN_SEQUENCE_ELEMENT — sequence element (same as sequence)
# ==========================================================================
def cfl_join_sequence_element(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# CFL_SEQUENCE_START_MAIN — sequence start (same as sequence)
# ==========================================================================
def cfl_sequence_start_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# CFL_SEQUENCE_PASS_MAIN — pass sequence (same as sequence)
# ==========================================================================
def cfl_sequence_pass_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# CFL_SEQUENCE_FAIL_MAIN — fail sequence (same as sequence)
# ==========================================================================
def cfl_sequence_fail_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# CFL_WHILE_MAIN — repeat child while guard is true
# ==========================================================================
def cfl_while_main(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]

    if event_id == rt.CT_EVENT_INIT:
        ns["state"] = 0
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        rt.child_terminate(inst, node, 0)
        ns["state"] = 0
        return rt.CT_CONTINUE

    # Guard predicate is checked by invoke_main before we get here.
    # If we're called, guard passed — run child.
    r = rt.child_invoke(inst, node, 0, event_id, event_data)

    if r == rt.CT_HALT:
        return rt.CT_HALT
    if r == rt.CT_CONTINUE or r == rt.CT_DISABLE:
        # Child completed one iteration — reset and run again next tick
        rt.child_terminate(inst, node, 0)
        rt.child_reset(inst, node, 0)
        ns["state"] += 1
        return rt.CT_HALT
    if r == rt.CT_TERMINATE or r == rt.CT_TERMINATE_SYSTEM:
        return r
    return rt.CT_HALT


# ==========================================================================
# CFL_EVENT_LOGGER — log events, then complete
# ==========================================================================
def cfl_event_logger(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    nd = node[rt.N_NODE_DATA]
    msg = None
    if nd:
        for i in range(0, len(nd) - 1, 2):
            if nd[i] == "message":
                msg = nd[i + 1]
                break
        if msg is None:
            msg = nd[0] if nd else "(nil)"
    tick = inst.get("_tick_count", 0)
    print("[CFL_EVENT_LOG %d] %s" % (tick, msg))
    return rt.CT_DISABLE


# ==========================================================================
# CFL_WATCH_DOG_MAIN — watchdog (stub: runs as sequence)
# ==========================================================================
def cfl_watch_dog_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# Pred stubs
# ==========================================================================
def cfl_sm_event_sync(inst, node):
    return True

def cfl_verify_bitmask(inst, node):
    return False

def cfl_verify_tests_active(inst, node):
    return True

def cfl_wait_for_bitmask(inst, node):
    return False

def cfl_wait_for_tests_complete(inst, node):
    return False


builtins = {
    "CFL_FORK_MAIN": cfl_fork_main,
    "CFL_JOIN_MAIN": cfl_join_main,
    "CFL_JOIN_SEQUENCE_ELEMENT": cfl_join_sequence_element,
    "CFL_SEQUENCE_START_MAIN": cfl_sequence_start_main,
    "CFL_SEQUENCE_PASS_MAIN": cfl_sequence_pass_main,
    "CFL_SEQUENCE_FAIL_MAIN": cfl_sequence_fail_main,
    "CFL_WHILE_MAIN": cfl_while_main,
    "CFL_EVENT_LOGGER": cfl_event_logger,
    "CFL_WATCH_DOG_MAIN": cfl_watch_dog_main,
    "CFL_SM_EVENT_SYNC": cfl_sm_event_sync,
    "CFL_VERIFY_BITMASK": cfl_verify_bitmask,
    "CFL_VERIFY_TESTS_ACTIVE": cfl_verify_tests_active,
    "CFL_WAIT_FOR_BITMASK": cfl_wait_for_bitmask,
    "CFL_WAIT_FOR_TESTS_COMPLETE": cfl_wait_for_tests_complete,
}
