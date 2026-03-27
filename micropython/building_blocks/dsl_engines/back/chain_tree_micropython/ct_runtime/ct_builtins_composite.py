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
# CFL_JOIN_MAIN — wait for target node to complete
#
# Reads parent_node_name (resolved to node index) from node_data.
# Returns HALT while target is active, DISABLE when target completes.
# Matches C: cfl_join_main_main_fn (cfl_main_functions.c:277)
# ==========================================================================
def cfl_join_main(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    nd = node[rt.N_NODE_DATA]
    kv = _parse_kv(nd) if nd else {}
    target_idx = kv.get("parent_node_name")
    if target_idx is not None and isinstance(target_idx, int):
        target_ns = inst["node_states"].get(target_idx)
        if target_ns and (target_ns["flags"] & rt.FLAG_ACTIVE):
            return rt.CT_HALT
    return rt.CT_DISABLE


# ==========================================================================
# CFL_JOIN_SEQUENCE_ELEMENT — wait for target (same as join)
# ==========================================================================
def cfl_join_sequence_element(inst, node, event_id, event_data):
    return cfl_join_main(inst, node, event_id, event_data)


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

    # Run child
    r = rt.child_invoke(inst, node, 0, event_id, event_data)

    if r == rt.CT_HALT:
        return rt.CT_HALT
    if r == rt.CT_CONTINUE or r == rt.CT_DISABLE or r == rt.CT_TERMINATE:
        # Child completed one iteration — reset for next
        rt.child_terminate(inst, node, 0)
        rt.child_reset(inst, node, 0)
        ns["state"] += 1
        # Check guard predicate directly (matches C: cfl_while_main_main_fn)
        guard_idx = node[rt.N_GUARD_INDEX]
        if guard_idx >= 0:
            guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
            if guard_fn and not guard_fn(inst, node):
                return rt.CT_DISABLE
        return rt.CT_HALT
    if r == rt.CT_TERMINATE_SYSTEM:
        return r
    return rt.CT_HALT


# ==========================================================================
# CFL_EVENT_LOGGER — log matching events, stay active
#
# Filters incoming events by a configured event ID list. Only prints when
# the current event_id matches one in the list. Always returns CONTINUE
# (stays active to keep listening). Matches C: cfl_event_logger_main_fn.
# ==========================================================================
def cfl_event_logger(inst, node, event_id, event_data):
    ni = node[rt.N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if event_id == rt.CT_EVENT_INIT:
        nd = node[rt.N_NODE_DATA]
        kv = _parse_kv(nd) if nd else {}
        ns["el_message"] = kv.get("message", "")
        events = kv.get("events", ())
        if isinstance(events, (list, tuple)):
            ns["el_events"] = events
        else:
            ns["el_events"] = ()
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE

    # Check if current event matches our filter list
    events = ns.get("el_events", ())
    if not events:
        return rt.CT_DISABLE  # No events to listen for

    for evt in events:
        if int(evt) == event_id:
            tick = inst.get("_tick_count", 0)
            msg = ns.get("el_message", "")
            print("[CFL_EVENT_LOG %d] %s (event %d)" % (tick, msg, event_id))
            break

    return rt.CT_CONTINUE  # Stay active, keep listening


# ==========================================================================
# CFL_WATCH_DOG_MAIN — watchdog (stub: runs as sequence)
# ==========================================================================
def cfl_watch_dog_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_sequence
    return cfl_sequence(inst, node, event_id, event_data)


# ==========================================================================
# CFL_DF_MASK_MAIN — bitmask data flow dispatch
#
# Enables/disables children based on required/excluded bitmask conditions.
# Matches C: cfl_df_mask_main_main_fn (cfl_main_functions.c:497)
# ==========================================================================
def _parse_kv(nd):
    d = {}
    if nd:
        for i in range(0, len(nd) - 1, 2):
            d[nd[i]] = nd[i + 1]
    return d


def _parse_column_data(nd):
    kv = _parse_kv(nd)
    cd = kv.get("column_data")
    if cd and isinstance(cd, (list, tuple)):
        return _parse_kv(cd)
    return {}


def cfl_df_mask_main(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]

    if event_id == rt.CT_EVENT_INIT:
        # Parse required/excluded bitmasks from node_data
        cd = _parse_column_data(node[rt.N_NODE_DATA])
        ns["required_bitmask"] = int(cd.get("required_bitmask", 0))
        ns["excluded_bitmask"] = int(cd.get("excluded_bitmask", 0))
        ns["mask_enabled"] = False
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        # Terminate all children
        for i in range(rt.child_count(node)):
            rt.child_terminate(inst, node, i)
        ns["mask_enabled"] = False
        return rt.CT_CONTINUE

    # Check guard predicate — if true, disable (matches C pattern)
    guard_idx = node[rt.N_GUARD_INDEX]
    if guard_idx >= 0:
        guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
        if guard_fn and guard_fn(inst, node):
            return rt.CT_DISABLE

    # Check bitmask conditions
    bm = inst["bitmask"]
    req = ns.get("required_bitmask", 0)
    exc = ns.get("excluded_bitmask", 0)
    required_met = (req & bm) == req
    excluded_clear = (exc & bm) == 0
    conditions_met = required_met and excluded_clear

    if not ns.get("mask_enabled", False):
        # Currently disabled — enable children if conditions met
        if conditions_met:
            for i in range(rt.child_count(node)):
                rt.child_reset(inst, node, i)
                rt.child_enable(inst, node, i)
            ns["mask_enabled"] = True
    else:
        # Currently enabled — terminate children if conditions no longer met
        if not conditions_met:
            for i in range(rt.child_count(node)):
                rt.child_terminate(inst, node, i)
            ns["mask_enabled"] = False

    if not ns.get("mask_enabled", False):
        return rt.CT_SKIP_CONTINUE
    return rt.CT_CONTINUE


# ==========================================================================
# Pred stubs
# ==========================================================================
def cfl_sm_event_sync(inst, node):
    return True

def cfl_verify_bitmask(inst, node):
    nd = node[rt.N_NODE_DATA]
    kv = _parse_kv(nd) if nd else {}
    # fn_data contains required/excluded bitmask
    fn_data = kv.get("fn_data")
    if fn_data and isinstance(fn_data, (list, tuple)):
        fd = _parse_kv(fn_data)
    else:
        fd = kv
    req = int(fd.get("required_bitmask", 0))
    exc = int(fd.get("excluded_bitmask", 0))
    bm = inst["bitmask"]
    return (req & bm) == req and (exc & bm) == 0

def cfl_verify_tests_active(inst, node):
    return True

def cfl_wait_for_bitmask(inst, node):
    nd = node[rt.N_NODE_DATA]
    kv = _parse_kv(nd) if nd else {}
    # wait_fn_data contains required/excluded bitmask
    wfd = kv.get("wait_fn_data")
    if wfd and isinstance(wfd, (list, tuple)):
        fd = _parse_kv(wfd)
    else:
        fd = kv
    req = int(fd.get("required_bitmask", 0))
    exc = int(fd.get("excluded_bitmask", 0))
    bm = inst["bitmask"]
    return (req & bm) == req and (exc & bm) == 0

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
    "CFL_DF_MASK_MAIN": cfl_df_mask_main,
    "CFL_SM_EVENT_SYNC": cfl_sm_event_sync,
    "CFL_VERIFY_BITMASK": cfl_verify_bitmask,
    "CFL_VERIFY_TESTS_ACTIVE": cfl_verify_tests_active,
    "CFL_WAIT_FOR_BITMASK": cfl_wait_for_bitmask,
    "CFL_WAIT_FOR_TESTS_COMPLETE": cfl_wait_for_tests_complete,
}
