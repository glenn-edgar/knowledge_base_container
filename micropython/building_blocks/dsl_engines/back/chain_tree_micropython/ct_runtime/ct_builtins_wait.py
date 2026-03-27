# ==========================================================================
# ct_builtins_wait.py
# CFL wait and verify builtins
#
# CFL_WAIT calls its own guard predicate internally (not via invoke_main).
# The guard in invoke_main is bypassed for these nodes — they always run.
# Matches C: cfl_wait_main_fn (cfl_main_functions.c:170)
# ==========================================================================
import ct_runtime as rt


def _parse_kv(nd):
    d = {}
    if nd:
        for i in range(0, len(nd) - 1, 2):
            d[nd[i]] = nd[i + 1]
    return d


# ==========================================================================
# CFL_WAIT — wait for guard predicate to pass, with timeout support
#
# Calls guard predicate internally. If guard passes → DISABLE (done).
# Tracks timeout via time_out_event counter. On timeout, fires error
# function and returns RESET or TERMINATE based on reset_flag.
# Matches C: cfl_wait_main_fn
# ==========================================================================
def cfl_wait(inst, node, event_id, event_data):
    ni = node[rt.N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if event_id == rt.CT_EVENT_INIT:
        nd = node[rt.N_NODE_DATA]
        kv = _parse_kv(nd) if nd else {}
        ns["wait_timeout"] = int(kv.get("timeout", 0))
        ns["wait_timeout_event"] = int(kv.get("time_out_event", 0))
        ns["wait_reset_flag"] = kv.get("reset_flag", False)
        ns["wait_error_fn"] = kv.get("error_function", "")
        ns["wait_event_count"] = 0
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE

    # Call guard predicate internally (matches C pattern)
    guard_idx = node[rt.N_GUARD_INDEX]
    if guard_idx >= 0:
        guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
        if guard_fn and guard_fn(inst, node):
            return rt.CT_DISABLE  # Guard passed → done

    # Timeout check
    timeout = ns.get("wait_timeout", 0)
    if timeout > 0:
        timeout_evt = ns.get("wait_timeout_event", 0)
        if event_id == rt.CT_EVENT_TICK or event_id == timeout_evt:
            ns["wait_event_count"] = ns.get("wait_event_count", 0) + 1
            if ns["wait_event_count"] >= timeout:
                # Timeout — fire error function
                tick = inst.get("_tick_count", 0)
                err_fn = ns.get("wait_error_fn", "")
                if err_fn:
                    print("[WAIT_TIMEOUT %d] %s node %d" % (tick, err_fn, ni))
                if ns.get("wait_reset_flag", False):
                    return rt.CT_RESET
                return rt.CT_TERMINATE

    return rt.CT_HALT


# ==========================================================================
# CFL_VERIFY — check guard predicate, succeed when true
# With timeout support similar to CFL_WAIT.
# Matches C: cfl_verify_main_fn
# ==========================================================================
def cfl_verify(inst, node, event_id, event_data):
    ni = node[rt.N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if event_id == rt.CT_EVENT_INIT:
        nd = node[rt.N_NODE_DATA]
        kv = _parse_kv(nd) if nd else {}
        ns["verify_reset_flag"] = kv.get("reset_flag", False)
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE

    # Call guard predicate internally
    guard_idx = node[rt.N_GUARD_INDEX]
    if guard_idx >= 0:
        guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
        if guard_fn and guard_fn(inst, node):
            return rt.CT_DISABLE  # Verified

    return rt.CT_HALT


# ==========================================================================
# CFL_WAIT_FOR_EVENT — pred: check if named event matches
#
# Reads wait_fn_data.event_id and wait_fn_data.event_count from node_data.
# Tracks received events. Returns True when count reaches 0.
# Matches C: cfl_wait_for_event_boolean_fn
# ==========================================================================
def cfl_wait_for_event(inst, node):
    ni = node[rt.N_NODE_INDEX]
    ns = inst["node_states"][ni]

    # Initialize on first call
    if "wfe_event_id" not in ns:
        nd = node[rt.N_NODE_DATA]
        kv = _parse_kv(nd) if nd else {}
        wfd = kv.get("wait_fn_data")
        if wfd and isinstance(wfd, (list, tuple)):
            wfkv = _parse_kv(wfd)
        else:
            wfkv = {}
        ns["wfe_event_id"] = int(wfkv.get("event_id", 0))
        ns["wfe_remaining"] = int(wfkv.get("event_count", 1))

    # Check current event
    current_event = inst.get("current_event_id", 0)
    if current_event == ns["wfe_event_id"]:
        ns["wfe_remaining"] -= 1
        if ns["wfe_remaining"] <= 0:
            return True
    return False


# ==========================================================================
# CFL_VERIFY_TIME_OUT — pred: check if time has elapsed
# ==========================================================================
def cfl_verify_time_out(inst, node):
    ns = rt.get_ns(inst, node[rt.N_NODE_INDEX])
    if not (ns["flags"] & rt.FLAG_EVER_INIT):
        ns["flags"] |= rt.FLAG_EVER_INIT
        ns["user_data"] = 0
    ns["user_data"] += 1
    nd = node[rt.N_NODE_DATA]
    kv = _parse_kv(nd) if nd else {}
    timeout = int(kv.get("timeout", kv.get("time_delay", 50)))
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
