# ==========================================================================
# ct_builtins_exception.py
# CFL exception handling: catch-all, catch, recovery, heartbeat
#
# Three-phase exception model (matches C: cfl_exception_support.c):
#   MAIN → RECOVERY → FINALIZE
#
# Exception events are queued via event_push and processed by tick_once's
# event drain loop in main_mpy.py.
# ==========================================================================
import ct_runtime as rt

# Exception event IDs (match C: cfl_engine.h)
CFL_RAISE_EXCEPTION_EVENT       = 12
CFL_TURN_HEARTBEAT_ON_EVENT     = 13
CFL_TURN_HEARTBEAT_OFF_EVENT    = 14
CFL_HEARTBEAT_EVENT             = 15
CFL_SET_EXCEPTION_STEP_EVENT    = 16
CFL_RECOVERY_CHECK_EVENT        = 0xFFFE

# Exception stages
STAGE_MAIN     = 0
STAGE_RECOVERY = 1
STAGE_FINALIZE = 2

# Recovery FSM states
REC_SEQ_EVAL       = 0
REC_SEQ_WAIT       = 1
REC_PARALLEL_ENABLE = 2
REC_PARALLEL_WAIT  = 3


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
    return kv


def _find_node_by_index(node, target_idx):
    """Walk tree to find node tuple by node_index."""
    if node[rt.N_NODE_INDEX] == target_idx:
        return node
    for child in node[rt.N_CHILDREN]:
        found = _find_node_by_index(child, target_idx)
        if found is not None:
            return found
    return None


def _find_parent_exception(inst, node):
    """Walk up tree to find nearest ancestor exception handler node index."""
    root = inst["tree"][rt.T_ROOT]
    target = node[rt.N_NODE_INDEX]

    # Build parent map if not cached
    if "_parent_map" not in inst:
        pmap = {}
        def _build(n, parent_idx):
            pmap[n[rt.N_NODE_INDEX]] = parent_idx
            for c in n[rt.N_CHILDREN]:
                _build(c, n[rt.N_NODE_INDEX])
        _build(root, -1)
        inst["_parent_map"] = pmap

    pmap = inst["_parent_map"]
    cur = pmap.get(target, -1)
    while cur >= 0:
        cur_node = _find_node_by_index(root, cur)
        if cur_node:
            fn = cur_node[rt.N_FUNC_NAME]
            if fn in ("CFL_EXCEPTION_CATCH_ALL_MAIN", "CFL_EXCEPTION_CATCH_MAIN"):
                return cur
        cur = pmap.get(cur, -1)
    return -1


def _any_child_active(inst, node):
    for child in node[rt.N_CHILDREN]:
        cns = inst["node_states"].get(child[rt.N_NODE_INDEX])
        if cns and (cns["flags"] & rt.FLAG_ACTIVE):
            return True
    return False


def _terminate_child_by_index(inst, node_idx):
    """Terminate a node and all its descendants by node_index."""
    ns = inst["node_states"].get(node_idx)
    if ns:
        ns["flags"] &= ~(rt.FLAG_ACTIVE | rt.FLAG_INITIALIZED)
    # Also terminate children (need to find node in tree)
    root = inst["tree"][rt.T_ROOT]
    n = _find_node_by_index(root, node_idx)
    if n:
        for child in n[rt.N_CHILDREN]:
            _terminate_child_by_index(inst, child[rt.N_NODE_INDEX])


def _enable_node_by_index(inst, node_idx):
    """Enable a node by node_index."""
    ns = inst["node_states"].get(node_idx)
    if ns:
        ns["flags"] = (ns["flags"] & rt.FLAGS_USER) | rt.FLAG_ACTIVE
        ns["state"] = 0
        ns["user_data"] = 0


def _dispatch_one_shot(inst, node, init_fn, kv):
    """Dispatch exception-related one-shot init functions.
    Directly modifies the parent exception handler's node_state.
    Called from cfl_disable's INIT handler."""
    parent_idx = _find_parent_exception(inst, node)
    if parent_idx < 0:
        return
    pns = inst["node_states"].get(parent_idx)
    if not pns:
        return

    if init_fn == "CFL_RAISE_EXCEPTION":
        # Queue exception event for processing on next tick
        # Store as pending exception on the parent node
        rt.event_push(inst, rt.CT_EVENT_TICK,
                      CFL_RAISE_EXCEPTION_EVENT,
                      {"target": parent_idx, "origin": node[rt.N_NODE_INDEX]})

    elif init_fn == "CFL_SET_EXCEPTION_STEP":
        pns["step_count"] = int(kv.get("step", 0))

    elif init_fn == "CFL_TURN_HEARTBEAT_ON":
        pns["heartbeat_enabled"] = True
        pns["heartbeat_timeout"] = int(kv.get("time_out", 50))
        pns["heartbeat_count"] = 0

    elif init_fn == "CFL_TURN_HEARTBEAT_OFF":
        pns["heartbeat_enabled"] = False

    elif init_fn == "CFL_HEARTBEAT_EVENT":
        pns["heartbeat_count"] = 0


# ==========================================================================
# CFL_EXCEPTION_CATCH_ALL_MAIN
#
# Catch-all wrapper. Runs children normally. When exception event arrives,
# evaluates guard filter. If matched → caught (CONTINUE). Else → DISABLE.
# ==========================================================================
def cfl_exception_catch_all_main(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]

    if event_id == rt.CT_EVENT_INIT:
        # Enable all children
        for i in range(rt.child_count(node)):
            rt.child_reset(inst, node, i)
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        rt.children_terminate_all(inst, node)
        return rt.CT_CONTINUE

    if event_id == CFL_RAISE_EXCEPTION_EVENT:
        # Forward exception event to all active children so the target
        # exception handler can process it
        for child in node[rt.N_CHILDREN]:
            cns = inst["node_states"].get(child[rt.N_NODE_INDEX])
            if cns and (cns["flags"] & rt.FLAG_ACTIVE):
                rt.invoke_main(inst, child, event_id, event_data)
        tick = inst.get("_tick_count", 0)
        print("[EXCEPTION_CATCH_ALL %d] caught exception" % tick)
        return rt.CT_CONTINUE

    # Normal tick — walk children like a column
    return rt.walk_column_dfs(inst, node, event_id, event_data)


# ==========================================================================
# CFL_EXCEPTION_CATCH_MAIN
#
# Three-phase exception handler: MAIN → RECOVERY → FINALIZE
# Node data has exception_catch_links: (main_idx, recovery_idx, finalize_idx)
# ==========================================================================
def cfl_exception_catch_main(inst, node, event_id, event_data):
    ni = node[rt.N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if event_id == rt.CT_EVENT_INIT:
        cd = _parse_column_data(node[rt.N_NODE_DATA])
        links = cd.get("exception_catch_links", ())
        if isinstance(links, (list, tuple)) and len(links) >= 3:
            ns["exc_main"] = int(links[0])
            ns["exc_recovery"] = int(links[1])
            ns["exc_finalize"] = int(links[2])
        else:
            ns["exc_main"] = -1
            ns["exc_recovery"] = -1
            ns["exc_finalize"] = -1
        ns["exc_stage"] = STAGE_MAIN
        ns["exc_type"] = 0  # NONE
        ns["step_count"] = 0
        ns["heartbeat_enabled"] = False
        ns["heartbeat_timeout"] = 0
        ns["heartbeat_count"] = 0
        # Disable all children, then enable only MAIN link
        for child in node[rt.N_CHILDREN]:
            cns = inst["node_states"][child[rt.N_NODE_INDEX]]
            cns["flags"] &= ~(rt.FLAG_ACTIVE | rt.FLAG_INITIALIZED)
        if ns["exc_main"] >= 0:
            _enable_node_by_index(inst, ns["exc_main"])
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        for child in node[rt.N_CHILDREN]:
            _terminate_child_by_index(inst, child[rt.N_NODE_INDEX])
        ns["heartbeat_enabled"] = False
        return rt.CT_CONTINUE

    # --- Exception event ---
    if event_id == CFL_RAISE_EXCEPTION_EVENT:
        # Check if this event is targeted at us
        if isinstance(event_data, dict):
            target = event_data.get("target", -1)
            if target != ni:
                return rt.CT_CONTINUE  # Not for us
            origin = event_data.get("origin", -1)
        else:
            origin = event_data

        ns["exc_type"] = 1  # RAISED
        ns["exc_original_node"] = origin
        tick = inst.get("_tick_count", 0)
        print("[EXCEPTION_CATCH %d] exception from node %s, stage=%d" % (
            tick, origin, ns["exc_stage"]))

        if ns["exc_stage"] == STAGE_MAIN:
            # Transition: MAIN → RECOVERY
            if ns["exc_main"] >= 0:
                _terminate_child_by_index(inst, ns["exc_main"])
            ns["exc_stage"] = STAGE_RECOVERY
            if ns["exc_recovery"] >= 0:
                _enable_node_by_index(inst, ns["exc_recovery"])
            return rt.CT_CONTINUE
        elif ns["exc_stage"] == STAGE_RECOVERY:
            # Exception during recovery → go to finalize
            if ns["exc_recovery"] >= 0:
                _terminate_child_by_index(inst, ns["exc_recovery"])
            ns["exc_stage"] = STAGE_FINALIZE
            if ns["exc_finalize"] >= 0:
                _enable_node_by_index(inst, ns["exc_finalize"])
            return rt.CT_CONTINUE
        else:
            # Exception during finalize → forward to parent
            parent_exc = _find_parent_exception(inst, node)
            if parent_exc >= 0:
                rt.event_push(inst, rt.CT_EVENT_TICK,
                              CFL_RAISE_EXCEPTION_EVENT,
                              {"target": parent_exc, "origin": origin})
            return rt.CT_DISABLE

    # --- Heartbeat events ---
    if event_id == CFL_TURN_HEARTBEAT_ON_EVENT:
        ns["heartbeat_enabled"] = True
        ns["heartbeat_timeout"] = int(event_data) if event_data else 50
        ns["heartbeat_count"] = 0
        return rt.CT_CONTINUE

    if event_id == CFL_TURN_HEARTBEAT_OFF_EVENT:
        ns["heartbeat_enabled"] = False
        return rt.CT_CONTINUE

    if event_id == CFL_HEARTBEAT_EVENT:
        ns["heartbeat_count"] = 0
        return rt.CT_CONTINUE

    if event_id == CFL_SET_EXCEPTION_STEP_EVENT:
        ns["step_count"] = int(event_data) if event_data else 0
        return rt.CT_CONTINUE

    # --- Normal tick ---
    # Check heartbeat timeout
    if ns.get("heartbeat_enabled") and event_id == rt.CT_EVENT_TICK:
        ns["heartbeat_count"] = ns.get("heartbeat_count", 0) + 1
        if ns["heartbeat_count"] >= ns.get("heartbeat_timeout", 50):
            ns["heartbeat_enabled"] = False
            tick = inst.get("_tick_count", 0)
            print("[HEARTBEAT_TIMEOUT %d] node %d" % (tick, ni))
            ns["exc_type"] = 2  # HEARTBEAT_TIMEOUT
            if ns["exc_stage"] == STAGE_MAIN:
                if ns["exc_main"] >= 0:
                    _terminate_child_by_index(inst, ns["exc_main"])
                ns["exc_stage"] = STAGE_RECOVERY
                if ns["exc_recovery"] >= 0:
                    _enable_node_by_index(inst, ns["exc_recovery"])
                return rt.CT_CONTINUE

    # Check if current stage's child is still active
    stage = ns.get("exc_stage", STAGE_MAIN)
    stage_node = -1
    if stage == STAGE_MAIN:
        stage_node = ns.get("exc_main", -1)
    elif stage == STAGE_RECOVERY:
        stage_node = ns.get("exc_recovery", -1)
    elif stage == STAGE_FINALIZE:
        stage_node = ns.get("exc_finalize", -1)

    if stage_node >= 0:
        sn_state = inst["node_states"].get(stage_node)
        if sn_state and (sn_state["flags"] & rt.FLAG_ACTIVE):
            # Stage child still running — invoke it
            root = inst["tree"][rt.T_ROOT]
            stage_child = _find_node_by_index(root, stage_node)
            if stage_child:
                r = rt.invoke_main(inst, stage_child, event_id, event_data)
                if r == rt.CT_TERMINATE_SYSTEM:
                    return r
                if r == rt.CT_TERMINATE:
                    _terminate_child_by_index(inst, stage_node)
                return rt.CT_HALT  # still running
            return rt.CT_HALT
        else:
            # Stage child completed — advance to next stage
            if stage == STAGE_MAIN:
                # Main completed normally → skip recovery, go to finalize
                ns["exc_stage"] = STAGE_FINALIZE
                if ns["exc_finalize"] >= 0:
                    _enable_node_by_index(inst, ns["exc_finalize"])
                return rt.CT_HALT
            elif stage == STAGE_RECOVERY:
                # Recovery completed → go to finalize
                ns["exc_stage"] = STAGE_FINALIZE
                if ns["exc_finalize"] >= 0:
                    _enable_node_by_index(inst, ns["exc_finalize"])
                return rt.CT_HALT
            elif stage == STAGE_FINALIZE:
                # Finalize completed → we're done
                return rt.CT_DISABLE

    return rt.CT_DISABLE


# ==========================================================================
# CFL_RECOVERY_MAIN
#
# Recovery state machine. Executes recovery steps conditionally.
# Children are recovery step columns. Evaluates skip condition (guard pred)
# to decide which steps to run. Step numbering goes from max_steps down.
# ==========================================================================
def cfl_recovery_main(inst, node, event_id, event_data):
    ni = node[rt.N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if event_id == rt.CT_EVENT_INIT:
        cd = _parse_column_data(node[rt.N_NODE_DATA])
        max_steps = int(cd.get("max_steps", 0))
        ns["rec_max_steps"] = max_steps
        # Get parent exception node's step_count
        parent_exc_idx = _find_parent_exception(inst, node)
        step_count = 0
        if parent_exc_idx >= 0:
            pns = inst["node_states"].get(parent_exc_idx)
            if pns:
                step_count = pns.get("step_count", 0)
        ns["rec_step_count"] = step_count
        ns["rec_current_step"] = max_steps - step_count
        ns["rec_state"] = REC_SEQ_EVAL
        # Disable all children initially
        for child in node[rt.N_CHILDREN]:
            cns = inst["node_states"][child[rt.N_NODE_INDEX]]
            cns["flags"] &= ~(rt.FLAG_ACTIVE | rt.FLAG_INITIALIZED)
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        for child in node[rt.N_CHILDREN]:
            _terminate_child_by_index(inst, child[rt.N_NODE_INDEX])
        return rt.CT_CONTINUE

    if event_id != rt.CT_EVENT_TICK:
        return rt.CT_CONTINUE

    children = node[rt.N_CHILDREN]
    n_children = len(children)
    max_steps = ns.get("rec_max_steps", 0)
    current_step = ns.get("rec_current_step", 0)
    state = ns.get("rec_state", REC_SEQ_EVAL)

    if state == REC_SEQ_EVAL:
        # Evaluate steps from current_step down to 0
        while current_step >= 0 and current_step < n_children:
            # Check skip condition via guard predicate
            guard_idx = node[rt.N_GUARD_INDEX]
            skip = False
            if guard_idx >= 0:
                guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
                if guard_fn:
                    skip = guard_fn(inst, node)
            if not skip:
                # Enable this step child
                child = children[current_step]
                _enable_node_by_index(inst, child[rt.N_NODE_INDEX])
                ns["rec_state"] = REC_SEQ_WAIT
                ns["rec_current_step"] = current_step
                return rt.CT_HALT
            current_step -= 1
            ns["rec_current_step"] = current_step
        # All sequential steps done or skipped → parallel enable
        ns["rec_state"] = REC_PARALLEL_ENABLE
        return rt.CT_HALT

    if state == REC_SEQ_WAIT:
        # Wait for current step child to complete
        child = children[current_step]
        cns = inst["node_states"][child[rt.N_NODE_INDEX]]
        if cns["flags"] & rt.FLAG_ACTIVE:
            r = rt.invoke_main(inst, child, event_id, event_data)
            if r == rt.CT_TERMINATE_SYSTEM:
                return r
            return rt.CT_HALT
        # Step completed — move to next
        ns["rec_current_step"] = current_step - 1
        ns["rec_state"] = REC_SEQ_EVAL
        return rt.CT_HALT

    if state == REC_PARALLEL_ENABLE:
        # Enable all remaining children beyond max_steps
        for i in range(max_steps, n_children):
            child = children[i]
            _enable_node_by_index(inst, child[rt.N_NODE_INDEX])
        ns["rec_state"] = REC_PARALLEL_WAIT
        return rt.CT_HALT

    if state == REC_PARALLEL_WAIT:
        # Tick all parallel children, wait for completion
        any_active = False
        for i in range(max_steps, n_children):
            child = children[i]
            cns = inst["node_states"][child[rt.N_NODE_INDEX]]
            if cns["flags"] & rt.FLAG_ACTIVE:
                r = rt.invoke_main(inst, child, event_id, event_data)
                if r == rt.CT_TERMINATE_SYSTEM:
                    return r
                if inst["node_states"][child[rt.N_NODE_INDEX]]["flags"] & rt.FLAG_ACTIVE:
                    any_active = True
        if any_active:
            return rt.CT_HALT
        return rt.CT_DISABLE

    return rt.CT_DISABLE


# ==========================================================================
# Exception filter predicates (stubs for test — always catch)
# ==========================================================================
def exception_filter_pred(inst, node):
    """EXCEPTION_FILTER — always return True so invoke_main runs the handler.
    In C, the boolean has dual semantics (filter on exception, disable check on tick).
    Here we always let the handler run; it does its own filtering internally."""
    return True


def catch_all_exception_pred(inst, node):
    """CATCH_ALL_EXCEPTION — always return True so invoke_main runs the handler."""
    return True


def user_skip_condition_pred(inst, node):
    """USER_SKIP_CONDITION — for recovery step evaluation.
    Returns True to skip this step. In the test, we always return True
    (skip all steps up to the current_step mark)."""
    return True


def exception_logging_pred(inst, node):
    """EXCEPTION_LOGGING — logging callback (stub)."""
    return True


# ==========================================================================
# Builtin table
# ==========================================================================
builtins = {
    "CFL_EXCEPTION_CATCH_ALL_MAIN": cfl_exception_catch_all_main,
    "CFL_EXCEPTION_CATCH_MAIN": cfl_exception_catch_main,
    "CFL_RECOVERY_MAIN": cfl_recovery_main,
    # User predicates for exception tests
    "EXCEPTION_FILTER": exception_filter_pred,
    "CATCH_ALL_EXCEPTION": catch_all_exception_pred,
    "USER_SKIP_CONDITION": user_skip_condition_pred,
    "EXCEPTION_LOGGING": exception_logging_pred,
}
