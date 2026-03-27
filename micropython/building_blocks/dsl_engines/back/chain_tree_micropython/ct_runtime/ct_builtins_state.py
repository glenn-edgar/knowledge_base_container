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


def _enable_state_child(inst, node, idx):
    """Enable a state child WITHOUT clearing INITIALIZED flags.
    Matches C: cfl_enable_node (sets ACTIVE, preserves engine flags).
    This means init one-shots don't re-fire on state re-entry."""
    children = node[rt.N_CHILDREN]
    if idx >= len(children):
        return
    child = children[idx]
    ns = inst["node_states"][child[rt.N_NODE_INDEX]]
    ns["flags"] |= rt.FLAG_ACTIVE


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
    """State machine with index-based dispatch (generated from C DSL).

    Matches C: cfl_state_machine_main_main_fn.
    - ns["state"] = new_state (set by change_state one-shot, deferred)
    - ns["user_data"] = current_state (active child index)
    - Transition happens at the START of each tick (not mid-tick).
    - Sync events block the SM from ticking children until consumed.
    """
    if event_id == rt.CT_EVENT_INIT:
        initial = col_data.get("initial_state_number", 0)
        ns["state"] = initial       # new_state (requested)
        ns["user_data"] = _SM_NO_ACTION  # current_state (active child)
        ns["sync_event_id_valid"] = False
        ns["sync_event_id"] = 0
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        cur = ns["user_data"]
        if cur != _SM_NO_ACTION and cur < len(children):
            rt.child_terminate(inst, node, cur)
        ns["user_data"] = _SM_NO_ACTION
        return rt.CT_CONTINUE

    # --- State transition (deferred from change_state one-shot) ---
    new_state = ns["state"]
    if new_state >= len(children):
        new_state = len(children) - 1
    cur = ns["user_data"]

    if new_state != cur:
        # Terminate old state, enable new state
        # In C, cfl_terminate_node_tree terminates the old state subtree,
        # then cfl_enable_node enables the new state WITHOUT clearing
        # INITIALIZED flags. This means init one-shots don't fire again
        # on re-entry — the state resumes where it left off.
        if cur != _SM_NO_ACTION and cur < len(children):
            rt.child_terminate(inst, node, cur)
        # Enable new state (just set ACTIVE, preserve INITIALIZED)
        _enable_state_child(inst, node, new_state)
        ns["user_data"] = new_state

    # --- Sync gate (matches C: cfl_sm_event_sync_boolean_fn) ---
    # In C, change_state with sync queues a sync event to the SM.
    # When the SM processes that event: boolean sees sync_event_id_valid=true
    # and event matches sync_event_id → clears flag, returns true → SKIP_CONTINUE.
    # On subsequent events where sync_event_id_valid=false → returns false → CONTINUE.
    # The SM processes ALL events (tick is just one event type).
    skip_children = False
    if ns.get("sync_event_id_valid", False):
        if event_id == ns.get("sync_event_id", 0):
            ns["sync_event_id_valid"] = False  # Consume sync event
        skip_children = True  # SKIP_CONTINUE: don't invoke children

    # --- Invoke active state child ---
    action_idx = ns["user_data"]
    if action_idx == _SM_NO_ACTION or action_idx >= len(children):
        return rt.CT_DISABLE

    if skip_children:
        return rt.CT_HALT

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


def _dispatch_change_state(inst, kv):
    """Handle CFL_CHANGE_STATE one-shot with sync support.
    Called from _dispatch_init_fn in ct_runtime.py."""
    new_state_name = kv.get("new_state")
    sm_node_id = kv.get("node_id")
    sync_event_id = int(kv.get("sync_event_id", 0))

    if sm_node_id is None or new_state_name is None:
        return

    sm_idx = int(sm_node_id)
    sm_ns = inst["node_states"].get(sm_idx)
    if sm_ns is None:
        return

    # Find the SM node in the tree to get state_names
    root = inst["tree"][rt.T_ROOT]
    sm_node = _find_node_recursive(root, sm_idx)
    if sm_node is None:
        return

    nd = sm_node[rt.N_NODE_DATA]
    col_data = _parse_sm_column_data(nd)
    if not col_data:
        return

    state_names = col_data.get("state_names")
    if not state_names or not isinstance(state_names, (list, tuple)):
        return

    # Find new state index
    for i, name in enumerate(state_names):
        if name == new_state_name:
            sm_ns["state"] = i
            break

    # Handle sync event
    # In C, change_state with sync queues the sync event to the SM.
    # The event arrives via cfl_execute_event, walking the SM subtree.
    # The SM's boolean sees sync_event_id_valid=true, event matches → clears flag.
    if sync_event_id != 0:
        sm_ns["sync_event_id_valid"] = True
        sm_ns["sync_event_id"] = sync_event_id
        # Queue sync event targeted at the SM (same as C: cfl_send_null_event)
        rt.event_push(inst, sync_event_id, sync_event_id,
                      {"target_node": sm_idx, "sender": -1})
    else:
        sm_ns["sync_event_id_valid"] = False
        sm_ns["sync_event_id"] = 0


def _find_node_recursive(node, target_idx):
    if node[rt.N_NODE_INDEX] == target_idx:
        return node
    for child in node[rt.N_CHILDREN]:
        found = _find_node_recursive(child, target_idx)
        if found is not None:
            return found
    return None


builtins = {
    "CFL_STATE_MACHINE_MAIN": cfl_state_machine,
    "CT_STATE_MACHINE": cfl_state_machine,
}
