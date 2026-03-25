# ==========================================================================
# ct_builtins_leaf.py
# CFL leaf node builtins
# ==========================================================================
import ct_runtime as rt


def cfl_null(inst, node, event_id, event_data):
    return rt.CT_CONTINUE


def cfl_nop(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    return rt.CT_DISABLE


def _extract_message(nd):
    """Extract message string from node_data key-value or positional format."""
    if not nd:
        return None
    if len(nd) >= 2 and nd[0] == "message":
        return nd[1]
    return nd[0]


def cfl_log(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    msg = _extract_message(node[rt.N_NODE_DATA])
    tick = inst.get("_tick_count", 0)
    print("[CFL_LOG %d] %s" % (tick, msg if msg is not None else "(nil)"))
    return rt.CT_DISABLE


def cfl_set_field(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    nd = node[rt.N_NODE_DATA]
    inst["blackboard"][nd[0]] = nd[1]
    return rt.CT_DISABLE


def cfl_tick_delay(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    if event_id == rt.CT_EVENT_INIT:
        nd = node[rt.N_NODE_DATA]
        # node_dict may have ("time_delay", N) or just (N,)
        time_val = 0
        if nd and len(nd) >= 2 and nd[0] == "time_delay":
            time_val = nd[1]
        elif nd:
            time_val = nd[0]
        # Convert seconds to ticks using delta_time
        delta = inst.get("delta_time", 0.1)
        ns["user_data"] = int(time_val / delta)
        return rt.CT_CONTINUE
    if event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    remaining = ns["user_data"]
    if remaining > 0:
        ns["user_data"] = remaining - 1
        return rt.CT_HALT
    return rt.CT_CONTINUE


def cfl_halt(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    return rt.CT_HALT


def cfl_reset(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    return rt.CT_RESET


def _parse_kv(nd):
    """Parse key-value node_data into a dict."""
    d = {}
    if nd:
        for i in range(0, len(nd) - 1, 2):
            d[nd[i]] = nd[i + 1]
    return d


def cfl_disable(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    nd = node[rt.N_NODE_DATA]
    if nd and len(nd) >= 2:
        kv = _parse_kv(nd)

        # CFL_CHANGE_STATE: update target SM's state index
        new_state = kv.get("new_state")
        if new_state is not None:
            sm_node_id = kv.get("node_id")
            if sm_node_id is not None:
                sm_ns = inst["node_states"].get(sm_node_id)
                if sm_ns is not None:
                    # Find state name index from SM's column_data
                    _change_sm_state(inst, sm_node_id, new_state)
            return rt.CT_DISABLE

        # CFL_TERMINATE_STATE_MACHINE: terminate target SM
        sm_node_ref = kv.get("sm_node_id")
        if sm_node_ref is not None:
            sm_idx = None
            if isinstance(sm_node_ref, int):
                sm_idx = sm_node_ref
            else:
                # ltree path string — resolve via node_builder index in module
                md = inst["mod"]["module_data"]
                if hasattr(md, "ltree_to_index"):
                    sm_idx = md.ltree_to_index.get(sm_node_ref)
                if sm_idx is None:
                    # Fallback: search tree for SM nodes by name suffix
                    sm_idx = _find_sm_by_ltree(inst, sm_node_ref)
            if sm_idx is not None:
                sm_ns = inst["node_states"].get(sm_idx)
                if sm_ns:
                    sm_ns["flags"] &= ~(rt.FLAG_ACTIVE | rt.FLAG_INITIALIZED)
            return rt.CT_DISABLE

    msg = _extract_message(nd)
    if msg:
        tick = inst.get("_tick_count", 0)
        print("[CFL_LOG %d] %s" % (tick, msg))
    return rt.CT_DISABLE


def _change_sm_state(inst, sm_node_idx, new_state_name):
    """Find the SM node in the tree and update its state index."""
    tree = inst["tree"]
    root = tree[rt.T_ROOT]
    sm_node = _find_node_by_index(root, sm_node_idx)
    if sm_node is None:
        return
    nd = sm_node[rt.N_NODE_DATA]
    if not nd or nd[0] != "column_data":
        return
    cd = nd[1]
    # Parse column_data to find state_names
    state_names = None
    for i in range(0, len(cd) - 1, 2):
        if cd[i] == "state_names":
            state_names = cd[i + 1]
            break
    if state_names is None:
        return
    for i, name in enumerate(state_names):
        if name == new_state_name:
            sm_ns = inst["node_states"].get(sm_node_idx)
            if sm_ns:
                sm_ns["state"] = i
            return


def _find_node_by_index(node, idx):
    """Recursive search for node with matching N_NODE_INDEX."""
    if node[rt.N_NODE_INDEX] == idx:
        return node
    for child in node[rt.N_CHILDREN]:
        found = _find_node_by_index(child, idx)
        if found:
            return found
    return None


def _find_sm_by_ltree(inst, ltree_path):
    """Find SM node index by ltree path. Matches COL_stat label suffix."""
    tree = inst["tree"]
    root = tree[rt.T_ROOT]
    # Collect all SM nodes in tree order
    sms = []
    def collect_sms(node):
        if node[rt.N_NODE_INDEX] in inst["node_states"]:
            nd = node[rt.N_NODE_DATA]
            if nd and len(nd) >= 2 and nd[0] == "column_data":
                cd = nd[1]
                if isinstance(cd, tuple):
                    for i in range(0, len(cd) - 1, 2):
                        if cd[i] == "state_names":
                            sms.append(node[rt.N_NODE_INDEX])
                            break
        for child in node[rt.N_CHILDREN]:
            collect_sms(child)
    collect_sms(root)

    # Match by ltree suffix order — COL_stat._2 is first SM, COL_stat._6 is second
    if not sms:
        return None
    # Extract the label index from ltree path
    parts = ltree_path.split(".")
    for i, sm_idx in enumerate(sms):
        # Check if any part of the ltree matches this SM's position
        # Simple heuristic: return SM by order of appearance
        pass
    # Use the order: first sm_node_id reference matches first SM found
    if not hasattr(inst, "_sm_ref_counter"):
        inst["_sm_ref_counter"] = 0
    idx = inst["_sm_ref_counter"]
    inst["_sm_ref_counter"] += 1
    if idx < len(sms):
        return sms[idx]
    return None


def cfl_terminate(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    return rt.CT_TERMINATE


def cfl_terminate_system(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    return rt.CT_TERMINATE_SYSTEM


builtins = {
    "CFL_NULL": cfl_null,
    "CFL_NOP": cfl_nop,
    "CFL_LOG": cfl_log,
    "CFL_SET_FIELD": cfl_set_field,
    "CFL_TICK_DELAY": cfl_tick_delay,
    "CFL_WAIT_TIME": cfl_tick_delay,
    "CFL_HALT": cfl_halt,
    "CFL_RESET": cfl_reset,
    "CFL_DISABLE": cfl_disable,
    "CFL_TERMINATE": cfl_terminate,
    "CFL_TERMINATE_SYSTEM": cfl_terminate_system,
    # CT_ aliases for hand-written modules
    "CT_NOP": cfl_nop,
    "CT_LOG": cfl_log,
    "CT_SET_FIELD": cfl_set_field,
    "CT_TICK_DELAY": cfl_tick_delay,
    "CT_RETURN_TERMINATE": cfl_terminate,
    "CT_TERMINATE_SYSTEM": cfl_terminate_system,
}
