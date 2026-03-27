# ==========================================================================
# ct_runtime.py
# MicroPython ChainTree Runtime — Core Engine
#
# Execution model:
#   tick_once() -> invoke_main(root) -> composite calls invoke_main(child)
#   Main functions: fn(inst, node, event_id, event_data) -> result code
#   Init/term handled inside invoke_main via event_id lifecycle.
#
# API:
#   mod  = ct_runtime.new_module(module_data, fns)
#   inst = ct_runtime.new_instance(mod, "tree_name")
#   result = ct_runtime.tick_once(inst)
#
# Builtins are in separate ct_builtins_*.py files.
# Import ct_builtins.py for the aggregated builtin table.
# ==========================================================================

# ==========================================================================
# Result codes (match C: cfl_global_definitions.h)
# ==========================================================================
CT_CONTINUE         = 0
CT_HALT             = 1
CT_TERMINATE        = 2
CT_RESET            = 3
CT_DISABLE          = 4
CT_SKIP_CONTINUE    = 5
CT_TERMINATE_SYSTEM = 6

# Event types
CT_EVENT_TICK       = 0xFFFF
CT_EVENT_INIT       = 0xFFFE
CT_EVENT_TERMINATE  = 0xFFFD

# ==========================================================================
# Node state flags
# ==========================================================================
FLAG_ACTIVE      = 0x01
FLAG_INITIALIZED = 0x02
FLAG_EVER_INIT   = 0x04
FLAGS_SYSTEM     = 0x0F
FLAGS_USER       = 0xF0

# ==========================================================================
# Node tuple indices (ROM constant data from _module_mpy.py)
# ==========================================================================
N_FUNC_NAME    = 0
N_FUNC_INDEX   = 1
N_NODE_INDEX   = 2
N_GUARD_INDEX  = 3
N_NODE_DATA    = 4
N_CHILDREN     = 5
N_INIT_FN      = 6   # Optional: init function name for one-shot dispatch

# Tree tuple indices
T_NAME         = 0
T_NAME_HASH    = 1
T_NODE_COUNT   = 2
T_RECORD       = 3
T_ROOT         = 4

# Record tuple indices
R_NAME         = 0
R_FIELDS       = 1

# Field tuple indices
F_NAME         = 0
F_TYPE         = 1
F_DEFAULT      = 2

# ==========================================================================
# Event queue (pre-allocated, zero allocation during tick)
# ==========================================================================
EVENT_QUEUE_SIZE = 16

_EQ_TICK_TYPE  = 0
_EQ_EVENT_ID   = 1
_EQ_EVENT_DATA = 2

def _eq_init(inst):
    inst["eq_slots"] = []
    for _ in range(EVENT_QUEUE_SIZE):
        inst["eq_slots"].append([0, 0, None])
    inst["eq_head"] = 0
    inst["eq_count"] = 0

def event_push(inst, tick_type, event_id, event_data):
    assert inst["eq_count"] < EVENT_QUEUE_SIZE, "ct_runtime: event queue full"
    tail = (inst["eq_head"] + inst["eq_count"]) % EVENT_QUEUE_SIZE
    slot = inst["eq_slots"][tail]
    slot[_EQ_TICK_TYPE] = tick_type
    slot[_EQ_EVENT_ID] = event_id
    slot[_EQ_EVENT_DATA] = event_data
    inst["eq_count"] += 1

def event_pop(inst):
    assert inst["eq_count"] > 0, "ct_runtime: event queue empty"
    slot = inst["eq_slots"][inst["eq_head"]]
    tt, eid, edata = slot[_EQ_TICK_TYPE], slot[_EQ_EVENT_ID], slot[_EQ_EVENT_DATA]
    slot[_EQ_EVENT_DATA] = None
    inst["eq_head"] = (inst["eq_head"] + 1) % EVENT_QUEUE_SIZE
    inst["eq_count"] -= 1
    return tt, eid, edata

def event_count(inst):
    return inst["eq_count"]

def event_clear(inst):
    inst["eq_head"] = 0
    inst["eq_count"] = 0

# ==========================================================================
# Module
# ==========================================================================
def new_module(module_data, initial_fns=None):
    mod = {
        "module_data": module_data,
        "main_fns": {},
        "pred_fns": {},
        "_main_idx": {},
        "_pred_idx": {},
        "_tree_by_name": {},
    }

    for i, name in enumerate(module_data.main_funcs):
        mod["_main_idx"][name.upper()] = i
    if hasattr(module_data, "pred_funcs"):
        for i, name in enumerate(module_data.pred_funcs):
            mod["_pred_idx"][name.upper()] = i

    for i, tname in enumerate(module_data.tree_order):
        mod["_tree_by_name"][tname] = i

    if initial_fns:
        register_fns(mod, initial_fns)

    return mod


def register_fns(mod, fns):
    for raw_name, fn in fns.items():
        uname = raw_name.upper()
        idx = mod["_main_idx"].get(uname)
        if idx is not None:
            mod["main_fns"][idx] = fn
        idx = mod["_pred_idx"].get(uname)
        if idx is not None:
            mod["pred_fns"][idx] = fn


def validate_module(mod):
    missing = []
    md = mod["module_data"]
    for i, name in enumerate(md.main_funcs):
        if i not in mod["main_fns"]:
            missing.append(("main", name))
    if hasattr(md, "pred_funcs"):
        for i, name in enumerate(md.pred_funcs):
            if i not in mod["pred_fns"]:
                missing.append(("pred", name))
    return (len(missing) == 0), missing


# ==========================================================================
# Instance
# ==========================================================================
def new_instance(mod, tree_name):
    tree_idx = mod["_tree_by_name"].get(tree_name)
    assert tree_idx is not None, "unknown tree: " + str(tree_name)
    tree = mod["module_data"].trees[tree_idx]

    inst = {
        "mod": mod,
        "tree": tree,
        "node_states": {},
        "node_count": tree[T_NODE_COUNT],
        "blackboard": {},
        "current_event_id": 0,
        "current_event_data": None,
        "tick_type": 0,
        "user_ctx": None,
        "bitmask": 0,
        "shadow_bitmask": 0,
    }

    # Collect actual node indices from the tree (may be global offsets)
    def _collect_indices(node, out):
        out.append(node[N_NODE_INDEX])
        for child in node[N_CHILDREN]:
            _collect_indices(child, out)
    _indices = []
    _collect_indices(tree[T_ROOT], _indices)
    for i in _indices:
        inst["node_states"][i] = {"flags": FLAG_ACTIVE, "state": 0, "user_data": 0}

    record_name = tree[T_RECORD]
    if record_name:
        md = mod["module_data"]
        if hasattr(md, "records") and md.records:
            for rec in md.records:
                if rec[R_NAME] == record_name:
                    for field in rec[R_FIELDS]:
                        inst["blackboard"][field[F_NAME]] = field[F_DEFAULT]
                    break

    _eq_init(inst)
    return inst


# ==========================================================================
# Core invocation
# ==========================================================================
def get_ns(inst, node_index):
    return inst["node_states"][node_index]


def _parse_kv_rt(nd):
    d = {}
    if nd:
        for i in range(0, len(nd) - 1, 2):
            d[nd[i]] = nd[i + 1]
    return d


def _dispatch_init_fn(inst, node):
    """Fire one-shot init function if present (matches C one-shot dispatch)."""
    init_fn = get_init_fn(node)
    if not init_fn:
        return
    nd = node[N_NODE_DATA]
    kv = _parse_kv_rt(nd) if nd else {}

    if init_fn == "CFL_SET_BITMASK":
        inst["shadow_bitmask"] |= int(kv.get("bit_mask", 0))
    elif init_fn == "CFL_CLEAR_BITMASK":
        inst["shadow_bitmask"] &= ~int(kv.get("bit_mask", 0))
    elif init_fn in ("CFL_RAISE_EXCEPTION", "CFL_SET_EXCEPTION_STEP",
                     "CFL_TURN_HEARTBEAT_ON", "CFL_TURN_HEARTBEAT_OFF",
                     "CFL_HEARTBEAT_EVENT"):
        from ct_builtins_exception import _dispatch_one_shot
        _dispatch_one_shot(inst, node, init_fn, kv)
    elif init_fn == "CFL_LOG_MESSAGE":
        msg = kv.get("message", "")
        if msg:
            tick = inst.get("_tick_count", 0)
            print("[CFL_LOG %d] %s" % (tick, msg))
    elif init_fn == "CFL_CHANGE_STATE":
        from ct_builtins_state import _dispatch_change_state
        _dispatch_change_state(inst, kv)
    elif init_fn == "CFL_TERMINATE_STATE_MACHINE":
        sm_id = kv.get("sm_node_id")
        if sm_id is not None:
            sm_idx = int(sm_id)
            sm_ns = inst["node_states"].get(sm_idx)
            if sm_ns:
                sm_ns["flags"] &= ~(FLAG_ACTIVE | FLAG_INITIALIZED)
    elif init_fn == "CFL_SEND_NAMED_EVENT":
        # Send named event to target node — queued for next event drain
        target_node = int(kv.get("node_id", 0))
        evt_id = int(kv.get("event_id", 0))
        event_push(inst, evt_id,  # tick_type = the event_id (used as event)
                   evt_id,         # event_id
                   {"target_node": target_node, "sender": node[N_NODE_INDEX]})
    # Other init functions are no-ops or handled by the main function


def invoke_main(inst, node, event_id, event_data):
    ni = node[N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if not (ns["flags"] & FLAG_ACTIVE):
        return CT_CONTINUE

    fn = inst["mod"]["main_fns"].get(node[N_FUNC_INDEX])
    assert fn is not None, "invoke_main: no function for: " + str(node[N_FUNC_NAME])

    guard_idx = node[N_GUARD_INDEX]
    fn_name = node[N_FUNC_NAME]
    # Skip guard for nodes that call their own guard internally
    _SELF_GUARD = ("CFL_WAIT", "CFL_VERIFY", "CFL_EXCEPTION_CATCH_MAIN",
                   "CFL_EXCEPTION_CATCH_ALL_MAIN", "CFL_STATE_MACHINE_MAIN")
    if guard_idx >= 0 and event_id != CT_EVENT_INIT and event_id != CT_EVENT_TERMINATE:
        if fn_name not in _SELF_GUARD:
            guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
            assert guard_fn is not None, "invoke_main: no guard for index: %d" % guard_idx
            if not guard_fn(inst, node):
                return CT_HALT

    if not (ns["flags"] & FLAG_INITIALIZED):
        ns["flags"] |= FLAG_INITIALIZED
        fn(inst, node, CT_EVENT_INIT, None)
        # Fire one-shot init function (matches C one-shot dispatch)
        _dispatch_init_fn(inst, node)

    result = fn(inst, node, event_id, event_data)
    if result is None:
        result = CT_CONTINUE

    if result == CT_DISABLE:
        fn(inst, node, CT_EVENT_TERMINATE, None)
        ns["flags"] &= ~FLAG_ACTIVE

    if result == CT_TERMINATE:
        fn(inst, node, CT_EVENT_TERMINATE, None)
        ns["flags"] &= ~(FLAG_ACTIVE | FLAG_INITIALIZED)

    return result


def invoke_pred(inst, node):
    fn = inst["mod"]["pred_fns"].get(node[N_FUNC_INDEX])
    assert fn is not None, "invoke_pred: no function for: " + str(node[N_FUNC_NAME])
    return True if fn(inst, node) else False


# ==========================================================================
# Child helpers
# ==========================================================================
def get_init_fn(node):
    if len(node) > N_INIT_FN:
        return node[N_INIT_FN]
    return None


def child_count(node):
    return len(node[N_CHILDREN])


def child_invoke(inst, node, idx, event_id, event_data):
    return invoke_main(inst, node[N_CHILDREN][idx], event_id, event_data)


def child_terminate(inst, node, idx):
    children = node[N_CHILDREN]
    if idx >= len(children):
        return
    child = children[idx]
    ni = child[N_NODE_INDEX]
    ns = inst["node_states"][ni]
    if ns["flags"] & FLAG_INITIALIZED:
        fn = inst["mod"]["main_fns"].get(child[N_FUNC_INDEX])
        fn(inst, child, CT_EVENT_TERMINATE, None)
        ns["flags"] &= FLAGS_USER
        ns["state"] = 0
        ns["user_data"] = 0


def child_reset(inst, node, idx):
    children = node[N_CHILDREN]
    if idx >= len(children):
        return
    ns = inst["node_states"][children[idx][N_NODE_INDEX]]
    ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE
    ns["state"] = 0
    ns["user_data"] = 0


def child_enable(inst, node, idx):
    children = node[N_CHILDREN]
    if idx >= len(children):
        return
    ns = inst["node_states"][children[idx][N_NODE_INDEX]]
    ns["flags"] |= FLAG_ACTIVE


def _reset_recursive(inst, node):
    ns = inst["node_states"][node[N_NODE_INDEX]]
    ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE | FLAG_EVER_INIT
    ns["state"] = 0
    ns["user_data"] = 0
    for child in node[N_CHILDREN]:
        _reset_recursive(inst, child)


def child_reset_recursive(inst, node, idx):
    children = node[N_CHILDREN]
    if idx < len(children):
        _reset_recursive(inst, children[idx])


def children_terminate_all(inst, node):
    children = node[N_CHILDREN]
    for i in range(len(children) - 1, -1, -1):
        child_terminate(inst, node, i)
    for child in children:
        ns = inst["node_states"][child[N_NODE_INDEX]]
        ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE | FLAG_EVER_INIT
        ns["state"] = 0
        ns["user_data"] = 0


def children_reset_all(inst, node):
    for child in node[N_CHILDREN]:
        ns = inst["node_states"][child[N_NODE_INDEX]]
        ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE | FLAG_EVER_INIT
        ns["state"] = 0
        ns["user_data"] = 0


# ==========================================================================
# Node data accessors
# ==========================================================================
def node_data(node, idx):
    return node[N_NODE_DATA][idx]


def node_data_count(node):
    return len(node[N_NODE_DATA])


# ==========================================================================
# Blackboard accessors
# ==========================================================================
def bb_get(inst, name):
    return inst["blackboard"].get(name)


def bb_set(inst, name, value):
    inst["blackboard"][name] = value


# ==========================================================================
# DFS column walker
#
# Visits active children of a column/gate node in order.  Stops at the
# first child that returns CT_HALT (halt-stops-siblings).  One-shot
# children (CT_DISABLE) fire and disable; the walker moves past them.
# After the walk, checks whether ANY child is still active.
#
# This matches the C runtime's CT_Tree_Walker behaviour for
# CFL_COLUMN_MAIN / CFL_GATE_NODE_MAIN nodes.
# ==========================================================================
def walk_column_dfs(inst, node, event_id, event_data):
    """Walk active children of a column/gate node.

    Matches C DFS walker semantics (cfl_engine.c cfl_execute_node):
    - HALT from a leaf stops siblings (CT_STOP_SIBLINGS)
    - HALT from a composite does NOT stop siblings (already handled internally)
    - TERMINATE terminates this column (parent), does NOT propagate further.
      In C: cfl_terminate_node_tree(parent) + CT_SKIP_CHILDREN.
    - TERMINATE_SYSTEM propagates immediately to stop everything.

    Returns HALT if any child still active, DISABLE if all done.
    """
    children = node[N_CHILDREN]

    for i in range(len(children)):
        child = children[i]
        cns = inst["node_states"][child[N_NODE_INDEX]]
        if not (cns["flags"] & FLAG_ACTIVE):
            continue

        r = child_invoke(inst, node, i, event_id, event_data)

        if r == CT_TERMINATE_SYSTEM:
            return CT_TERMINATE_SYSTEM
        if r == CT_TERMINATE:
            # In C, TERMINATE terminates the parent (this column) and the
            # walker continues with siblings of the parent. We terminate all
            # remaining active children and return DISABLE (column done).
            for j in range(len(children)):
                c = children[j]
                cs = inst["node_states"][c[N_NODE_INDEX]]
                if cs["flags"] & FLAG_ACTIVE:
                    child_terminate(inst, node, j)
            return CT_DISABLE
        if r == CT_RESET:
            # RESET: terminate and reset all children, column restarts
            for j in range(len(children)):
                child_terminate(inst, node, j)
                child_reset(inst, node, j)
            return CT_HALT
        if r == CT_HALT:
            # Only break for leaf nodes (no children). Composites returning
            # HALT just mean "still running" — the walker continues.
            if not child[N_CHILDREN]:
                break

    # Return HALT if any child still active, DISABLE if all done
    for child in children:
        cns = inst["node_states"][child[N_NODE_INDEX]]
        if cns["flags"] & FLAG_ACTIVE:
            return CT_HALT

    return CT_DISABLE


# ==========================================================================
# tick_once
# ==========================================================================
def tick_once(inst, event_id=None, event_data=None):
    if event_id is None:
        event_id = CT_EVENT_TICK

    tree = inst["tree"]
    root = tree[T_ROOT]

    root_ns = inst["node_states"][root[N_NODE_INDEX]]
    if not (root_ns["flags"] & FLAG_ACTIVE):
        return CT_TERMINATE

    inst["current_event_id"] = event_id
    inst["current_event_data"] = event_data
    inst["tick_type"] = event_id

    # Copy shadow bitmask at tick start (matches C: cfl_runtime.c:210)
    if event_id == CT_EVENT_TICK:
        inst["bitmask"] = inst["shadow_bitmask"]

    return invoke_main(inst, root, event_id, event_data)


def _find_node(node, target_idx):
    """Find node tuple by node_index in tree."""
    if node[N_NODE_INDEX] == target_idx:
        return node
    for child in node[N_CHILDREN]:
        found = _find_node(child, target_idx)
        if found is not None:
            return found
    return None


def tick_targeted(inst, event_id, event_data, target_node_idx):
    """Deliver an event to a specific node subtree (matches C: cfl_execute_event)."""
    tree = inst["tree"]
    root = tree[T_ROOT]
    target = _find_node(root, target_node_idx)
    if target is None:
        return CT_CONTINUE
    ns = inst["node_states"].get(target_node_idx)
    if not ns or not (ns["flags"] & FLAG_ACTIVE):
        return CT_CONTINUE
    inst["current_event_id"] = event_id
    inst["current_event_data"] = event_data
    return invoke_main(inst, target, event_id, event_data)


# ==========================================================================
# Helpers
# ==========================================================================
def merge_fns(*tables):
    out = {}
    for t in tables:
        out.update(t)
    return out
