# ==========================================================================
# ct_runtime.py
# MicroPython ChainTree Runtime
#
# S-Engine style execution: composites invoke children directly.
# No tree walker. All static data is ROM-resident tuples.
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
N_FUNC_NAME    = 0   # "ct_sequence" (debug)
N_FUNC_INDEX   = 1   # index into main_fns
N_NODE_INDEX   = 2   # DFS pre-order index
N_GUARD_INDEX  = 3   # pred fn index, -1 = no guard
N_NODE_DATA    = 4   # tuple of config values
N_CHILDREN     = 5   # tuple of child node tuples

# Tree tuple indices
T_NAME         = 0
T_NAME_HASH    = 1
T_NODE_COUNT   = 2
T_RECORD       = 3   # blackboard record name or None
T_ROOT         = 4   # root node tuple

# Record tuple indices
R_NAME         = 0
R_FIELDS       = 1   # tuple of (name, type, default) tuples

# Field tuple indices
F_NAME         = 0
F_TYPE         = 1
F_DEFAULT      = 2

# ==========================================================================
# Event queue (pre-allocated, zero allocation during tick)
# ==========================================================================
EVENT_QUEUE_SIZE = 16

# Event slot indices
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
    slot[_EQ_EVENT_DATA] = None  # release ref
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
    ok, missing = validate_module(mod)
    if not ok:
        lines = ["new_instance: unregistered functions:"]
        for kind, name in missing:
            lines.append("  [%s] %s" % (kind, name))
        raise RuntimeError("\n".join(lines))

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
    }

    # Init node states
    for i in range(tree[T_NODE_COUNT]):
        inst["node_states"][i] = {"flags": FLAG_ACTIVE, "state": 0, "user_data": 0}

    # Init blackboard from record
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


def invoke_main(inst, node, event_id, event_data):
    ni = node[N_NODE_INDEX]
    ns = inst["node_states"][ni]

    if not (ns["flags"] & FLAG_ACTIVE):
        return CT_CONTINUE

    fn = inst["mod"]["main_fns"].get(node[N_FUNC_INDEX])
    assert fn is not None, "invoke_main: no function for: " + str(node[N_FUNC_NAME])

    # Guard check (before init and tick)
    guard_idx = node[N_GUARD_INDEX]
    if guard_idx >= 0 and event_id != CT_EVENT_INIT and event_id != CT_EVENT_TERMINATE:
        guard_fn = inst["mod"]["pred_fns"].get(guard_idx)
        assert guard_fn is not None, "invoke_main: no guard for index: %d" % guard_idx
        if not guard_fn(inst, node):
            return CT_HALT  # guard failed, skip this node

    # INIT on first activation
    if not (ns["flags"] & FLAG_INITIALIZED):
        ns["flags"] |= FLAG_INITIALIZED
        fn(inst, node, CT_EVENT_INIT, None)

    # TICK (or event)
    result = fn(inst, node, event_id, event_data)
    if result is None:
        result = CT_CONTINUE

    # DISABLE -> terminate and deactivate
    if result == CT_DISABLE:
        fn(inst, node, CT_EVENT_TERMINATE, None)
        ns["flags"] &= ~FLAG_ACTIVE

    # TERMINATE -> terminate
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
    """Get config value from node data tuple (0-based)."""
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

    return invoke_main(inst, root, event_id, event_data)


# ==========================================================================
# Helpers
# ==========================================================================
def merge_fns(*tables):
    out = {}
    for t in tables:
        out.update(t)
    return out


# **************************************************************************
# COMPOSITE BUILTINS
# **************************************************************************

# ==========================================================================
# CT_SEQUENCE — execute children in order
# Child returns CONTINUE: advance to next
# Child returns HALT: pause, resume next tick
# Child returns TERMINATE/DISABLE: skip, advance
# Child returns RESET: restart sequence
# ==========================================================================
def ct_sequence(inst, node, event_id, event_data):
    ns = inst["node_states"][node[N_NODE_INDEX]]
    children = node[N_CHILDREN]
    n = len(children)

    if event_id == CT_EVENT_INIT:
        ns["state"] = 0
        return CT_CONTINUE

    if event_id == CT_EVENT_TERMINATE:
        s = ns["state"]
        if s < n:
            child_terminate(inst, node, s)
        ns["state"] = 0
        return CT_CONTINUE

    while ns["state"] < n:
        s = ns["state"]
        r = child_invoke(inst, node, s, event_id, event_data)

        if r == CT_HALT:
            return CT_HALT  # child still running, pause
        if r == CT_CONTINUE:
            # child done, advance
            ns["state"] = s + 1
            continue
        if r == CT_TERMINATE:
            child_terminate(inst, node, s)
            return CT_TERMINATE  # propagate hard termination
        if r == CT_DISABLE:
            child_terminate(inst, node, s)
            ns["state"] = s + 1
            continue
        if r == CT_RESET:
            children_terminate_all(inst, node)
            children_reset_all(inst, node)
            ns["state"] = 0
            return CT_HALT
        if r == CT_SKIP_CONTINUE:
            ns["state"] = s + 1
            continue
        if r == CT_TERMINATE_SYSTEM:
            return CT_TERMINATE_SYSTEM
        # Unknown — treat as continue
        ns["state"] = s + 1
        continue

    # All children done
    return CT_CONTINUE


# ==========================================================================
# CT_SELECTOR — try children until one succeeds
# Child returns CONTINUE: selector succeeds
# Child returns HALT: pause, resume next tick
# Child returns TERMINATE/DISABLE/SKIP: try next child
# ==========================================================================
def ct_selector(inst, node, event_id, event_data):
    ns = inst["node_states"][node[N_NODE_INDEX]]
    children = node[N_CHILDREN]
    n = len(children)

    if event_id == CT_EVENT_INIT:
        ns["state"] = 0
        return CT_CONTINUE

    if event_id == CT_EVENT_TERMINATE:
        s = ns["state"]
        if s < n:
            child_terminate(inst, node, s)
        ns["state"] = 0
        return CT_CONTINUE

    while ns["state"] < n:
        s = ns["state"]
        r = child_invoke(inst, node, s, event_id, event_data)

        if r == CT_CONTINUE:
            return CT_CONTINUE  # child succeeded, selector succeeds
        if r == CT_HALT:
            return CT_HALT  # child still running
        if r == CT_DISABLE or r == CT_TERMINATE or r == CT_SKIP_CONTINUE:
            child_terminate(inst, node, s)
            child_reset(inst, node, s)
            ns["state"] = s + 1
            continue
        if r == CT_TERMINATE_SYSTEM:
            return CT_TERMINATE_SYSTEM
        ns["state"] = s + 1
        continue

    # All children failed
    return CT_DISABLE


# ==========================================================================
# CT_PARALLEL — tick all children each tick
# Completes when all children complete (AND policy)
# Any child TERMINATE_SYSTEM -> immediate propagation
# ==========================================================================
def ct_parallel(inst, node, event_id, event_data):
    children = node[N_CHILDREN]
    n = len(children)

    if event_id == CT_EVENT_INIT:
        for i in range(n):
            child_reset(inst, node, i)
        return CT_CONTINUE

    if event_id == CT_EVENT_TERMINATE:
        children_terminate_all(inst, node)
        return CT_CONTINUE

    active = 0

    for i in range(n):
        child = children[i]
        cns = inst["node_states"][child[N_NODE_INDEX]]
        if not (cns["flags"] & FLAG_ACTIVE):
            continue

        r = child_invoke(inst, node, i, event_id, event_data)

        if r == CT_TERMINATE_SYSTEM:
            return CT_TERMINATE_SYSTEM
        if r == CT_CONTINUE or r == CT_HALT:
            active += 1
        elif r == CT_DISABLE or r == CT_TERMINATE:
            child_terminate(inst, node, i)
        elif r == CT_RESET:
            child_terminate(inst, node, i)
            child_reset(inst, node, i)
            active += 1
        else:
            active += 1

    if active == 0:
        return CT_CONTINUE  # all children done
    return CT_HALT  # still running


# ==========================================================================
# CT_STATE_MACHINE — dispatch based on blackboard field
# node_data[0] = field_name
# Children are case branches, node_data[1..] = case values (-1 = default)
# ==========================================================================
_SM_NO_ACTION = 0xFFFF

def ct_state_machine(inst, node, event_id, event_data):
    ns = inst["node_states"][node[N_NODE_INDEX]]
    children = node[N_CHILDREN]
    nd = node[N_NODE_DATA]

    if event_id == CT_EVENT_INIT:
        ns["user_data"] = _SM_NO_ACTION
        return CT_CONTINUE

    if event_id == CT_EVENT_TERMINATE:
        prev = ns["user_data"]
        if prev != _SM_NO_ACTION and prev < len(children):
            child_terminate(inst, node, prev)
        ns["user_data"] = _SM_NO_ACTION
        return CT_CONTINUE

    # Read field value
    field_name = nd[0]
    val = inst["blackboard"].get(field_name, 0)

    # Find matching case: nd[1..] are case values, children[0..] are actions
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
        raise RuntimeError("ct_state_machine: no case for %d" % val)

    # Handle branch change
    prev = ns["user_data"]
    if action_idx != prev:
        if prev != _SM_NO_ACTION and prev < len(children):
            child_terminate(inst, node, prev)
            child_reset_recursive(inst, node, prev)
        child_reset_recursive(inst, node, action_idx)
        ns["user_data"] = action_idx

    r = child_invoke(inst, node, action_idx, event_id, event_data)

    # TERMINATE/TERMINATE_SYSTEM propagate — state machine ends
    if r == CT_TERMINATE or r == CT_TERMINATE_SYSTEM:
        return r
    # Everything else: state machine keeps running (re-evaluate next tick)
    if r == CT_CONTINUE or r == CT_DISABLE:
        # Case child completed — reset it for potential re-entry
        child_terminate(inst, node, action_idx)
        child_reset_recursive(inst, node, action_idx)
    return CT_HALT


# ==========================================================================
# CT_FOR_LOOP — repeat child N times
# node_data[0] = iteration count
# ==========================================================================
def ct_for_loop(inst, node, event_id, event_data):
    ns = inst["node_states"][node[N_NODE_INDEX]]

    if event_id == CT_EVENT_INIT:
        ns["state"] = 0
        ns["user_data"] = node[N_NODE_DATA][0]  # max iterations
        return CT_CONTINUE

    if event_id == CT_EVENT_TERMINATE:
        child_terminate(inst, node, 0)
        ns["state"] = 0
        return CT_CONTINUE

    if ns["state"] >= ns["user_data"]:
        return CT_CONTINUE  # all iterations done

    r = child_invoke(inst, node, 0, event_id, event_data)

    if r == CT_HALT:
        return CT_HALT  # child still running
    if r == CT_CONTINUE or r == CT_DISABLE:
        # Iteration complete, start next
        child_terminate(inst, node, 0)
        child_reset(inst, node, 0)
        ns["state"] += 1
        if ns["state"] >= ns["user_data"]:
            return CT_CONTINUE  # all done
        return CT_HALT  # more iterations, resume next tick
    if r == CT_TERMINATE or r == CT_TERMINATE_SYSTEM:
        return r
    if r == CT_RESET:
        child_terminate(inst, node, 0)
        child_reset(inst, node, 0)
        ns["state"] = 0
        return CT_HALT

    return CT_HALT


# ==========================================================================
# CT_SUPERVISOR — parallel with failure tracking
# Monitors all children. If a child terminates/disables,
# logs failure. If too many failures in window, escalates.
# node_data[0] = max_resets, node_data[1] = reset_window_ticks
# ==========================================================================
def ct_supervisor(inst, node, event_id, event_data):
    # Supervisor is parallel + failure tracking
    # For now, just run as parallel — add failure logic later
    return ct_parallel(inst, node, event_id, event_data)


# ==========================================================================
# Leaf helpers
# ==========================================================================
def ct_nop(inst, node, event_id, event_data):
    """Do nothing, immediately complete."""
    if event_id == CT_EVENT_INIT or event_id == CT_EVENT_TERMINATE:
        return CT_CONTINUE
    return CT_DISABLE


def ct_log(inst, node, event_id, event_data):
    """Log message from node_data[0], then complete."""
    if event_id == CT_EVENT_INIT or event_id == CT_EVENT_TERMINATE:
        return CT_CONTINUE
    msg = node[N_NODE_DATA][0] if node[N_NODE_DATA] else "(nil)"
    print("[CT_LOG] " + str(msg))
    return CT_DISABLE


def ct_set_field(inst, node, event_id, event_data):
    """Set blackboard field. node_data[0]=name, node_data[1]=value."""
    if event_id == CT_EVENT_INIT or event_id == CT_EVENT_TERMINATE:
        return CT_CONTINUE
    nd = node[N_NODE_DATA]
    inst["blackboard"][nd[0]] = nd[1]
    return CT_DISABLE


def ct_return_terminate(inst, node, event_id, event_data):
    """Return TERMINATE to end parent composite."""
    if event_id == CT_EVENT_INIT or event_id == CT_EVENT_TERMINATE:
        return CT_CONTINUE
    return CT_TERMINATE


def ct_tick_delay(inst, node, event_id, event_data):
    """Wait N ticks. node_data[0]=tick_count."""
    ns = inst["node_states"][node[N_NODE_INDEX]]
    if event_id == CT_EVENT_INIT:
        ns["user_data"] = node[N_NODE_DATA][0]
        return CT_CONTINUE
    if event_id == CT_EVENT_TERMINATE:
        return CT_CONTINUE
    remaining = ns["user_data"]
    if remaining > 0:
        ns["user_data"] = remaining - 1
        return CT_HALT
    return CT_CONTINUE


def ct_terminate_system(inst, node, event_id, event_data):
    """Return TERMINATE_SYSTEM to shut down the entire runtime."""
    if event_id == CT_EVENT_INIT or event_id == CT_EVENT_TERMINATE:
        return CT_CONTINUE
    return CT_TERMINATE_SYSTEM


# ==========================================================================
# S-Engine bridge
# node_data[0] = S-Engine module (Python module object or name)
# node_data[1] = tree name within that module
# node_data[2] = blackboard field to store instance (optional)
#
# On INIT: loads S-Engine module, creates tree instance, stores in bb
# On TICK: ticks the S-Engine tree, maps result codes
# On TERMINATE: cleans up
# ==========================================================================
_SE_RESULT_MAP = None  # lazy init

def _map_se_result(se_result):
    """Map S-Engine result code to ChainTree result code."""
    # SE codes 0-5 map directly to CT codes 0-5
    if se_result <= 5:
        return se_result
    # SE_FUNCTION_* (6-11) -> CT_HALT (still running) or CT_TERMINATE
    if se_result == 8:   # SE_FUNCTION_TERMINATE
        return CT_TERMINATE
    if se_result == 10:  # SE_FUNCTION_DISABLE
        return CT_DISABLE
    if 6 <= se_result <= 11:
        return CT_HALT
    # SE_PIPELINE_* (12-17) -> similar mapping
    if se_result == 14:  # SE_PIPELINE_TERMINATE
        return CT_TERMINATE
    if se_result == 16:  # SE_PIPELINE_DISABLE
        return CT_DISABLE
    if 12 <= se_result <= 17:
        return CT_HALT
    return CT_HALT


def ct_se_tick(inst, node, event_id, event_data):
    """Tick an S-Engine tree from a ChainTree leaf node.
    node_data[0] = se_module_key (key into inst["user_ctx"]["se_modules"])
    node_data[1] = tree_name
    node_data[2] = bb_field for instance (optional)

    inst["user_ctx"] must contain:
      "se_runtime": the se_runtime module
      "se_modules": {key: {"module_data": ..., "builtins": ..., "user_fns": ...}}
    """
    ns = inst["node_states"][node[N_NODE_INDEX]]
    nd = node[N_NODE_DATA]

    if event_id == CT_EVENT_INIT:
        ctx = inst["user_ctx"]
        assert ctx is not None, "ct_se_tick: no user_ctx"
        se_rt = ctx["se_runtime"]
        se_mod_key = nd[0]
        se_tree_name = nd[1]
        se_mod_cfg = ctx["se_modules"][se_mod_key]

        fns = dict(se_mod_cfg["builtins"])
        if se_mod_cfg.get("user_fns"):
            fns.update(se_mod_cfg["user_fns"])
        se_mod = se_rt.new_module(se_mod_cfg["module_data"], fns)
        se_inst = se_rt.new_instance(se_mod, se_tree_name)

        ns["se_inst"] = se_inst
        ns["se_rt"] = se_rt
        if len(nd) > 2 and nd[2]:
            inst["blackboard"][nd[2]] = se_inst
        return CT_CONTINUE

    if event_id == CT_EVENT_TERMINATE:
        ns.pop("se_inst", None)
        ns.pop("se_rt", None)
        if len(nd) > 2 and nd[2]:
            inst["blackboard"][nd[2]] = None
        return CT_CONTINUE

    # TICK
    se_inst = ns.get("se_inst")
    se_rt = ns.get("se_rt")
    assert se_inst is not None, "ct_se_tick: no S-Engine instance"

    result = se_rt.tick_once(se_inst, se_rt.SE_EVENT_TICK, None)

    # Drain S-Engine event queue
    while se_rt.event_count(se_inst) > 0:
        tt, eid, edata = se_rt.event_pop(se_inst)
        saved = se_inst["tick_type"]
        se_inst["tick_type"] = tt
        er = se_rt.tick_once(se_inst, eid, edata)
        se_inst["tick_type"] = saved
        mapped = _map_se_result(er)
        if mapped in (CT_TERMINATE, CT_DISABLE, CT_TERMINATE_SYSTEM):
            return mapped

    return _map_se_result(result)


# ==========================================================================
# Builtin table
# ==========================================================================
builtins = {
    "ct_sequence": ct_sequence,
    "ct_selector": ct_selector,
    "ct_parallel": ct_parallel,
    "ct_state_machine": ct_state_machine,
    "ct_for_loop": ct_for_loop,
    "ct_supervisor": ct_supervisor,
    "ct_nop": ct_nop,
    "ct_log": ct_log,
    "ct_set_field": ct_set_field,
    "ct_tick_delay": ct_tick_delay,
    "ct_return_terminate": ct_return_terminate,
    "ct_terminate_system": ct_terminate_system,
    "ct_se_tick": ct_se_tick,
}
