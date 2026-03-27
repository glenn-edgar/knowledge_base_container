# ==========================================================================
# se_runtime.py
# MicroPython S-Expression Engine Runtime
#
# Operates directly on the constant tuple structures from _module_mpy.py.
# No heap wrapping — node tuples stay in ROM when frozen.
#
# Execution model:
#   External caller owns the tick loop.
#   tick_once() runs one main tick (does NOT drain event queue).
#   Builtins signature: fn(inst, node, event_id, event_data)
#     - main (m_call/pt_m_call): called on INIT, TICK, TERMINATE events.
#     - oneshot (o_call/io_call): fn(inst, node)  -- no event, no return
#     - pred (p_call/p_call_composite): fn(inst, node) -> bool
#
# API:
#   import se_runtime
#   mod  = se_runtime.new_module(module_data)
#   inst = se_runtime.new_instance(mod, "tree_name")
#   result = se_runtime.tick_once(inst)
# ==========================================================================

import time

# ==========================================================================
# Result codes  (s_engine_types.h)
# ==========================================================================
SE_CONTINUE               = 0
SE_HALT                   = 1
SE_TERMINATE              = 2
SE_RESET                  = 3
SE_DISABLE                = 4
SE_SKIP_CONTINUE          = 5

SE_FUNCTION_CONTINUE      = 6
SE_FUNCTION_HALT          = 7
SE_FUNCTION_TERMINATE     = 8
SE_FUNCTION_RESET         = 9
SE_FUNCTION_DISABLE       = 10
SE_FUNCTION_SKIP_CONTINUE = 11

SE_PIPELINE_CONTINUE      = 12
SE_PIPELINE_HALT          = 13
SE_PIPELINE_TERMINATE     = 14
SE_PIPELINE_RESET         = 15
SE_PIPELINE_DISABLE       = 16
SE_PIPELINE_SKIP_CONTINUE = 17

# Special event IDs
SE_EVENT_TICK      = 0xFFFF
SE_EVENT_INIT      = 0xFFFE
SE_EVENT_TERMINATE = 0xFFFD

# ==========================================================================
# Node state flags  (s_engine_types.h)
# ==========================================================================
FLAG_ACTIVE      = 0x01
FLAG_INITIALIZED = 0x02
FLAG_EVER_INIT   = 0x04
FLAG_ERROR       = 0x08
FLAGS_SYSTEM     = 0x0F
FLAGS_USER       = 0xF0

# ==========================================================================
# Event queue size
# ==========================================================================
EVENT_QUEUE_SIZE = 16

# ==========================================================================
# Node tuple indices (match _module_mpy.py constants)
# ==========================================================================
N_FUNC_NAME     = 0
N_FUNC_HASH     = 1
N_FUNC_INDEX    = 2
N_NODE_INDEX    = 3
N_CALL_TYPE     = 4
N_ORDER         = 5
N_PARAM_COUNT   = 6
N_POINTER_INDEX = 7
N_PARAMS        = 8
N_CHILDREN      = 9

# Tree tuple indices
T_NAME           = 0
T_NAME_HASH      = 1
T_NODE_COUNT     = 2
T_POINTER_COUNT  = 3
T_RECORD_NAME    = 4
T_DEFAULTS_INDEX = 5
T_NODES          = 6

# Param tuple indices
P_TYPE  = 0
P_VALUE = 1
P_ORDER = 2

# ==========================================================================
# Default time function
# ==========================================================================
def default_get_time():
    return time.ticks_ms() / 1000.0


# ==========================================================================
# Node state helpers
# ==========================================================================
def get_ns(inst, node_index):
    return inst["node_states"][node_index]


# ==========================================================================
# Event queue (circular buffer, mirrors s_engine_event_queue.c)
# ==========================================================================
def _eq_init(inst):
    inst["event_queue"] = [None] * EVENT_QUEUE_SIZE
    inst["event_queue_head"] = 0
    inst["event_queue_count"] = 0


def event_push(inst, tick_type, event_id, event_data):
    assert inst["event_queue_count"] < EVENT_QUEUE_SIZE, "se_runtime: event_queue full"
    tail = (inst["event_queue_head"] + inst["event_queue_count"]) % EVENT_QUEUE_SIZE
    inst["event_queue"][tail] = (tick_type, event_id, event_data)
    inst["event_queue_count"] += 1


def event_pop(inst):
    assert inst["event_queue_count"] > 0, "se_runtime: event_queue empty"
    e = inst["event_queue"][inst["event_queue_head"]]
    inst["event_queue_head"] = (inst["event_queue_head"] + 1) % EVENT_QUEUE_SIZE
    inst["event_queue_count"] -= 1
    return e


def event_count(inst):
    return inst["event_queue_count"]


def event_clear(inst):
    inst["event_queue_head"] = 0
    inst["event_queue_count"] = 0


# ==========================================================================
# new_module: build runtime structure from constant module data.
# No tree wrapping — nodes stay as ROM tuples.
# ==========================================================================
def new_module(module_data, initial_fns=None):
    mod = {
        "module_data": module_data,
        "oneshot_fns": {},
        "main_fns": {},
        "pred_fns": {},
        "_oneshot_idx": {},
        "_main_idx": {},
        "_pred_idx": {},
        "trees_by_hash": {},
        "get_time": default_get_time,
    }

    # Build name->index maps for register_fns
    for i, name in enumerate(module_data.oneshot_funcs):
        mod["_oneshot_idx"][name.upper()] = i
    for i, name in enumerate(module_data.main_funcs):
        mod["_main_idx"][name.upper()] = i
    for i, name in enumerate(module_data.pred_funcs):
        mod["_pred_idx"][name.upper()] = i

    # Build hash->tree_index map and tree_name->tree_index map
    mod["_tree_by_name"] = {}
    for i, tname in enumerate(module_data.tree_order):
        tree = module_data.trees[i]
        mod["_tree_by_name"][tname] = i
        mod["trees_by_hash"][tree[T_NAME_HASH]] = tname

    if initial_fns:
        register_fns(mod, initial_fns)

    return mod


# ==========================================================================
# register_fns: add functions to module
# ==========================================================================
def register_fns(mod, fns):
    for raw_name, fn in fns.items():
        uname = raw_name.upper()
        idx = mod["_oneshot_idx"].get(uname)
        if idx is not None:
            mod["oneshot_fns"][idx] = fn
        idx = mod["_main_idx"].get(uname)
        if idx is not None:
            mod["main_fns"][idx] = fn
        idx = mod["_pred_idx"].get(uname)
        if idx is not None:
            mod["pred_fns"][idx] = fn


# ==========================================================================
# validate_module
# ==========================================================================
def validate_module(mod):
    missing = []
    md = mod["module_data"]
    for i, name in enumerate(md.oneshot_funcs):
        if i not in mod["oneshot_fns"]:
            missing.append(("oneshot", name))
    for i, name in enumerate(md.main_funcs):
        if i not in mod["main_fns"]:
            missing.append(("main", name))
    for i, name in enumerate(md.pred_funcs):
        if i not in mod["pred_fns"]:
            missing.append(("pred", name))
    return (len(missing) == 0), missing


# ==========================================================================
# new_instance: allocate node_states, blackboard, event queue
# ==========================================================================
def new_instance(mod, tree_name):
    ok, missing = validate_module(mod)
    if not ok:
        lines = ["new_instance: unregistered functions:"]
        for kind, name in missing:
            lines.append("  [%s] %s" % (kind, name))
        raise RuntimeError("\n".join(lines))

    tree_idx = mod["_tree_by_name"].get(tree_name)
    assert tree_idx is not None, "new_instance: unknown tree: " + str(tree_name)
    tree = mod["module_data"].trees[tree_idx]

    inst = {
        "mod": mod,
        "tree": tree,
        "node_states": {},
        "node_count": tree[T_NODE_COUNT],
        "pointer_array": {},
        "slot_flags": {},
        "pointer_count": tree[T_POINTER_COUNT],
        "blackboard": {},
        "current_node_index": 0,
        "current_event_id": 0,
        "current_event_data": None,
        "in_pointer_call": False,
        "pointer_base": 0,
        "stack": None,
        "tick_type": 0,
        "user_ctx": None,
        "current_dict": None,
    }

    # Initialize node states
    for i in range(tree[T_NODE_COUNT]):
        inst["node_states"][i] = {"flags": FLAG_ACTIVE, "state": 0, "user_data": 0}

    # Initialize pointer array
    for i in range(tree[T_POINTER_COUNT]):
        inst["pointer_array"][i] = {"ptr": None, "u64": 0, "i64": 0, "f64": 0.0}

    # Initialize blackboard from record descriptor
    record_name = tree[T_RECORD_NAME]
    if record_name:
        md = mod["module_data"]
        if hasattr(md, "records") and md.records:
            # records is a tuple of record tuples; find by name
            for rec in md.records:
                if rec[0] == record_name:  # rec[0] = name
                    if len(rec) > 4:  # rec[4] = fields tuple
                        for field in rec[4]:
                            # field[0] = name, default to 0
                            inst["blackboard"][field[0]] = 0
                    break

    _eq_init(inst)
    return inst


# ==========================================================================
# Core invocation (mirrors s_engine_eval.c)
# Node is a ROM tuple — indexed via N_* constants.
# ==========================================================================
def invoke_main(inst, node, event_id, event_data):
    ni = node[N_NODE_INDEX]
    ns = get_ns(inst, ni)

    if not (ns["flags"] & FLAG_ACTIVE):
        return SE_PIPELINE_CONTINUE

    fn = inst["mod"]["main_fns"].get(node[N_FUNC_INDEX])
    assert fn is not None, "invoke_main: no function for: " + str(node[N_FUNC_NAME])

    saved_pb = inst["pointer_base"]
    saved_ipc = inst["in_pointer_call"]
    if node[N_CALL_TYPE] == "pt_m_call":
        inst["in_pointer_call"] = True
        pi = node[N_POINTER_INDEX]
        inst["pointer_base"] = pi if pi is not None else 0

    if not (ns["flags"] & FLAG_INITIALIZED):
        ns["flags"] |= FLAG_INITIALIZED
        inst["current_node_index"] = ni
        fn(inst, node, SE_EVENT_INIT, None)

    inst["current_node_index"] = ni
    result = fn(inst, node, event_id, event_data)
    if result is None:
        result = SE_PIPELINE_CONTINUE

    if result == SE_PIPELINE_DISABLE:
        inst["current_node_index"] = ni
        fn(inst, node, SE_EVENT_TERMINATE, None)
        ns["flags"] &= ~FLAG_ACTIVE

    inst["pointer_base"] = saved_pb
    inst["in_pointer_call"] = saved_ipc

    return result


def invoke_oneshot(inst, node):
    ni = node[N_NODE_INDEX]
    ns = get_ns(inst, ni)
    survives = (node[N_CALL_TYPE] == "io_call")
    check = FLAG_EVER_INIT if survives else FLAG_INITIALIZED
    if ns["flags"] & check:
        return
    ns["flags"] |= check
    fn = inst["mod"]["oneshot_fns"].get(node[N_FUNC_INDEX])
    assert fn is not None, "invoke_oneshot: no function for: " + str(node[N_FUNC_NAME])
    inst["current_node_index"] = ni
    fn(inst, node)


def invoke_pred(inst, node):
    fn = inst["mod"]["pred_fns"].get(node[N_FUNC_INDEX])
    assert fn is not None, "invoke_pred: no function for: " + str(node[N_FUNC_NAME])
    inst["current_node_index"] = node[N_NODE_INDEX]
    return True if fn(inst, node) else False


def invoke_any(inst, node, event_id, event_data):
    ct = node[N_CALL_TYPE]
    if ct == "m_call" or ct == "pt_m_call":
        return invoke_main(inst, node, event_id, event_data)
    elif ct == "o_call" or ct == "io_call":
        invoke_oneshot(inst, node)
        return SE_PIPELINE_CONTINUE
    elif ct == "p_call" or ct == "p_call_composite":
        return SE_PIPELINE_CONTINUE if invoke_pred(inst, node) else SE_PIPELINE_HALT
    raise ValueError("invoke_any: unknown call_type: " + str(ct))


# ==========================================================================
# Child helpers (mirrors s_engine_node.c)
# All child indices are 0-based.
# node[N_CHILDREN] is a ROM tuple of child tuples.
# ==========================================================================
def child_count(node):
    return len(node[N_CHILDREN])


def child_invoke(inst, node, idx, event_id, event_data):
    children = node[N_CHILDREN]
    assert idx < len(children), "child_invoke: bad index %d" % idx
    return invoke_any(inst, children[idx], event_id, event_data)


def child_invoke_pred(inst, node, idx):
    children = node[N_CHILDREN]
    assert idx < len(children), "child_invoke_pred: bad index %d" % idx
    return invoke_pred(inst, children[idx])


def child_invoke_oneshot(inst, node, idx):
    children = node[N_CHILDREN]
    assert idx < len(children), "child_invoke_oneshot: bad index %d" % idx
    invoke_oneshot(inst, children[idx])


def child_terminate(inst, node, idx):
    children = node[N_CHILDREN]
    if idx >= len(children):
        return
    child = children[idx]
    ct = child[N_CALL_TYPE]
    if ct == "m_call" or ct == "pt_m_call":
        ns = get_ns(inst, child[N_NODE_INDEX])
        if ns["flags"] & FLAG_INITIALIZED:
            fn = inst["mod"]["main_fns"].get(child[N_FUNC_INDEX])
            inst["current_node_index"] = child[N_NODE_INDEX]
            fn(inst, child, SE_EVENT_TERMINATE, None)
            ns["flags"] &= FLAGS_USER
            ns["state"] = 0
            ns["user_data"] = 0


def child_reset(inst, node, idx):
    children = node[N_CHILDREN]
    if idx >= len(children):
        return
    ns = get_ns(inst, children[idx][N_NODE_INDEX])
    ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE
    ns["state"] = 0
    ns["user_data"] = 0


def _reset_recursive(inst, node):
    ns = get_ns(inst, node[N_NODE_INDEX])
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
        ns = get_ns(inst, child[N_NODE_INDEX])
        ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE | FLAG_EVER_INIT
        ns["state"] = 0
        ns["user_data"] = 0


def children_reset_all(inst, node):
    for child in node[N_CHILDREN]:
        ns = get_ns(inst, child[N_NODE_INDEX])
        ns["flags"] = (ns["flags"] & FLAGS_USER) | FLAG_ACTIVE | FLAG_EVER_INIT
        ns["state"] = 0
        ns["user_data"] = 0


# ==========================================================================
# Parameter accessors
# node[N_PARAMS] is a ROM tuple of param tuples: (type, value, order)
# Param indices are 1-based (matching Lua convention)
# ==========================================================================
def param(node, i):
    return node[N_PARAMS][i - 1]


def param_int(node, i):
    v = node[N_PARAMS][i - 1][P_VALUE]
    if isinstance(v, str):
        return int(float(v))
    if isinstance(v, float):
        return int(v)
    return v


def param_float(node, i):
    v = node[N_PARAMS][i - 1][P_VALUE]
    if isinstance(v, str):
        return float(v)
    return float(v) if v is not None else 0.0


def param_str(node, i):
    p = node[N_PARAMS][i - 1]
    if p[P_TYPE] == "str_hash":
        v = p[P_VALUE]
        return v[1] if isinstance(v, tuple) else str(v)
    return p[P_VALUE]


def param_field_name(node, i):
    return node[N_PARAMS][i - 1][P_VALUE]


def field_get(inst, node, i):
    v = inst["blackboard"].get(node[N_PARAMS][i - 1][P_VALUE])
    if isinstance(v, str):
        try:
            return int(v)
        except (ValueError, TypeError):
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return v


def field_set(inst, node, i, value):
    inst["blackboard"][node[N_PARAMS][i - 1][P_VALUE]] = value


def param_result(node, i):
    return node[N_PARAMS][i - 1][P_VALUE]


# ==========================================================================
# Extended node-state accessors
# ==========================================================================
def get_u64(inst, node):
    return inst["pointer_array"][inst["pointer_base"]].get("u64", 0)

def set_u64(inst, node, v):
    inst["pointer_array"][inst["pointer_base"]]["u64"] = v

def get_f64(inst, node):
    return inst["pointer_array"][inst["pointer_base"]].get("f64", 0.0)

def set_f64(inst, node, v):
    inst["pointer_array"][inst["pointer_base"]]["f64"] = v

def get_user_u64(inst, node):
    return get_ns(inst, node[N_NODE_INDEX]).get("user_u64", 0)

def set_user_u64(inst, node, v):
    get_ns(inst, node[N_NODE_INDEX])["user_u64"] = v

def get_user_f64(inst, node):
    return get_ns(inst, node[N_NODE_INDEX]).get("user_f64", 0.0)

def set_user_f64(inst, node, v):
    get_ns(inst, node[N_NODE_INDEX])["user_f64"] = v

def get_state(inst, node):
    return get_ns(inst, node[N_NODE_INDEX])["state"]

def set_state(inst, node, v):
    get_ns(inst, node[N_NODE_INDEX])["state"] = v


# ==========================================================================
# tick_once: the ONLY tick entry point
# ==========================================================================
def tick_once(inst, event_id=None, event_data=None):
    if event_id is None:
        event_id = SE_EVENT_TICK

    tree = inst["tree"]
    root = tree[T_NODES][0]

    root_ns = get_ns(inst, root[N_NODE_INDEX])
    if not (root_ns["flags"] & FLAG_ACTIVE):
        return SE_FUNCTION_TERMINATE

    inst["current_event_id"] = event_id
    inst["current_event_data"] = event_data
    inst["tick_type"] = event_id

    if inst["stack"]:
        inst["stack"]["top"] = 0

    return invoke_main(inst, root, event_id, event_data)


# ==========================================================================
# merge_fns: merge multiple builtin dicts into one
# ==========================================================================
def merge_fns(*tables):
    out = {}
    for t in tables:
        out.update(t)
    return out
