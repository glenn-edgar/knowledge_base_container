# ==========================================================================
# ct_builtins_se_bridge.py
# CFL S-Engine bridge — ticks an S-Engine tree from a ChainTree leaf
#
# Omit this file on Pybricks if not using S-Engine integration.
# ==========================================================================
import ct_runtime as rt


def _map_se_result(se_result):
    if se_result <= 5:
        return se_result
    if se_result == 8:
        return rt.CT_TERMINATE
    if se_result == 10:
        return rt.CT_DISABLE
    if 6 <= se_result <= 11:
        return rt.CT_HALT
    if se_result == 14:
        return rt.CT_TERMINATE
    if se_result == 16:
        return rt.CT_DISABLE
    if 12 <= se_result <= 17:
        return rt.CT_HALT
    return rt.CT_HALT


# ==========================================================================
# CFL_SE_MODULE_LOAD_MAIN — load S-Engine module into user_ctx
# node_dict has ("column_data", ("module_name", name, ...))
# ==========================================================================
def cfl_se_module_load(inst, node, event_id, event_data):
    if event_id == rt.CT_EVENT_INIT or event_id == rt.CT_EVENT_TERMINATE:
        return rt.CT_CONTINUE
    # Module load is a no-op in MicroPython — modules are pre-loaded
    # in user_ctx["se_modules"] by the test harness.
    return rt.CT_DISABLE


# ==========================================================================
# CFL_SE_TREE_LOAD_MAIN — create S-Engine tree instance
# node_dict has ("column_data", ("module_name", ..., "tree_name", ..., "tree_bb_field", ...))
# ==========================================================================
def cfl_se_tree_load(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    nd = node[rt.N_NODE_DATA]

    if event_id == rt.CT_EVENT_INIT:
        ctx = inst["user_ctx"]
        if ctx is None:
            return rt.CT_CONTINUE
        # Parse key-value pairs from node_data
        kv = _parse_kv(nd)
        # Also check inside column_data if present
        col_data = _parse_column_data(nd)
        if col_data:
            kv.update(col_data)
        mod_name = kv.get("module_name")
        tree_name = kv.get("tree_name")
        bb_field = kv.get("bb_field_name") or kv.get("tree_bb_field")
        if not mod_name or not tree_name:
            return rt.CT_CONTINUE

        se_rt = ctx["se_runtime"]
        se_mod_cfg = ctx["se_modules"].get(mod_name)
        if not se_mod_cfg:
            return rt.CT_CONTINUE

        fns = dict(se_mod_cfg["builtins"])
        if se_mod_cfg.get("user_fns"):
            fns.update(se_mod_cfg["user_fns"])
        se_mod = se_rt.new_module(se_mod_cfg["module_data"], fns)
        se_inst = se_rt.new_instance(se_mod, tree_name)

        ns["se_inst"] = se_inst
        ns["se_rt"] = se_rt
        if bb_field:
            inst["blackboard"][bb_field] = se_inst
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        ns.pop("se_inst", None)
        ns.pop("se_rt", None)
        return rt.CT_CONTINUE

    return rt.CT_DISABLE


# ==========================================================================
# CFL_SE_ENGINE_MAIN — composite: load + tick S-Engine tree, run children
# node_dict has ("column_data", ("module_name", ..., "tree_name", ..., "tree_bb_field", ...))
# ==========================================================================
def cfl_se_engine_main(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    nd = node[rt.N_NODE_DATA]

    if event_id == rt.CT_EVENT_INIT:
        ctx = inst["user_ctx"]
        if ctx:
            col_data = _parse_column_data(nd)
            if col_data:
                mod_name = col_data.get("module_name")
                tree_name = col_data.get("tree_name")
                bb_field = col_data.get("tree_bb_field")
                if mod_name and tree_name:
                    se_rt = ctx["se_runtime"]
                    se_mod_cfg = ctx["se_modules"].get(mod_name)
                    if se_mod_cfg:
                        fns = dict(se_mod_cfg["builtins"])
                        if se_mod_cfg.get("user_fns"):
                            fns.update(se_mod_cfg["user_fns"])
                        se_mod = se_rt.new_module(se_mod_cfg["module_data"], fns)
                        se_inst = se_rt.new_instance(se_mod, tree_name)
                        ns["se_inst"] = se_inst
                        ns["se_rt"] = se_rt
                        if bb_field:
                            inst["blackboard"][bb_field] = se_inst
        ns["state"] = 0
        return rt.CT_CONTINUE

    if event_id == rt.CT_EVENT_TERMINATE:
        ns.pop("se_inst", None)
        ns.pop("se_rt", None)
        ns["state"] = 0
        return rt.CT_CONTINUE

    # Tick S-Engine tree
    se_inst = ns.get("se_inst")
    se_rt = ns.get("se_rt")
    if se_inst and se_rt:
        result = se_rt.tick_once(se_inst, se_rt.SE_EVENT_TICK, None)
        while se_rt.event_count(se_inst) > 0:
            tt, eid, edata = se_rt.event_pop(se_inst)
            saved = se_inst["tick_type"]
            se_inst["tick_type"] = tt
            se_rt.tick_once(se_inst, eid, edata)
            se_inst["tick_type"] = saved
        mapped = _map_se_result(result)
        if mapped in (rt.CT_TERMINATE, rt.CT_DISABLE, rt.CT_TERMINATE_SYSTEM):
            return mapped

    return rt.CT_HALT


def _parse_column_data(nd):
    """Extract column_data dict from node_dict key-value tuple pairs."""
    if not nd:
        return None
    # nd is ("column_data", ("module_name", "x", "tree_name", "y", ...))
    for i in range(0, len(nd) - 1, 2):
        if nd[i] == "column_data":
            val = nd[i + 1]
            if isinstance(val, tuple) and len(val) >= 2:
                result = {}
                for j in range(0, len(val) - 1, 2):
                    result[val[j]] = val[j + 1]
                return result
    return None


def _parse_kv(nd):
    """Parse key-value tuple pairs into a dict."""
    d = {}
    if nd:
        for i in range(0, len(nd) - 1, 2):
            d[nd[i]] = nd[i + 1]
    return d


def _tick_se_inst(ns):
    """Tick the S-Engine instance stored in node state, return mapped result."""
    se_inst = ns.get("se_inst")
    se_rt = ns.get("se_rt")
    assert se_inst is not None, "cfl_se_tick: no S-Engine instance"

    result = se_rt.tick_once(se_inst, se_rt.SE_EVENT_TICK, None)

    while se_rt.event_count(se_inst) > 0:
        tt, eid, edata = se_rt.event_pop(se_inst)
        saved = se_inst["tick_type"]
        se_inst["tick_type"] = tt
        er = se_rt.tick_once(se_inst, eid, edata)
        se_inst["tick_type"] = saved
        mapped = _map_se_result(er)
        if mapped == rt.CT_TERMINATE_SYSTEM:
            return rt.CT_TERMINATE_SYSTEM
        if mapped in (rt.CT_TERMINATE, rt.CT_DISABLE):
            return rt.CT_DISABLE  # SE tree done -> child complete

    mapped = _map_se_result(result)
    # SE tree terminal -> CT child complete (DISABLE advances the sequence)
    if mapped == rt.CT_TERMINATE:
        return rt.CT_DISABLE
    return mapped


# ==========================================================================
# CFL_SE_TICK / CFL_SE_TICK_MAIN — tick an S-Engine tree
#
# Two formats supported:
#   Generated: ("tree_bb_field", "se_tree_ptr") — instance already in bb
#   Hand-written: (module_key, tree_name, bb_field) — create on init
# ==========================================================================
def cfl_se_tick(inst, node, event_id, event_data):
    ns = inst["node_states"][node[rt.N_NODE_INDEX]]
    nd = node[rt.N_NODE_DATA]

    # Detect format: key-value pairs have string key at [0]
    kv = _parse_kv(nd)
    bb_field = kv.get("tree_bb_field")

    if bb_field:
        # Generated format: instance loaded by SE_TREE_LOAD or SE_ENGINE
        if event_id == rt.CT_EVENT_INIT:
            se_inst = inst["blackboard"].get(bb_field)
            if se_inst:
                ns["se_inst"] = se_inst
                ctx = inst["user_ctx"]
                ns["se_rt"] = ctx["se_runtime"] if ctx else None
            return rt.CT_CONTINUE
        if event_id == rt.CT_EVENT_TERMINATE:
            ns.pop("se_inst", None)
            ns.pop("se_rt", None)
            return rt.CT_CONTINUE
        return _tick_se_inst(ns)
    else:
        # Hand-written format: (module_key, tree_name, bb_field)
        if event_id == rt.CT_EVENT_INIT:
            ctx = inst["user_ctx"]
            assert ctx is not None, "cfl_se_tick: no user_ctx"
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
            return rt.CT_CONTINUE
        if event_id == rt.CT_EVENT_TERMINATE:
            ns.pop("se_inst", None)
            ns.pop("se_rt", None)
            if len(nd) > 2 and nd[2]:
                inst["blackboard"][nd[2]] = None
            return rt.CT_CONTINUE
        return _tick_se_inst(ns)


builtins = {
    "CFL_SE_MODULE_LOAD_MAIN": cfl_se_module_load,
    "CFL_SE_TREE_LOAD_MAIN": cfl_se_tree_load,
    "CFL_SE_ENGINE_MAIN": cfl_se_engine_main,
    "CFL_SE_TICK": cfl_se_tick,
    "CFL_SE_TICK_MAIN": cfl_se_tick,
    "CT_SE_TICK": cfl_se_tick,
}
