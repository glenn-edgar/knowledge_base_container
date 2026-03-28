# ==========================================================================
# se_builtins_spawn.py
# Mirrors s_engine_builtins_spawn.h
# ==========================================================================

import se_runtime

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE
_SE_EVENT_TICK = se_runtime.SE_EVENT_TICK
_SE_PIPELINE_CONTINUE = se_runtime.SE_PIPELINE_CONTINUE
_SE_PIPELINE_DISABLE = se_runtime.SE_PIPELINE_DISABLE
_SE_PIPELINE_TERMINATE = se_runtime.SE_PIPELINE_TERMINATE
_SE_PIPELINE_RESET = se_runtime.SE_PIPELINE_RESET
_FLAG_ACTIVE = se_runtime.FLAG_ACTIVE

_N_NODE_INDEX = se_runtime.N_NODE_INDEX
_N_CHILDREN = se_runtime.N_CHILDREN
_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE


def _result_is_complete(result):
    return result != _SE_PIPELINE_CONTINUE and result != _SE_PIPELINE_DISABLE


def _resolve_tree(inst, node, param_idx):
    params = node[_N_PARAMS]
    p = params[param_idx - 1]
    v = p[_P_VALUE]
    h = v[0] if isinstance(v, tuple) and len(v) == 2 else v
    name = inst["mod"]["trees_by_hash"].get(h)
    assert name is not None, "spawn: unknown tree hash 0x%08x" % h
    return name


def se_spawn_and_tick_tree(inst, node, event_id, event_data):
    params = node[_N_PARAMS]
    assert len(params) >= 2

    slot = inst["pointer_array"][inst["pointer_base"]]
    state = se_runtime.get_state(inst, node)

    if event_id == _SE_EVENT_INIT or state == 0:
        tree_name = _resolve_tree(inst, node, 1)
        stack_size = se_runtime.param_int(node, 2)
        child = se_runtime.new_instance(inst["mod"], tree_name)
        if stack_size > 0:
            import se_stack
            child["stack"] = se_stack.new_stack(stack_size)
        slot["ptr"] = child
        se_runtime.tick_once(child, 0, None)
        se_runtime.set_state(inst, node, 1)
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        slot["ptr"] = None
        return _SE_PIPELINE_CONTINUE

    child = slot["ptr"]
    assert child is not None, "se_spawn_and_tick_tree: child is None"

    result = se_runtime.tick_once(child, event_id, event_data)

    eq_count = se_runtime.event_count(inst)
    while eq_count > 0 and not _result_is_complete(result):
        tick_type, ev_id, ev_data = se_runtime.event_pop(inst)
        saved = child["tick_type"]
        child["tick_type"] = tick_type
        result = se_runtime.tick_once(child, ev_id, ev_data)
        child["tick_type"] = saved
        eq_count = se_runtime.event_count(inst)

    if _result_is_complete(result):
        slot["ptr"] = None
    return result


def se_spawn_tree(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        params = node[_N_PARAMS]
        assert len(params) >= 3
        field_name = se_runtime.param_field_name(node, 1)
        tree_name = _resolve_tree(inst, node, 2)
        stack_size = se_runtime.param_int(node, 3)
        child = se_runtime.new_instance(inst["mod"], tree_name)
        if stack_size > 0:
            import se_stack
            child["stack"] = se_stack.new_stack(stack_size)
        inst["pointer_array"][inst["pointer_base"]]["ptr"] = child
        inst["blackboard"][field_name] = child
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        child = inst["pointer_array"][inst["pointer_base"]]["ptr"]
        if child:
            se_runtime.tick_once(child, _SE_EVENT_TERMINATE, None)
            field_name = se_runtime.param_field_name(node, 1)
            inst["blackboard"][field_name] = None
            inst["pointer_array"][inst["pointer_base"]]["ptr"] = None
        return _SE_PIPELINE_CONTINUE

    return _SE_PIPELINE_CONTINUE


def se_tick_tree(inst, node, event_id, event_data):
    params = node[_N_PARAMS]
    assert len(params) >= 1
    field_name = se_runtime.param_field_name(node, 1)
    child = inst["blackboard"].get(field_name)
    assert child is not None, "se_tick_tree: child nil in field: " + str(field_name)

    if event_id == _SE_EVENT_INIT:
        for i in range(child["node_count"]):
            ns = child["node_states"][i]
            ns["flags"] = _FLAG_ACTIVE
            ns["state"] = 0
            ns["user_data"] = 0
        se_runtime.event_clear(child)
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.tick_once(child, _SE_EVENT_TERMINATE, None)
        return _SE_PIPELINE_CONTINUE

    return se_runtime.tick_once(child, event_id, event_data)


def se_load_function(inst, node):
    field_name = se_runtime.param_field_name(node, 1)
    assert field_name is not None
    children = node[_N_CHILDREN]
    assert len(children) >= 1, "se_load_function: no child subtree"
    child_node = children[0]

    def fn(calling_inst, _exec_node, eid, edata):
        return se_runtime.invoke_any(calling_inst, child_node, eid, edata)

    inst["blackboard"][field_name] = fn


def se_exec_fn(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        field_name = se_runtime.param_field_name(node, 1)
        fn = inst["blackboard"].get(field_name)
        assert callable(fn), "se_exec_fn: not callable: " + str(field_name)
        se_runtime.get_ns(inst, node[_N_NODE_INDEX])["cached_fn"] = fn
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE

    fn = se_runtime.get_ns(inst, node[_N_NODE_INDEX]).get("cached_fn")
    assert fn is not None, "se_exec_fn: nil cached function"
    result = fn(inst, node, event_id, event_data)
    if result is None:
        result = _SE_PIPELINE_CONTINUE
    if result == _SE_PIPELINE_DISABLE:
        result = _SE_PIPELINE_CONTINUE
    return result


def se_exec_dict_internal(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT or event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    d = inst.get("current_dict")
    assert d is not None
    params = node[_N_PARAMS]
    p1 = params[0]
    v = p1[_P_VALUE]
    key = v[0] if isinstance(v, tuple) and len(v) == 2 else v
    entry = d.get(key) or d.get(str(key))
    assert entry is not None and callable(entry)
    result = entry(inst, node, event_id, event_data)
    if result is None:
        result = _SE_PIPELINE_CONTINUE
    if result == _SE_PIPELINE_DISABLE:
        result = _SE_PIPELINE_CONTINUE
    return result


def se_exec_dict_dispatch(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        field_name = se_runtime.param_field_name(node, 1)
        d = inst["blackboard"].get(field_name)
        assert isinstance(d, dict)
        inst["current_dict"] = d
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        inst["current_dict"] = None
        return _SE_PIPELINE_CONTINUE
    d = inst.get("current_dict")
    assert d is not None
    params = node[_N_PARAMS]
    p2 = params[1]
    v = p2[_P_VALUE]
    key = v[0] if isinstance(v, tuple) and len(v) == 2 else v
    entry = d.get(key) or d.get(str(key))
    assert entry is not None and callable(entry)
    result = entry(inst, node, event_id, event_data)
    if result is None:
        result = _SE_PIPELINE_CONTINUE
    if result == _SE_PIPELINE_DISABLE:
        result = _SE_PIPELINE_CONTINUE
    return result


def se_exec_dict_fn_ptr(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        field_name = se_runtime.param_field_name(node, 1)
        d = inst["blackboard"].get(field_name)
        assert isinstance(d, dict)
        inst["current_dict"] = d
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        inst["current_dict"] = None
        return _SE_PIPELINE_CONTINUE
    d = inst.get("current_dict")
    assert d is not None
    key_field = se_runtime.param_field_name(node, 2)
    key = inst["blackboard"].get(key_field)
    assert key is not None
    entry = d.get(key) or d.get(str(key))
    assert entry is not None and callable(entry)
    result = entry(inst, node, event_id, event_data)
    if result is None:
        result = _SE_PIPELINE_CONTINUE
    if result == _SE_PIPELINE_DISABLE:
        result = _SE_PIPELINE_CONTINUE
    return result


builtins = {
    "se_spawn_and_tick_tree": se_spawn_and_tick_tree,
    "se_spawn_tree": se_spawn_tree,
    "se_tick_tree": se_tick_tree,
    "se_load_function": se_load_function,
    "se_exec_fn": se_exec_fn,
    "se_exec_dict_internal": se_exec_dict_internal,
    "se_exec_dict_dispatch": se_exec_dict_dispatch,
    "se_exec_dict_fn_ptr": se_exec_dict_fn_ptr,
}
