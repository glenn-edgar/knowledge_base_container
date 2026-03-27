# ==========================================================================
# se_builtins_delays.py
# Mirrors s_engine_builtins_delays.h
# ==========================================================================

import se_runtime

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE
_SE_EVENT_TICK = se_runtime.SE_EVENT_TICK
_SE_PIPELINE_CONTINUE = se_runtime.SE_PIPELINE_CONTINUE
_SE_PIPELINE_DISABLE = se_runtime.SE_PIPELINE_DISABLE
_SE_PIPELINE_HALT = se_runtime.SE_PIPELINE_HALT
_SE_PIPELINE_RESET = se_runtime.SE_PIPELINE_RESET
_SE_PIPELINE_TERMINATE = se_runtime.SE_PIPELINE_TERMINATE
_SE_FUNCTION_HALT = se_runtime.SE_FUNCTION_HALT
_SE_DISABLE = se_runtime.SE_DISABLE

_N_NODE_INDEX = se_runtime.N_NODE_INDEX
_N_CHILDREN = se_runtime.N_CHILDREN
_N_PARAMS = se_runtime.N_PARAMS


def se_tick_delay(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        params = node[_N_PARAMS]
        ticks = se_runtime.param_int(node, 1) if params else 0
        se_runtime.set_u64(inst, node, ticks + 1)
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    remaining = se_runtime.get_u64(inst, node)
    if remaining > 0:
        se_runtime.set_u64(inst, node, remaining - 1)
        return _SE_FUNCTION_HALT
    return _SE_PIPELINE_DISABLE


def se_time_delay(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_INIT:
        params = node[_N_PARAMS]
        seconds = se_runtime.param_float(node, 1) if params else 0.0
        if seconds <= 0.0:
            return _SE_PIPELINE_CONTINUE
        se_runtime.set_f64(inst, node, inst["mod"]["get_time"]() + seconds)
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id != _SE_EVENT_TICK:
        return _SE_FUNCTION_HALT
    if inst["mod"]["get_time"]() >= se_runtime.get_f64(inst, node):
        return _SE_PIPELINE_DISABLE
    return _SE_FUNCTION_HALT


def se_wait_event(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        params = node[_N_PARAMS]
        assert len(params) >= 2, "se_wait_event: requires 2 params"
        ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
        ns["wait_target"] = se_runtime.param_int(node, 1)
        ns["wait_remain"] = se_runtime.param_int(node, 2)
        return _SE_PIPELINE_CONTINUE

    ns = se_runtime.get_ns(inst, node[_N_NODE_INDEX])
    remaining = ns.get("wait_remain", 0)
    if remaining == 0:
        return _SE_PIPELINE_DISABLE
    if event_id == ns.get("wait_target"):
        remaining -= 1
        ns["wait_remain"] = remaining
        if remaining == 0:
            return _SE_PIPELINE_DISABLE
    return _SE_PIPELINE_HALT


def se_nop(inst, node, event_id, event_data):
    return _SE_DISABLE


def se_wait(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        children = node[_N_CHILDREN]
        assert len(children) >= 1, "se_wait: requires 1 pred child"
        return _SE_PIPELINE_CONTINUE
    if se_runtime.child_invoke_pred(inst, node, 0):
        return _SE_PIPELINE_DISABLE
    return _SE_PIPELINE_HALT


def se_wait_timeout(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        children = node[_N_CHILDREN]
        params = node[_N_PARAMS]
        assert len(children) >= 2 and len(params) >= 2, \
            "se_wait_timeout: requires [pred] [error] [timeout] [reset_flag]"
        se_runtime.set_user_f64(inst, node, inst["mod"]["get_time"]())
        return _SE_PIPELINE_CONTINUE
    if event_id != _SE_EVENT_TICK:
        return _SE_PIPELINE_HALT

    if se_runtime.child_invoke_pred(inst, node, 0):
        return _SE_PIPELINE_DISABLE

    timeout = se_runtime.param_float(node, 1)
    reset_flag = se_runtime.param_int(node, 2) != 0
    elapsed = inst["mod"]["get_time"]() - se_runtime.get_user_f64(inst, node)

    if elapsed > timeout:
        se_runtime.child_reset(inst, node, 1)
        se_runtime.child_invoke_oneshot(inst, node, 1)
        return _SE_PIPELINE_RESET if reset_flag else _SE_PIPELINE_TERMINATE
    return _SE_PIPELINE_HALT


builtins = {
    "se_tick_delay": se_tick_delay,
    "se_time_delay": se_time_delay,
    "se_wait_event": se_wait_event,
    "se_nop": se_nop,
    "se_wait": se_wait,
    "se_wait_timeout": se_wait_timeout,
}
