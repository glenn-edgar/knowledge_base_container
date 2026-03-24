# ==========================================================================
# se_builtins_verify.py
# Mirrors s_engine_builtins_verify.h
# ==========================================================================

import se_runtime

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE
_SE_EVENT_TICK = se_runtime.SE_EVENT_TICK
_SE_PIPELINE_CONTINUE = se_runtime.SE_PIPELINE_CONTINUE
_SE_PIPELINE_RESET = se_runtime.SE_PIPELINE_RESET
_SE_PIPELINE_TERMINATE = se_runtime.SE_PIPELINE_TERMINATE


def se_verify_and_check_elapsed_time(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        se_runtime.set_user_f64(inst, node, inst["mod"]["get_time"]())
        return _SE_PIPELINE_CONTINUE
    if event_id != _SE_EVENT_TICK:
        return _SE_PIPELINE_CONTINUE

    timeout = se_runtime.param_float(node, 1)
    reset_flag = se_runtime.param_int(node, 2) != 0
    elapsed = inst["mod"]["get_time"]() - se_runtime.get_user_f64(inst, node)

    if elapsed > timeout:
        se_runtime.child_reset(inst, node, 0)
        se_runtime.child_invoke_oneshot(inst, node, 0)
        return _SE_PIPELINE_RESET if reset_flag else _SE_PIPELINE_TERMINATE
    return _SE_PIPELINE_CONTINUE


def se_verify_and_check_elapsed_events(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        se_runtime.set_user_u64(inst, node, 0)
        return _SE_PIPELINE_CONTINUE

    target_event = se_runtime.param_int(node, 1)
    max_count = se_runtime.param_int(node, 2)
    reset_flag = se_runtime.param_int(node, 3) != 0

    if event_id != target_event:
        return _SE_PIPELINE_CONTINUE

    current = se_runtime.get_user_u64(inst, node) + 1
    se_runtime.set_user_u64(inst, node, current)

    if current > max_count:
        se_runtime.child_reset(inst, node, 0)
        se_runtime.child_invoke_oneshot(inst, node, 0)
        return _SE_PIPELINE_RESET if reset_flag else _SE_PIPELINE_TERMINATE
    return _SE_PIPELINE_CONTINUE


def se_verify(inst, node, event_id, event_data):
    if event_id == _SE_EVENT_TERMINATE:
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        return _SE_PIPELINE_CONTINUE
    if event_id != _SE_EVENT_TICK:
        return _SE_PIPELINE_CONTINUE

    reset_flag = se_runtime.param_int(node, 1) != 0
    if se_runtime.child_invoke_pred(inst, node, 0):
        return _SE_PIPELINE_CONTINUE

    se_runtime.child_reset(inst, node, 1)
    se_runtime.child_invoke_oneshot(inst, node, 1)
    return _SE_PIPELINE_RESET if reset_flag else _SE_PIPELINE_TERMINATE


builtins = {
    "se_verify_and_check_elapsed_time": se_verify_and_check_elapsed_time,
    "se_verify_and_check_elapsed_events": se_verify_and_check_elapsed_events,
    "se_verify": se_verify,
}
