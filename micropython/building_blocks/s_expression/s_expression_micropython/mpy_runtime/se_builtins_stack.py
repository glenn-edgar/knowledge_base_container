# ==========================================================================
# se_builtins_stack.py
# Mirrors s_engine_builtins_stack.h
# ==========================================================================

import se_runtime
import se_stack

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE
_SE_PIPELINE_CONTINUE = se_runtime.SE_PIPELINE_CONTINUE
_SE_PIPELINE_DISABLE = se_runtime.SE_PIPELINE_DISABLE
_SE_PIPELINE_TERMINATE = se_runtime.SE_PIPELINE_TERMINATE
_SE_PIPELINE_RESET = se_runtime.SE_PIPELINE_RESET
_SE_PIPELINE_HALT = se_runtime.SE_PIPELINE_HALT
_SE_PIPELINE_SKIP_CONTINUE = se_runtime.SE_PIPELINE_SKIP_CONTINUE
_SE_FUNCTION_HALT = se_runtime.SE_FUNCTION_HALT
_FLAG_ACTIVE = se_runtime.FLAG_ACTIVE
_FLAG_INITIALIZED = se_runtime.FLAG_INITIALIZED

_N_NODE_INDEX = se_runtime.N_NODE_INDEX
_N_CALL_TYPE = se_runtime.N_CALL_TYPE
_N_CHILDREN = se_runtime.N_CHILDREN
_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE


def se_frame_allocate(inst, node, event_id, event_data):
    stk = inst["stack"]
    assert stk is not None, "se_frame_allocate: no stack"

    if event_id == _SE_EVENT_TERMINATE:
        se_runtime.children_terminate_all(inst, node)
        return _SE_PIPELINE_CONTINUE
    if event_id == _SE_EVENT_INIT:
        return _SE_PIPELINE_CONTINUE

    params = node[_N_PARAMS]
    assert len(params) >= 3
    num_params = se_runtime.param_int(node, 1)
    num_locals = se_runtime.param_int(node, 2)
    scratch_depth = se_runtime.param_int(node, 3)
    children = node[_N_CHILDREN]

    assert se_stack.push_frame(stk, num_params, num_locals), "frame push failed"
    for _ in range(scratch_depth):
        se_stack.push_int(stk, 0)

    active_count = 0

    for i in range(len(children)):
        child = children[i]
        ct = child[_N_CALL_TYPE]

        if ct == "m_call" or ct == "pt_m_call":
            if not (se_runtime.get_ns(inst, child[_N_NODE_INDEX])["flags"] & _FLAG_ACTIVE):
                continue

        if ct == "o_call" or ct == "io_call":
            se_runtime.invoke_oneshot(inst, child)
            se_runtime.child_terminate(inst, node, i)
            continue
        if ct == "p_call" or ct == "p_call_composite":
            se_runtime.invoke_pred(inst, child)
            se_runtime.child_terminate(inst, node, i)
            continue

        r = se_runtime.invoke_any(inst, child, event_id, event_data)

        if r == _SE_FUNCTION_HALT:
            se_stack.pop_frame(stk)
            return _SE_PIPELINE_HALT
        if r < _SE_PIPELINE_CONTINUE:
            se_stack.pop_frame(stk)
            return r
        if r == _SE_PIPELINE_CONTINUE:
            active_count += 1
        elif r == _SE_PIPELINE_HALT:
            se_stack.pop_frame(stk)
            return _SE_PIPELINE_CONTINUE
        elif r == _SE_PIPELINE_DISABLE:
            se_runtime.child_terminate(inst, node, i)
        elif r == _SE_PIPELINE_TERMINATE:
            se_runtime.children_terminate_all(inst, node)
            se_stack.pop_frame(stk)
            return _SE_PIPELINE_TERMINATE
        elif r == _SE_PIPELINE_RESET:
            se_runtime.children_terminate_all(inst, node)
            se_runtime.children_reset_all(inst, node)
            se_stack.pop_frame(stk)
            return _SE_PIPELINE_CONTINUE
        elif r == _SE_PIPELINE_SKIP_CONTINUE:
            active_count += 1
            se_stack.pop_frame(stk)
            return _SE_PIPELINE_DISABLE if active_count == 0 else _SE_PIPELINE_CONTINUE
        else:
            active_count += 1

    se_stack.pop_frame(stk)
    return _SE_PIPELINE_DISABLE if active_count == 0 else _SE_PIPELINE_CONTINUE


def se_frame_free(inst, node, event_id, event_data):
    if event_id != _SE_EVENT_INIT:
        return _SE_PIPELINE_CONTINUE
    if inst["stack"]:
        se_stack.pop_frame(inst["stack"])
    return _SE_PIPELINE_CONTINUE


def se_log_stack(inst, node):
    stk = inst["stack"]
    if not stk:
        print("SE_LOG_STACK: no stack")
        return
    print("SE_LOG_STACK: capacity=%d free=%d sp=%d frames=%d" % (
        stk["capacity"], stk["capacity"] - stk["sp"], stk["sp"], stk["frame_count"]))


def se_stack_frame_instance(inst, node, event_id, event_data):
    stk = inst["stack"]
    assert stk is not None

    num_params = se_runtime.param_int(node, 1)
    num_locals = se_runtime.param_int(node, 2)

    if event_id == _SE_EVENT_TERMINATE:
        if se_runtime.get_state(inst, node) == 1:
            params = node[_N_PARAMS]
            ret_p = params[3] if len(params) >= 4 else None
            if ret_p and ret_p[_P_TYPE] == "list_start":
                # Collect return value local indices between list_start and list_end
                temps = []
                for j in range(4, len(params)):
                    rp = params[j]
                    if rp[_P_TYPE] == "list_end":
                        break
                    temps.append(se_stack.get_local(stk, rp[_P_VALUE]))
                se_stack.pop_frame(stk)
                for v in temps:
                    se_stack.push(stk, v)
            else:
                se_stack.pop_frame(stk)
        se_runtime.set_state(inst, node, 0)
        return _SE_PIPELINE_CONTINUE

    if event_id == _SE_EVENT_INIT:
        passed = se_stack.pop(stk) or 0
        assert passed == num_params, \
            "se_stack_frame_instance: param mismatch: expected %d got %d" % (num_params, passed)
        assert se_stack.push_frame(stk, num_params, num_locals), "frame push failed"
        se_runtime.set_state(inst, node, 1)
        return _SE_PIPELINE_CONTINUE

    return _SE_PIPELINE_CONTINUE


builtins = {
    "se_frame_allocate": se_frame_allocate,
    "se_frame_free": se_frame_free,
    "se_log_stack": se_log_stack,
    "se_stack_frame_instance": se_stack_frame_instance,
}
