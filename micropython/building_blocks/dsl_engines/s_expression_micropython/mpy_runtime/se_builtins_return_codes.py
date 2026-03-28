# ==========================================================================
# se_builtins_return_codes.py
# Mirrors s_engine_builtins_return_codes.h
#
# Trivial main functions that return a fixed result code on every TICK.
# All functions: fn(inst, node, event_id, event_data) -> result_code
# INIT and TERMINATE events are silently ignored.
# ==========================================================================

import se_runtime

_SE_EVENT_INIT = se_runtime.SE_EVENT_INIT
_SE_EVENT_TERMINATE = se_runtime.SE_EVENT_TERMINATE


def _make_return(code):
    def _fn(inst, node, event_id, event_data):
        if event_id == _SE_EVENT_INIT:
            return None
        if event_id == _SE_EVENT_TERMINATE:
            return None
        return code
    return _fn


builtins = {
    "se_return_continue":          _make_return(se_runtime.SE_CONTINUE),
    "se_return_halt":              _make_return(se_runtime.SE_HALT),
    "se_return_terminate":         _make_return(se_runtime.SE_TERMINATE),
    "se_return_reset":             _make_return(se_runtime.SE_RESET),
    "se_return_disable":           _make_return(se_runtime.SE_DISABLE),
    "se_return_skip_continue":     _make_return(se_runtime.SE_SKIP_CONTINUE),
    "se_return_function_continue":      _make_return(se_runtime.SE_FUNCTION_CONTINUE),
    "se_return_function_halt":          _make_return(se_runtime.SE_FUNCTION_HALT),
    "se_return_function_terminate":     _make_return(se_runtime.SE_FUNCTION_TERMINATE),
    "se_return_function_reset":         _make_return(se_runtime.SE_FUNCTION_RESET),
    "se_return_function_disable":       _make_return(se_runtime.SE_FUNCTION_DISABLE),
    "se_return_function_skip_continue": _make_return(se_runtime.SE_FUNCTION_SKIP_CONTINUE),
    "se_return_pipeline_continue":      _make_return(se_runtime.SE_PIPELINE_CONTINUE),
    "se_return_pipeline_halt":          _make_return(se_runtime.SE_PIPELINE_HALT),
    "se_return_pipeline_terminate":     _make_return(se_runtime.SE_PIPELINE_TERMINATE),
    "se_return_pipeline_reset":         _make_return(se_runtime.SE_PIPELINE_RESET),
    "se_return_pipeline_disable":       _make_return(se_runtime.SE_PIPELINE_DISABLE),
    "se_return_pipeline_skip_continue": _make_return(se_runtime.SE_PIPELINE_SKIP_CONTINUE),
}
