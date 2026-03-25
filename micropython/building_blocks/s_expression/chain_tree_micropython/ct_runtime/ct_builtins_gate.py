# ==========================================================================
# ct_builtins_gate.py
# CFL gate node — top-level entry point that runs children as parallel
# ==========================================================================
import ct_runtime as rt
from ct_builtins_flow import cfl_column


def cfl_gate_node_main(inst, node, event_id, event_data):
    from ct_builtins_flow import cfl_parallel
    return cfl_parallel(inst, node, event_id, event_data)


def cfl_gate_node_null(inst, node):
    # Guard pass — lets invoke_main call the function.
    # The C column checks this internally with opposite semantics
    # (true=disable), but invoke_main uses true=pass.
    return True


def cfl_column_null(inst, node):
    return True


def cfl_state_machine_null(inst, node):
    return True


builtins = {
    "CFL_GATE_NODE_MAIN": cfl_gate_node_main,
    "CFL_GATE_NODE_NULL": cfl_gate_node_null,
    "CFL_COLUMN_NULL": cfl_column_null,
    "CFL_STATE_MACHINE_NULL": cfl_state_machine_null,
}
