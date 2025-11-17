import json
from s_functions.lisp_sequencer import LispSequencer


def test_run_function(handle, func_type, func_name, node, event_id, event_data, params=[]):
    print(f"Running function: {func_type}{func_name} with params: {params}")
    return "CFL_CONTINUE"

def test_debug_function(handle, message, node, event_id, event_data):
    print(f"Debug message: {message}")

handle = None
seq = LispSequencer(handle,run_function=test_run_function,
                    debug_function=None)

# Your AST
ast = ["pipeline", ["@CFL_LOGM", "test_message"], ["quote", "CFL_DISABLE"]]

# Try to execute
try:
    result = seq.run_lisp_instruction("test-node", ast, "test.event", {})
    print(f"Success: {result}")
except Exception as e:
    print(f"Error: {e}")
    print(f"Error type: {type(e)}")
    import traceback
    traceback.print_exc()
