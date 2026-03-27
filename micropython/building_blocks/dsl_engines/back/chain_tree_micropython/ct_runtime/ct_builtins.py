# ==========================================================================
# ct_builtins.py
# Aggregated CFL builtin table — imports all builtin files.
#
# Pybricks users: customize this file to include only the builtins
# your application needs. For example, if you don't use S-Engine
# integration, remove the ct_builtins_se_bridge import to save memory.
#
# Only CFL_* and SE_* functions belong here.
# User functions (DRONE_CONTROL_*, PACKET_*, etc.) are registered
# separately by the application via ct_runtime.register_fns().
# ==========================================================================
import ct_runtime as rt

from ct_builtins_flow import builtins as _flow
from ct_builtins_state import builtins as _state
from ct_builtins_leaf import builtins as _leaf
from ct_builtins_gate import builtins as _gate
from ct_builtins_composite import builtins as _composite
from ct_builtins_wait import builtins as _wait
from ct_builtins_se_bridge import builtins as _se
from ct_builtins_exception import builtins as _exception

builtins = {}
builtins.update(_flow)
builtins.update(_state)
builtins.update(_leaf)
builtins.update(_gate)
builtins.update(_composite)
builtins.update(_wait)
builtins.update(_se)
builtins.update(_exception)
