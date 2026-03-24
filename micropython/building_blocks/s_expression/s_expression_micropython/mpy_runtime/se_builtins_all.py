# ==========================================================================
# se_builtins_all.py
# Convenience module: merges all builtin function tables into one dict.
#
# Usage:
#   import se_builtins_all
#   mod = se_runtime.new_module(module_data, se_builtins_all.builtins)
# ==========================================================================

import se_runtime

from se_builtins_flow_control import builtins as _fc
from se_builtins_dispatch import builtins as _disp
from se_builtins_pred import builtins as _pred
from se_builtins_oneshot import builtins as _os
from se_builtins_return_codes import builtins as _rc
from se_builtins_delays import builtins as _del
from se_builtins_verify import builtins as _ver
from se_builtins_stack import builtins as _stk
from se_builtins_spawn import builtins as _sp
from se_builtins_quads import builtins as _q
from se_builtins_dict import builtins as _d

builtins = se_runtime.merge_fns(_fc, _disp, _pred, _os, _rc, _del, _ver, _stk, _sp, _q, _d)
