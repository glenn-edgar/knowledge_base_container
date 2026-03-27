# ==========================================================================
# se_builtins_dict.py
# Mirrors se_load_dictionary.c / s_engine_builtins_dict.h
# ==========================================================================

import se_runtime
import math

_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE


def _s_expr_hash(s):
    """FNV-1a 32-bit hash."""
    h = 2166136261
    for c in s:
        h ^= ord(c)
        h = ((h << 24) + h * 403) & 0xFFFFFFFF
    return h


def _parse_scalar(p):
    t = p[_P_TYPE]
    if t == "int" or t == "uint" or t == "float":
        return p[_P_VALUE]
    if t == "str_idx" or t == "str_ptr":
        s = p[_P_VALUE]
        return {"str": s, "hash": _s_expr_hash(s)}
    return None


def _parse_dict(params, start_i):
    result = {}
    i = start_i + 1
    while i < len(params):
        p = params[i]
        t = p[_P_TYPE]
        if t == "dict_end":
            return result, i + 1
        if t == "dict_key" or t == "dict_key_hash":
            key = p[_P_VALUE]
            i += 1
            if i >= len(params):
                break
            vp = params[i]
            if vp[_P_TYPE] == "dict_start":
                val, i = _parse_dict(params, i)
            elif vp[_P_TYPE] == "array_start":
                val, i = _parse_array(params, i)
            else:
                val = _parse_scalar(vp)
                i += 1
            if i < len(params) and params[i][_P_TYPE] == "end_dict_key":
                i += 1
            result[key] = val
        else:
            i += 1
    return result, i


def _parse_array(params, start_i):
    result = {}
    idx = 0
    i = start_i + 1
    while i < len(params):
        p = params[i]
        t = p[_P_TYPE]
        if t == "array_end":
            return result, i + 1
        if t == "dict_start":
            val, i = _parse_dict(params, i)
        elif t == "array_start":
            val, i = _parse_array(params, i)
        else:
            val = _parse_scalar(p)
            i += 1
        if val is not None:
            result[idx] = val
            idx += 1
    return result, i


def _as_number(v):
    if isinstance(v, dict) and "str" in v:
        try:
            return float(v["str"])
        except (ValueError, TypeError):
            return 0
    try:
        return float(v) if v is not None else 0
    except (ValueError, TypeError):
        return 0


def _as_hash(v):
    if isinstance(v, dict) and "hash" in v:
        return v["hash"]
    if isinstance(v, str):
        return _s_expr_hash(v)
    return int(float(v)) if v is not None else 0


def _navigate_str_path(d, path):
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        v = cur.get(key)
        if v is None:
            try:
                v = cur.get(int(key))
            except (ValueError, TypeError):
                pass
        cur = v
    return cur


def _collect_path_items(node, start_idx, end_idx):
    items = []
    params = node[_N_PARAMS]
    for i in range(start_idx, end_idx + 1):
        if i >= len(params):
            break
        p = params[i]
        v = p[_P_VALUE]
        if isinstance(v, tuple) and len(v) == 2:
            items.append({"hash": v[0], "str": v[1]})
        elif isinstance(v, dict):
            items.append({"hash": v.get("hash", 0), "str": v.get("str")})
        else:
            items.append({"hash": v, "str": None})
    return items


def _navigate_hash_path(d, path_items):
    cur = d
    for item in path_items:
        if not isinstance(cur, dict):
            return None
        v = cur.get(item["hash"])
        if v is None and item["str"]:
            v = cur.get(item["str"])
            if v is None:
                try:
                    v = cur.get(int(item["str"]))
                except (ValueError, TypeError):
                    pass
        cur = v
    return cur


def _last_field_idx(node):
    params = node[_N_PARAMS]
    for i in range(len(params) - 1, -1, -1):
        t = params[i][_P_TYPE]
        if t == "field_ref" or t == "nested_field_ref":
            return i + 1  # 1-based
    return None


def _bb_dict(inst, node, param_idx):
    fname = se_runtime.param_field_name(node, param_idx)
    d = inst["blackboard"].get(fname)
    assert isinstance(d, dict), "se_dict: field '%s' is not dict" % str(fname)
    return d


def se_load_dictionary(inst, node):
    params = node[_N_PARAMS]
    assert len(params) >= 2
    fname = se_runtime.param_field_name(node, 1)
    p2 = params[1]
    assert p2[_P_TYPE] == "dict_start"
    d, _ = _parse_dict(params, 1)
    inst["blackboard"][fname] = d

se_load_dictionary_hash = se_load_dictionary


def se_dict_extract_int(inst, node):
    d = _bb_dict(inst, node, 1)
    v = _navigate_str_path(d, se_runtime.param_str(node, 2))
    inst["blackboard"][se_runtime.param_field_name(node, 3)] = int(_as_number(v)) if v is not None else 0

def se_dict_extract_uint(inst, node):
    d = _bb_dict(inst, node, 1)
    v = _navigate_str_path(d, se_runtime.param_str(node, 2))
    inst["blackboard"][se_runtime.param_field_name(node, 3)] = int(abs(_as_number(v))) if v is not None else 0

def se_dict_extract_float(inst, node):
    d = _bb_dict(inst, node, 1)
    v = _navigate_str_path(d, se_runtime.param_str(node, 2))
    inst["blackboard"][se_runtime.param_field_name(node, 3)] = float(_as_number(v)) if v is not None else 0.0

def se_dict_extract_bool(inst, node):
    d = _bb_dict(inst, node, 1)
    v = _navigate_str_path(d, se_runtime.param_str(node, 2))
    n = _as_number(v) if v is not None else 0
    inst["blackboard"][se_runtime.param_field_name(node, 3)] = 1 if n != 0 else 0

def se_dict_extract_hash(inst, node):
    d = _bb_dict(inst, node, 1)
    v = _navigate_str_path(d, se_runtime.param_str(node, 2))
    inst["blackboard"][se_runtime.param_field_name(node, 3)] = _as_hash(v) if v is not None else 0


def _hash_extract(inst, node, conv):
    d = _bb_dict(inst, node, 1)
    dest = _last_field_idx(node)
    assert dest and dest > 2
    path = _collect_path_items(node, 1, dest - 2)
    v = _navigate_hash_path(d, path)
    inst["blackboard"][se_runtime.param_field_name(node, dest)] = conv(v)

def se_dict_extract_int_h(inst, node):
    _hash_extract(inst, node, lambda v: int(_as_number(v)) if v is not None else 0)
def se_dict_extract_uint_h(inst, node):
    _hash_extract(inst, node, lambda v: int(abs(_as_number(v))) if v is not None else 0)
def se_dict_extract_float_h(inst, node):
    _hash_extract(inst, node, lambda v: float(_as_number(v)) if v is not None else 0.0)
def se_dict_extract_bool_h(inst, node):
    _hash_extract(inst, node, lambda v: 1 if (_as_number(v) if v is not None else 0) != 0 else 0)
def se_dict_extract_hash_h(inst, node):
    _hash_extract(inst, node, lambda v: _as_hash(v) if v is not None else 0)

def se_dict_store_ptr(inst, node):
    d = _bb_dict(inst, node, 1)
    sub = _navigate_str_path(d, se_runtime.param_str(node, 2))
    inst["blackboard"][se_runtime.param_field_name(node, 3)] = sub if isinstance(sub, dict) else None

def se_dict_store_ptr_h(inst, node):
    d = _bb_dict(inst, node, 1)
    dest = _last_field_idx(node)
    assert dest and dest > 2
    path = _collect_path_items(node, 1, dest - 2)
    sub = _navigate_hash_path(d, path)
    inst["blackboard"][se_runtime.param_field_name(node, dest)] = sub if isinstance(sub, dict) else None


def se_load_function_dict(inst, node):
    params = node[_N_PARAMS]
    children = node[se_runtime.N_CHILDREN]
    assert len(params) >= 2, "se_load_function_dict: need >= 2 params"

    fname = se_runtime.param_field_name(node, 1)

    # Collect dict_key names in order; each pairs with the next child
    keys = []
    for p in params:
        if p[_P_TYPE] == "dict_key":
            keys.append(p[_P_VALUE])

    assert len(keys) == len(children), \
        "se_load_function_dict: %d keys but %d children" % (len(keys), len(children))

    d = {}
    for i in range(len(keys)):
        key_hash = _s_expr_hash(keys[i])
        child_node = children[i]
        def _make_fn(cn):
            def fn(calling_inst, exec_node, eid, edata):
                return se_runtime.invoke_any(calling_inst, cn, eid, edata)
            return fn
        d[key_hash] = _make_fn(child_node)

    inst["blackboard"][fname] = d


builtins = {
    "se_load_dictionary": se_load_dictionary,
    "se_load_dictionary_hash": se_load_dictionary_hash,
    "se_load_function_dict": se_load_function_dict,
    "se_dict_extract_int": se_dict_extract_int,
    "se_dict_extract_uint": se_dict_extract_uint,
    "se_dict_extract_float": se_dict_extract_float,
    "se_dict_extract_bool": se_dict_extract_bool,
    "se_dict_extract_hash": se_dict_extract_hash,
    "se_dict_extract_int_h": se_dict_extract_int_h,
    "se_dict_extract_uint_h": se_dict_extract_uint_h,
    "se_dict_extract_float_h": se_dict_extract_float_h,
    "se_dict_extract_bool_h": se_dict_extract_bool_h,
    "se_dict_extract_hash_h": se_dict_extract_hash_h,
    "se_dict_store_ptr": se_dict_store_ptr,
    "se_dict_store_ptr_h": se_dict_store_ptr_h,
}
