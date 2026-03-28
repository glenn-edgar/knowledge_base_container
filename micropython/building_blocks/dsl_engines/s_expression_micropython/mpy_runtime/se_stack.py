# ==========================================================================
# se_stack.py
# Call-stack implementation for S-Expression engine (MicroPython).
# Mirrors se_stack.lua / s_expr_stack_t C structure.
# ==========================================================================


def new_stack(capacity):
    return {
        "data": [None] * capacity,
        "sp": 0,
        "capacity": capacity,
        "frames": [],
        "frame_count": 0,
    }


def push(stk, value):
    assert stk["sp"] < stk["capacity"], "se_stack: push overflow"
    stk["data"][stk["sp"]] = value
    stk["sp"] += 1


def push_int(stk, n):
    push(stk, n)


def pop(stk):
    if stk["sp"] == 0:
        return None
    stk["sp"] -= 1
    v = stk["data"][stk["sp"]]
    stk["data"][stk["sp"]] = None
    return v


def peek_tos(stk, offset=0):
    idx = stk["sp"] - 1 - offset
    if idx < 0:
        return None
    return stk["data"][idx]


def poke(stk, offset, value):
    idx = stk["sp"] - 1 - offset
    if idx >= 0:
        stk["data"][idx] = value


def push_frame(stk, num_params, num_locals):
    base_ptr = stk["sp"] - num_params
    if base_ptr < 0:
        return False
    # Allocate locals (zeroed)
    for _ in range(num_locals):
        if stk["sp"] >= stk["capacity"]:
            return False
        stk["data"][stk["sp"]] = 0
        stk["sp"] += 1

    frame = {
        "base_ptr": base_ptr,
        "num_params": num_params,
        "num_locals": num_locals,
        "scratch_base": stk["sp"],
        "saved_sp": stk["sp"],
    }
    stk["frames"].append(frame)
    stk["frame_count"] += 1
    return True


def pop_frame(stk):
    if stk["frame_count"] == 0:
        return
    f = stk["frames"].pop()
    stk["sp"] = f["base_ptr"]
    stk["frame_count"] -= 1


def get_local(stk, local_idx):
    if stk["frame_count"] == 0:
        return None
    f = stk["frames"][-1]
    idx = f["base_ptr"] + f["num_params"] + local_idx
    if idx >= stk["sp"]:
        return None
    return stk["data"][idx]


def set_local(stk, local_idx, value):
    if stk["frame_count"] == 0:
        return
    f = stk["frames"][-1]
    idx = f["base_ptr"] + f["num_params"] + local_idx
    if idx < stk["capacity"]:
        stk["data"][idx] = value
        if idx >= stk["sp"]:
            stk["sp"] = idx + 1


def get_param(stk, param_idx):
    if stk["frame_count"] == 0:
        return None
    f = stk["frames"][-1]
    idx = f["base_ptr"] + param_idx
    return stk["data"][idx]
