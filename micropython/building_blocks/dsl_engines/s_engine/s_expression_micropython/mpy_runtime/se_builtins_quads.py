# ==========================================================================
# se_builtins_quads.py
# Mirrors s_engine_builtins_quads.h
# ==========================================================================

import math
import se_runtime
import se_stack as _se_stack

_N_PARAMS = se_runtime.N_PARAMS
_P_TYPE = se_runtime.P_TYPE
_P_VALUE = se_runtime.P_VALUE

# Opcode constants
_OP_IADD=0x00; _OP_ISUB=0x01; _OP_IMUL=0x02; _OP_IDIV=0x03; _OP_IMOD=0x04; _OP_INEG=0x05
_OP_FADD=0x08; _OP_FSUB=0x09; _OP_FMUL=0x0A; _OP_FDIV=0x0B; _OP_FMOD=0x0C; _OP_FNEG=0x0D
_OP_BIT_AND=0x10; _OP_BIT_OR=0x11; _OP_BIT_XOR=0x12; _OP_BIT_NOT=0x13
_OP_BIT_SHL=0x14; _OP_BIT_SHR=0x15
_OP_ICMP_EQ=0x20; _OP_ICMP_NE=0x21; _OP_ICMP_LT=0x22; _OP_ICMP_LE=0x23
_OP_ICMP_GT=0x24; _OP_ICMP_GE=0x25
_OP_FCMP_EQ=0x28; _OP_FCMP_NE=0x29; _OP_FCMP_LT=0x2A; _OP_FCMP_LE=0x2B
_OP_FCMP_GT=0x2C; _OP_FCMP_GE=0x2D
_OP_LOG_AND=0x30; _OP_LOG_OR=0x31; _OP_LOG_NOT=0x32
_OP_LOG_NAND=0x33; _OP_LOG_NOR=0x34; _OP_LOG_XOR=0x35
_OP_MOVE=0x40; _OP_MOV=0x6E
_OP_FSQRT=0x50; _OP_FPOW=0x51; _OP_FEXP=0x52; _OP_FLOG=0x53
_OP_FLOG10=0x54; _OP_FLOG2=0x55; _OP_FABS=0x56
_OP_FSIN=0x58; _OP_FCOS=0x59; _OP_FTAN=0x5A
_OP_FASIN=0x5B; _OP_FACOS=0x5C; _OP_FATAN=0x5D; _OP_FATAN2=0x5E
_OP_FSINH=0x60; _OP_FCOSH=0x61; _OP_FTANH=0x62
_OP_IABS=0x68; _OP_IMIN=0x69; _OP_IMAX=0x6A
_OP_FMIN=0x6C; _OP_FMAX=0x6D


def _read_int(inst, p):
    if p is None:
        return 0
    t = p[_P_TYPE]
    if t == "int" or t == "uint":
        return p[_P_VALUE]
    if t == "float":
        return int(p[_P_VALUE])
    if t == "field_ref" or t == "nested_field_ref":
        return inst["blackboard"].get(p[_P_VALUE], 0) or 0
    if t == "stack_tos":
        stk = inst["stack"]
        return _se_stack.peek_tos(stk, p[_P_VALUE]) if stk else 0
    if t == "stack_local":
        stk = inst["stack"]
        return _se_stack.get_local(stk, p[_P_VALUE]) if stk else 0
    if t == "stack_pop":
        stk = inst["stack"]
        return (_se_stack.pop(stk) or 0) if stk else 0
    return 0


def _read_float(inst, p):
    if p is None:
        return 0.0
    t = p[_P_TYPE]
    if t == "float":
        return float(p[_P_VALUE])
    if t == "int" or t == "uint":
        return float(p[_P_VALUE])
    if t == "field_ref" or t == "nested_field_ref":
        return float(inst["blackboard"].get(p[_P_VALUE], 0) or 0)
    if t == "stack_tos":
        stk = inst["stack"]
        return float(_se_stack.peek_tos(stk, p[_P_VALUE]) or 0) if stk else 0.0
    if t == "stack_local":
        stk = inst["stack"]
        return float(_se_stack.get_local(stk, p[_P_VALUE]) or 0) if stk else 0.0
    if t == "stack_pop":
        stk = inst["stack"]
        return float((_se_stack.pop(stk) or 0)) if stk else 0.0
    return 0.0


def _write_int(inst, p, val):
    if p is None:
        return
    t = p[_P_TYPE]
    if t == "field_ref" or t == "nested_field_ref":
        inst["blackboard"][p[_P_VALUE]] = val
    elif t == "stack_tos":
        stk = inst["stack"]
        if stk:
            _se_stack.poke(stk, p[_P_VALUE], val)
    elif t == "stack_local":
        stk = inst["stack"]
        if stk:
            _se_stack.set_local(stk, p[_P_VALUE], val)
    elif t == "stack_push":
        stk = inst["stack"]
        if stk:
            _se_stack.push_int(stk, val)


def _write_float(inst, p, val):
    if p is None:
        return
    t = p[_P_TYPE]
    if t == "field_ref" or t == "nested_field_ref":
        inst["blackboard"][p[_P_VALUE]] = val
    elif t == "stack_tos":
        stk = inst["stack"]
        if stk:
            _se_stack.poke(stk, p[_P_VALUE], val)
    elif t == "stack_local":
        stk = inst["stack"]
        if stk:
            _se_stack.set_local(stk, p[_P_VALUE], val)
    elif t == "stack_push":
        stk = inst["stack"]
        if stk:
            _se_stack.push(stk, val)


def _exec_quad(inst, opcode, s1, s2, dp):
    if opcode == _OP_IADD: r = _read_int(inst,s1)+_read_int(inst,s2); _write_int(inst,dp,r); return r
    if opcode == _OP_ISUB: r = _read_int(inst,s1)-_read_int(inst,s2); _write_int(inst,dp,r); return r
    if opcode == _OP_IMUL: r = _read_int(inst,s1)*_read_int(inst,s2); _write_int(inst,dp,r); return r
    if opcode == _OP_IDIV: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=i1//i2 if i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_IMOD: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=i1%i2 if i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_INEG: r = -_read_int(inst,s1); _write_int(inst,dp,r); return r
    if opcode == _OP_FADD: v=_read_float(inst,s1)+_read_float(inst,s2); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FSUB: v=_read_float(inst,s1)-_read_float(inst,s2); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FMUL: v=_read_float(inst,s1)*_read_float(inst,s2); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FDIV: f1=_read_float(inst,s1); f2=_read_float(inst,s2); v=f1/f2 if f2!=0 else 0.0; _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FMOD: f1=_read_float(inst,s1); f2=_read_float(inst,s2); v=math.fmod(f1,f2) if f2!=0 else 0.0; _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FNEG: v=-_read_float(inst,s1); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_BIT_AND: r=_read_int(inst,s1)&_read_int(inst,s2); _write_int(inst,dp,r); return r
    if opcode == _OP_BIT_OR:  r=_read_int(inst,s1)|_read_int(inst,s2); _write_int(inst,dp,r); return r
    if opcode == _OP_BIT_XOR: r=_read_int(inst,s1)^_read_int(inst,s2); _write_int(inst,dp,r); return r
    if opcode == _OP_BIT_NOT: r=~_read_int(inst,s1); _write_int(inst,dp,r); return r
    if opcode == _OP_BIT_SHL: r=_read_int(inst,s1)<<(_read_int(inst,s2)&0x1F); _write_int(inst,dp,r); return r
    if opcode == _OP_BIT_SHR: r=_read_int(inst,s1)>>(_read_int(inst,s2)&0x1F); _write_int(inst,dp,r); return r
    if opcode == _OP_ICMP_EQ: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if i1==i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_ICMP_NE: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if i1!=i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_ICMP_LT: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if i1<i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_ICMP_LE: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if i1<=i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_ICMP_GT: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if i1>i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_ICMP_GE: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if i1>=i2 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_FCMP_EQ: r=1 if _read_float(inst,s1)==_read_float(inst,s2) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_FCMP_NE: r=1 if _read_float(inst,s1)!=_read_float(inst,s2) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_FCMP_LT: r=1 if _read_float(inst,s1)<_read_float(inst,s2) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_FCMP_LE: r=1 if _read_float(inst,s1)<=_read_float(inst,s2) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_FCMP_GT: r=1 if _read_float(inst,s1)>_read_float(inst,s2) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_FCMP_GE: r=1 if _read_float(inst,s1)>=_read_float(inst,s2) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_LOG_AND:  i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if (i1!=0 and i2!=0) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_LOG_OR:   i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if (i1!=0 or i2!=0) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_LOG_NOT:  i1=_read_int(inst,s1); r=1 if i1==0 else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_LOG_NAND: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=0 if (i1!=0 and i2!=0) else 1; _write_int(inst,dp,r); return r
    if opcode == _OP_LOG_NOR:  i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=0 if (i1!=0 or i2!=0) else 1; _write_int(inst,dp,r); return r
    if opcode == _OP_LOG_XOR:  i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=1 if ((i1!=0)!=(i2!=0)) else 0; _write_int(inst,dp,r); return r
    if opcode == _OP_MOVE or opcode == _OP_MOV: i1=_read_int(inst,s1); _write_int(inst,dp,i1); return i1
    if opcode == _OP_FSQRT:  v=math.sqrt(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FPOW:   v=_read_float(inst,s1)**_read_float(inst,s2); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FEXP:   v=math.exp(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FLOG:   v=math.log(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FLOG10: v=math.log10(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FLOG2:  v=math.log(_read_float(inst,s1))/math.log(2); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FABS:   v=abs(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FSIN:   v=math.sin(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FCOS:   v=math.cos(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FTAN:   v=math.tan(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FASIN:  v=math.asin(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FACOS:  v=math.acos(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FATAN:  v=math.atan(_read_float(inst,s1)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FATAN2: v=math.atan2(_read_float(inst,s1),_read_float(inst,s2)); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FSINH:  f1=_read_float(inst,s1); e=math.exp(f1); v=(e-1/e)/2; _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FCOSH:  f1=_read_float(inst,s1); e=math.exp(f1); v=(e+1/e)/2; _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FTANH:  f1=_read_float(inst,s1); e=math.exp(2*f1); v=(e-1)/(e+1); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_IABS: i1=_read_int(inst,s1); r=abs(i1); _write_int(inst,dp,r); return r
    if opcode == _OP_IMIN: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=min(i1,i2); _write_int(inst,dp,r); return r
    if opcode == _OP_IMAX: i1=_read_int(inst,s1); i2=_read_int(inst,s2); r=max(i1,i2); _write_int(inst,dp,r); return r
    if opcode == _OP_FMIN: f1=_read_float(inst,s1); f2=_read_float(inst,s2); v=min(f1,f2); _write_float(inst,dp,v); return 1 if v!=0 else 0
    if opcode == _OP_FMAX: f1=_read_float(inst,s1); f2=_read_float(inst,s2); v=max(f1,f2); _write_float(inst,dp,v); return 1 if v!=0 else 0

    raise ValueError("se_quad: unknown opcode 0x%02x" % opcode)


def se_quad(inst, node):
    params = node[_N_PARAMS]
    assert len(params) >= 4, "se_quad: requires 4 params"
    _exec_quad(inst, params[0][_P_VALUE], params[1], params[2], params[3])


def se_p_quad(inst, node):
    params = node[_N_PARAMS]
    assert len(params) >= 4, "se_p_quad: requires 4 params"
    opcode = params[0][_P_VALUE]
    s1, s2, dp = params[1], params[2], params[3]

    if 0x40 <= opcode <= 0x4D:
        if opcode <= 0x45:
            i1 = _read_int(inst, s1); i2 = _read_int(inst, s2)
            cmp_map = {0x40: i1==i2, 0x41: i1!=i2, 0x42: i1<i2, 0x43: i1<=i2, 0x44: i1>i2, 0x45: i1>=i2}
            cmp = 1 if cmp_map[opcode] else 0
        else:
            f1 = _read_float(inst, s1); f2 = _read_float(inst, s2)
            cmp_map = {0x48: f1==f2, 0x49: f1!=f2, 0x4A: f1<f2, 0x4B: f1<=f2, 0x4C: f1>f2, 0x4D: f1>=f2}
            cmp = 1 if cmp_map.get(opcode, False) else 0
        prev = _read_int(inst, dp)
        _write_int(inst, dp, prev + cmp)
        return cmp != 0

    result = _exec_quad(inst, opcode, s1, s2, dp)
    return result != 0


builtins = {
    "se_quad": se_quad,
    "se_p_quad": se_p_quad,
}
