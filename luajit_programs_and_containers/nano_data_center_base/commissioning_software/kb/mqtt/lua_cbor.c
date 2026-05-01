/**
 * @file lua_cbor.c
 * @brief JSON↔CBOR codec for robot wire protocol.
 *
 * Maps with values: int, float, string, bool, null, arrays, nested maps.
 * Arrays may contain int, float, string, bool, null.
 * No external dependencies beyond libc.
 */

#include "include/lua_cbor.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <float.h>

/* ================================================================== */
/*  CBOR major types                                                   */
/* ================================================================== */

#define CBOR_UINT    0  /* major 0 */
#define CBOR_NEGINT  1  /* major 1 */
#define CBOR_BSTR    2  /* major 2 */
#define CBOR_TSTR    3  /* major 3 */
#define CBOR_ARRAY   4  /* major 4 */
#define CBOR_MAP     5  /* major 5 */
#define CBOR_TAG     6  /* major 6 */
#define CBOR_SIMPLE  7  /* major 7 */

#define CBOR_FALSE   0xf4
#define CBOR_TRUE    0xf5
#define CBOR_NULL    0xf6
#define CBOR_FLOAT32 0xfa
#define CBOR_FLOAT64 0xfb

/* ================================================================== */
/*  CBOR encoder helpers                                               */
/* ================================================================== */

static int enc_head(uint8_t *out, int max, int major, uint64_t val) {
    uint8_t mt = (uint8_t)(major << 5);
    if (val <= 23) {
        if (max < 1) return -1;
        out[0] = mt | (uint8_t)val;
        return 1;
    } else if (val <= 0xFF) {
        if (max < 2) return -1;
        out[0] = mt | 24;
        out[1] = (uint8_t)val;
        return 2;
    } else if (val <= 0xFFFF) {
        if (max < 3) return -1;
        out[0] = mt | 25;
        out[1] = (uint8_t)(val >> 8);
        out[2] = (uint8_t)val;
        return 3;
    } else if (val <= 0xFFFFFFFF) {
        if (max < 5) return -1;
        out[0] = mt | 26;
        out[1] = (uint8_t)(val >> 24);
        out[2] = (uint8_t)(val >> 16);
        out[3] = (uint8_t)(val >> 8);
        out[4] = (uint8_t)val;
        return 5;
    } else {
        if (max < 9) return -1;
        out[0] = mt | 27;
        for (int i = 7; i >= 0; i--)
            out[8 - i] = (uint8_t)(val >> (i * 8));
        return 9;
    }
}

static int enc_tstr(uint8_t *out, int max, const char *s, int len) {
    int n = enc_head(out, max, CBOR_TSTR, (uint64_t)len);
    if (n < 0 || n + len > max) return -1;
    memcpy(out + n, s, len);
    return n + len;
}

static int enc_float64(uint8_t *out, int max, double val) {
    if (max < 9) return -1;
    out[0] = CBOR_FLOAT64;
    union { double d; uint64_t u; } conv;
    conv.d = val;
    for (int i = 7; i >= 0; i--)
        out[8 - i] = (uint8_t)(conv.u >> (i * 8));
    return 9;
}

/* ================================================================== */
/*  Simple JSON parser (flat objects only)                             */
/* ================================================================== */

static void skip_ws(const char **p) {
    while (**p == ' ' || **p == '\t' || **p == '\n' || **p == '\r') (*p)++;
}

/* Parse a JSON string, return pointer past closing quote. Writes to buf. */
static const char *parse_json_string(const char *p, char *buf, int max, int *len) {
    if (*p != '"') return NULL;
    p++;
    int i = 0;
    while (*p && *p != '"') {
        if (*p == '\\') {
            p++;
            char c = *p;
            if (c == '"' || c == '\\' || c == '/') { if (i < max-1) buf[i++] = c; }
            else if (c == 'n') { if (i < max-1) buf[i++] = '\n'; }
            else if (c == 't') { if (i < max-1) buf[i++] = '\t'; }
            else if (c == 'r') { if (i < max-1) buf[i++] = '\r'; }
            else { if (i < max-1) buf[i++] = c; }  /* best effort */
        } else {
            if (i < max-1) buf[i++] = *p;
        }
        p++;
    }
    if (*p == '"') p++;
    buf[i] = '\0';
    *len = i;
    return p;
}

/* ================================================================== */
/*  Recursive JSON value → CBOR encoder                                */
/* ================================================================== */

static int enc_json_value(const char **pp, uint8_t *out, int max_out) {
    const char *p = *pp;
    skip_ws(&p);
    int n, pos = 0;

    if (*p == '"') {
        /* String */
        char val[4096];
        int val_len;
        p = parse_json_string(p, val, sizeof(val), &val_len);
        if (!p) return -1;
        n = enc_tstr(out + pos, max_out - pos, val, val_len);
        if (n < 0) return -1;
        pos += n;
    } else if (*p == 't' && strncmp(p, "true", 4) == 0) {
        if (pos >= max_out) return -1;
        out[pos++] = CBOR_TRUE;
        p += 4;
    } else if (*p == 'f' && strncmp(p, "false", 5) == 0) {
        if (pos >= max_out) return -1;
        out[pos++] = CBOR_FALSE;
        p += 5;
    } else if (*p == 'n' && strncmp(p, "null", 4) == 0) {
        if (pos >= max_out) return -1;
        out[pos++] = CBOR_NULL;
        p += 4;
    } else if (*p == '[') {
        /* Array: count elements first, then encode */
        p++;
        /* Collect element start positions by scanning */
        const char *starts[1024];
        int arr_count = 0;
        const char *scan = p;
        skip_ws(&scan);
        if (*scan != ']') {
            starts[arr_count++] = scan;
            int depth = 0;
            while (*scan) {
                if (*scan == '"') {
                    scan++;
                    while (*scan && *scan != '"') { if (*scan == '\\') scan++; scan++; }
                    if (*scan == '"') scan++;
                } else if (*scan == '[' || *scan == '{') { depth++; scan++; }
                else if (*scan == ']' || *scan == '}') {
                    if (depth == 0) break;
                    depth--; scan++;
                } else if (*scan == ',' && depth == 0) {
                    scan++;
                    skip_ws(&scan);
                    if (arr_count < 1024) starts[arr_count++] = scan;
                } else { scan++; }
            }
        }
        /* Encode array header */
        n = enc_head(out + pos, max_out - pos, CBOR_ARRAY, (uint64_t)arr_count);
        if (n < 0) return -1;
        pos += n;
        /* Reset p and encode each element */
        skip_ws(&p);
        for (int i = 0; i < arr_count; i++) {
            if (i > 0) {
                skip_ws(&p);
                if (*p == ',') { p++; skip_ws(&p); }
            }
            int en = enc_json_value(&p, out + pos, max_out - pos);
            if (en < 0) return -1;
            pos += en;
        }
        skip_ws(&p);
        if (*p == ']') p++;
    } else if (*p == '{') {
        /* Nested object — recurse into cbor_from_json logic */
        /* Count entries */
        const char *q = p + 1;
        int count = 0, depth = 0;
        while (*q) {
            if (*q == '"') { q++; while (*q && *q != '"') { if (*q == '\\') q++; q++; } if (*q == '"') q++; }
            else if (*q == '{') { depth++; q++; }
            else if (*q == '}') { if (depth == 0) break; depth--; q++; }
            else { if (depth == 0 && *q == ':') count++; q++; }
        }
        n = enc_head(out + pos, max_out - pos, CBOR_MAP, (uint64_t)count);
        if (n < 0) return -1;
        pos += n;
        p++; /* skip { */
        skip_ws(&p);
        while (*p && *p != '}') {
            char key[256]; int key_len;
            p = parse_json_string(p, key, sizeof(key), &key_len);
            if (!p) return -1;
            skip_ws(&p);
            if (*p != ':') return -1;
            p++; skip_ws(&p);
            n = enc_tstr(out + pos, max_out - pos, key, key_len);
            if (n < 0) return -1;
            pos += n;
            int vn = enc_json_value(&p, out + pos, max_out - pos);
            if (vn < 0) return -1;
            pos += vn;
            skip_ws(&p);
            if (*p == ',') { p++; skip_ws(&p); }
        }
        if (*p == '}') p++;
    } else if (*p == '-' || (*p >= '0' && *p <= '9')) {
        /* Number */
        char *end;
        int is_float = 0;
        const char *scan = p;
        if (*scan == '-') scan++;
        while (*scan >= '0' && *scan <= '9') scan++;
        if (*scan == '.' || *scan == 'e' || *scan == 'E') is_float = 1;

        if (is_float) {
            double d = strtod(p, &end);
            p = end;
            n = enc_float64(out + pos, max_out - pos, d);
            if (n < 0) return -1;
            pos += n;
        } else {
            long long ll = strtoll(p, &end, 10);
            p = end;
            if (ll >= 0) {
                n = enc_head(out + pos, max_out - pos, CBOR_UINT, (uint64_t)ll);
            } else {
                n = enc_head(out + pos, max_out - pos, CBOR_NEGINT, (uint64_t)(-1 - ll));
            }
            if (n < 0) return -1;
            pos += n;
        }
    } else {
        return -1;
    }

    *pp = p;
    return pos;
}

/* ================================================================== */
/*  cbor_from_json: JSON string → CBOR bytes                          */
/* ================================================================== */

int cbor_from_json(const char *json, uint8_t *out, int max_out) {
    const char *p = json;
    skip_ws(&p);
    if (*p != '{') return -1;
    p++;

    /* First pass: count map entries */
    int count = 0;
    {
        const char *q = p;
        int depth = 0;
        while (*q) {
            if (*q == '"') {
                q++;
                while (*q && *q != '"') { if (*q == '\\') q++; q++; }
                if (*q == '"') q++;
            } else if (*q == '{') { depth++; q++; }
            else if (*q == '}') {
                if (depth == 0) break;
                depth--; q++;
            } else {
                if (depth == 0 && *q == ':') count++;
                q++;
            }
        }
    }

    int pos = 0;
    int n = enc_head(out + pos, max_out - pos, CBOR_MAP, (uint64_t)count);
    if (n < 0) return -1;
    pos += n;

    /* Parse key-value pairs */
    skip_ws(&p);
    while (*p && *p != '}') {
        /* Key */
        char key[256];
        int key_len;
        p = parse_json_string(p, key, sizeof(key), &key_len);
        if (!p) return -1;

        skip_ws(&p);
        if (*p != ':') return -1;
        p++;
        skip_ws(&p);

        /* Encode key */
        n = enc_tstr(out + pos, max_out - pos, key, key_len);
        if (n < 0) return -1;
        pos += n;

        /* Value — encode any JSON value */
        int vn = enc_json_value(&p, out + pos, max_out - pos);
        if (vn < 0) return -1;
        pos += vn;

        skip_ws(&p);
        if (*p == ',') { p++; skip_ws(&p); }
    }

    return pos;
}

/* ================================================================== */
/*  CBOR decoder helpers                                               */
/* ================================================================== */

static int dec_head(const uint8_t *buf, int len, int *major, uint64_t *val) {
    if (len < 1) return -1;
    *major = (buf[0] >> 5) & 0x07;
    int ai = buf[0] & 0x1f;

    if (ai <= 23) {
        *val = (uint64_t)ai;
        return 1;
    } else if (ai == 24) {
        if (len < 2) return -1;
        *val = buf[1];
        return 2;
    } else if (ai == 25) {
        if (len < 3) return -1;
        *val = ((uint64_t)buf[1] << 8) | buf[2];
        return 3;
    } else if (ai == 26) {
        if (len < 5) return -1;
        *val = ((uint64_t)buf[1] << 24) | ((uint64_t)buf[2] << 16) |
               ((uint64_t)buf[3] << 8) | buf[4];
        return 5;
    } else if (ai == 27) {
        if (len < 9) return -1;
        *val = 0;
        for (int i = 0; i < 8; i++)
            *val = (*val << 8) | buf[1 + i];
        return 9;
    }
    return -1;
}

/* ================================================================== */
/*  Recursive CBOR value → JSON decoder                                */
/* ================================================================== */

static int dec_cbor_value(const uint8_t *cbor, int cbor_len, int *cpos,
                          char *out, int max_out, int *jpos) {
    if (*cpos >= cbor_len) return -1;
    uint8_t first = cbor[*cpos];
    int n, major;
    uint64_t val;

    if (first == CBOR_TRUE) {
        if (*jpos + 4 >= max_out) return -1;
        memcpy(out + *jpos, "true", 4); *jpos += 4; (*cpos)++;
    } else if (first == CBOR_FALSE) {
        if (*jpos + 5 >= max_out) return -1;
        memcpy(out + *jpos, "false", 5); *jpos += 5; (*cpos)++;
    } else if (first == CBOR_NULL) {
        if (*jpos + 4 >= max_out) return -1;
        memcpy(out + *jpos, "null", 4); *jpos += 4; (*cpos)++;
    } else if (first == CBOR_FLOAT64) {
        if (*cpos + 9 > cbor_len) return -1;
        union { double d; uint64_t u; } conv;
        conv.u = 0;
        for (int b = 0; b < 8; b++)
            conv.u = (conv.u << 8) | cbor[*cpos + 1 + b];
        *cpos += 9;
        if (conv.d == floor(conv.d) && fabs(conv.d) < 1e15 && conv.d == conv.d) {
            int w = snprintf(out + *jpos, max_out - *jpos, "%.0f", conv.d);
            if (w < 0 || *jpos + w >= max_out) return -1;
            *jpos += w;
        } else {
            int w = snprintf(out + *jpos, max_out - *jpos, "%.17g", conv.d);
            if (w < 0 || *jpos + w >= max_out) return -1;
            *jpos += w;
        }
    } else if (first == CBOR_FLOAT32) {
        if (*cpos + 5 > cbor_len) return -1;
        union { float f; uint32_t u; } conv;
        conv.u = ((uint32_t)cbor[*cpos+1] << 24) | ((uint32_t)cbor[*cpos+2] << 16) |
                 ((uint32_t)cbor[*cpos+3] << 8) | cbor[*cpos+4];
        *cpos += 5;
        int w = snprintf(out + *jpos, max_out - *jpos, "%g", (double)conv.f);
        if (w < 0 || *jpos + w >= max_out) return -1;
        *jpos += w;
    } else {
        n = dec_head(cbor + *cpos, cbor_len - *cpos, &major, &val);
        if (n < 0) return -1;
        *cpos += n;

        if (major == CBOR_UINT) {
            int w = snprintf(out + *jpos, max_out - *jpos, "%llu",
                (unsigned long long)val);
            if (w < 0 || *jpos + w >= max_out) return -1;
            *jpos += w;
        } else if (major == CBOR_NEGINT) {
            int w = snprintf(out + *jpos, max_out - *jpos, "%lld",
                (long long)(-1 - (int64_t)val));
            if (w < 0 || *jpos + w >= max_out) return -1;
            *jpos += w;
        } else if (major == CBOR_TSTR) {
            int slen = (int)val;
            if (*cpos + slen > cbor_len) return -1;
            if (*jpos + slen + 2 >= max_out) return -1;
            out[(*jpos)++] = '"';
            memcpy(out + *jpos, cbor + *cpos, slen);
            *jpos += slen;
            out[(*jpos)++] = '"';
            *cpos += slen;
        } else if (major == CBOR_ARRAY) {
            int arr_count = (int)val;
            if (*jpos >= max_out) return -1;
            out[(*jpos)++] = '[';
            for (int i = 0; i < arr_count; i++) {
                if (i > 0) {
                    if (*jpos >= max_out) return -1;
                    out[(*jpos)++] = ',';
                }
                if (dec_cbor_value(cbor, cbor_len, cpos, out, max_out, jpos) < 0)
                    return -1;
            }
            if (*jpos >= max_out) return -1;
            out[(*jpos)++] = ']';
        } else if (major == CBOR_MAP) {
            int mc = (int)val;
            if (*jpos >= max_out) return -1;
            out[(*jpos)++] = '{';
            for (int i = 0; i < mc; i++) {
                if (i > 0) {
                    if (*jpos >= max_out) return -1;
                    out[(*jpos)++] = ',';
                }
                /* Key */
                int km; uint64_t kv;
                n = dec_head(cbor + *cpos, cbor_len - *cpos, &km, &kv);
                if (n < 0 || km != CBOR_TSTR) return -1;
                *cpos += n;
                int klen = (int)kv;
                if (*cpos + klen > cbor_len) return -1;
                if (*jpos + klen + 3 >= max_out) return -1;
                out[(*jpos)++] = '"';
                memcpy(out + *jpos, cbor + *cpos, klen);
                *jpos += klen;
                out[(*jpos)++] = '"';
                out[(*jpos)++] = ':';
                *cpos += klen;
                /* Value */
                if (dec_cbor_value(cbor, cbor_len, cpos, out, max_out, jpos) < 0)
                    return -1;
            }
            if (*jpos >= max_out) return -1;
            out[(*jpos)++] = '}';
        } else {
            return -1;
        }
    }
    return 0;
}

/* ================================================================== */
/*  cbor_to_json: CBOR bytes → JSON string                            */
/* ================================================================== */

int cbor_to_json(const uint8_t *cbor, int cbor_len, char *out, int max_out) {
    int cpos = 0, jpos = 0;
    if (dec_cbor_value(cbor, cbor_len, &cpos, out, max_out, &jpos) < 0)
        return -1;
    if (jpos >= max_out) return -1;
    out[jpos] = '\0';
    return jpos;
}
