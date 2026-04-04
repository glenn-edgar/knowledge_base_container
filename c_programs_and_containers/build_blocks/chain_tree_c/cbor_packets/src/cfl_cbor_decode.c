/* ========================================================================
 * cfl_cbor_decode.c — CBOR decoder (RFC 8949 subset)
 *
 * Pull-style decoder: call decode_next() to advance, then read the
 * decoded value via accessor functions.
 * EXCEPTION on malformed input or type mismatch.
 * ======================================================================== */

#include <string.h>
#include "cfl_cbor_packets.h"
#include "cfl_exception.h"

/* ── CBOR major types ── */
#define CBOR_MT_UINT    0
#define CBOR_MT_NINT    1
#define CBOR_MT_BSTR    2
#define CBOR_MT_TSTR    3
#define CBOR_MT_ARRAY   4
#define CBOR_MT_MAP     5
#define CBOR_MT_TAG     6
#define CBOR_MT_SIMPLE  7

/* ── Simple values ── */
#define CBOR_FALSE  20
#define CBOR_TRUE   21
#define CBOR_NULL   22

/* ── Float additional info ── */
#define CBOR_AI_FLOAT16 25
#define CBOR_AI_FLOAT32 26
#define CBOR_AI_FLOAT64 27

/* ── Internal: ensure bytes available ── */
static void ensure_bytes(const cfl_cbor_decoder_t *dec, size_t needed) {
    if (dec->pos + needed > dec->len) {
        EXCEPTION("cfl_cbor_decode: unexpected end of input");
    }
}

/* ── Internal: read argument value ── */
static uint64_t read_argument(cfl_cbor_decoder_t *dec, uint8_t ai) {
    if (ai <= 23) {
        return ai;
    } else if (ai == 24) {
        ensure_bytes(dec, 1);
        return dec->buf[dec->pos++];
    } else if (ai == 25) {
        ensure_bytes(dec, 2);
        uint16_t val = (uint16_t)((uint16_t)dec->buf[dec->pos] << 8 |
                                   dec->buf[dec->pos + 1]);
        dec->pos += 2;
        return val;
    } else if (ai == 26) {
        ensure_bytes(dec, 4);
        uint32_t val = (uint32_t)dec->buf[dec->pos] << 24 |
                       (uint32_t)dec->buf[dec->pos + 1] << 16 |
                       (uint32_t)dec->buf[dec->pos + 2] << 8 |
                       (uint32_t)dec->buf[dec->pos + 3];
        dec->pos += 4;
        return val;
    } else if (ai == 27) {
        ensure_bytes(dec, 8);
        uint64_t val = (uint64_t)dec->buf[dec->pos] << 56 |
                       (uint64_t)dec->buf[dec->pos + 1] << 48 |
                       (uint64_t)dec->buf[dec->pos + 2] << 40 |
                       (uint64_t)dec->buf[dec->pos + 3] << 32 |
                       (uint64_t)dec->buf[dec->pos + 4] << 24 |
                       (uint64_t)dec->buf[dec->pos + 5] << 16 |
                       (uint64_t)dec->buf[dec->pos + 6] << 8 |
                       (uint64_t)dec->buf[dec->pos + 7];
        dec->pos += 8;
        return val;
    } else {
        EXCEPTION("cfl_cbor_decode: unsupported additional info");
    }
    return 0; /* unreachable */
}

/* ======================================================================== */

void cfl_cbor_decoder_init(cfl_cbor_decoder_t *dec, const uint8_t *buf, size_t len) {
    if (!dec) EXCEPTION("cfl_cbor_decoder_init: NULL decoder");
    if (!buf && len > 0) EXCEPTION("cfl_cbor_decoder_init: NULL buffer");
    dec->buf = buf;
    dec->pos = 0;
    dec->len = len;
    dec->last_type = CFL_CBOR_END;
    dec->last_uint = 0;
    dec->last_int = 0;
    dec->last_float = 0.0;
    dec->last_bool = false;
    dec->last_text = NULL;
    dec->last_text_len = 0;
    dec->last_count = 0;
}

cfl_cbor_type_t cfl_cbor_decode_next(cfl_cbor_decoder_t *dec) {
    if (!dec) EXCEPTION("cfl_cbor_decode_next: NULL decoder");
    if (dec->pos >= dec->len) {
        dec->last_type = CFL_CBOR_END;
        return CFL_CBOR_END;
    }

    uint8_t initial = dec->buf[dec->pos++];
    uint8_t major = initial >> 5;
    uint8_t ai = initial & 0x1F;

    switch (major) {
    case CBOR_MT_UINT: {
        dec->last_uint = read_argument(dec, ai);
        dec->last_type = CFL_CBOR_UINT;
        return CFL_CBOR_UINT;
    }
    case CBOR_MT_NINT: {
        uint64_t raw = read_argument(dec, ai);
        dec->last_int = -1 - (int64_t)raw;
        dec->last_type = CFL_CBOR_INT;
        return CFL_CBOR_INT;
    }
    case CBOR_MT_TSTR: {
        uint64_t slen = read_argument(dec, ai);
        ensure_bytes(dec, (size_t)slen);
        dec->last_text = (const char *)(dec->buf + dec->pos);
        dec->last_text_len = (size_t)slen;
        dec->pos += (size_t)slen;
        dec->last_type = CFL_CBOR_TEXT;
        return CFL_CBOR_TEXT;
    }
    case CBOR_MT_ARRAY: {
        dec->last_count = (unsigned)read_argument(dec, ai);
        dec->last_type = CFL_CBOR_ARRAY;
        return CFL_CBOR_ARRAY;
    }
    case CBOR_MT_MAP: {
        dec->last_count = (unsigned)read_argument(dec, ai);
        dec->last_type = CFL_CBOR_MAP;
        return CFL_CBOR_MAP;
    }
    case CBOR_MT_SIMPLE: {
        if (ai == CBOR_FALSE) {
            dec->last_bool = false;
            dec->last_type = CFL_CBOR_BOOL;
            return CFL_CBOR_BOOL;
        } else if (ai == CBOR_TRUE) {
            dec->last_bool = true;
            dec->last_type = CFL_CBOR_BOOL;
            return CFL_CBOR_BOOL;
        } else if (ai == CBOR_NULL) {
            dec->last_type = CFL_CBOR_NULL;
            return CFL_CBOR_NULL;
        } else if (ai == CBOR_AI_FLOAT64) {
            ensure_bytes(dec, 8);
            uint64_t bits = (uint64_t)dec->buf[dec->pos] << 56 |
                            (uint64_t)dec->buf[dec->pos + 1] << 48 |
                            (uint64_t)dec->buf[dec->pos + 2] << 40 |
                            (uint64_t)dec->buf[dec->pos + 3] << 32 |
                            (uint64_t)dec->buf[dec->pos + 4] << 24 |
                            (uint64_t)dec->buf[dec->pos + 5] << 16 |
                            (uint64_t)dec->buf[dec->pos + 6] << 8 |
                            (uint64_t)dec->buf[dec->pos + 7];
            dec->pos += 8;
            memcpy(&dec->last_float, &bits, sizeof(double));
            dec->last_type = CFL_CBOR_FLOAT64;
            return CFL_CBOR_FLOAT64;
        } else if (ai == CBOR_AI_FLOAT32) {
            ensure_bytes(dec, 4);
            uint32_t bits = (uint32_t)dec->buf[dec->pos] << 24 |
                            (uint32_t)dec->buf[dec->pos + 1] << 16 |
                            (uint32_t)dec->buf[dec->pos + 2] << 8 |
                            (uint32_t)dec->buf[dec->pos + 3];
            dec->pos += 4;
            float f;
            memcpy(&f, &bits, sizeof(float));
            dec->last_float = (double)f;
            dec->last_type = CFL_CBOR_FLOAT64;
            return CFL_CBOR_FLOAT64;
        } else {
            EXCEPTION("cfl_cbor_decode: unsupported simple value");
        }
    }
    /* fall through */
    default:
        EXCEPTION("cfl_cbor_decode: unsupported major type");
    }
    return CFL_CBOR_END; /* unreachable */
}

/* ── Accessors ── */

uint64_t cfl_cbor_decode_uint(const cfl_cbor_decoder_t *dec) {
    if (!dec) EXCEPTION("cfl_cbor_decode_uint: NULL decoder");
    if (dec->last_type != CFL_CBOR_UINT) EXCEPTION("cfl_cbor_decode_uint: type mismatch");
    return dec->last_uint;
}

int64_t cfl_cbor_decode_int(const cfl_cbor_decoder_t *dec) {
    if (!dec) EXCEPTION("cfl_cbor_decode_int: NULL decoder");
    if (dec->last_type != CFL_CBOR_INT) EXCEPTION("cfl_cbor_decode_int: type mismatch");
    return dec->last_int;
}

double cfl_cbor_decode_float(const cfl_cbor_decoder_t *dec) {
    if (!dec) EXCEPTION("cfl_cbor_decode_float: NULL decoder");
    if (dec->last_type != CFL_CBOR_FLOAT64) EXCEPTION("cfl_cbor_decode_float: type mismatch");
    return dec->last_float;
}

bool cfl_cbor_decode_bool(const cfl_cbor_decoder_t *dec) {
    if (!dec) EXCEPTION("cfl_cbor_decode_bool: NULL decoder");
    if (dec->last_type != CFL_CBOR_BOOL) EXCEPTION("cfl_cbor_decode_bool: type mismatch");
    return dec->last_bool;
}

const char *cfl_cbor_decode_text(const cfl_cbor_decoder_t *dec, size_t *out_len) {
    if (!dec) EXCEPTION("cfl_cbor_decode_text: NULL decoder");
    if (dec->last_type != CFL_CBOR_TEXT) EXCEPTION("cfl_cbor_decode_text: type mismatch");
    if (out_len) *out_len = dec->last_text_len;
    return dec->last_text;
}

unsigned cfl_cbor_decode_count(const cfl_cbor_decoder_t *dec) {
    if (!dec) EXCEPTION("cfl_cbor_decode_count: NULL decoder");
    if (dec->last_type != CFL_CBOR_MAP && dec->last_type != CFL_CBOR_ARRAY) {
        EXCEPTION("cfl_cbor_decode_count: not a map or array");
    }
    return dec->last_count;
}
