/* ========================================================================
 * cfl_cbor_encode.c — CBOR encoder (RFC 8949 subset)
 *
 * Writes CBOR bytes into a caller-provided buffer.
 * String keys, no tags, no indefinite-length.
 * EXCEPTION on buffer overflow.
 * ======================================================================== */

#include <string.h>
#include "cfl_cbor_packets.h"
#include "cfl_exception.h"

/* ── CBOR major types ── */
#define CBOR_MT_UINT    0   /* major type 0: unsigned integer */
#define CBOR_MT_NINT    1   /* major type 1: negative integer */
#define CBOR_MT_BSTR    2   /* major type 2: byte string */
#define CBOR_MT_TSTR    3   /* major type 3: text string */
#define CBOR_MT_ARRAY   4   /* major type 4: array */
#define CBOR_MT_MAP     5   /* major type 5: map */
#define CBOR_MT_SIMPLE  7   /* major type 7: simple values + floats */

/* ── Simple values (major type 7) ── */
#define CBOR_FALSE  20
#define CBOR_TRUE   21
#define CBOR_NULL   22

/* ── Additional info for float sizes ── */
#define CBOR_AI_FLOAT64 27

/* ── Internal: ensure capacity ── */
static void ensure_space(cfl_cbor_encoder_t *enc, size_t needed) {
    if (enc->pos + needed > enc->capacity) {
        EXCEPTION("cfl_cbor_encode: buffer overflow");
    }
}

/* ── Internal: write initial byte + argument ── */
static void write_type_arg(cfl_cbor_encoder_t *enc, uint8_t major, uint64_t val) {
    uint8_t mt = (uint8_t)(major << 5);

    if (val <= 23) {
        ensure_space(enc, 1);
        enc->buf[enc->pos++] = mt | (uint8_t)val;
    } else if (val <= 0xFF) {
        ensure_space(enc, 2);
        enc->buf[enc->pos++] = mt | 24;
        enc->buf[enc->pos++] = (uint8_t)val;
    } else if (val <= 0xFFFF) {
        ensure_space(enc, 3);
        enc->buf[enc->pos++] = mt | 25;
        enc->buf[enc->pos++] = (uint8_t)(val >> 8);
        enc->buf[enc->pos++] = (uint8_t)val;
    } else if (val <= 0xFFFFFFFF) {
        ensure_space(enc, 5);
        enc->buf[enc->pos++] = mt | 26;
        enc->buf[enc->pos++] = (uint8_t)(val >> 24);
        enc->buf[enc->pos++] = (uint8_t)(val >> 16);
        enc->buf[enc->pos++] = (uint8_t)(val >> 8);
        enc->buf[enc->pos++] = (uint8_t)val;
    } else {
        ensure_space(enc, 9);
        enc->buf[enc->pos++] = mt | 27;
        enc->buf[enc->pos++] = (uint8_t)(val >> 56);
        enc->buf[enc->pos++] = (uint8_t)(val >> 48);
        enc->buf[enc->pos++] = (uint8_t)(val >> 40);
        enc->buf[enc->pos++] = (uint8_t)(val >> 32);
        enc->buf[enc->pos++] = (uint8_t)(val >> 24);
        enc->buf[enc->pos++] = (uint8_t)(val >> 16);
        enc->buf[enc->pos++] = (uint8_t)(val >> 8);
        enc->buf[enc->pos++] = (uint8_t)val;
    }
}

/* ======================================================================== */

void cfl_cbor_encoder_init(cfl_cbor_encoder_t *enc, uint8_t *buf, size_t capacity) {
    if (!enc) EXCEPTION("cfl_cbor_encoder_init: NULL encoder");
    if (!buf) EXCEPTION("cfl_cbor_encoder_init: NULL buffer");
    enc->buf = buf;
    enc->pos = 0;
    enc->capacity = capacity;
}

void cfl_cbor_encode_uint(cfl_cbor_encoder_t *enc, uint64_t val) {
    if (!enc) EXCEPTION("cfl_cbor_encode_uint: NULL encoder");
    write_type_arg(enc, CBOR_MT_UINT, val);
}

void cfl_cbor_encode_int(cfl_cbor_encoder_t *enc, int64_t val) {
    if (!enc) EXCEPTION("cfl_cbor_encode_int: NULL encoder");
    if (val >= 0) {
        write_type_arg(enc, CBOR_MT_UINT, (uint64_t)val);
    } else {
        /* CBOR negative: -1 - n, so n = -1 - val */
        write_type_arg(enc, CBOR_MT_NINT, (uint64_t)(-1 - val));
    }
}

void cfl_cbor_encode_float64(cfl_cbor_encoder_t *enc, double val) {
    if (!enc) EXCEPTION("cfl_cbor_encode_float64: NULL encoder");
    ensure_space(enc, 9);
    enc->buf[enc->pos++] = (CBOR_MT_SIMPLE << 5) | CBOR_AI_FLOAT64;

    /* IEEE 754 double, big-endian */
    uint64_t bits;
    memcpy(&bits, &val, sizeof(bits));
    enc->buf[enc->pos++] = (uint8_t)(bits >> 56);
    enc->buf[enc->pos++] = (uint8_t)(bits >> 48);
    enc->buf[enc->pos++] = (uint8_t)(bits >> 40);
    enc->buf[enc->pos++] = (uint8_t)(bits >> 32);
    enc->buf[enc->pos++] = (uint8_t)(bits >> 24);
    enc->buf[enc->pos++] = (uint8_t)(bits >> 16);
    enc->buf[enc->pos++] = (uint8_t)(bits >> 8);
    enc->buf[enc->pos++] = (uint8_t)bits;
}

void cfl_cbor_encode_bool(cfl_cbor_encoder_t *enc, bool val) {
    if (!enc) EXCEPTION("cfl_cbor_encode_bool: NULL encoder");
    ensure_space(enc, 1);
    enc->buf[enc->pos++] = (CBOR_MT_SIMPLE << 5) | (val ? CBOR_TRUE : CBOR_FALSE);
}

void cfl_cbor_encode_null(cfl_cbor_encoder_t *enc) {
    if (!enc) EXCEPTION("cfl_cbor_encode_null: NULL encoder");
    ensure_space(enc, 1);
    enc->buf[enc->pos++] = (CBOR_MT_SIMPLE << 5) | CBOR_NULL;
}

void cfl_cbor_encode_text(cfl_cbor_encoder_t *enc, const char *str) {
    if (!enc) EXCEPTION("cfl_cbor_encode_text: NULL encoder");
    if (!str) EXCEPTION("cfl_cbor_encode_text: NULL string");
    size_t len = strlen(str);
    write_type_arg(enc, CBOR_MT_TSTR, len);
    ensure_space(enc, len);
    memcpy(enc->buf + enc->pos, str, len);
    enc->pos += len;
}

void cfl_cbor_encode_map_begin(cfl_cbor_encoder_t *enc, unsigned count) {
    if (!enc) EXCEPTION("cfl_cbor_encode_map_begin: NULL encoder");
    write_type_arg(enc, CBOR_MT_MAP, count);
}

void cfl_cbor_encode_array_begin(cfl_cbor_encoder_t *enc, unsigned count) {
    if (!enc) EXCEPTION("cfl_cbor_encode_array_begin: NULL encoder");
    write_type_arg(enc, CBOR_MT_ARRAY, count);
}

size_t cfl_cbor_encoder_finish(cfl_cbor_encoder_t *enc) {
    if (!enc) EXCEPTION("cfl_cbor_encoder_finish: NULL encoder");
    return enc->pos;
}
