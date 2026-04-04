#ifndef CFL_CBOR_PACKETS_H
#define CFL_CBOR_PACKETS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ========================================================================
 * CBOR PACKET LIBRARY
 *
 * Minimal CBOR encoder/decoder (RFC 8949 subset) plus JSON<->CBOR
 * transcoding with string keys.
 *
 * Supported types: unsigned int, negative int, text string, float64,
 * bool, null, map (string keys), array. No tags, no indefinite-length,
 * no byte strings, max nesting depth 8.
 *
 * EXCEPTION on all errors (matches ChainTree conventions).
 * ======================================================================== */

#ifndef CFL_CBOR_MAX_PACKET_SIZE
#define CFL_CBOR_MAX_PACKET_SIZE 256
#endif

#define CFL_CBOR_MAX_NESTING 8

/* ── Heap interface (same as json_packets) ── */
typedef struct cfl_cbor_heap_interface {
    void *(*alloc)(void *ctx, uint16_t size);
    void  (*free)(void *ctx, void *ptr);
    void  *ctx;
} cfl_cbor_heap_interface_t;

/* ========================================================================
 * ENCODER — writes CBOR bytes into a caller-provided buffer
 *
 * Usage:
 *   cfl_cbor_encoder_t enc;
 *   cfl_cbor_encoder_init(&enc, buf, sizeof(buf));
 *   cfl_cbor_encode_map_begin(&enc, 3);
 *   cfl_cbor_encode_text(&enc, "key1");
 *   cfl_cbor_encode_uint(&enc, 42);
 *   ...
 *   size_t len = cfl_cbor_encoder_finish(&enc);
 * ======================================================================== */

typedef struct {
    uint8_t *buf;
    size_t   pos;
    size_t   capacity;
} cfl_cbor_encoder_t;

void     cfl_cbor_encoder_init(cfl_cbor_encoder_t *enc, uint8_t *buf, size_t capacity);
void     cfl_cbor_encode_uint(cfl_cbor_encoder_t *enc, uint64_t val);
void     cfl_cbor_encode_int(cfl_cbor_encoder_t *enc, int64_t val);
void     cfl_cbor_encode_float64(cfl_cbor_encoder_t *enc, double val);
void     cfl_cbor_encode_bool(cfl_cbor_encoder_t *enc, bool val);
void     cfl_cbor_encode_null(cfl_cbor_encoder_t *enc);
void     cfl_cbor_encode_text(cfl_cbor_encoder_t *enc, const char *str);
void     cfl_cbor_encode_map_begin(cfl_cbor_encoder_t *enc, unsigned count);
void     cfl_cbor_encode_array_begin(cfl_cbor_encoder_t *enc, unsigned count);
size_t   cfl_cbor_encoder_finish(cfl_cbor_encoder_t *enc);

/* ========================================================================
 * DECODER — iterates CBOR bytes, pull-style
 *
 * Usage:
 *   cfl_cbor_decoder_t dec;
 *   cfl_cbor_decoder_init(&dec, buf, len);
 *   cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
 *   if (t == CFL_CBOR_UINT) { uint64_t v = cfl_cbor_decode_uint(&dec); }
 * ======================================================================== */

typedef enum {
    CFL_CBOR_UINT,
    CFL_CBOR_INT,       /* negative int */
    CFL_CBOR_FLOAT64,
    CFL_CBOR_BOOL,
    CFL_CBOR_NULL,
    CFL_CBOR_TEXT,
    CFL_CBOR_MAP,
    CFL_CBOR_ARRAY,
    CFL_CBOR_END        /* no more data */
} cfl_cbor_type_t;

typedef struct {
    const uint8_t *buf;
    size_t         pos;
    size_t         len;
    /* last decoded item */
    cfl_cbor_type_t last_type;
    uint64_t        last_uint;
    int64_t         last_int;
    double          last_float;
    bool            last_bool;
    const char     *last_text;      /* points into buf (NOT NUL-terminated) */
    size_t          last_text_len;
    unsigned        last_count;     /* map/array item count */
} cfl_cbor_decoder_t;

void            cfl_cbor_decoder_init(cfl_cbor_decoder_t *dec, const uint8_t *buf, size_t len);
cfl_cbor_type_t cfl_cbor_decode_next(cfl_cbor_decoder_t *dec);

/* Accessors — call after decode_next returns the matching type. EXCEPTION on mismatch. */
uint64_t        cfl_cbor_decode_uint(const cfl_cbor_decoder_t *dec);
int64_t         cfl_cbor_decode_int(const cfl_cbor_decoder_t *dec);
double          cfl_cbor_decode_float(const cfl_cbor_decoder_t *dec);
bool            cfl_cbor_decode_bool(const cfl_cbor_decoder_t *dec);
/* Returns pointer into buffer and length. NOT NUL-terminated. */
const char     *cfl_cbor_decode_text(const cfl_cbor_decoder_t *dec, size_t *out_len);
unsigned        cfl_cbor_decode_count(const cfl_cbor_decoder_t *dec);

/* ========================================================================
 * TRANSCODE — JSON text <-> CBOR bytes (string keys)
 *
 * Uses cJSON internally. Requires json_packets library for cJSON.
 * ======================================================================== */

/* JSON text -> CBOR bytes.
 * Returns number of CBOR bytes written.
 * EXCEPTION if json_text is invalid or cbor_buf too small. */
size_t cfl_json_text_to_cbor(const char *json_text,
                              uint8_t *cbor_buf, size_t cbor_buflen);

/* CBOR bytes -> JSON text.
 * Returns heap-allocated NUL-terminated JSON string.
 * Caller must free via heap->free(). EXCEPTION on decode error. */
char *cfl_cbor_to_json_text(const uint8_t *cbor_buf, size_t cbor_len,
                             cfl_cbor_heap_interface_t *heap);

#ifdef __cplusplus
}
#endif

#endif /* CFL_CBOR_PACKETS_H */
