/* ========================================================================
 * test_cbor.c — Standalone CBOR library tests
 *
 * Tests encode/decode round-trip and JSON<->CBOR transcoding.
 * No CFL runtime dependency — just the cbor_packets + json_packets libs.
 *
 * Build: see Makefile in this directory
 * Run:   ./test_cbor
 * ======================================================================== */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <setjmp.h>
#include "cfl_cbor_packets.h"

/* ── Exception handler for standalone test ── */
static jmp_buf s_jmp;
static char s_exception_msg[256];

void cfl_exception_handler(const char *file, const char *func,
                           int line, const char *msg) {
    snprintf(s_exception_msg, sizeof(s_exception_msg),
             "%s:%d (%s): %s", file, line, func, msg);
    longjmp(s_jmp, 1);
}

/* ── Test helpers ── */
static int s_pass = 0, s_fail = 0;

#define TEST(name) \
    do { printf("  %-50s ", name); } while(0)

#define PASS() \
    do { printf("PASS\n"); s_pass++; } while(0)

#define FAIL(msg) \
    do { printf("FAIL: %s\n", msg); s_fail++; } while(0)

#define ASSERT_EQ_INT(a, b) \
    do { if ((a) != (b)) { \
        char _buf[128]; snprintf(_buf, sizeof(_buf), "expected %lld, got %lld", \
            (long long)(a), (long long)(b)); FAIL(_buf); return; } \
    } while(0)

#define ASSERT_EQ_FLOAT(a, b, tol) \
    do { if (fabs((a) - (b)) > (tol)) { \
        char _buf[128]; snprintf(_buf, sizeof(_buf), "expected %.6f, got %.6f", \
            (double)(a), (double)(b)); FAIL(_buf); return; } \
    } while(0)

#define ASSERT_EQ_STR(a, b, len) \
    do { if (memcmp((a), (b), (len)) != 0) { \
        FAIL("string mismatch"); return; } \
    } while(0)

/* ── Heap for transcoder ── */
static void *test_alloc(void *ctx, uint16_t size) {
    (void)ctx;
    return malloc(size);
}
static void test_free(void *ctx, void *ptr) {
    (void)ctx;
    free(ptr);
}
static cfl_cbor_heap_interface_t s_heap = { test_alloc, test_free, NULL };

/* ======================================================================== */
/* TEST 1: Encode/decode unsigned integers                                   */
/* ======================================================================== */
static void test_uint_roundtrip(void) {
    TEST("uint round-trip (0, 23, 24, 255, 256, 65535, 100000)");

    uint64_t values[] = { 0, 23, 24, 255, 256, 65535, 100000 };
    int n = (int)(sizeof(values) / sizeof(values[0]));

    uint8_t buf[128];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    for (int i = 0; i < n; i++) {
        cfl_cbor_encode_uint(&enc, values[i]);
    }
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    for (int i = 0; i < n; i++) {
        cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
        ASSERT_EQ_INT(CFL_CBOR_UINT, t);
        ASSERT_EQ_INT(values[i], cfl_cbor_decode_uint(&dec));
    }
    PASS();
}

/* ======================================================================== */
/* TEST 2: Encode/decode negative integers                                   */
/* ======================================================================== */
static void test_int_roundtrip(void) {
    TEST("negative int round-trip (-1, -24, -100, -1000)");

    int64_t values[] = { -1, -24, -100, -1000 };
    int n = (int)(sizeof(values) / sizeof(values[0]));

    uint8_t buf[64];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    for (int i = 0; i < n; i++) {
        cfl_cbor_encode_int(&enc, values[i]);
    }
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    for (int i = 0; i < n; i++) {
        cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
        ASSERT_EQ_INT(CFL_CBOR_INT, t);
        ASSERT_EQ_INT(values[i], cfl_cbor_decode_int(&dec));
    }
    PASS();
}

/* ======================================================================== */
/* TEST 3: Encode/decode positive integers via int encoder                   */
/* ======================================================================== */
static void test_positive_via_int(void) {
    TEST("positive int via encode_int (0, 42, 1000)");

    int64_t values[] = { 0, 42, 1000 };
    int n = (int)(sizeof(values) / sizeof(values[0]));

    uint8_t buf[64];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    for (int i = 0; i < n; i++) {
        cfl_cbor_encode_int(&enc, values[i]);
    }
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    for (int i = 0; i < n; i++) {
        cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
        ASSERT_EQ_INT(CFL_CBOR_UINT, t);
        ASSERT_EQ_INT((uint64_t)values[i], cfl_cbor_decode_uint(&dec));
    }
    PASS();
}

/* ======================================================================== */
/* TEST 4: Float64 round-trip                                                */
/* ======================================================================== */
static void test_float64_roundtrip(void) {
    TEST("float64 round-trip (3.14, -0.001, 9.81, 0.0)");

    double values[] = { 3.14159265358979, -0.001, 9.81, 0.0 };
    int n = (int)(sizeof(values) / sizeof(values[0]));

    uint8_t buf[128];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    for (int i = 0; i < n; i++) {
        cfl_cbor_encode_float64(&enc, values[i]);
    }
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    for (int i = 0; i < n; i++) {
        cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
        ASSERT_EQ_INT(CFL_CBOR_FLOAT64, t);
        ASSERT_EQ_FLOAT(values[i], cfl_cbor_decode_float(&dec), 1e-15);
    }
    PASS();
}

/* ======================================================================== */
/* TEST 5: Bool and null                                                     */
/* ======================================================================== */
static void test_bool_null(void) {
    TEST("bool and null round-trip");

    uint8_t buf[16];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    cfl_cbor_encode_bool(&enc, true);
    cfl_cbor_encode_bool(&enc, false);
    cfl_cbor_encode_null(&enc);
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    cfl_cbor_type_t t;
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_BOOL, t);
    ASSERT_EQ_INT(true, cfl_cbor_decode_bool(&dec));

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_BOOL, t);
    ASSERT_EQ_INT(false, cfl_cbor_decode_bool(&dec));

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_NULL, t);

    PASS();
}

/* ======================================================================== */
/* TEST 6: Text string round-trip                                            */
/* ======================================================================== */
static void test_text_roundtrip(void) {
    TEST("text string round-trip");

    uint8_t buf[128];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    cfl_cbor_encode_text(&enc, "hello");
    cfl_cbor_encode_text(&enc, "");
    cfl_cbor_encode_text(&enc, "sensors/accel");
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    cfl_cbor_type_t t;
    size_t slen;
    const char *text;

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    text = cfl_cbor_decode_text(&dec, &slen);
    ASSERT_EQ_INT(5, (int)slen);
    ASSERT_EQ_STR("hello", text, slen);

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    text = cfl_cbor_decode_text(&dec, &slen);
    ASSERT_EQ_INT(0, (int)slen);

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    text = cfl_cbor_decode_text(&dec, &slen);
    ASSERT_EQ_INT(13, (int)slen);
    ASSERT_EQ_STR("sensors/accel", text, slen);

    PASS();
}

/* ======================================================================== */
/* TEST 7: Map with mixed values                                             */
/* ======================================================================== */
static void test_map_roundtrip(void) {
    TEST("map round-trip {type:str, seq:uint, x:float}");

    uint8_t buf[128];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    cfl_cbor_encode_map_begin(&enc, 3);
    cfl_cbor_encode_text(&enc, "type");
    cfl_cbor_encode_text(&enc, "telemetry");
    cfl_cbor_encode_text(&enc, "seq");
    cfl_cbor_encode_uint(&enc, 42);
    cfl_cbor_encode_text(&enc, "x");
    cfl_cbor_encode_float64(&enc, 1.5);
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    cfl_cbor_type_t t;
    size_t slen;
    const char *text;

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_MAP, t);
    ASSERT_EQ_INT(3, (int)cfl_cbor_decode_count(&dec));

    /* key: "type" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    text = cfl_cbor_decode_text(&dec, &slen);
    ASSERT_EQ_STR("type", text, slen);
    /* val: "telemetry" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    text = cfl_cbor_decode_text(&dec, &slen);
    ASSERT_EQ_STR("telemetry", text, slen);

    /* key: "seq" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    /* val: 42 */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_UINT, t);
    ASSERT_EQ_INT(42, (int)cfl_cbor_decode_uint(&dec));

    /* key: "x" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    /* val: 1.5 */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_FLOAT64, t);
    ASSERT_EQ_FLOAT(1.5, cfl_cbor_decode_float(&dec), 1e-15);

    PASS();
}

/* ======================================================================== */
/* TEST 8: Nested map (like telemetry packet)                                */
/* ======================================================================== */
static void test_nested_map(void) {
    TEST("nested map {type, seq, payload:{x,y,z}}");

    uint8_t buf[256];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    cfl_cbor_encode_map_begin(&enc, 3);
      cfl_cbor_encode_text(&enc, "type");
      cfl_cbor_encode_text(&enc, "telemetry");
      cfl_cbor_encode_text(&enc, "seq");
      cfl_cbor_encode_uint(&enc, 1);
      cfl_cbor_encode_text(&enc, "payload");
      cfl_cbor_encode_map_begin(&enc, 3);
        cfl_cbor_encode_text(&enc, "x");
        cfl_cbor_encode_float64(&enc, 1.5);
        cfl_cbor_encode_text(&enc, "y");
        cfl_cbor_encode_float64(&enc, -0.3);
        cfl_cbor_encode_text(&enc, "z");
        cfl_cbor_encode_float64(&enc, 9.81);
    size_t len = cfl_cbor_encoder_finish(&enc);

    /* Decode and spot-check */
    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_MAP, t);
    ASSERT_EQ_INT(3, (int)cfl_cbor_decode_count(&dec));

    /* Skip to payload by reading key-value pairs */
    /* type: telemetry */
    cfl_cbor_decode_next(&dec); cfl_cbor_decode_next(&dec);
    /* seq: 1 */
    cfl_cbor_decode_next(&dec); cfl_cbor_decode_next(&dec);
    /* payload key */
    cfl_cbor_decode_next(&dec);
    /* payload value: map of 3 */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_MAP, t);
    ASSERT_EQ_INT(3, (int)cfl_cbor_decode_count(&dec));

    /* x: 1.5 */
    cfl_cbor_decode_next(&dec); /* key "x" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_FLOAT64, t);
    ASSERT_EQ_FLOAT(1.5, cfl_cbor_decode_float(&dec), 1e-15);

    /* y: -0.3 */
    cfl_cbor_decode_next(&dec); /* key "y" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_FLOAT64, t);
    ASSERT_EQ_FLOAT(-0.3, cfl_cbor_decode_float(&dec), 1e-15);

    /* z: 9.81 */
    cfl_cbor_decode_next(&dec); /* key "z" */
    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_FLOAT64, t);
    ASSERT_EQ_FLOAT(9.81, cfl_cbor_decode_float(&dec), 1e-15);

    PASS();
}

/* ======================================================================== */
/* TEST 9: Array round-trip                                                  */
/* ======================================================================== */
static void test_array_roundtrip(void) {
    TEST("array round-trip [1, 2.5, \"hello\", true]");

    uint8_t buf[64];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, buf, sizeof(buf));

    cfl_cbor_encode_array_begin(&enc, 4);
    cfl_cbor_encode_uint(&enc, 1);
    cfl_cbor_encode_float64(&enc, 2.5);
    cfl_cbor_encode_text(&enc, "hello");
    cfl_cbor_encode_bool(&enc, true);
    size_t len = cfl_cbor_encoder_finish(&enc);

    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, len);

    cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_ARRAY, t);
    ASSERT_EQ_INT(4, (int)cfl_cbor_decode_count(&dec));

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_UINT, t);
    ASSERT_EQ_INT(1, (int)cfl_cbor_decode_uint(&dec));

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_FLOAT64, t);
    ASSERT_EQ_FLOAT(2.5, cfl_cbor_decode_float(&dec), 1e-15);

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_TEXT, t);
    size_t slen;
    const char *text = cfl_cbor_decode_text(&dec, &slen);
    ASSERT_EQ_INT(5, (int)slen);
    ASSERT_EQ_STR("hello", text, slen);

    t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_BOOL, t);
    ASSERT_EQ_INT(true, cfl_cbor_decode_bool(&dec));

    PASS();
}

/* ======================================================================== */
/* TEST 10: JSON -> CBOR -> JSON round-trip (telemetry packet)               */
/* ======================================================================== */
static void test_json_cbor_roundtrip(void) {
    TEST("JSON->CBOR->JSON telemetry packet round-trip");

    const char *json_in =
        "{\"type\":\"telemetry\",\"seq\":1,\"topic\":\"sensors/accel\","
        "\"payload\":{\"x\":1.5,\"y\":-0.3,\"z\":9.81}}";

    uint8_t cbor_buf[512];
    size_t cbor_len = cfl_json_text_to_cbor(json_in, cbor_buf, sizeof(cbor_buf));

    if (cbor_len == 0) { FAIL("cbor_len is 0"); return; }

    char *json_out = cfl_cbor_to_json_text(cbor_buf, cbor_len, &s_heap);
    if (!json_out) { FAIL("json_out is NULL"); return; }

    /* Verify by parsing both and comparing fields */
    /* Since cJSON may reorder object keys, compare field-by-field */
    /* For now, just verify it parses and contains expected substrings */
    if (!strstr(json_out, "\"type\":\"telemetry\"")) {
        FAIL("missing type field"); free(json_out); return;
    }
    if (!strstr(json_out, "\"topic\":\"sensors/accel\"")) {
        FAIL("missing topic field"); free(json_out); return;
    }
    if (!strstr(json_out, "\"seq\":1")) {
        FAIL("missing seq field"); free(json_out); return;
    }

    free(json_out);
    PASS();
}

/* ======================================================================== */
/* TEST 11: JSON -> CBOR -> JSON with nested object and floats               */
/* ======================================================================== */
static void test_json_cbor_nested(void) {
    TEST("JSON->CBOR->JSON nested with floats");

    const char *json_in =
        "{\"command\":\"fly_straight\",\"distance\":100.5,"
        "\"final_altitude\":50.0,\"heading\":90.0}";

    uint8_t cbor_buf[512];
    size_t cbor_len = cfl_json_text_to_cbor(json_in, cbor_buf, sizeof(cbor_buf));

    char *json_out = cfl_cbor_to_json_text(cbor_buf, cbor_len, &s_heap);
    if (!json_out) { FAIL("json_out is NULL"); return; }

    if (!strstr(json_out, "\"command\":\"fly_straight\"")) {
        FAIL("missing command field"); free(json_out); return;
    }
    /* 100.5 should survive as float */
    if (!strstr(json_out, "100.5")) {
        FAIL("missing distance value 100.5"); free(json_out); return;
    }

    free(json_out);
    PASS();
}

/* ======================================================================== */
/* TEST 12: Buffer overflow detection                                        */
/* ======================================================================== */
static void test_buffer_overflow(void) {
    TEST("buffer overflow detection");

    uint8_t tiny[2];
    cfl_cbor_encoder_t enc;
    cfl_cbor_encoder_init(&enc, tiny, sizeof(tiny));

    if (setjmp(s_jmp) == 0) {
        /* This should overflow — text "hello" needs 6 bytes */
        cfl_cbor_encode_text(&enc, "hello");
        FAIL("should have raised exception");
    } else {
        /* Exception caught */
        if (strstr(s_exception_msg, "buffer overflow")) {
            PASS();
        } else {
            FAIL(s_exception_msg);
        }
    }
}

/* ======================================================================== */
/* TEST 13: End-of-input detection                                           */
/* ======================================================================== */
static void test_end_of_input(void) {
    TEST("decoder end-of-input");

    uint8_t buf[1];
    cfl_cbor_decoder_t dec;
    cfl_cbor_decoder_init(&dec, buf, 0);

    cfl_cbor_type_t t = cfl_cbor_decode_next(&dec);
    ASSERT_EQ_INT(CFL_CBOR_END, t);

    PASS();
}

/* ======================================================================== */
/* MAIN                                                                      */
/* ======================================================================== */
int main(void) {
    printf("=== CBOR Library Tests ===\n\n");

    printf("Encode/Decode:\n");
    test_uint_roundtrip();
    test_int_roundtrip();
    test_positive_via_int();
    test_float64_roundtrip();
    test_bool_null();
    test_text_roundtrip();
    test_map_roundtrip();
    test_nested_map();
    test_array_roundtrip();

    printf("\nTranscode:\n");
    test_json_cbor_roundtrip();
    test_json_cbor_nested();

    printf("\nError handling:\n");
    test_buffer_overflow();
    test_end_of_input();

    printf("\n=== Results: %d passed, %d failed ===\n", s_pass, s_fail);

    return s_fail > 0 ? 1 : 0;
}
