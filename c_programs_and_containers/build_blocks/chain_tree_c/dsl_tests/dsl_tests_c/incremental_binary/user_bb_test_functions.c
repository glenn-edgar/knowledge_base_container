/*
 * user_bb_test_functions.c - Blackboard verification one-shot functions
 *
 * Test 29: exercises the blackboard API with:
 *   - Basic mutable field read/write (int32, float, uint16)
 *   - Nested struct fields (nav.heading, nav.altitude, nav.speed)
 *   - Constant record lookup and field access
 *   - 64-bit pointer storage
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <math.h>
#include "cfl_runtime.h"
#include "cfl_blackboard.h"
#include "cfl_image_loader.h"
#include "chaintree_handle_blackboard.h"

/* ========================================================================
 * Helper: float comparison with epsilon
 * ======================================================================== */

static bool float_eq(float a, float b) {
    return fabsf(a - b) < 0.001f;
}

/* ========================================================================
 * BB_INIT_FIELDS - Set blackboard fields to known values
 * ======================================================================== */

void bb_init_fields_one_shot_fn(void *handle, unsigned node_index) {
    (void)node_index;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;

    /* Write via compile-time offset macros (fast path) */
    *CFL_BB_FIELD(rt, BB_MODE_OFFSET, int32_t)         = 42;
    *CFL_BB_FIELD(rt, BB_TEMPERATURE_OFFSET, float)     = 98.6f;
    *CFL_BB_FIELD(rt, BB_ERROR_COUNT_OFFSET, uint16_t)  = 7;

    /* Write nested fields via offset macros */
    *CFL_BB_FIELD(rt, BB_NAV_HEADING_OFFSET, int32_t)   = 270;
    *CFL_BB_FIELD(rt, BB_NAV_ALTITUDE_OFFSET, float)    = 3500.0f;
    *CFL_BB_FIELD(rt, BB_NAV_SPEED_OFFSET, float)       = 125.5f;

    /* Write 64-bit value */
    *CFL_BB_FIELD(rt, BB_DEBUG_PTR_OFFSET, uint64_t)    = 0xDEADBEEFCAFEBABEULL;

    printf("[BB_INIT] Fields initialized\n");
}

/* ========================================================================
 * BB_VERIFY_BASIC_FIELDS - Verify int32, float, uint16 via both
 *   compile-time offsets and dynamic hash lookup
 * ======================================================================== */

void bb_verify_basic_fields_one_shot_fn(void *handle, unsigned node_index) {
    (void)node_index;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    int pass = 0, fail = 0;

    /* --- Verify via compile-time offset macros --- */
    int32_t mode = *CFL_BB_FIELD(rt, BB_MODE_OFFSET, int32_t);
    if (mode == 42) { pass++; } else {
        printf("[BB_VERIFY] FAIL: mode=%d expected 42\n", mode);
        fail++;
    }

    float temp = *CFL_BB_FIELD(rt, BB_TEMPERATURE_OFFSET, float);
    if (float_eq(temp, 98.6f)) { pass++; } else {
        printf("[BB_VERIFY] FAIL: temperature=%f expected 98.6\n", temp);
        fail++;
    }

    uint16_t errors = *CFL_BB_FIELD(rt, BB_ERROR_COUNT_OFFSET, uint16_t);
    if (errors == 7) { pass++; } else {
        printf("[BB_VERIFY] FAIL: error_count=%u expected 7\n", errors);
        fail++;
    }

    /* --- Verify via dynamic hash lookup --- */
    int32_t mode2 = cfl_bb_get_int32(rt, cfl_bb_hash("mode"), -1);
    if (mode2 == 42) { pass++; } else {
        printf("[BB_VERIFY] FAIL: mode (hash)=%d expected 42\n", mode2);
        fail++;
    }

    float temp2 = cfl_bb_get_float(rt, cfl_bb_hash("temperature"), 0.0f);
    if (float_eq(temp2, 98.6f)) { pass++; } else {
        printf("[BB_VERIFY] FAIL: temperature (hash)=%f expected 98.6\n", temp2);
        fail++;
    }

    uint16_t errors2 = cfl_bb_get_uint16(rt, cfl_bb_hash("error_count"), 0);
    if (errors2 == 7) { pass++; } else {
        printf("[BB_VERIFY] FAIL: error_count (hash)=%u expected 7\n", errors2);
        fail++;
    }

    /* --- Verify via field_by_name --- */
    float *temp_ptr = (float *)cfl_bb_field_by_name(rt, "temperature");
    if (temp_ptr && float_eq(*temp_ptr, 98.6f)) { pass++; } else {
        printf("[BB_VERIFY] FAIL: temperature (by_name) ptr=%p\n", (void *)temp_ptr);
        fail++;
    }

    printf("[BB_VERIFY_BASIC] pass=%d fail=%d\n", pass, fail);
}

/* ========================================================================
 * BB_VERIFY_NESTED_FIELDS - Verify nested struct fields (nav.*)
 * ======================================================================== */

void bb_verify_nested_fields_one_shot_fn(void *handle, unsigned node_index) {
    (void)node_index;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    int pass = 0, fail = 0;

    /* Read via offset macros */
    int32_t heading = *CFL_BB_FIELD(rt, BB_NAV_HEADING_OFFSET, int32_t);
    float altitude  = *CFL_BB_FIELD(rt, BB_NAV_ALTITUDE_OFFSET, float);
    float speed     = *CFL_BB_FIELD(rt, BB_NAV_SPEED_OFFSET, float);

    if (heading == 270) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.heading=%d expected 270\n", heading);
        fail++;
    }
    if (float_eq(altitude, 3500.0f)) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.altitude=%f expected 3500.0\n", altitude);
        fail++;
    }
    if (float_eq(speed, 125.5f)) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.speed=%f expected 125.5\n", speed);
        fail++;
    }

    /* Read via dynamic hash lookup (dotted names) */
    int32_t h2 = cfl_bb_get_int32(rt, cfl_bb_hash("nav.heading"), -1);
    float a2   = cfl_bb_get_float(rt, cfl_bb_hash("nav.altitude"), 0.0f);
    float s2   = cfl_bb_get_float(rt, cfl_bb_hash("nav.speed"), 0.0f);

    if (h2 == 270) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.heading (hash)=%d\n", h2);
        fail++;
    }
    if (float_eq(a2, 3500.0f)) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.altitude (hash)=%f\n", a2);
        fail++;
    }
    if (float_eq(s2, 125.5f)) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.speed (hash)=%f\n", s2);
        fail++;
    }

    /* Modify a nested field and read it back */
    cfl_bb_set_int32(rt, cfl_bb_hash("nav.heading"), 90);
    int32_t h3 = *CFL_BB_FIELD(rt, BB_NAV_HEADING_OFFSET, int32_t);
    if (h3 == 90) { pass++; } else {
        printf("[BB_NESTED] FAIL: nav.heading after set=%d expected 90\n", h3);
        fail++;
    }

    printf("[BB_VERIFY_NESTED] pass=%d fail=%d\n", pass, fail);
}

/* ========================================================================
 * BB_VERIFY_CONST_RECORD - Verify constant record lookup and field access
 * ======================================================================== */

void bb_verify_const_record_one_shot_fn(void *handle, unsigned node_index) {
    (void)node_index;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    int pass = 0, fail = 0;

    /* Find the calibration constant record */
    const cfl_bb_const_record_t *cal = cfl_bb_const_find(rt, cfl_bb_hash("calibration"));
    if (!cal) {
        printf("[BB_CONST] FAIL: calibration record not found\n");
        return;
    }
    pass++;

    /* Read via compile-time offset macros */
    float gain = *CFL_BB_CONST_FIELD(cal->data, CONST_CALIBRATION_GAIN_OFFSET, float);
    if (float_eq(gain, 1.5f)) { pass++; } else {
        printf("[BB_CONST] FAIL: gain=%f expected 1.5\n", gain);
        fail++;
    }

    float offset = *CFL_BB_CONST_FIELD(cal->data, CONST_CALIBRATION_OFFSET_OFFSET, float);
    if (float_eq(offset, -0.25f)) { pass++; } else {
        printf("[BB_CONST] FAIL: offset=%f expected -0.25\n", offset);
        fail++;
    }

    int32_t max_val = *CFL_BB_CONST_FIELD(cal->data, CONST_CALIBRATION_MAX_VALUE_OFFSET, int32_t);
    if (max_val == 1000) { pass++; } else {
        printf("[BB_CONST] FAIL: max_value=%d expected 1000\n", max_val);
        fail++;
    }

    /* Nested const fields */
    float sx = *CFL_BB_CONST_FIELD(cal->data, CONST_CALIBRATION_SCALE_X_OFFSET, float);
    float sy = *CFL_BB_CONST_FIELD(cal->data, CONST_CALIBRATION_SCALE_Y_OFFSET, float);
    if (float_eq(sx, 2.0f)) { pass++; } else {
        printf("[BB_CONST] FAIL: scale_x=%f expected 2.0\n", sx);
        fail++;
    }
    if (float_eq(sy, 3.0f)) { pass++; } else {
        printf("[BB_CONST] FAIL: scale_y=%f expected 3.0\n", sy);
        fail++;
    }

    /* Read via dynamic hash lookup */
    const float *gain_ptr = (const float *)cfl_bb_const_field_by_hash(cal, cfl_bb_hash("gain"));
    if (gain_ptr && float_eq(*gain_ptr, 1.5f)) { pass++; } else {
        printf("[BB_CONST] FAIL: gain (hash) lookup failed\n");
        fail++;
    }

    const int32_t *max_ptr = (const int32_t *)cfl_bb_const_field_by_hash(cal, cfl_bb_hash("max_value"));
    if (max_ptr && *max_ptr == 1000) { pass++; } else {
        printf("[BB_CONST] FAIL: max_value (hash) lookup failed\n");
        fail++;
    }

    printf("[BB_VERIFY_CONST] pass=%d fail=%d\n", pass, fail);
}

/* ========================================================================
 * BB_VERIFY_PTR64_FIELD - Verify 64-bit pointer/value in blackboard
 * ======================================================================== */

void bb_verify_ptr64_field_one_shot_fn(void *handle, unsigned node_index) {
    (void)node_index;
    cfl_runtime_handle_t *rt = (cfl_runtime_handle_t *)handle;
    int pass = 0, fail = 0;

    /* Read the 64-bit value via offset macro */
    uint64_t val = *CFL_BB_FIELD(rt, BB_DEBUG_PTR_OFFSET, uint64_t);
    if (val == 0xDEADBEEFCAFEBABEULL) { pass++; } else {
        printf("[BB_PTR64] FAIL: debug_ptr=0x%016llX expected 0xDEADBEEFCAFEBABE\n",
               (unsigned long long)val);
        fail++;
    }

    /* Store a pointer, read it back */
    static int32_t sentinel = 12345;
    *CFL_BB_FIELD(rt, BB_DEBUG_PTR_OFFSET, uint64_t) = (uint64_t)(uintptr_t)&sentinel;

    uint64_t stored = *CFL_BB_FIELD(rt, BB_DEBUG_PTR_OFFSET, uint64_t);
    int32_t *recovered = (int32_t *)(uintptr_t)stored;
    if (recovered == &sentinel && *recovered == 12345) { pass++; } else {
        printf("[BB_PTR64] FAIL: pointer roundtrip failed\n");
        fail++;
    }

    /* Verify via hash lookup */
    void *ptr = cfl_bb_field_by_hash(rt, cfl_bb_hash("debug_ptr"));
    if (ptr) {
        uint64_t v = *(uint64_t *)ptr;
        int32_t *r = (int32_t *)(uintptr_t)v;
        if (r == &sentinel && *r == 12345) { pass++; } else {
            printf("[BB_PTR64] FAIL: hash lookup roundtrip failed\n");
            fail++;
        }
    } else {
        printf("[BB_PTR64] FAIL: debug_ptr field not found by hash\n");
        fail++;
    }

    printf("[BB_VERIFY_PTR64] pass=%d fail=%d\n", pass, fail);
}

/* ========================================================================
 * Registration
 * ======================================================================== */

void cfl_register_bb_test_functions(cfl_image_loader_t *img) {
    int rc;
    int missing = 0;

    #define REG_BB_OS(name_str, sym) do { \
        rc = cfl_image_register_one_shot(img, (name_str), (sym)); \
        if (rc < 0) { fprintf(stderr, "  WARN: bb one_shot not found: %s\n", name_str); missing++; } \
    } while(0)

    REG_BB_OS("bb_init_fields_one_shot",          bb_init_fields_one_shot_fn);
    REG_BB_OS("bb_verify_basic_fields_one_shot",   bb_verify_basic_fields_one_shot_fn);
    REG_BB_OS("bb_verify_nested_fields_one_shot",  bb_verify_nested_fields_one_shot_fn);
    REG_BB_OS("bb_verify_const_record_one_shot",   bb_verify_const_record_one_shot_fn);
    REG_BB_OS("bb_verify_ptr64_field_one_shot",    bb_verify_ptr64_field_one_shot_fn);

    #undef REG_BB_OS

    if (missing > 0) {
        printf("  BB test: %d function(s) not in image (test 29 not active?)\n", missing);
    }
}
