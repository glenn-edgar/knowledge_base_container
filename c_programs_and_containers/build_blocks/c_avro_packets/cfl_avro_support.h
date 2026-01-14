/*
 * cfl_avro_support.h - Runtime support for Avro DSL packets
 * 
 * Provides hash calculation for schema verification at runtime.
 * The hash is computed using DJB2 algorithm (fast, good distribution).
 */

#ifndef CFL_AVRO_SUPPORT_H
#define CFL_AVRO_SUPPORT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Compute 32-bit hash of a null-terminated string.
 * Uses DJB2 algorithm by Dan Bernstein.
 * 
 * This is used to verify packet schema at runtime without
 * storing string pointers in packet headers.
 */
static inline uint32_t cfl_avro_hash(const char* str)
{
    uint32_t hash = 5381;
    int c;
    
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;  /* hash * 33 + c */
    }
    
    return hash;
}

/*
 * Compute hash at compile time (for constant strings).
 * Use this in static assertions or compile-time checks.
 * 
 * Note: This is a macro version for preprocessor use.
 * For runtime use, call cfl_avro_hash() instead.
 */
#define CFL_AVRO_HASH_INIT 5381
#define CFL_AVRO_HASH_STEP(hash, c) (((hash) << 5) + (hash) + (c))

#ifdef __cplusplus
}
#endif

#endif /* CFL_AVRO_SUPPORT_H */

