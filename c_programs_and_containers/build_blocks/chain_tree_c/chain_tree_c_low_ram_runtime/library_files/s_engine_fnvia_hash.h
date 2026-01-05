#ifndef S_ENGINE_FNVIA_HASH_H
#define S_ENGINE_FNVIA_HASH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

#define FNV1A_32_INIT  0x811c9dc5U
#define FNV1A_32_PRIME 0x01000193U

/* Hash a byte buffer */
static inline uint32_t fnv1a_32(const void *data, size_t len)
{
    const uint8_t *p = (const uint8_t *)data;
    uint32_t hash = FNV1A_32_INIT;
    
    while (len--) {
        hash ^= *p++;
        hash *= FNV1A_32_PRIME;
    }
    return hash;
}

/* Hash a null-terminated string */
static inline uint32_t fnv1a_32_str(const char *str)
{
    uint32_t hash = FNV1A_32_INIT;
    
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= FNV1A_32_PRIME;
    }
    return hash;
}

/* Incremental hashing - useful for streaming */
static inline uint32_t fnv1a_32_update(uint32_t hash, uint8_t byte)
{
    return (hash ^ byte) * FNV1A_32_PRIME;
}

#ifdef __cplusplus
}
#endif

#endif