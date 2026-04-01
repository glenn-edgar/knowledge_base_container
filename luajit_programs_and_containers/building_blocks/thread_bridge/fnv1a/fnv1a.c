/**
 * fnv1a.c — FNV-1a 32-bit hash, proper uint32_t arithmetic.
 *
 * Used by Thread bridge field registry and ChainTree dispatch.
 * Matches the Lua implementation in chain_tree_luajit/lua_dsl/lua_support/fnv1a.lua
 * but without integer overflow concerns.
 */

#include <stdint.h>
#include <stddef.h>

#define FNV_OFFSET_32 0x811C9DC5u
#define FNV_PRIME_32  0x01000193u

uint32_t fnv1a_32(const char *data, size_t len) {
    uint32_t hash = FNV_OFFSET_32;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint8_t)data[i];
        hash *= FNV_PRIME_32;
    }
    return hash;
}

/* String variant (null-terminated) */
uint32_t fnv1a_32_str(const char *str) {
    uint32_t hash = FNV_OFFSET_32;
    while (*str) {
        hash ^= (uint8_t)*str++;
        hash *= FNV_PRIME_32;
    }
    return hash;
}

/* Schema hash: "file.h:record" format, returns signed int32 for JSON */
int32_t fnv1a_schema_hash(const char *file_name, const char *record_name) {
    /* Build key: "{file_name}.h:{record_name}" inline */
    uint32_t hash = FNV_OFFSET_32;

    /* Hash file_name */
    const char *p = file_name;
    while (*p) { hash ^= (uint8_t)*p++; hash *= FNV_PRIME_32; }

    /* Hash ".h:" */
    hash ^= '.'; hash *= FNV_PRIME_32;
    hash ^= 'h'; hash *= FNV_PRIME_32;
    hash ^= ':'; hash *= FNV_PRIME_32;

    /* Hash record_name */
    p = record_name;
    while (*p) { hash ^= (uint8_t)*p++; hash *= FNV_PRIME_32; }

    return (int32_t)hash;
}
