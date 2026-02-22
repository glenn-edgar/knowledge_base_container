#include <stdint.h>


uint32_t fnv1a_hash(const char *str) {
    uint32_t h = 2166136261u;
    while (*str) { h ^= (uint8_t)*str++; h *= 16777619u; }
    return h;
}