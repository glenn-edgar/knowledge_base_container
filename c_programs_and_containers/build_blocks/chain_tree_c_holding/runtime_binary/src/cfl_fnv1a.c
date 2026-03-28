/*
 * cfl_fnv1a.c - FNV-1a 32-bit Hash
 */

 #include "cfl_fnv1a.h"

 uint32_t cfl_fnv1a_32(const char *str)
 {
     uint32_t hash = 0x811C9DC5u;  /* FNV offset basis */
     while (*str) {
         hash ^= (uint8_t)*str++;
         hash *= 0x01000193u;      /* FNV prime */
     }
     return hash;
 }