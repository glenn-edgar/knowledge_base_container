/*
 * cfl_fnv1a.h - FNV-1a 32-bit Hash
 *
 * Used by binary image loader for function registration.
 * Hash values in the binary image are pre-sorted for binary search.
 */

 #ifndef CFL_FNV1A_H
 #define CFL_FNV1A_H
 
 #include <stdint.h>
 
 uint32_t cfl_fnv1a_32(const char *str);
 
 #endif /* CFL_FNV1A_H */