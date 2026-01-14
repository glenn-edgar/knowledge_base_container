/*
 * cfl_avro_support.c - Runtime support for Avro DSL packets
 * 
 * Non-inline implementation for cases where inline is not desired.
 */

#include "cfl_avro_support.h"

/*
 * Non-inline version of hash function.
 * Useful when code size is more important than speed,
 * or when taking function pointer.
 */
uint32_t cfl_avro_hash_func(const char* str)
{
    return cfl_avro_hash(str);
}

