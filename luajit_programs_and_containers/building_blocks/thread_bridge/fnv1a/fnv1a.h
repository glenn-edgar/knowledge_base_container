#ifndef FNV1A_H
#define FNV1A_H

#include <stdint.h>
#include <stddef.h>

/* FNV-1a 32-bit hash (raw bytes) */
uint32_t fnv1a_32(const char *data, size_t len);

/* FNV-1a 32-bit hash (null-terminated string) */
uint32_t fnv1a_32_str(const char *str);

/* Schema hash: fnv1a_32("{file_name}.h:{record_name}"), signed for JSON */
int32_t fnv1a_schema_hash(const char *file_name, const char *record_name);

#endif
