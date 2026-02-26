#ifndef KB_UUID_H
#define KB_UUID_H
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
#define KB_UUID_LEN 37
void kb_uuid4(char *buf, size_t buf_size);
void kb_uuid_seed(void);
#ifdef __cplusplus
}
#endif
#endif
