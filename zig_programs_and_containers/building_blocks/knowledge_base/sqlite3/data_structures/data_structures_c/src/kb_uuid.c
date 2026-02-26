#include "kb_uuid.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

void kb_uuid_seed(void) { srand((unsigned int)time(NULL)); }

static int rh(void) { return rand() % 16; }

void kb_uuid4(char *buf, size_t buf_size)
{
    if (!buf || buf_size < KB_UUID_LEN) return;
    snprintf(buf, buf_size,
        "%x%x%x%x%x%x%x%x-%x%x%x%x-4%x%x%x-%x%x%x%x-%x%x%x%x%x%x%x%x%x%x%x%x",
        rh(),rh(),rh(),rh(),rh(),rh(),rh(),rh(),
        rh(),rh(),rh(),rh(), rh(),rh(),rh(),
        (8|(rand()%4)),rh(),rh(),rh(),
        rh(),rh(),rh(),rh(),rh(),rh(),rh(),rh(),rh(),rh(),rh(),rh());
}
