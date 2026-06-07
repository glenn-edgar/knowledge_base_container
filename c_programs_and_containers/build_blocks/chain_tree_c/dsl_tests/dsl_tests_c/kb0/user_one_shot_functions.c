/* KB0 user one-shot functions (host test). */
#include <stdio.h>
#include <stdint.h>
#include "cfl_runtime.h"

void mon_ping_reply_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle; (void)node_index;
    printf(">>> MON_PING_REPLY fired (node_index=%u)\n", node_index);
}
