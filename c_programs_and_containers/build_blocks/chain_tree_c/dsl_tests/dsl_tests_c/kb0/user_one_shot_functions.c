/* KB0 user one-shot functions (host test). */
#include <stdio.h>
#include <stdint.h>
#include "cfl_runtime.h"
void mon_ping_reply_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle; (void)node_index; printf(">>> MON_PING_REPLY fired\n");
}
void mon_snapshot_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle; (void)node_index; printf(">>> MON_SNAPSHOT fired\n");
}
void mon_cmd_timeout_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle; (void)node_index;
}
