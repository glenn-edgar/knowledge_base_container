/* KB0 user one-shot functions (host test). */
#include <stdio.h>
#include <stdint.h>
#include "cfl_runtime.h"

/* Fired when command_branch receives CMD_MON_PING. (The embed build replies with
 * [req_id][status][uptime][boot][kb0_ver] over the up-queue; here it just proves
 * the event dispatch fired.) */
void mon_ping_reply_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle; (void)node_index;
    printf(">>> MON_PING_REPLY fired (node_index=%u)\n", node_index);
}

/* command_branch wait_for_event timeout error handler (long timeout; host test
 * never hits it). */
void mon_cmd_timeout_one_shot_fn(void *handle, unsigned node_index) {
    (void)handle;
    printf(">>> MON_CMD_TIMEOUT (node_index=%u)\n", node_index);
}
