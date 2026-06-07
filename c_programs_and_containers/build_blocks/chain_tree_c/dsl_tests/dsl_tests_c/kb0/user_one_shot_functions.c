/* KB0+KB1 user one-shots (host test). */
#include <stdio.h>
#include <stdint.h>
#include "cfl_runtime.h"
void mon_ping_reply_one_shot_fn(void *h, unsigned n){(void)h;(void)n;printf(">>> MON_PING_REPLY fired\n");}
void mon_snapshot_one_shot_fn(void *h, unsigned n){(void)h;(void)n;printf(">>> MON_SNAPSHOT fired\n");}
void mon_cmd_timeout_one_shot_fn(void *h, unsigned n){(void)h;(void)n;}
void adc_read_one_shot_fn(void *h, unsigned n){(void)h;(void)n;printf(">>> ADC_READ fired\n");}
