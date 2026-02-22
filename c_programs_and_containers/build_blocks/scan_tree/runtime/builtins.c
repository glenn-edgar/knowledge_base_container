/* =============================================================
 * System VFTs
 *
 * Access buffer data through handle + input descriptor buf_id.
 * ============================================================= */
#include "scan_tree.h"
 uint8_t st_vft_and(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     (void)st; (void)n;
     const uint8_t *d = st_buf_data(h, in[0].buf_id);
     for (uint16_t i = 0; i < in[0].count; i++)
         if (!d[in[0].start + i]) return 0;
     return 1;
 }
 
 uint8_t st_vft_or(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     (void)st; (void)n;
     const uint8_t *d = st_buf_data(h, in[0].buf_id);
     for (uint16_t i = 0; i < in[0].count; i++)
         if (d[in[0].start + i]) return 1;
     return 0;
 }
 
 uint8_t st_vft_not(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     (void)st; (void)n;
     return st_buf_data(h, in[0].buf_id)[in[0].start] ? 0 : 1;
 }
 
 uint8_t st_vft_latch(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     const uint8_t *sd = NULL, *cd = NULL;
     uint16_t sp = 0, cp = 0;
     for (uint32_t i = 0; i < n; i++) {
         if (in[i].role == ST_ROLE_SET)   { sd = st_buf_data(h, in[i].buf_id); sp = in[i].start; }
         if (in[i].role == ST_ROLE_CLEAR) { cd = st_buf_data(h, in[i].buf_id); cp = in[i].start; }
     }
     if (cd && cd[cp])      *st = 0;
     else if (sd && sd[sp]) *st = 1;
     return *st;
 }
 
 uint8_t st_vft_k_of_n(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     (void)st;
     uint8_t thresh = 0;
     const uint8_t *bits = NULL; uint16_t bs = 0, bc = 0;
     for (uint32_t i = 0; i < n; i++) {
         if (in[i].role == ST_ROLE_THRESHOLD) thresh = st_buf_data(h, in[i].buf_id)[in[i].start];
         if (in[i].role == ST_ROLE_BITS) { bits = st_buf_data(h, in[i].buf_id); bs = in[i].start; bc = in[i].count; }
     }
     uint8_t pop = 0;
     if (bits) for (uint16_t i = 0; i < bc; i++) if (bits[bs + i]) pop++;
     return (pop >= thresh) ? 1 : 0;
 }
 
 uint8_t st_vft_gt(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     (void)st;
     float a = 0, b = 0;
     for (uint32_t i = 0; i < n; i++) {
         const float *fd = (const float *)st_buf_data(h, in[i].buf_id);
         if (in[i].role == ST_ROLE_A) a = fd[in[i].start];
         if (in[i].role == ST_ROLE_B) b = fd[in[i].start];
     }
     return (a > b) ? 1 : 0;
 }
 
 uint8_t st_vft_copy(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     (void)st; (void)n;
     return st_buf_data(h, in[0].buf_id)[in[0].start];
 }
 
 
 /*
  * st_vft_fuse - one-shot with clear cycle
  *
  * State machine (encoded in *st):
  *   0 = intact:   input high -> blow, fire action, output=1, state=1
  *   1 = blown:    output=1, wait for clear high -> state=2
  *   2 = clearing: wait for clear low -> state=0, output=0
  *
  * Inputs by role:
  *   ST_ROLE_INPUT - trip signal
  *   ST_ROLE_CLEAR - clear signal (must cycle low->high->low to re-arm)
  */
 uint8_t st_vft_fuse(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
 {
     uint8_t input_val = 0, clear_val = 0;
     for (uint32_t i = 0; i < n; i++) {
         if (in[i].role == ST_ROLE_INPUT)
             input_val = st_buf_data(h, in[i].buf_id)[in[i].start];
         if (in[i].role == ST_ROLE_CLEAR)
             clear_val = st_buf_data(h, in[i].buf_id)[in[i].start];
     }
 
     switch (*st) {
     case 0: /* intact */
         if (input_val) {
             *st = 1;
             /* Fire the fuse action callback */
             /* node_id is not directly available here; the evaluate loop
              * passes the node index via node_state pointer arithmetic */
             uint16_t node_id = (uint16_t)(st - h->node_state);
             st_fuse_action_t action = st_fuse_lookup(h, node_id);
             if (action) action(h->user_handle);
             return 1;
         }
         return 0;
 
     case 1: /* blown, waiting for clear high */
         if (clear_val) *st = 2;
         return 1;
 
     case 2: /* clearing, waiting for clear low */
         if (!clear_val) {
             *st = 0;
             return 0;
         }
         return 1;
 
     default:
         *st = 0;
         return 0;
     }
 }

 uint8_t st_vft_ge(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
{
    (void)st;
    float a = 0, b = 0;
    for (uint32_t i = 0; i < n; i++) {
        const float *fd = (const float *)st_buf_data(h, in[i].buf_id);
        if (in[i].role == ST_ROLE_A) a = fd[in[i].start];
        if (in[i].role == ST_ROLE_B) b = fd[in[i].start];
    }
    return (a >= b) ? 1 : 0;
}

uint8_t st_vft_eq(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
{
    (void)st;
    float a = 0, b = 0;
    for (uint32_t i = 0; i < n; i++) {
        const float *fd = (const float *)st_buf_data(h, in[i].buf_id);
        if (in[i].role == ST_ROLE_A) a = fd[in[i].start];
        if (in[i].role == ST_ROLE_B) b = fd[in[i].start];
    }
    return (a == b) ? 1 : 0;
}

uint8_t st_vft_lt(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
{
    (void)st;
    float a = 0, b = 0;
    for (uint32_t i = 0; i < n; i++) {
        const float *fd = (const float *)st_buf_data(h, in[i].buf_id);
        if (in[i].role == ST_ROLE_A) a = fd[in[i].start];
        if (in[i].role == ST_ROLE_B) b = fd[in[i].start];
    }
    return (a < b) ? 1 : 0;
}

uint8_t st_vft_le(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
{
    (void)st;
    float a = 0, b = 0;
    for (uint32_t i = 0; i < n; i++) {
        const float *fd = (const float *)st_buf_data(h, in[i].buf_id);
        if (in[i].role == ST_ROLE_A) a = fd[in[i].start];
        if (in[i].role == ST_ROLE_B) b = fd[in[i].start];
    }
    return (a <= b) ? 1 : 0;
}

/*
 * st_vft_range_check: output = 1 if low <= value <= high
 *   inputs[DEFAULT] = value
 *   inputs[A]       = low bound
 *   inputs[B]       = high bound
 */
uint8_t st_vft_range_check(uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n)
{
    (void)st;
    float val = 0, lo = 0, hi = 0;
    for (uint32_t i = 0; i < n; i++) {
        const float *fd = (const float *)st_buf_data(h, in[i].buf_id);
        if (in[i].role == ST_ROLE_DEFAULT) val = fd[in[i].start];
        if (in[i].role == ST_ROLE_A)       lo  = fd[in[i].start];
        if (in[i].role == ST_ROLE_B)       hi  = fd[in[i].start];
    }
    return (val >= lo && val <= hi) ? 1 : 0;
}