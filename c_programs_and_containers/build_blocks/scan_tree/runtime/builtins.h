#ifndef BUILTINS_H
#define BUILTINS_H
#ifdef __cplusplus
extern "C" {
#endif


 /* System VFTs */
 uint8_t st_vft_and    (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_or     (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_not    (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_latch  (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_k_of_n (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_gt     (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_copy   (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
 uint8_t st_vft_ge          (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
uint8_t st_vft_eq          (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
uint8_t st_vft_lt          (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
uint8_t st_vft_le          (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
uint8_t st_vft_range_check (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);
uint8_t st_vft_fuse       (uint8_t *st, const st_handle_t *h, const st_input_desc_t *in, uint32_t n);


 #ifdef __cplusplus
 }
 #endif

 #endif