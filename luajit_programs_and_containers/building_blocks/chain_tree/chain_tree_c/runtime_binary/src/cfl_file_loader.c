/*
 * cfl_file_loader.c - File I/O and embedded loading
 */

 #include "cfl_file_loader.h"

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 int cfl_file_load(const char *path, cfl_image_loader_t *out)
 {
     if (!path || !out) return CFL_IMAGE_ERR_NULL_PTR;
     memset(out, 0, sizeof(cfl_image_loader_t));
 
     FILE *f = fopen(path, "rb");
     if (!f) return CFL_FILE_ERR_OPEN;
 
     if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return CFL_FILE_ERR_READ; }
     long file_size = ftell(f);
     if (file_size <= 0 || file_size > 0x7FFFFFFF) { fclose(f); return CFL_FILE_ERR_SIZE; }
     if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return CFL_FILE_ERR_READ; }
 
     uint8_t *buf = (uint8_t *)malloc((uint32_t)file_size);
     if (!buf) { fclose(f); return CFL_IMAGE_ERR_ALLOC; }
 
     size_t n = fread(buf, 1, (size_t)file_size, f);
     fclose(f);
     if (n != (size_t)file_size) { free(buf); return CFL_FILE_ERR_READ; }
 
     int rc = cfl_image_load(buf, (uint32_t)file_size, out);
     if (rc != CFL_IMAGE_OK) { free(buf); return rc; }
 
     return CFL_IMAGE_OK;
 }
 
 void cfl_file_unload(cfl_image_loader_t *img)
 {
     if (!img) return;
     void *buf = (void *)img->image_base;
     cfl_image_free(img);
     free(buf);
 }
 
 int cfl_embedded_load(const uint8_t *data, uint32_t size, cfl_image_loader_t *out)
 {
     if (!data || !out) return CFL_IMAGE_ERR_NULL_PTR;
     return cfl_image_load(data, size, out);
 }