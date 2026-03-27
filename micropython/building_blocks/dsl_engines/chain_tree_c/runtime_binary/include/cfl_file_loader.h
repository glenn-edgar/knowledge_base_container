/*
 * cfl_file_loader.h - File I/O and embedded loading
 */

 #ifndef CFL_FILE_LOADER_H
 #define CFL_FILE_LOADER_H
 
 #include "cfl_image_loader.h"
 
 #define CFL_FILE_ERR_OPEN  -20
 #define CFL_FILE_ERR_READ  -21
 #define CFL_FILE_ERR_SIZE  -22
 
 int  cfl_file_load(const char *path, cfl_image_loader_t *out);
 void cfl_file_unload(cfl_image_loader_t *img);
 int  cfl_embedded_load(const uint8_t *data, uint32_t size, cfl_image_loader_t *out);
 
 #endif /* CFL_FILE_LOADER_H */