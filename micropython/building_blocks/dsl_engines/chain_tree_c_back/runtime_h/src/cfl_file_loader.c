/*
 * ct_file.c - ChainTree Binary Image File Layer
 *
 * Provides file I/O and embedded array loading on top of ct_runtime.
 *
 * Build:
 *   gcc -O2 -c ct_file.c
 *   ar rcs libct_runtime.a ct_runtime.o ct_file.o fnv1a.o
 */

#include "cfl_file_loader.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ===================================================================
 * ct_file_load - Read .ctb from filesystem, call ct_image_load
 * =================================================================== */

int cfl_file_loader_load(const char *path, cfl_image_loader_t *out)
{
    if (!path || !out) return CFL_IMAGE_LOADER_NULL_PTR;

    memset(out, 0, sizeof(cfl_image_loader_t));

    /* Open file */
    FILE *f = fopen(path, "rb");
    if (!f) return CFL_FILE_LOADER_ERR_FILE_OPEN;

    /* Get file size */
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return CFL_FILE_LOADER_ERR_FILE_READ;
    }

    long file_size = ftell(f);
    if (file_size <= 0 || file_size > 0x7FFFFFFF) {
        fclose(f);
        return CFL_FILE_LOADER_ERR_FILE_SIZE;
    }

    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return CFL_FILE_LOADER_ERR_FILE_READ;
    }

    /* Allocate buffer */
    uint8_t *buf = (uint8_t *)malloc((uint32_t)file_size);
    if (!buf) {
        fclose(f);
        return CFL_IMAGE_LOADER_ALLOC ;
    }

    /* Read entire file */
    size_t bytes_read = fread(buf, 1, (size_t)file_size, f);
    fclose(f);

    if (bytes_read != (size_t)file_size) {
        free(buf);
        return CFL_FILE_LOADER_ERR_FILE_READ;
    }

    /* Load the image */
    int rc = cfl_image_loader_load(buf, (uint32_t)file_size, out);
    if (rc != CFL_IMAGE_LOADER_OK) {
        free(buf);
        return rc;
    }

    /*
     * image_base now points to buf. The ct_image_t holds const pointers
     * into this buffer. We need to remember buf so ct_file_unload can
     * free it. We store it by convention: image_base IS the malloc'd
     * pointer (ct_image_load sets it to the image_data argument).
     */

    return CFL_IMAGE_LOADER_OK;
}

/* ===================================================================
 * ct_file_unload - Free file buffer + runtime resources
 * =================================================================== */

void cfl_file_loader_unload(cfl_image_loader_t *img)
{
    if (!img) return;

    /* Save the file buffer pointer before ct_image_free zeroes the struct */
    void *file_buf = (void *)img->image_base;

    /* Free runtime-allocated arrays (function ptrs, name ptrs, etc.) */
    cfl_image_loader_free(img);

    /* Free the file buffer */
    free(file_buf);
}

/* ===================================================================
 * ct_embedded_load - Load from const uint8_t[] in flash
 * =================================================================== */

int cfl_embedded_load(const uint8_t *data, uint32_t size, cfl_image_loader_t *out)
{
    if (!data || !out) return CFL_IMAGE_LOADER_NULL_PTR;

    /*
     * Direct pass-through to ct_image_load. Zero-copy: the const array
     * is used in place. Caller must NOT call ct_file_unload() on this —
     * use ct_image_free() instead, which frees only the runtime arrays
     * and does not touch image_base.
     */
    return cfl_embedded_load(data, size, out);
}