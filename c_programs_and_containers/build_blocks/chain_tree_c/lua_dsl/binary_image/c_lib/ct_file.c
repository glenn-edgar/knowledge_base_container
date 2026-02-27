/*
 * ct_file.c - ChainTree Binary Image File Layer
 *
 * Provides file I/O and embedded array loading on top of ct_runtime.
 *
 * Build:
 *   gcc -O2 -c ct_file.c
 *   ar rcs libct_runtime.a ct_runtime.o ct_file.o fnv1a.o
 */

#include "ct_file.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ===================================================================
 * ct_file_load - Read .ctb from filesystem, call ct_image_load
 * =================================================================== */

int ct_file_load(const char *path, ct_image_t *out)
{
    if (!path || !out) return CT_ERR_NULL_PTR;

    memset(out, 0, sizeof(ct_image_t));

    /* Open file */
    FILE *f = fopen(path, "rb");
    if (!f) return CT_ERR_FILE_OPEN;

    /* Get file size */
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return CT_ERR_FILE_READ;
    }

    long file_size = ftell(f);
    if (file_size <= 0 || file_size > 0x7FFFFFFF) {
        fclose(f);
        return CT_ERR_FILE_SIZE;
    }

    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return CT_ERR_FILE_READ;
    }

    /* Allocate buffer */
    uint8_t *buf = (uint8_t *)malloc((uint32_t)file_size);
    if (!buf) {
        fclose(f);
        return CT_ERR_ALLOC;
    }

    /* Read entire file */
    size_t bytes_read = fread(buf, 1, (size_t)file_size, f);
    fclose(f);

    if (bytes_read != (size_t)file_size) {
        free(buf);
        return CT_ERR_FILE_READ;
    }

    /* Load the image */
    int rc = ct_image_load(buf, (uint32_t)file_size, out);
    if (rc != CT_OK) {
        free(buf);
        return rc;
    }

    /*
     * image_base now points to buf. The ct_image_t holds const pointers
     * into this buffer. We need to remember buf so ct_file_unload can
     * free it. We store it by convention: image_base IS the malloc'd
     * pointer (ct_image_load sets it to the image_data argument).
     */

    return CT_OK;
}

/* ===================================================================
 * ct_file_unload - Free file buffer + runtime resources
 * =================================================================== */

void ct_file_unload(ct_image_t *img)
{
    if (!img) return;

    /* Save the file buffer pointer before ct_image_free zeroes the struct */
    void *file_buf = (void *)img->image_base;

    /* Free runtime-allocated arrays (function ptrs, name ptrs, etc.) */
    ct_image_free(img);

    /* Free the file buffer */
    free(file_buf);
}

/* ===================================================================
 * ct_embedded_load - Load from const uint8_t[] in flash
 * =================================================================== */

int ct_embedded_load(const uint8_t *data, uint32_t size, ct_image_t *out)
{
    if (!data || !out) return CT_ERR_NULL_PTR;

    /*
     * Direct pass-through to ct_image_load. Zero-copy: the const array
     * is used in place. Caller must NOT call ct_file_unload() on this —
     * use ct_image_free() instead, which frees only the runtime arrays
     * and does not touch image_base.
     */
    return ct_image_load(data, size, out);
}