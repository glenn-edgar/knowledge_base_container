/**
 * @file json_extract.c
 * @brief Lightweight JSON field extraction for flat objects.
 */

#include "json_extract.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/**
 * Find the value position for a given key in a flat JSON object.
 * Returns pointer to first char of the value, or NULL if not found.
 */
static const char *find_key(const char *json, const char *key) {
    int key_len = (int)strlen(key);
    const char *p = json;

    while (*p) {
        /* Find next quote */
        const char *q = strchr(p, '"');
        if (!q) return NULL;
        q++;  /* past opening quote */

        /* Check if this key matches */
        if (strncmp(q, key, key_len) == 0 && q[key_len] == '"') {
            /* Skip past key, closing quote, and colon */
            const char *v = q + key_len + 1;
            while (*v == ' ' || *v == '\t' || *v == ':') v++;
            return v;
        }

        /* Skip past this string value */
        while (*q && *q != '"') {
            if (*q == '\\') q++;
            q++;
        }
        if (*q == '"') q++;
        p = q;
    }
    return NULL;
}

bool json_get_int(const char *json, const char *key, int *out) {
    const char *v = find_key(json, key);
    if (!v) return false;

    /* Skip potential string wrapper */
    if (*v == '"') return false;

    char *end;
    long val = strtol(v, &end, 10);
    if (end == v) return false;
    *out = (int)val;
    return true;
}

bool json_get_double(const char *json, const char *key, double *out) {
    const char *v = find_key(json, key);
    if (!v) return false;
    if (*v == '"') return false;

    char *end;
    double val = strtod(v, &end);
    if (end == v) return false;
    *out = val;
    return true;
}

bool json_get_string(const char *json, const char *key,
                     char *out, int max_out) {
    const char *v = find_key(json, key);
    if (!v || *v != '"') return false;
    v++;  /* past opening quote */

    int i = 0;
    while (*v && *v != '"' && i < max_out - 1) {
        if (*v == '\\') {
            v++;
            if (*v == '"') out[i++] = '"';
            else if (*v == '\\') out[i++] = '\\';
            else if (*v == 'n') out[i++] = '\n';
            else if (*v == 't') out[i++] = '\t';
            else out[i++] = *v;
        } else {
            out[i++] = *v;
        }
        v++;
    }
    out[i] = '\0';
    return true;
}
