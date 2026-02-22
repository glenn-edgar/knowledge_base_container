/* st_display.c - Hierarchical colored layer state display */

#include "st_display.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ANSI color codes */
#define ANSI_GREEN  "\033[32m"
#define ANSI_RED    "\033[31m"
#define ANSI_GREY   "\033[90m"
#define ANSI_BOLD   "\033[1m"
#define ANSI_RESET  "\033[0m"

/* Pre-computed display entry */
typedef struct {
    uint16_t    buf_id;         /* index into buf_descs[] */
    uint8_t     indent;         /* display indentation level */
    const char *title;          /* pointer into path string (relative) */
} st_disp_entry_t;

struct st_display {
    const char      *name;      /* system name */
    st_disp_entry_t *entries;   /* display order, pre-sorted */
    uint16_t         n_entries;
};

/* Count dots in a path */
static int path_depth(const char *path)
{
    int d = 0;
    while (*path) { if (*path == '.') d++; path++; }
    return d;
}

/* Skip N dot-separated segments, return pointer to remainder */
static const char *skip_segments(const char *path, int n)
{
    int d = 0;
    while (*path && d < n) { if (*path == '.') d++; path++; }
    return path;
}

/* Check if child_path is a direct sub-level child of parent_path */
static int is_child_of(const char *parent_path, const char *child_path, int parent_depth)
{
    const char *p = parent_path;
    const char *last_dot = NULL;
    while (*p) { if (*p == '.') last_dot = p; p++; }
    int prefix_len = last_dot ? (int)(last_dot - parent_path) : 0;

    if (prefix_len == 0) return 0;
    if (strncmp(parent_path, child_path, prefix_len) != 0) return 0;
    if (child_path[prefix_len] != '.') return 0;

    int child_depth = path_depth(child_path);
    return (child_depth == parent_depth + 1) ? 1 : 0;
}

/* Recursively add entries in display order */
static void build_recursive(const st_handle_t *h, uint16_t buf_id,
                              const uint16_t *layer_ids, int n_layers,
                              uint8_t indent,
                              st_disp_entry_t *entries, uint16_t *count)
{
    const st_buf_desc_t *bd = &h->desc->buf_descs[buf_id];

    entries[*count].buf_id = buf_id;
    entries[*count].indent = indent;
    entries[*count].title  = skip_segments(bd->path, 1); /* skip root name */
    (*count)++;

    int my_depth = path_depth(bd->path);
    uint8_t my_level = bd->level;

    for (int i = 0; i < n_layers; i++) {
        if (layer_ids[i] == buf_id) continue;
        const st_buf_desc_t *cbd = &h->desc->buf_descs[layer_ids[i]];
        if (cbd->level == my_level &&
            is_child_of(bd->path, cbd->path, my_depth)) {
            build_recursive(h, layer_ids[i], layer_ids, n_layers,
                            indent + 1, entries, count);
        }
    }
}

st_display_t *st_display_init(const st_handle_t *h)
{
    const st_system_desc_t *d = h->desc;

    /* Collect layer buffer ids */
    uint16_t layer_ids[256];
    int n_layers = 0;
    for (uint16_t i = 0; i < d->n_bufs && n_layers < 256; i++) {
        if (d->buf_descs[i].is_layer)
            layer_ids[n_layers++] = i;
    }

    st_display_t *disp = (st_display_t *)calloc(1, sizeof(st_display_t));
    if (!disp) return NULL;

    disp->name = d->name;
    disp->entries = (st_disp_entry_t *)calloc(n_layers, sizeof(st_disp_entry_t));
    if (!disp->entries) { free(disp); return NULL; }
    disp->n_entries = 0;

    if (n_layers == 0) return disp;

    /* Find max level */
    uint8_t max_level = 0;
    for (int i = 0; i < n_layers; i++) {
        if (d->buf_descs[layer_ids[i]].level > max_level)
            max_level = d->buf_descs[layer_ids[i]].level;
    }

    /* Build display order: levels from highest to lowest,
     * root buffers first within each level, children indented */
    for (int lvl = (int)max_level; lvl >= 0; lvl--) {
        /* Find minimum path depth for this level */
        int min_depth = 9999;
        for (int i = 0; i < n_layers; i++) {
            if (d->buf_descs[layer_ids[i]].level != (uint8_t)lvl) continue;
            int dep = path_depth(d->buf_descs[layer_ids[i]].path);
            if (dep < min_depth) min_depth = dep;
        }

        /* Add root buffers of this level */
        for (int i = 0; i < n_layers; i++) {
            if (d->buf_descs[layer_ids[i]].level != (uint8_t)lvl) continue;
            int dep = path_depth(d->buf_descs[layer_ids[i]].path);
            if (dep == min_depth) {
                build_recursive(h, layer_ids[i], layer_ids, n_layers,
                                0, disp->entries, &disp->n_entries);
            }
        }
    }

    return disp;
}

void st_display_destroy(st_display_t *disp)
{
    if (disp) {
        free(disp->entries);
        free(disp);
    }
}

void st_display_tree(const st_display_t *disp, const st_handle_t *h)
{
    printf(ANSI_BOLD "=== %s ===" ANSI_RESET "\n", disp->name);

    for (uint16_t i = 0; i < disp->n_entries; i++) {
        const st_disp_entry_t *e = &disp->entries[i];
        const st_buf_desc_t *bd = &h->desc->buf_descs[e->buf_id];
        const st_layer_rt_t *l = &h->layer_bufs[bd->buf_index];

        for (uint8_t j = 0; j < e->indent; j++) printf("  ");

        printf(ANSI_BOLD "%s" ANSI_RESET " [", e->title);

        for (uint16_t j = 0; j < bd->size; j++) {
            if (j > 0) printf(" ");
            int8_t s = l->states[j];
            if (s == 1)
                printf(ANSI_GREEN "T" ANSI_RESET);
            else if (s == 0)
                printf(ANSI_RED "F" ANSI_RESET);
            else
                printf(ANSI_GREY "N" ANSI_RESET);
        }
        printf("]\n");
    }
}