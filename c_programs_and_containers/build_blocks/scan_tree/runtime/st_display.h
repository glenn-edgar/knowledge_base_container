/* st_display.h - Hierarchical colored layer state display */

#ifndef ST_DISPLAY_H
#define ST_DISPLAY_H

#include "scan_tree.h"

typedef struct st_display st_display_t;

/* Build cached display hierarchy from handle. Call once after st_init. */
st_display_t *st_display_init(const st_handle_t *h);

/* Free cached display hierarchy. */
void st_display_destroy(st_display_t *disp);

/* Print layer states using cached hierarchy. Call after any st_cycle. */
void st_display_tree(const st_display_t *disp, const st_handle_t *h);

#endif