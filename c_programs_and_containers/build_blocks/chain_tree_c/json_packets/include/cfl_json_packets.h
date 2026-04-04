#ifndef CFL_JSON_PACKETS_H
#define CFL_JSON_PACKETS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

/* ========================================================================
 * JSON PACKET LIBRARY
 *
 * Wraps cJSON with ChainTree conventions:
 *   - Allocator passed per-call (no global state)
 *   - EXCEPTION on all errors (fail fast)
 *   - Path-based field access ("payload.sensors[2].temp")
 *   - cfl_json_has_field() is the only soft probe
 * ======================================================================== */

/* Maximum JSON packet size for arena-allocated text buffers */
#ifndef CFL_JSON_MAX_PACKET_SIZE
#define CFL_JSON_MAX_PACKET_SIZE 256
#endif

/* Forward declare — callers don't need cJSON internals */
typedef struct cfl_json_packet cfl_json_packet_t;

/* Heap interface — matches cfl_heap function signatures */
typedef struct cfl_json_heap_interface {
    void *(*alloc)(void *ctx, uint16_t size);
    void  (*free)(void *ctx, void *ptr);
    void  *ctx;
} cfl_json_heap_interface_t;

/* ── Parse ──
 * EXCEPTION if text is NULL, len is 0, or JSON syntax is invalid.
 * Pass len == -1 for NUL-terminated strings.
 */
cfl_json_packet_t *cfl_json_parse(cfl_json_heap_interface_t *heap,
                                   const char *text, int len);

/* ── Extract — all EXCEPTION on missing path or wrong type ── */
void        cfl_json_get_string(cfl_json_packet_t *pkt, const char *path, const char **out);
void        cfl_json_get_int(cfl_json_packet_t *pkt, const char *path, int32_t *out);
void        cfl_json_get_float(cfl_json_packet_t *pkt, const char *path, double *out);
void        cfl_json_get_bool(cfl_json_packet_t *pkt, const char *path, bool *out);
int         cfl_json_array_size(cfl_json_packet_t *pkt, const char *path);

/* ── Probe — the ONE soft query, no EXCEPTION ── */
bool        cfl_json_has_field(cfl_json_packet_t *pkt, const char *path);

/* ── Build — EXCEPTION on NULL pkt, NULL path, allocation failure ── */
cfl_json_packet_t *cfl_json_create(cfl_json_heap_interface_t *heap);
void        cfl_json_set_string(cfl_json_packet_t *pkt, const char *path, const char *val);
void        cfl_json_set_int(cfl_json_packet_t *pkt, const char *path, int32_t val);
void        cfl_json_set_float(cfl_json_packet_t *pkt, const char *path, double val);
void        cfl_json_set_bool(cfl_json_packet_t *pkt, const char *path, bool val);
void        cfl_json_set_object(cfl_json_packet_t *pkt, const char *path);
void        cfl_json_set_array(cfl_json_packet_t *pkt, const char *path);

/* ── Serialize ── */
/* Returns heap-allocated string. Caller must free via heap. EXCEPTION on failure. */
char       *cfl_json_to_string(cfl_json_heap_interface_t *heap, cfl_json_packet_t *pkt);

/* Serialize into caller-provided buffer. EXCEPTION if buffer too small.
 * Returns number of bytes written (excluding NUL). */
int         cfl_json_to_buffer(cfl_json_packet_t *pkt, char *buf, int buflen);

/* ── Free — NULL-safe (no-op on NULL) ── */
void        cfl_json_free(cfl_json_heap_interface_t *heap, cfl_json_packet_t *pkt);

/* ── Dispatch ── */

typedef void (*cfl_json_handler_fn)(cfl_json_packet_t *pkt, void *user_ctx);

typedef struct {
    const char          *type_name;
    uint32_t             type_hash;   /* FNV-1a of type_name — set by dispatcher_create */
    cfl_json_handler_fn  handler;
    unsigned             column_id;   /* target event column for CFL dispatch */
} cfl_json_route_t;

typedef struct cfl_json_dispatcher cfl_json_dispatcher_t;

cfl_json_dispatcher_t *cfl_json_dispatcher_create(cfl_json_heap_interface_t *heap,
                                                   cfl_json_route_t *routes,
                                                   int count,
                                                   unsigned default_column_id);
void                   cfl_json_dispatcher_free(cfl_json_heap_interface_t *heap,
                                                cfl_json_dispatcher_t *d);

/* Returns matched column_id, or default_column_id if no route matches.
 * EXCEPTION if pkt has no "type" field. */
unsigned               cfl_json_dispatch(cfl_json_dispatcher_t *d,
                                         cfl_json_packet_t *pkt);

#ifdef __cplusplus
}
#endif

#endif /* CFL_JSON_PACKETS_H */
