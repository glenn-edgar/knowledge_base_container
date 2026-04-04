/**
 * @file cbor_codec.h
 * @brief Minimal JSON<->CBOR codec for robot wire protocol.
 *
 * Handles flat CBOR maps with text string keys and simple values:
 *   - unsigned/negative integers
 *   - floats (IEEE 754 single/double)
 *   - text strings
 *   - booleans (true/false)
 *   - null
 *
 * No nested maps, no arrays, no tags, no byte strings.
 * Designed for the planner<->robot command/response protocol.
 *
 * Copied from knowledge_base/mqtt/lua_cbor for standalone deployment.
 */

#ifndef CBOR_CODEC_H
#define CBOR_CODEC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Encode a JSON string into CBOR bytes.
 * @param json   NUL-terminated JSON string (must be a flat object)
 * @param out    Output buffer for CBOR bytes
 * @param max_out Size of output buffer
 * @return Number of CBOR bytes written, or -1 on error
 */
int cbor_from_json(const char *json, uint8_t *out, int max_out);

/**
 * Decode CBOR bytes into a JSON string.
 * @param cbor     CBOR bytes
 * @param cbor_len Length of CBOR data
 * @param out      Output buffer for JSON string (NUL-terminated)
 * @param max_out  Size of output buffer
 * @return Number of JSON chars written (excluding NUL), or -1 on error
 */
int cbor_to_json(const uint8_t *cbor, int cbor_len, char *out, int max_out);

#ifdef __cplusplus
}
#endif

#endif /* CBOR_CODEC_H */
