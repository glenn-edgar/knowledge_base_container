/*
 * kb_all.h
 * Knowledge Base C Library (PostgreSQL) — Master include
 *
 * Include this single header to get all KB functionality.
 * Excludes: document table (not ported).
 */

#ifndef KB_ALL_H
#define KB_ALL_H

#include "kb_common.h"
#include "kb_search.h"
#include "kb_status.h"
#include "kb_job_queue.h"
#include "kb_stream.h"
#include "kb_rpc_server.h"
#include "kb_rpc_client.h"
#include "kb_bit_structures.h"
#include "kb_link_table.h"
#include "kb_document.h"

#endif /* KB_ALL_H */
