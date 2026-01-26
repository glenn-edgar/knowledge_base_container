// s_expr_event_queue.h

#ifndef S_EXPR_EVENT_QUEUE_H
#define S_EXPR_EVENT_QUEUE_H
#ifdef __cplusplus
extern "C" {
#endif

#include "s_engine_types.h"

void s_expr_event_queue_init(s_expr_tree_instance_t* inst);
void s_expr_event_queue_destroy(s_expr_tree_instance_t* inst);
uint16_t s_expr_event_queue_count(s_expr_tree_instance_t* inst);
void s_expr_event_push(s_expr_tree_instance_t* inst, uint16_t tick_type, uint16_t event_id, void* event_data);
void s_expr_event_pop(s_expr_tree_instance_t* inst, uint16_t* tick_type, uint16_t* event_id, void** event_data);
void s_expr_event_queue_clear(s_expr_tree_instance_t* inst);

#ifdef __cplusplus
}
#endif

#endif