#ifndef KB_STREAM_TABLE_H   
#define KB_STREAM_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#
int push_stream_data(void *conn, const char *base_table, const char *path, const char *data, int max_retries, double retry_delay, char **error_msg);

#ifdef __cplusplus
}
#endif

#endif
