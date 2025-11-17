#ifndef KB_STATUS_TABLE_H
#define KB_STATUS_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif


int get_status_data(void *conn, const char *base_table, const char *path, char **data_str);
int set_status_data(void *conn, const char *base_table, const char *path, const char *data, int retry_count, double retry_delay, int *success, char *message);

#ifdef __cplusplus
}
#endif

#endif
