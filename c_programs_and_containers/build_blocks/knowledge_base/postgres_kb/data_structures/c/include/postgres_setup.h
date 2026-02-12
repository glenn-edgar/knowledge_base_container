#ifndef POSTGRES_SETUP_H
#define POSTGRES_SETUP_H

#ifdef __cplusplus
extern "C" {
#endif

#include "system_def.h"
#ifndef __MAIN__

void *create_pg_connection(const char *dbname, const char *user, const char *password, const char *host, const char *port);
#endif

#ifdef __cplusplus
}
#endif

#endif

