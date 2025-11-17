#include "system_def.h"
#include "postgres_setup.h"
#include <libpq-fe.h> // PostgreSQL library

#ifndef __MAIN__


void *create_pg_connection(const char *dbname, const char *user, const char *password, const char *host, const char *port) {
    // Build connection string
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), 
             "dbname=%s user=%s password=%s host=%s port=%s",
             dbname ? dbname : "",
             user ? user : "",
             password ? password : "",
             host ? host : "localhost",
             port ? port : "5432");

    // Create connection
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return NULL;
    }

    return (void *)conn;
}
#endif