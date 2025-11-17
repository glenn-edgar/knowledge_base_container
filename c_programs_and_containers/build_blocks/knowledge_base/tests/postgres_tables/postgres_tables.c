#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

// Helper function to get human-readable type name from OID
const char* getTypeName(Oid typeOid) {
    switch (typeOid) {
        case 16:   return "bool";
        case 17:   return "bytea";
        case 20:   return "int8";
        case 21:   return "int2";
        case 23:   return "int4";
        case 25:   return "text";
        case 1042: return "char";
        case 1043: return "varchar";
        case 1082: return "date";
        case 1114: return "timestamp";
        case 1184: return "timestamptz";
        case 1700: return "numeric";
        case 701:  return "float8";
        case 700:  return "float4";
        default:   return "unknown";
    }
}

int main() {
    char password[256];
    char conninfo[512];

    // Step 1: Ask user for postgresdb password
    printf("Enter PostgreSQL password for user 'gedgar': ");
    scanf("%255s", password);

    // Step 2: Open PostgreSQL database connection
    snprintf(conninfo, sizeof(conninfo), "host=localhost dbname=knowledge_base user=gedgar password=%s", password);
    PGconn *conn = PQconnectdb(conninfo);

    // Check connection
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    // Step 3: Issue SELECT * FROM knowledge_base;
    PGresult *res = PQexec(conn, "SELECT * FROM knowledge_base;");

    // Check query execution
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return 1;
    }

    // Step 4: Print out column names
    int num_fields = PQnfields(res);
    for (int i = 0; i < num_fields; i++) {
        printf("Column %d: %s (Type: %s)\n", i+1, PQfname(res, i), getTypeName(PQftype(res, i)));
    }

    // Step 5: Print out records, one column per line, with column type
    int num_rows = PQntuples(res);
    for (int row = 0; row < num_rows; row++) {
        printf("\nRow %d:\n", row+1);
        for (int col = 0; col < num_fields; col++) {
            printf("  %s (Type: %s): %s\n", PQfname(res, col), getTypeName(PQftype(res, col)), PQgetvalue(res, row, col));
        }
        printf("---\n");
    }

    // Step 6: Close the database connection and clean up
    PQclear(res);
    PQfinish(conn);

    // Step 7: Exit
    return 0;
}


