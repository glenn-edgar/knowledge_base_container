rm postgres_tables

gcc -o postgres_tables postgres_tables.c -I/usr/include/postgresql -I/usr/include/cjson -lpq -lcjson



