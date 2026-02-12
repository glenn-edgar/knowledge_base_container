rm kb_stream_table

gcc -o kb_stream_table kb_stream_table.c -I/usr/include/postgresql -lpq -lcjson
