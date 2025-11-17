rm kb_rpc_server_table

gcc -o kb_rpc_server_table kb_rpc_server_table.c -I/usr/include/postgresql -lpq -luuid -lm
