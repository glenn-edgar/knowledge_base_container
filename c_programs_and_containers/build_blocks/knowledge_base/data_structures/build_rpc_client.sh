rm kb_rpc_client_table

gcc -o kb_rpc_client_table kb_rpc_client_table.c -I/usr/include/postgresql -lpq -luuid
