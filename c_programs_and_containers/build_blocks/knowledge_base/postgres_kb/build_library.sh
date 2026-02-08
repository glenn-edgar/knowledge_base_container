


gcc -c kb_search.c -o kb_search.o -I/usr/include/postgresql 
gcc -c kb_status_table.c -o kb_status_table.o -I/usr/include/postgresql 
gcc -c kb_stream_table.c -o kb_stream_table.o -I/usr/include/postgresql 
gcc -c kb_job_table.c -o kb_job_table.o -I/usr/include/postgresql 
gcc -c kb_rpc_server_table.c -o kb_rpc_server_table.o -I/usr/include/postgresql 
gcc -c kb_rpc_client_table.c -o kb_rpc_client_table.o -I/usr/include/postgresql 
gcc -c postgres_setup.c -o postgres_setup.o -I/usr/include/postgresql 

gcc -shared -o libkb_data_structures.so kb_search.o kb_status_table.o kb_stream_table.o kb_job_table.o kb_rpc_server_table.o kb_rpc_client_table.o postgres_setup.o

ar rcs libkb_data_structures.a kb_search.o kb_status_table.o kb_stream_table.o kb_job_table.o kb_rpc_server_table.o kb_rpc_client_table.o postgres_setup.o
rm *.o

cp libkb_data_structures.so libs/libkb_data_structures_shared.so
cp libkb_data_structures.a libs/libkb_data_structures_static.a
cp *.h include/

rm libkb_data_structures.so 
rm libkb_data_structures.a 
