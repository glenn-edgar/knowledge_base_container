cd nats_luajit
export LD_LIBRARY_PATH=/home/gedgar/knowledge_base_assembly/zig_programs_and_containers/building_blocks/knowledge_base/nats
luajit test/test_key_store.lua
luajit test/test_pubsub.lua
luajit test/test_rpc.lua

