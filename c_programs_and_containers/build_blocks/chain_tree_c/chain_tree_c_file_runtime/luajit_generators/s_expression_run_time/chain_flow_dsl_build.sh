 luajit s_compile.lua chain_flow_dsl_tests.lua --all
 luajit s_gen_registry.lua chain_flow_registry.registry --output=chain_flow_dsl_registry.h
 cp chain_flow_dsl_*.c ../../
 cp chain_flow_dsl_*.h ../../
