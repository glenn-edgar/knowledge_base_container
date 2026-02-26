# Generate all outputs (headers + binary)
luajit s_compile.lua chain_flow_dsl_tests.lua --all-bin --outdir=output/

# Generate only text outputs (no binary)
luajit s_compile.lua chain_flow_dsl_tests.lua --all --outdir=output/

# Generate specific outputs
luajit s_compile.lua mymodule.lua --binary=mymodule.bin
luajit s_compile.lua mymodule.lua --binary-h=mymodule_bin.h
luajit s_compile.lua mymodule.lua --header=mymodule.h
luajit s_compile.lua mymodule.lua --records=mymodule_records.h
luajit s_compile.lua mymodule.lua --user=mymodule_user.h
luajit s_compile.lua mymodule.lua --reg=mymodule_reg.c

# With CFL helpers (auto-detected if present, or explicit)
luajit s_compile.lua mymodule.lua --helpers=s_cfl_helpers.lua --all-bin

# Debug dump
luajit s_compile.lua mymodule.lua --dump

# 64-bit mode
luajit s_compile.lua mymodule.lua --64bit --all-bin --outdir=output/

# Inspect binary
luajit s_binary_dump.lua output/chain_flow_dsl_tests.bin

