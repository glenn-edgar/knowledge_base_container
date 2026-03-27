#!/usr/bin/env luajit
--[[
  main_micropython.lua - CLI entry point for ChainTree MicroPython Pipeline

  Usage:
    luajit main_micropython.lua <input_file> <output_dir> [handle_name]

  Arguments:
    input_file   : Path to ChainTree JSON configuration
    output_dir   : Directory for generated _module_mpy.py file
    handle_name  : Name prefix for output file (default: chaintree_handle)
--]]

-- Add script directory to package path so requires work
local script_dir = arg[0]:match("^(.*)/") or "."
package.path = script_dir .. "/?.lua;" .. package.path

local PipelineOrchestrator = require("orchestrator_micropython")

if #arg < 2 then
    print("Usage: luajit main_micropython.lua <input_file> <output_dir> [handle_name]")
    print("")
    print("  input_file   : Path to ChainTree JSON configuration")
    print("  output_dir   : Directory for generated _module_mpy.py file")
    print("  handle_name  : Name prefix for output file (default: chaintree_handle)")
    os.exit(1)
end

local input_file = arg[1]
local output_dir = arg[2]
local handle_name = arg[3] or "chaintree_handle"

local f = io.open(input_file, "r")
if not f then
    print("Error: " .. input_file .. " not found.")
    os.exit(1)
end
f:close()

local orch = PipelineOrchestrator.new({
    input_file = input_file,
    output_dir = output_dir,
    handle_name = handle_name,
})

orch:run()
