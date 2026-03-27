#!/usr/bin/env luajit
--[[
  main_binary.lua - CLI entry point for ChainTree Binary Image Pipeline
  
  Usage:
    luajit main_binary.lua <input_file> <output_dir> [handle_name] [--c-header]
  
  Arguments:
    input_file   : Path to ChainTree JSON configuration
    output_dir   : Directory for generated .ctb (and optional .h) file
    handle_name  : Base name for output files (default: chaintree_handle)
    --c-header   : Also emit a .h file with const uint8_t[] for firmware embedding
--]]

-- Add script directory to package path
local script_dir = arg[0]:match("^(.*)/") or "."
package.path = script_dir .. "/?.lua;" .. package.path

-- Also add the pipeline directory (stages 1-5 live there)
-- Adjust this path to match your project layout
local pipeline_dir = script_dir:gsub("/[^/]*$", "") .. "/luajit_pipeline"
if pipeline_dir ~= script_dir then
    package.path = pipeline_dir .. "/?.lua;" .. package.path
end

local PipelineOrchestrator = require("orchestrator_binary")

if #arg < 2 then
    print("Usage: luajit main_binary.lua <input_file> <output_dir> [handle_name] [--c-header]")
    print("")
    print("  input_file   : Path to ChainTree JSON configuration")
    print("  output_dir   : Directory for generated .ctb file")
    print("  handle_name  : Base name for output (default: chaintree_handle)")
    print("  --c-header   : Also emit {handle_name}_image.h for firmware embedding")
    os.exit(1)
end

local input_file = arg[1]
local output_dir = arg[2]
local handle_name = "chaintree_handle"
local emit_c_header = false

for i = 3, #arg do
    if arg[i] == "--c-header" then
        emit_c_header = true
    else
        handle_name = arg[i]
    end
end

-- Verify input file exists
local f = io.open(input_file, "r")
if not f then
    print("Error: " .. input_file .. " not found.")
    os.exit(1)
end
f:close()

-- Run pipeline
local orch = PipelineOrchestrator.new({
    input_file = input_file,
    output_dir = output_dir,
    handle_name = handle_name,
    emit_c_header = emit_c_header,
})

orch:run()