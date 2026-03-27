--[[
  orchestrator_micropython.lua - ChainTree Pipeline Orchestrator (MicroPython)

  Same 6-stage pipeline as orchestrator.lua but uses stage6_micropython.lua
  to generate _module_mpy.py instead of C headers.

  Usage:
    local Orchestrator = require("orchestrator_micropython")
    local orch = Orchestrator.new({
        input_file   = "chaintree_config.json",
        output_dir   = "./generated",
        handle_name  = "my_chaintree",
    })
    orch:run()
--]]

local ChainTreeHandle      = require("stage1_handle")
local NodeIndexBuilder      = require("stage2_node_index")
local FunctionIndexBuilder  = require("stage3_function_index")
local LinkTableBuilder      = require("stage4_link_table")
local NodeDataEncoder       = require("stage5_node_data")
local MicroPythonGenerator  = require("stage6_micropython")

local PipelineOrchestrator = {}
PipelineOrchestrator.__index = PipelineOrchestrator

local function generate_unique_id()
    math.randomseed(os.time() + os.clock() * 1000)
    local chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    local parts = {}
    for i = 1, 8 do
        local idx = math.random(1, #chars)
        parts[i] = chars:sub(idx, idx)
    end
    return "ct_" .. table.concat(parts)
end

function PipelineOrchestrator.new(opts)
    local self = setmetatable({}, PipelineOrchestrator)
    self.input_file = opts.input_file
    self.handle_name = opts.handle_name or "chaintree_handle"
    self.output_dir = opts.output_dir or "."
    self.unique_id = generate_unique_id()

    self.handle = nil
    self.node_builder = nil
    self.function_builder = nil
    self.link_builder = nil
    self.data_encoder = nil
    self.code_generator = nil
    self.main_function_usage = {}
    return self
end

function PipelineOrchestrator:run()
    print(string.rep("=", 70))
    print("ChainTree Pipeline (LuaJIT -> MicroPython)")
    print(string.rep("=", 70))
    print("  Input file:  " .. self.input_file)
    print("  Output dir:  " .. self.output_dir)
    print("  Handle name: " .. self.handle_name)
    print()

    os.execute("mkdir -p " .. self.output_dir)

    self:_run_stage1()
    self:_run_stage2()
    self:_run_stage3()
    self:_run_stage4()
    self:_run_stage5()
    self:_count_main_function_usage()
    self:_run_stage6()

    print()
    print(string.rep("=", 70))
    print("Pipeline completed successfully!")
    print(string.rep("=", 70))
end

function PipelineOrchestrator:_run_stage1()
    print("Stage 1: Loading JSON data...")
    self.handle = ChainTreeHandle.new(self.input_file)
    self.handle:print_summary()
end

function PipelineOrchestrator:_run_stage2()
    print("\nStage 2: Building node ordering...")
    self.node_builder = NodeIndexBuilder.new(self.handle)
    self.node_builder:build()
    self.node_builder:print_summary()
end

function PipelineOrchestrator:_run_stage3()
    print("\nStage 3: Building function indices...")
    self.function_builder = FunctionIndexBuilder.new(self.handle)
    self.function_builder:build()
    self.function_builder:print_summary()
end

function PipelineOrchestrator:_run_stage4()
    print("\nStage 4: Building link tables...")
    self.link_builder = LinkTableBuilder.new(self.handle, self.node_builder)
    self.link_builder:build()
    self.link_builder:print_summary()
end

function PipelineOrchestrator:_run_stage5()
    print("\nStage 5: Encoding node data...")
    self.data_encoder = NodeDataEncoder.new(self.handle, self.node_builder, self.function_builder)
    self.data_encoder:build()
    self.data_encoder:print_summary()
end

function PipelineOrchestrator:_count_main_function_usage()
    print("\nCounting main function usage...")

    for i = 0, self.function_builder.main_indexer:get_count() - 1 do
        self.main_function_usage[i] = 0
    end

    for ltree_name in pairs(self.node_builder.ltree_to_final_index) do
        local functions = self.handle:get_node_functions(ltree_name)
        local main_func = functions.main

        if main_func and main_func ~= "CFL_NULL" then
            local ok, func_index = pcall(self.function_builder.main_indexer.get_index,
                                         self.function_builder.main_indexer, main_func)
            if ok then
                self.main_function_usage[func_index] = (self.main_function_usage[func_index] or 0) + 1
            end
        end
    end

    local total = 0
    for _, count in pairs(self.main_function_usage) do total = total + count end
    print(string.format("  Total main function references: %d", total))
end

function PipelineOrchestrator:_run_stage6()
    print("\nStage 6: Generating MicroPython module...")

    self.code_generator = MicroPythonGenerator.new({
        output_dir = self.output_dir,
        handle_name = self.handle_name,
        unique_id = self.unique_id,
        handle = self.handle,
        node_builder = self.node_builder,
        function_builder = self.function_builder,
        link_builder = self.link_builder,
        data_encoder = self.data_encoder,
        main_function_usage = self.main_function_usage,
    })

    self.code_generator:generate_all()
end

return PipelineOrchestrator
