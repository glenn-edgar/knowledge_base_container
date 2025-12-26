--============================================================================
-- Test: Slotted Blackboards
--============================================================================

dofile("s_expr_dsl.lua")

local m = start_module("pump_ctrl")

-- Define pools (blackboards)
defpool("motor_state", "motor_state_t")
defpool("valve_state", "valve_state_t")

-- Define slots (named handles into pools)
defslot("pump_1", "motor_state")
defslot("pump_2", "motor_state")
defslot("inlet", "valve_state")
defslot("outlet", "valve_state")

-- Tree 1: main control loop
local t1 = start_tree("main_loop")
    local p = pipeline()
        main("set_motor", slot_ref("pump_1"), 100)
        main("set_motor", slot_ref("pump_2"), 50)
        main("set_valve", slot_ref("inlet"), 255)
    end_pipeline(p)
end_tree(t1)

-- Tree 2: shutdown sequence
local t2 = start_tree("shutdown")
    local p = pipeline()
        main("set_motor", slot_ref("pump_1"), 0)
        main("set_motor", slot_ref("pump_2"), 0)
        main("set_valve", slot_ref("outlet"), 0)
    end_pipeline(p)
end_tree(t2)

local gen = end_module(m)

-- Dump to console
gen:dump()

print("\n" .. string.rep("=", 60))
print("GENERATED: pump_ctrl_pools.h")
print(string.rep("=", 60))
print(gen:to_pools_header("pump_ctrl"))

print("\n" .. string.rep("=", 60))
print("GENERATED: pump_ctrl_pools.c")
print(string.rep("=", 60))
print(gen:to_pools_source("pump_ctrl"))

print("\n" .. string.rep("=", 60))
print("GENERATED: pump_ctrl_module.h (partial)")
print(string.rep("=", 60))
-- Just show first 100 lines of module header
local header = gen:to_c_header("pump_ctrl")
local line_count = 0
for line in header:gmatch("[^\n]+") do
    print(line)
    line_count = line_count + 1
    if line_count > 100 then
        print("... (truncated)")
        break
    end
end

return gen