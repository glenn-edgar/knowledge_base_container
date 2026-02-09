-- stack_test.lua
-- Test for stack frame and quad operations

local mod = start_module("function_dictionary")

-- ============================================================================
-- RECORD DEFINITION
-- ============================================================================



-- ============================================================================
-- RECORD: cpu_config_blackboard
-- ============================================================================

RECORD("cpu_config_blackboard")
    -- Dictionary pointer for function table
    PTR64_FIELD("fn_dict","void")
    
    -- GPIO configuration state
    FIELD("gpio_port", "uint32")
    FIELD("gpio_pin", "uint32")
    FIELD("gpio_mode", "uint32")
    FIELD("gpio_speed", "uint32")
    FIELD("gpio_pull", "uint32")
    
    -- UART configuration state
    FIELD("uart_channel", "uint32")
    FIELD("uart_baud", "uint32")
    FIELD("uart_parity", "uint32")
    FIELD("uart_stop_bits", "uint32")
    FIELD("uart_flow_ctrl", "uint32")
    
    -- SPI configuration state
    FIELD("spi_channel", "uint32")
    FIELD("spi_clock_div", "uint32")
    FIELD("spi_mode", "uint32")
    FIELD("spi_bit_order", "uint32")
    
    -- Status fields
    FIELD("config_state", "uint32")
    FIELD("error_code", "uint32")
    FIELD("peripherals_ready", "uint32")
    
    -- Scratch/temp
    FIELD("temp_reg_addr", "uint32")
    FIELD("temp_reg_value", "uint32")
END_RECORD()

-- ============================================================================
-- CONSTANTS: Register addresses and bit patterns
-- ============================================================================

-- GPIO modes
GPIO_MODE_INPUT   = 0x00
GPIO_MODE_OUTPUT  = 0x01
GPIO_MODE_ALT_FN  = 0x02
GPIO_MODE_ANALOG  = 0x03

-- GPIO speed
GPIO_SPEED_LOW    = 0x00
GPIO_SPEED_MED    = 0x01
GPIO_SPEED_HIGH   = 0x02
GPIO_SPEED_VHIGH  = 0x03

-- GPIO pull
GPIO_PULL_NONE    = 0x00
GPIO_PULL_UP      = 0x01
GPIO_PULL_DOWN    = 0x02

-- UART parity
UART_PARITY_NONE  = 0x00
UART_PARITY_EVEN  = 0x01
UART_PARITY_ODD   = 0x02

-- SPI modes
SPI_MODE_0        = 0x00
SPI_MODE_1        = 0x01
SPI_MODE_2        = 0x02
SPI_MODE_3        = 0x03

-- SPI bit order
SPI_MSB_FIRST     = 0x00
SPI_LSB_FIRST     = 0x01

-- Config states
CONFIG_IDLE       = 0
CONFIG_GPIO       = 1
CONFIG_UART       = 2
CONFIG_SPI        = 3
CONFIG_DONE       = 4
CONFIG_ERROR_STATE = 5

-- ============================================================================
-- TREE DEFINITION
-- ============================================================================

start_tree("function_dictionary")
use_record("cpu_config_blackboard")



    -- Load the function dictionary with all register config functions
-- Load the function dictionary with all register config functions
local input_dictionary = {
    -- ================================================================
    -- write_register: Low-level register write
    -- Stack params: [addr, value]
    -- Uses se_call for stack frame management
    -- ================================================================
    {"write_register", function()
        se_call(2, 0, 0, {}, {
            function()
                -- param 0 = register address
                -- param 1 = register value
                -- In real hardware: *(volatile uint32_t*)addr = value
                se_log("write_register")
            end
        })
    end},
    
    -- ================================================================
    -- read_modify_write: Read register, apply mask, set bits, write back
    -- Stack params: [addr, clear_mask, set_bits]
    -- ================================================================
    {"read_modify_write", function()
        se_call(3, 1, 2, {}, {
            function()
                -- param 0 = addr, param 1 = clear_mask, param 2 = set_bits
                -- local 3 = scratch for current value
                -- Read current value into local 3
                -- In real hardware: local3 = *(volatile uint32_t*)addr
                quad_mov(local_ref(0), local_ref(3))()
                -- Clear bits: local3 = local3 & ~clear_mask
                quad_not(local_ref(1), tos_ref(0))()
                quad_and(local_ref(3), tos_ref(0), local_ref(3))()
                -- Set bits: local3 = local3 | set_bits
                quad_or(local_ref(3), local_ref(2), local_ref(3))()
                -- Write back via internal call
                quad_mov(local_ref(0), stack_push_ref())()   -- addr
                quad_mov(local_ref(3), stack_push_ref())()   -- value
                se_exec_dict_internal("write_register")
            end
        })
    end},
    
    -- ================================================================
    -- enable_peripheral_clock: Enable clock for a peripheral
    -- Stack params: [peripheral_bit]
    -- ================================================================
    {"enable_peripheral_clock", function()
        se_call(1, 0, 1, {}, {
            function()
                -- RCC enable register at fixed address 0x40021000
                quad_mov(uint_val(0x40021000), stack_push_ref())()   -- addr
                quad_mov(uint_val(0x00000000), stack_push_ref())()   -- clear_mask (none)
                quad_mov(local_ref(0), stack_push_ref())()           -- set_bits = peripheral_bit
                se_exec_dict_internal("read_modify_write")
            end
        })
    end},
    
    -- ================================================================
    -- configure_gpio_pin: Full GPIO pin configuration
    -- Stack params: [port_base, pin, mode, speed, pull]
    -- ================================================================
    {"configure_gpio_pin", function()
        se_call(5, 2, 4, {}, {
            function()
                -- param 0 = port_base, 1 = pin, 2 = mode, 3 = speed, 4 = pull
                -- local 5 = computed shift, local 6 = computed mask
                
                -- Compute bit shift: shift = pin * 2
                quad_imul(local_ref(1), int_val(2), local_ref(5))()
                
                -- Compute clear mask: mask = 0x03 << shift
                quad_shl(int_val(0x03), local_ref(5), local_ref(6))()
                
                -- MODE register: port_base + 0x00
                -- Compute mode bits: mode << shift
                quad_shl(local_ref(2), local_ref(5), tos_ref(0))()
                quad_mov(local_ref(0), stack_push_ref())()           -- addr = port_base
                quad_mov(local_ref(6), stack_push_ref())()           -- clear_mask
                quad_mov(tos_ref(2), stack_push_ref())()             -- set_bits (mode << shift)
                se_exec_dict_internal("read_modify_write")
                
                -- SPEED register: port_base + 0x08
                quad_iadd(local_ref(0), int_val(0x08), tos_ref(0))()
                quad_shl(local_ref(3), local_ref(5), tos_ref(1))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(local_ref(6), stack_push_ref())()           -- clear_mask
                quad_mov(tos_ref(3), stack_push_ref())()             -- set_bits
                se_exec_dict_internal("read_modify_write")
                
                -- PULL register: port_base + 0x0C
                quad_iadd(local_ref(0), int_val(0x0C), tos_ref(0))()
                quad_shl(local_ref(4), local_ref(5), tos_ref(1))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(local_ref(6), stack_push_ref())()           -- clear_mask
                quad_mov(tos_ref(3), stack_push_ref())()             -- set_bits
                se_exec_dict_internal("read_modify_write")
            end
        })
    end},
    
    -- ================================================================
    -- configure_uart: Full UART channel setup
    -- Stack params: [channel_base, baud_div, config_bits]
    -- ================================================================
    {"configure_uart", function()
        se_call(3, 0, 2, {}, {
            function()
                -- param 0 = channel_base, 1 = baud_div, 2 = config_bits
                
                -- Disable UART first: CR1 = channel_base + 0x0C
                quad_iadd(local_ref(0), int_val(0x0C), tos_ref(0))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(uint_val(0x00000001), stack_push_ref())()   -- clear UE bit
                quad_mov(uint_val(0x00000000), stack_push_ref())()   -- set nothing
                se_exec_dict_internal("read_modify_write")
                
                -- Set baud rate: BRR = channel_base + 0x08
                quad_iadd(local_ref(0), int_val(0x08), tos_ref(0))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(local_ref(1), stack_push_ref())()           -- value = baud_div
                se_exec_dict_internal("write_register")
                
                -- Set config: CR1 = channel_base + 0x0C
                quad_iadd(local_ref(0), int_val(0x0C), tos_ref(0))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(uint_val(0x00000000), stack_push_ref())()   -- clear none
                quad_mov(local_ref(2), stack_push_ref())()           -- set config_bits
                se_exec_dict_internal("read_modify_write")
                
                -- Enable UART: set UE bit in CR1
                quad_iadd(local_ref(0), int_val(0x0C), tos_ref(0))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(uint_val(0x00000000), stack_push_ref())()   -- clear none
                quad_mov(uint_val(0x00000001), stack_push_ref())()   -- set UE
                se_exec_dict_internal("read_modify_write")
            end
        })
    end},
    
    -- ================================================================
    -- configure_spi: Full SPI channel setup
    -- Stack params: [channel_base, clock_div, mode, bit_order]
    -- ================================================================
    {"configure_spi", function()
        se_call(4, 1, 4, {}, {
            function()
                -- param 0 = channel_base, 1 = clock_div, 2 = mode, 3 = bit_order
                -- local 4 = assembled CR1 value
                
                -- Disable SPI first: CR1 bit 6 = SPE
                quad_iadd(local_ref(0), int_val(0x00), tos_ref(0))()
                quad_mov(tos_ref(0), stack_push_ref())()             -- addr
                quad_mov(uint_val(0x00000040), stack_push_ref())()   -- clear SPE
                quad_mov(uint_val(0x00000000), stack_push_ref())()   -- set nothing
                se_exec_dict_internal("read_modify_write")
                
                -- Build CR1: clock_div in bits[5:3], mode in bits[1:0], 
                -- bit_order in bit[7]
                quad_shl(local_ref(1), int_val(3), local_ref(4))()
                quad_or(local_ref(4), local_ref(2), local_ref(4))()
                quad_shl(local_ref(3), int_val(7), tos_ref(0))()
                quad_or(local_ref(4), tos_ref(0), local_ref(4))()
                
                -- Write CR1
                quad_mov(local_ref(0), stack_push_ref())()           -- addr
                quad_mov(local_ref(4), stack_push_ref())()           -- value
                se_exec_dict_internal("write_register")
                
                -- Enable SPI: set SPE
                quad_mov(local_ref(0), stack_push_ref())()           -- addr
                quad_mov(uint_val(0x00000000), stack_push_ref())()   -- clear none
                quad_mov(uint_val(0x00000040), stack_push_ref())()   -- set SPE
                se_exec_dict_internal("read_modify_write")
            end
        })
    end},
    
    -- ================================================================
    -- init_all_peripherals: Top-level init that calls sub-configs
    -- Demonstrates if/then/else based on field values
    -- ================================================================
    {"init_all_peripherals", function()
        se_sequence_once(function()
        -- Enable clocks for GPIO port A (bit 0), UART1 (bit 14), SPI1 (bit 12)
        quad_mov(uint_val(0x00000001), stack_push_ref())()
        se_exec_dict_internal("enable_peripheral_clock")
        
        quad_mov(uint_val(0x00004000), stack_push_ref())()
        se_exec_dict_internal("enable_peripheral_clock")
        
        quad_mov(uint_val(0x00001000), stack_push_ref())()
        se_exec_dict_internal("enable_peripheral_clock")
        
        -- Configure GPIO: PA5 as alt-function, high speed, no pull
        -- (typical for SPI1_SCK)
        quad_mov(uint_val(0x48000000), stack_push_ref())()       -- GPIOA base
        quad_mov(uint_val(5), stack_push_ref())()                -- pin 5
        quad_mov(uint_val(GPIO_MODE_ALT_FN), stack_push_ref())()
        quad_mov(uint_val(GPIO_SPEED_HIGH), stack_push_ref())()
        quad_mov(uint_val(GPIO_PULL_NONE), stack_push_ref())()
        se_exec_dict_internal("configure_gpio_pin")
        
        -- Configure UART conditionally based on uart_channel field
        se_if_then_else(
            
            se_field_ne("uart_channel", 0),
            
            function()
                se_sequence_once(function()
                -- UART enabled: configure with blackboard values
                quad_mov(field_val("uart_channel"), stack_push_ref())()
                quad_mov(field_val("uart_baud"), stack_push_ref())()
                -- Build config: parity | stop_bits | TX_EN | RX_EN | UE
                quad_mov(uint_val(0x0000000D), stack_push_ref())()
                se_exec_dict_internal("configure_uart")
                se_log("UART configured")
                end)
            end,
            function()
                se_log("UART skipped - channel not set")
            end
        )
        
        -- Configure SPI conditionally
        se_if_then_else(
            
            se_field_ne("spi_channel", 0),
            function()
             se_sequence_once(function()
                quad_mov(field_val("spi_channel"), stack_push_ref())()
                quad_mov(field_val("spi_clock_div"), stack_push_ref())()
                quad_mov(field_val("spi_mode"), stack_push_ref())()
                quad_mov(field_val("spi_bit_order"), stack_push_ref())()
                se_exec_dict_internal("configure_spi")
                se_log("SPI configured")
            end)
            end,
            function()
           
                se_log("SPI skipped - channel not set")
           end 
        )
        
        -- Mark complete
        se_set_field("peripherals_ready", 1)
        se_set_field("config_state", CONFIG_DONE)
        end)
    end},
}
se_function_interface(function()
    
   
    se_load_function_dict("fn_dict", input_dictionary)
        -- Entry point: call init_all_peripherals via the dictionary
    se_exec_dict_fn("fn_dict", "init_all_peripherals")
    se_return_pipeline_terminate()
        

end)

end_tree("function_dictionary")

return end_module(mod)