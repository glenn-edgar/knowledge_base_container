-- example_schema.lua
local D = require("dsl_runtime")

return D.Schema{
  name = "ChainBitTreeDemo",
  version = "1.0.0",

  bitspaces = {
    D.Bitspace{ name="STATE",     merge="PRIORITY", boundary="COPY"  },
    D.Bitspace{ name="ALARM",     merge="OR",       boundary="LATCH" },
    D.Bitspace{ name="INHIBIT",   merge="OR",       boundary="COPY"  },
    D.Bitspace{ name="PERMIT",    merge="AND",      boundary="COPY"  },
    D.Bitspace{ name="CMD_REQ",   merge="OR",       boundary="RESET" },
    D.Bitspace{ name="CMD_ACK",   merge="OR",       boundary="COPY"  },
  },

  classes = {
    D.DeviceClass{
      name = "RobotArm",
      banks = {
        STATE   = 256,
        ALARM   = 128,
        INHIBIT = 64,
        PERMIT  = 64,
        CMD_REQ = 64,
        CMD_ACK = 64,
      },
      bits = {
        STATE = {
          { name="Ready", idx=0 },
          { name="Running", idx=1 },
          { name="Faulted", idx=2 },
        },
        ALARM = {
          { name="OverTorque", idx=0, attrs={latch=true} },
          { name="CommLost", idx=1, attrs={latch=true} },
          { name="AnyActive", idx=127, attrs={export=true} }, -- export rollup source
        },
      },
      exports = {
        ["ALARM.AnyActive"] = { bitspace="ALARM", bit="AnyActive" },
        ["STATE.Ready"]     = { bitspace="STATE", bit="Ready" },
      }
    },

    D.DeviceClass{
      name = "Conveyor",
      banks = { STATE=128, ALARM=64, INHIBIT=32, PERMIT=32, CMD_REQ=32, CMD_ACK=32 },
      bits = {
        STATE = { { name="Ready", idx=0 }, { name="Running", idx=1 } },
        ALARM = { { name="AnyActive", idx=63, attrs={export=true} } },
      },
      exports = { ["ALARM.AnyActive"] = { bitspace="ALARM", bit="AnyActive" } }
    },
  },

  nodes = {
    D.Node{ path="Plant.Line1.Cell3.Robot2", class="RobotArm" },
    D.Node{ path="Plant.Line1.Cell3.ConvA",  class="Conveyor" },
  },

  rollups = {
    -- For each parent node, the generator can optionally create summary bits.
    -- Here we define a named rollup concept (implementation: generator emits ops lists).
    D.Rollup{
      name="AnyAlarm",
      bitspace="STATE",
      op="OR",
      sources={ "ALARM.AnyActive" }  -- from children exports
    }
  },

  profiles = {
    mcu_32k = {
      keep_names = false,
      enable_provenance = false,
      max_nodes = 64,
      max_banks = 256,
    },
    linux = {
      keep_names = true,
      enable_provenance = true,
    }
  }
}

