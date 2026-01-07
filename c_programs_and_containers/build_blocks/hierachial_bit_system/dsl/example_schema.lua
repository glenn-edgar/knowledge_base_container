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
          { name="AnyActive", idx=127, attrs={export=true} },
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

  -- ✅ Add config so cfg_json_recs/cfg_index are non-empty
  config = {
    Plant = {
      Line1 = {
        Cell3 = {
          Robot2 = {
            Config = {
              Motion = { MaxSpeed = 1200, MaxAccel = 3.5 },
              Comm   = { TimeoutMs = 250, Enabled = true },
            }
          },
          ConvA = {
            Config = {
              Speed = { Max = 2.0 },
            }
          }
        }
      }
    }
  },

  rollups = {
    D.Rollup{
      name="AnyAlarm",
      bitspace="ALARM",    -- (recommended; change back if you truly want STATE)
      op="OR",
      sources={ "ALARM.AnyActive" }
    }
  },

  profiles = {
    mcu_32k = {
      emit_json_sidecar = false,  -- ✅ MCU does not need the JSON file
      keep_names = false,
      enable_provenance = false,
      max_nodes = 64,
      max_banks = 256,
    },
    linux = {
      emit_json_sidecar = true,   -- ✅ Linux gets config.json for side use
      keep_names = true,
      enable_provenance = true,
      emit_cfg_records = true,
    }
  }
}
