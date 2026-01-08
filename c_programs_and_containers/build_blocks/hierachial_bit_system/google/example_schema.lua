local D = require("dsl_runtime")

return D.Schema{
  name = "ChainBitTreeDemo",
  version = "1.0.0",

  -- 1. Bitspaces: Behavior definitions
  bitspaces = {
    D.Bitspace{ name="STATE",   merge="PRIORITY", boundary="COPY"  },
    D.Bitspace{ name="ALARM",   merge="OR",       boundary="LATCH" },
    D.Bitspace{ name="INHIBIT", merge="OR",       boundary="COPY"  },
    D.Bitspace{ name="PERMIT",  merge="AND",      boundary="COPY"  },
    D.Bitspace{ name="CMD_REQ", merge="OR",       boundary="RESET" },
    D.Bitspace{ name="CMD_ACK", merge="OR",       boundary="COPY"  },
  },

  -- 2. Classes: Memory sizing and bit definitions
  classes = {
    D.DeviceClass{
      name = "RobotArm",
      banks = { STATE=256, ALARM=128, CMD_REQ=64, CMD_ACK=64 }, 
      bits = {
        STATE = {
          { name="Ready", idx=0 }, { name="Faulted", idx=2 }
        },
        ALARM = {
          { name="OverTorque", idx=0, attrs={latch=true, export=true} },
          { name="CommLost",   idx=1, attrs={latch=true, export=true} }
        }
      }
    },
    D.DeviceClass{
      name = "Conveyor",
      banks = { STATE=128, ALARM=64 },
      bits = {
        STATE = { { name="Running", idx=1 } },
        ALARM = { { name="Jam", idx=0, attrs={export=true} } }
      }
    }
  },

  -- 3. Topology: The Tree
  nodes = {
    D.Node{ path="Plant.Line1.Cell3.Robot2", class="RobotArm" },
    D.Node{ path="Plant.Line1.Cell3.ConvA",  class="Conveyor" },
  },

  -- 4. Configuration: Static data
  config = {
    Plant = {
      Line1 = {
        Cell3 = {
          Robot2 = {
            Config = {
              Motion = { MaxSpeed = 1200, MaxAccel = 3.5 },
              Comm   = { TimeoutMs = 250, Enabled = true },
              Name   = "Robot_02_B" 
            }
          },
          ConvA = {
            Config = { Speed = { Max = 2.0 } }
          }
        }
      }
    }
  }
}