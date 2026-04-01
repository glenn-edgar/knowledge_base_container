-- config.lua -- Full simulation test configuration
--
-- Specifies board, strategy, and remote model for this test.

return {
  board         = "workshop_floor/board.lua",
  strategy      = "workshop_floor/strategy_full.lua",
  remote_model  = "simulated/remote_template.lua",
}
