-- config.lua -- Full simulation using real ChainTree DSL trees
--
-- Uses compiled JSON IR from robots/test_robot/
-- with the global plan from boards/workshop_floor/

return {
  board           = "workshop_floor/board.lua",
  strategy        = "workshop_floor/strategy_full.lua",
  hub_json        = "robots/test_robot/hub.json",
  remote_json     = "robots/test_robot/remote.json",
}
