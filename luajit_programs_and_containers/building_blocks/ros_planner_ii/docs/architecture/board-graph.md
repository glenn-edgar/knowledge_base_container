# Board Graph

## Landing Zone (Moon Map)

```
lander_pad (0,0) ----spline----> habitat_site (800,0) ----spline----> mining_zone_a (1600,0)
     |                                |
     spline                           spline
     |                                |
survey_point_1 (0,800) --spline-> charging_station (800,800) --spline-> mining_zone_b (1600,800)
     |                                |
     line                             spline
     |                                |
survey_point_2 (0,1600) -spline-> construction_bay (800,1600)
```

Graph is bidirectional. Edge weights represent energy cost. Navigation VN type is specified per edge (`nav` field).

## Edge → VN Mapping

| Edge `nav` field | VN kb_name |
|-----------------|------------|
| spline_follow | path_spline |
| line_follow | path_line |
| wall_follow | path_wall |

`path_rotate` is auto-inserted by route_builder when heading changes between consecutive edges.

## Route Building

Given a mission with stops, the global planner:

1. Runs Dijkstra between consecutive stops (shortest path by weight)
2. Route builder converts each edge to its navigation VN with coordinates
3. Inserts `path_rotate` at heading changes
4. Inserts stop actions (task VNs) at destination nodes
5. Optionally adds `init_check` at start and `idle` at end (bookend)

Example: lander_pad → mining_zone_a (deliver_part) → lander_pad

```
1. init_check
2. path_spline (lander_pad → habitat_site)
3. path_spline (habitat_site → mining_zone_a)
4. deliver_part
5. path_rotate (heading change)
6. path_spline (mining_zone_a → habitat_site)
7. path_rotate
8. path_spline (habitat_site → lander_pad)
9. idle
```

## Energy Budget

Edge weights = energy cost. Total mission cost = sum of all edge weights. The action server rejects missions where total cost exceeds robot energy. Recharge VN restores energy to max at charging_station.
