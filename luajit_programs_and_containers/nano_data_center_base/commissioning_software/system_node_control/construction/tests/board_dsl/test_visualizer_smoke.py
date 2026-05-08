#!/usr/bin/env python3
"""
test_visualizer_smoke.py -- non-interactive smoke for visualizer.py.

Switches matplotlib to the headless 'Agg' backend so the test can run
without a display, then exercises:

  - hermite_points: sample count + endpoint match
  - render_L1: opens on a small fixture board without raising
  - render_L2: each edge in the fixture renders without raising
  - main(): rejects bad schema_version

The interactive bits (pick_event, key_press_event) are not testable
without a display + user input; that part of the contract is covered
by the spec in the visualizer.py docstring.
"""
import json
import math
import os
import subprocess
import sys
import tempfile
import unittest

import matplotlib
matplotlib.use("Agg")  # MUST come before importing pyplot anywhere

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DSL_DIR    = os.path.realpath(
    os.path.join(SCRIPT_DIR, "..", "..", "scripts", "board_dsl"))
sys.path.insert(0, DSL_DIR)

import visualizer  # noqa: E402


FIXTURE = {
    "schema_version": 2,
    "name": "test_board",
    "region": [{"x": 0, "y": 0}, {"x": 10, "y": 0},
               {"x": 10, "y": 10}, {"x": 0, "y": 10}],
    "capabilities": ["recharge"],
    "nodes": [
        {"name": "n1", "x": 1, "y": 1},
        {"name": "n2", "x": 5, "y": 5,
         "kb_ref": "system.x.site.s.infrastructure.registry.active_node_def.fake",
         "description": "test dock"},
    ],
    "edges": [
        {
            "from": "n1", "to": "n2",
            "path": [
                {"kind": "drive", "segments": [
                    {"kind": "straight_line", "end_pos": {"x": 2, "y": 2}},
                    {"kind": "spline", "end_pos": {"x": 4, "y": 4},
                     "end_heading": 0.5},
                    {"kind": "rotate", "end_heading": 1.57},
                    {"kind": "wall_follow",
                     "base": {"kind": "straight_line",
                              "end_pos": {"x": 4.5, "y": 4.5}},
                     "offset": 0.3},
                    {"kind": "line_follow",
                     "base": {"kind": "spline", "end_pos": {"x": 5, "y": 5},
                              "end_heading": 0.0}},
                ]},
                {"kind": "activate", "action_id": "recharge",
                 "kb_ref": "system.x.site.s.infrastructure.registry.active_node_def.fake",
                 "params": {"target_soc": 0.85}},
            ],
        },
    ],
}


class HermiteTests(unittest.TestCase):
    def test_endpoints(self):
        xs, ys = visualizer.hermite_points(
            (0.0, 0.0), (1.0, 0.0), 0.0, 0.0, n=20)
        self.assertEqual(len(xs), 21)
        self.assertEqual(len(ys), 21)
        self.assertAlmostEqual(xs[0], 0.0, places=6)
        self.assertAlmostEqual(xs[-1], 1.0, places=6)
        self.assertAlmostEqual(ys[0], 0.0, places=6)
        self.assertAlmostEqual(ys[-1], 0.0, places=6)

    def test_perpendicular_tangents(self):
        # Start tangent +x, end tangent +y -> curve bulges into +y on the way.
        xs, ys = visualizer.hermite_points(
            (0.0, 0.0), (1.0, 1.0), 0.0, math.pi / 2, n=20)
        # Midpoint should be off the straight diagonal (curve is real).
        mid = (xs[10], ys[10])
        diag = (0.5, 0.5)
        self.assertNotAlmostEqual(mid[0], diag[0], places=2)


class RenderTests(unittest.TestCase):
    def setUp(self):
        self.viewer = visualizer.Viewer(FIXTURE)
        # Open a fig manually since we won't call .run() (which blocks).
        import matplotlib.pyplot as plt
        self.viewer.fig, self.viewer.ax = plt.subplots()
        self.addCleanup(plt.close, self.viewer.fig)

    def test_L1_renders(self):
        # Smoke: no exception on render
        self.viewer.render_L1()
        self.assertEqual(len(self.viewer.node_artists), 2)
        self.assertEqual(len(self.viewer.edge_artists), 1)

    def test_L2_renders_all_5_segment_kinds(self):
        # The single edge in FIXTURE exercises all 5 sub-segment kinds
        # plus an activate leaf. If any kind raises during render, this
        # test catches it.
        self.viewer.render_L2(0)


class CLITests(unittest.TestCase):
    def test_rejects_schema_version_1(self):
        bad = dict(FIXTURE, schema_version=1)
        with tempfile.NamedTemporaryFile(
                "w", suffix=".json", delete=False) as f:
            json.dump(bad, f)
            tmp = f.name
        try:
            r = subprocess.run(
                [sys.executable,
                 os.path.join(DSL_DIR, "visualizer.py"), tmp],
                capture_output=True, text=True, timeout=10)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("schema_version != 2", r.stderr)
        finally:
            os.unlink(tmp)


if __name__ == "__main__":
    unittest.main(verbosity=2)
