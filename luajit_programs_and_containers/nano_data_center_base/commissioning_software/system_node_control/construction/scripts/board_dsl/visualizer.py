#!/usr/bin/env python3
"""
visualizer.py -- matplotlib-based viewer for compiled v2 boards.

Reads a board JSON produced by compile_board.lua and presents a
two-level interactive view:

  L1 (topology)   region polygon, nodes (passive = circle, active =
                  square), edges (straight from->to lines)
  L2 (path detail) the chosen edge's path tree, with sub-segments
                  rendered in geometric order, color-coded by leaf
                  kind. Activate leaves shown as a hollow diamond at
                  the current pose.

Interactions:
  click edge       L1 -> L2 for that edge
  click node       L1: pop up a modal with node properties
  Esc              dismiss popup; if no popup, L2 -> L1; if neither,
                   close the window

Read-only by design (no PNG/SVG save, no in-canvas edit). Per the
v2 board DSL design memo: "Interactive only -- no PNG / SVG save."

Usage:
    visualizer.py <board.json>

Quirks:
  - Splines: rendered as Hermite curves (endpoint + tangent angle).
    Tangent magnitude follows the same distance/3 convention used by
    physics_core.c for Bezier control-point reconstruction.
  - wall_follow / line_follow: rendered as the base primitive; the
    offset/centerline tracking is robot-side behavior, not geometry.
"""

import argparse
import json
import math
import sys
from pathlib import Path

import matplotlib

matplotlib.use("TkAgg")  # WSLg-friendly per design memo

import matplotlib.pyplot as plt
from matplotlib.patches import Polygon, FancyBboxPatch
from matplotlib.lines import Line2D

# ---------------------------------------------------------------------
# leaf-kind colors
# ---------------------------------------------------------------------
COLORS = {
    "straight_line": "#2c7fb8",   # blue
    "spline":        "#7fcdbb",   # teal
    "rotate":        "#edf8b1",   # light yellow (with marker)
    "wall_follow":   "#fc8d59",   # orange
    "line_follow":   "#d7301f",   # red
    "activate":      "#984ea3",   # purple
}

# ---------------------------------------------------------------------
# geometry helpers
# ---------------------------------------------------------------------

def hermite_points(p0, p1, h0, h1, n=30):
    """Sample n points on a cubic Hermite curve from p0 to p1 with
    tangent angles h0 (start, in radians) and h1 (end). Tangent
    magnitude = chord_length / 3 (matches physics_core's distance/3
    Bezier reconstruction). Returns parallel x and y lists."""
    chord = math.hypot(p1[0] - p0[0], p1[1] - p0[1])
    mag = chord / 3.0 if chord > 0 else 1.0
    t0x, t0y = math.cos(h0) * mag, math.sin(h0) * mag
    t1x, t1y = math.cos(h1) * mag, math.sin(h1) * mag
    xs, ys = [], []
    for i in range(n + 1):
        t = i / n
        h00 = 2 * t**3 - 3 * t**2 + 1
        h10 = t**3 - 2 * t**2 + t
        h01 = -2 * t**3 + 3 * t**2
        h11 = t**3 - t**2
        x = h00 * p0[0] + h10 * t0x + h01 * p1[0] + h11 * t1x
        y = h00 * p0[1] + h10 * t0y + h01 * p1[1] + h11 * t1y
        xs.append(x)
        ys.append(y)
    return xs, ys


def render_segment(ax, seg, start_pos, start_heading):
    """Draw one drive sub-segment on ax. Returns (end_x, end_y, end_heading)
    so the caller can chain. For wall_follow / line_follow the BASE
    primitive geometry is drawn (the offset/centerline tracking is
    robot behavior, not geometry)."""
    kind = seg["kind"]
    if kind == "straight_line":
        ep = seg["end_pos"]
        ax.plot([start_pos[0], ep["x"]], [start_pos[1], ep["y"]],
                color=COLORS[kind], linewidth=2.5,
                label=f"straight_line")
        new_heading = math.atan2(ep["y"] - start_pos[1],
                                 ep["x"] - start_pos[0])
        return (ep["x"], ep["y"], new_heading)
    if kind == "spline":
        ep = seg["end_pos"]
        eh = seg["end_heading"]
        xs, ys = hermite_points((start_pos[0], start_pos[1]),
                                (ep["x"], ep["y"]),
                                start_heading, eh)
        ax.plot(xs, ys, color=COLORS[kind], linewidth=2.5,
                label=f"spline")
        return (ep["x"], ep["y"], eh)
    if kind == "rotate":
        eh = seg["end_heading"]
        ax.plot([start_pos[0]], [start_pos[1]],
                marker="*", markersize=14, color="#cc7700",
                markeredgecolor="black", linewidth=0,
                label=f"rotate")
        # Tiny tangent indicator showing new heading
        dx, dy = math.cos(eh) * 0.15, math.sin(eh) * 0.15
        ax.plot([start_pos[0], start_pos[0] + dx],
                [start_pos[1], start_pos[1] + dy],
                color="#cc7700", linewidth=1.5)
        return (start_pos[0], start_pos[1], eh)
    if kind in ("wall_follow", "line_follow"):
        base = seg["base"]
        if base["kind"] == "straight_line":
            ep = base["end_pos"]
            ax.plot([start_pos[0], ep["x"]], [start_pos[1], ep["y"]],
                    color=COLORS[kind], linewidth=2.5, linestyle="--",
                    label=kind)
            new_heading = math.atan2(ep["y"] - start_pos[1],
                                     ep["x"] - start_pos[0])
            return (ep["x"], ep["y"], new_heading)
        else:  # spline base
            ep = base["end_pos"]
            eh = base["end_heading"]
            xs, ys = hermite_points((start_pos[0], start_pos[1]),
                                    (ep["x"], ep["y"]),
                                    start_heading, eh)
            ax.plot(xs, ys, color=COLORS[kind], linewidth=2.5, linestyle="--",
                    label=kind)
            return (ep["x"], ep["y"], eh)
    raise ValueError(f"unknown segment kind: {kind}")


def render_path(ax, path, edge_from_pos, edge_to_pos):
    """Render an edge's full path (folded leaves) on ax. For each leaf:
    drive leaves draw their segments chained from previous end_pos;
    activate leaves draw a marker at the current pose."""
    cur = (edge_from_pos[0], edge_from_pos[1])
    heading = math.atan2(edge_to_pos[1] - edge_from_pos[1],
                         edge_to_pos[0] - edge_from_pos[0])
    for leaf in path:
        if leaf["kind"] == "drive":
            for seg in leaf["segments"]:
                ex, ey, eh = render_segment(ax, seg, cur, heading)
                cur = (ex, ey)
                heading = eh
        elif leaf["kind"] == "activate":
            ax.plot([cur[0]], [cur[1]],
                    marker="D", markersize=14,
                    color="white",
                    markeredgecolor=COLORS["activate"],
                    markeredgewidth=2.5, linewidth=0,
                    label=f"activate({leaf['action_id']})")
            ax.annotate(leaf.get("action_id", "?"),
                        xy=(cur[0], cur[1]), xytext=(8, 8),
                        textcoords="offset points",
                        color=COLORS["activate"], fontsize=9)


# ---------------------------------------------------------------------
# state container
# ---------------------------------------------------------------------

class Viewer:
    def __init__(self, board):
        self.board = board
        self.fig = None
        self.ax = None
        self.level = "L1"
        self.current_edge = None     # int index when in L2
        self.popup = None
        self.node_artists = []       # [(artist, node_dict)]
        self.edge_artists = []       # [(artist, edge_index)]
        self._nodes_by_name = {n["name"]: n for n in board["nodes"]}

    # ---------- L1 topology ----------
    def render_L1(self):
        self.ax.clear()
        b = self.board
        # region polygon
        pts = [(p["x"], p["y"]) for p in b["region"]]
        poly = Polygon(pts, closed=True, fill=True,
                       facecolor="#f7f7f7", edgecolor="#bbbbbb",
                       linewidth=1.5)
        self.ax.add_patch(poly)

        self.node_artists = []
        self.edge_artists = []

        # edges (straight from->to)
        for idx, e in enumerate(b["edges"]):
            a = self._nodes_by_name[e["from"]]
            c = self._nodes_by_name[e["to"]]
            line, = self.ax.plot([a["x"], c["x"]], [a["y"], c["y"]],
                                 color="#777777", linewidth=2.0,
                                 picker=5)
            self.edge_artists.append((line, idx))

        # nodes (active = square + colored fill, passive = circle)
        for n in b["nodes"]:
            is_active = n.get("kb_ref") is not None
            marker = "s" if is_active else "o"
            color  = "#984ea3" if is_active else "#377eb8"
            artist, = self.ax.plot([n["x"]], [n["y"]],
                                   marker=marker, markersize=14,
                                   color=color,
                                   markeredgecolor="black",
                                   markeredgewidth=1.0,
                                   linewidth=0, picker=10)
            self.node_artists.append((artist, n))
            self.ax.annotate(n["name"], xy=(n["x"], n["y"]),
                             xytext=(10, -10),
                             textcoords="offset points",
                             fontsize=9)

        self.ax.set_title(
            f"L1 topology: {b['name']}  "
            f"({len(b['nodes'])} nodes, {len(b['edges'])} edges)  "
            "[click edge -> L2; click node -> info; Esc -> close]",
            fontsize=10)
        self.ax.set_aspect("equal", adjustable="datalim")
        self.ax.grid(True, alpha=0.3)
        self.fig.canvas.draw_idle()

    # ---------- L2 path detail ----------
    def render_L2(self, edge_idx):
        self.ax.clear()
        b = self.board
        e = b["edges"][edge_idx]
        a = self._nodes_by_name[e["from"]]
        c = self._nodes_by_name[e["to"]]

        # Show region polygon faintly for context
        pts = [(p["x"], p["y"]) for p in b["region"]]
        poly = Polygon(pts, closed=True, fill=True,
                       facecolor="#fbfbfb", edgecolor="#dddddd",
                       linewidth=1.0)
        self.ax.add_patch(poly)

        # Endpoint markers
        for node, label in ((a, "from"), (c, "to")):
            is_active = node.get("kb_ref") is not None
            marker = "s" if is_active else "o"
            color  = "#984ea3" if is_active else "#377eb8"
            self.ax.plot([node["x"]], [node["y"]],
                         marker=marker, markersize=14,
                         color=color, markeredgecolor="black",
                         markeredgewidth=1.0, linewidth=0)
            self.ax.annotate(f"{node['name']} ({label})",
                             xy=(node["x"], node["y"]),
                             xytext=(10, -10),
                             textcoords="offset points",
                             fontsize=9)

        path = e.get("path") or []
        if not path:
            self.ax.text((a["x"] + c["x"]) / 2,
                         (a["y"] + c["y"]) / 2,
                         "(no path)", ha="center",
                         color="#999999", fontsize=12)
        else:
            render_path(self.ax, path, (a["x"], a["y"]), (c["x"], c["y"]))

        leaf_count_drive = sum(1 for l in path if l["kind"] == "drive")
        leaf_count_act   = sum(1 for l in path if l["kind"] == "activate")
        self.ax.set_title(
            f"L2 path: {e['from']} -> {e['to']}  "
            f"({leaf_count_drive} drive leaf, {leaf_count_act} activate leaf)  "
            "[Esc -> back to L1]", fontsize=10)
        self.ax.set_aspect("equal", adjustable="datalim")
        self.ax.grid(True, alpha=0.3)

        # Legend (de-duplicated)
        handles, labels = self.ax.get_legend_handles_labels()
        seen = set()
        deduped = [(h, l) for h, l in zip(handles, labels)
                   if not (l in seen or seen.add(l))]
        if deduped:
            self.ax.legend([h for h, _ in deduped], [l for _, l in deduped],
                           loc="upper right", fontsize=8)

        self.fig.canvas.draw_idle()

    # ---------- node popup ----------
    def show_node_popup(self, node):
        self.dismiss_popup()
        lines = [
            f"name: {node['name']}",
            f"x, y: {node['x']:.3f}, {node['y']:.3f}",
        ]
        if node.get("kb_ref"):
            lines.append(f"kb_ref: {node['kb_ref']}")
        else:
            lines.append("kb_ref: (passive node)")
        if node.get("description"):
            lines.append(f"desc: {node['description']}")
        text = "\n".join(lines)

        # Anchor the box near the node but in axes-relative coordinates
        # so it doesn't move with data zoom.
        self.popup = self.ax.text(
            0.02, 0.98, text,
            transform=self.ax.transAxes,
            fontsize=9, va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.5",
                      facecolor="white",
                      edgecolor="#666666",
                      alpha=0.95))
        self.fig.canvas.draw_idle()

    def dismiss_popup(self):
        if self.popup is not None:
            self.popup.remove()
            self.popup = None
            self.fig.canvas.draw_idle()

    # ---------- event handlers ----------
    def on_pick(self, event):
        if self.level == "L2":
            return  # picks ignored in L2
        for artist, idx in self.edge_artists:
            if event.artist is artist:
                self.level = "L2"
                self.current_edge = idx
                self.dismiss_popup()
                self.render_L2(idx)
                return
        for artist, node in self.node_artists:
            if event.artist is artist:
                self.show_node_popup(node)
                return

    def on_key(self, event):
        if event.key == "escape":
            if self.popup is not None:
                self.dismiss_popup()
            elif self.level == "L2":
                self.level = "L1"
                self.current_edge = None
                self.render_L1()
            else:
                plt.close(self.fig)

    def run(self):
        self.fig, self.ax = plt.subplots(figsize=(10, 8))
        self.fig.canvas.mpl_connect("pick_event", self.on_pick)
        self.fig.canvas.mpl_connect("key_press_event", self.on_key)
        self.render_L1()
        plt.show()


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("board_json", help="path to compiled board JSON")
    args = ap.parse_args()

    p = Path(args.board_json)
    if not p.is_file():
        print(f"visualizer: {p} not found", file=sys.stderr)
        sys.exit(1)
    with p.open() as f:
        board = json.load(f)
    if board.get("schema_version") != 2:
        print(f"visualizer: schema_version != 2 (got {board.get('schema_version')})",
              file=sys.stderr)
        sys.exit(1)

    Viewer(board).run()


if __name__ == "__main__":
    main()
