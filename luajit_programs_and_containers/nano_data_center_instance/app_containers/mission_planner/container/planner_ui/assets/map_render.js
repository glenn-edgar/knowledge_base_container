// planner_ui :: map renderer (Phase 5b C3 + C4).
//
// Two-level interactive view, mirroring construction/scripts/board_dsl/
// visualizer.py:
//
//   L1 (topology)   region polygon, nodes (passive=circle, active=square),
//                   straight-line edges. Click an edge -> L2.
//                   Click a node -> properties popup.
//
//   L2 (path detail) the chosen edge's path tree, with sub-segments
//                   rendered in geometric order, color-coded by leaf
//                   kind. Activate leaves shown as a hollow diamond at
//                   the current pose. Back button returns to L1.
//
// Interactions:
//   click edge       L1 -> L2 for that edge
//   click node       L1: pop up a panel with node properties
//   Esc              dismiss popup; if no popup, L2 -> L1; if neither,
//                    no-op
//
// Splines use Hermite reconstruction with tangent magnitude = chord/3
// (matches physics_core.c's Bezier reconstruction + visualizer.py).

(function () {
    "use strict";

    const SVG_NS = "http://www.w3.org/2000/svg";

    // -- Leaf-kind colors (mirror visualizer.py COLORS) -----------------
    const LEAF_COLORS = {
        straight_line: "#2c7fb8",   // blue
        spline:        "#7fcdbb",   // teal
        rotate:        "#edf8b1",   // light yellow (with marker)
        wall_follow:   "#fc8d59",   // orange
        line_follow:   "#d7301f",   // red
        activate:      "#984ea3",   // purple
    };

    // -- View state -----------------------------------------------------
    const state = {
        boards: [],          // /api/boards result
        currentBoard: null,  // currently-selected board JSON
        currentView: "L1",   // "L1" or "L2"
        currentEdgeIdx: -1,  // edge index when in L2
    };

    // -- DOM helpers ----------------------------------------------------

    function $(sel) { return document.querySelector(sel); }
    function svg(name, attrs) {
        const el = document.createElementNS(SVG_NS, name);
        if (attrs) {
            for (const k in attrs) el.setAttribute(k, attrs[k]);
        }
        return el;
    }

    function showError(msg) {
        const region = $("#map-region");
        region.innerHTML = "";
        const div = document.createElement("div");
        div.className = "error";
        div.textContent = "ERROR: " + msg;
        region.appendChild(div);
    }

    function showLoading(msg) {
        const region = $("#map-region");
        region.innerHTML = '<p class="loading">' + (msg || "loading...") + "</p>";
    }

    // -- API client -----------------------------------------------------

    async function fetchJSON(url) {
        const r = await fetch(url, { headers: { "Accept": "application/json" } });
        if (!r.ok) {
            let body = "";
            try { body = await r.text(); } catch (_) {}
            throw new Error("HTTP " + r.status + " on " + url +
                            (body ? ": " + body.slice(0, 200) : ""));
        }
        return r.json();
    }

    async function loadBoards() {
        const data = await fetchJSON("/api/boards");
        return data.boards || [];
    }

    async function loadBoard(name) {
        // /api/board/<name> returns board JSON verbatim (not wrapped).
        return fetchJSON("/api/board/" + encodeURIComponent(name));
    }

    // -- Geometry helpers -----------------------------------------------

    function hermitePoints(p0x, p0y, p1x, p1y, h0, h1, n) {
        // Sample n+1 points on a cubic Hermite curve. Tangent magnitude
        // = chord/3 to match physics_core.c's Bezier reconstruction.
        n = n || 30;
        const dx = p1x - p0x, dy = p1y - p0y;
        const chord = Math.hypot(dx, dy);
        const mag = chord > 0 ? chord / 3 : 1;
        const t0x = Math.cos(h0) * mag, t0y = Math.sin(h0) * mag;
        const t1x = Math.cos(h1) * mag, t1y = Math.sin(h1) * mag;
        const pts = [];
        for (let i = 0; i <= n; i++) {
            const t = i / n;
            const h00 = 2 * t * t * t - 3 * t * t + 1;
            const h10 = t * t * t - 2 * t * t + t;
            const h01 = -2 * t * t * t + 3 * t * t;
            const h11 = t * t * t - t * t;
            pts.push({
                x: h00 * p0x + h10 * t0x + h01 * p1x + h11 * t1x,
                y: h00 * p0y + h10 * t0y + h01 * p1y + h11 * t1y,
            });
        }
        return pts;
    }

    function bboxOfBoard(board) {
        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        function tally(x, y) {
            if (x < minX) minX = x;
            if (y < minY) minY = y;
            if (x > maxX) maxX = x;
            if (y > maxY) maxY = y;
        }
        if (board.region && Array.isArray(board.region)) {
            for (const p of board.region) tally(p.x, p.y);
        }
        if (board.nodes) {
            for (const n of board.nodes) tally(n.x, n.y);
        }
        const w = maxX - minX, h = maxY - minY;
        const pad = Math.max(1, 0.05 * Math.max(w, h));
        return {
            minX: minX - pad, minY: minY - pad,
            maxX: maxX + pad, maxY: maxY + pad,
            width: w + 2 * pad, height: h + 2 * pad,
        };
    }

    // -- L1 renderers ---------------------------------------------------

    function renderRegion(svgRoot, board) {
        if (!board.region || !Array.isArray(board.region)) return;
        const points = board.region.map(p => p.x + "," + p.y).join(" ");
        svgRoot.appendChild(svg("polygon", { points: points, class: "region" }));
    }

    function renderEdges(svgRoot, board, byName) {
        if (!board.edges) return;
        for (let i = 0; i < board.edges.length; i++) {
            const e = board.edges[i];
            const from = byName[e.from], to = byName[e.to];
            if (!from || !to) continue;
            // Wider invisible hit target on top of a visible thin edge,
            // so click-on-edge is forgiving without making the visual
            // line ugly.
            svgRoot.appendChild(svg("line", {
                x1: from.x, y1: from.y, x2: to.x, y2: to.y, class: "edge",
            }));
            const hit = svg("line", {
                x1: from.x, y1: from.y, x2: to.x, y2: to.y,
                class: "edge-hit",
                "data-edge-idx": String(i),
            });
            hit.addEventListener("click", function (ev) {
                ev.stopPropagation();
                renderL2(state.currentBoard, i);
            });
            svgRoot.appendChild(hit);
        }
    }

    function renderNodes(svgRoot, board) {
        if (!board.nodes) return;
        for (const n of board.nodes) {
            const isActive = !!(n.kb_ref && n.kb_ref !== "");
            const node = isActive
                ? svg("rect", {
                    x: n.x - 0.3, y: n.y - 0.3, width: 0.6, height: 0.6,
                    class: "node-active",
                })
                : svg("circle", {
                    cx: n.x, cy: n.y, r: 0.3, class: "node-passive",
                });
            node.addEventListener("click", function (ev) {
                ev.stopPropagation();
                showNodePopup(n);
            });
            svgRoot.appendChild(node);

            const label = svg("text", {
                x: n.x, y: n.y - 0.6, "text-anchor": "middle",
                class: "node-label",
            });
            label.textContent = n.name || "";
            svgRoot.appendChild(label);
        }
    }

    function renderBoard(board) {
        state.currentBoard = board;
        state.currentView = "L1";
        state.currentEdgeIdx = -1;
        const region = $("#map-region");
        region.innerHTML = "";

        const root = svg("svg", { id: "map-svg" });
        const bb = bboxOfBoard(board);
        root.setAttribute("viewBox",
            bb.minX + " " + bb.minY + " " + bb.width + " " + bb.height);
        root.setAttribute("preserveAspectRatio", "xMidYMid meet");

        const g = svg("g", {
            transform: "translate(0," + (bb.minY + bb.maxY) + ") scale(1,-1)",
        });
        renderRegion(g, board);

        const byName = {};
        for (const n of (board.nodes || [])) byName[n.name] = n;

        renderEdges(g, board, byName);
        renderNodes(g, board);
        root.appendChild(g);

        region.appendChild(root);
    }

    // -- L2: per-edge path detail ---------------------------------------

    // Render one drive sub-segment. Returns {x, y, heading} for the
    // next segment to chain off. wall_follow / line_follow render the
    // BASE primitive geometry as a dashed line in the leaf's color
    // (the offset/centerline tracking is robot behavior, not geometry).
    function renderSegment(g, seg, sx, sy, sh) {
        const k = seg.kind;
        const color = LEAF_COLORS[k] || "#aaa";

        if (k === "straight_line") {
            const ep = seg.end_pos;
            const ln = svg("line", {
                x1: sx, y1: sy, x2: ep.x, y2: ep.y,
                class: "leaf-straight_line",
                style: "stroke:" + color,
            });
            g.appendChild(ln);
            return { x: ep.x, y: ep.y, heading: Math.atan2(ep.y - sy, ep.x - sx) };
        }

        if (k === "spline") {
            const ep = seg.end_pos;
            const eh = seg.end_heading;
            const pts = hermitePoints(sx, sy, ep.x, ep.y, sh, eh);
            const d = pts.map((p, i) =>
                (i === 0 ? "M " : "L ") + p.x + " " + p.y).join(" ");
            g.appendChild(svg("path", {
                d: d, class: "leaf-spline",
                style: "stroke:" + color,
            }));
            return { x: ep.x, y: ep.y, heading: eh };
        }

        if (k === "rotate") {
            const eh = seg.end_heading;
            // Star marker at current pose (uses a small filled circle
            // for SVG simplicity; visualizer.py uses matplotlib '*').
            g.appendChild(svg("circle", {
                cx: sx, cy: sy, r: 0.25,
                class: "leaf-rotate",
                style: "fill:" + color,
            }));
            // Tangent indicator showing new heading.
            const dx = Math.cos(eh) * 0.4, dy = Math.sin(eh) * 0.4;
            g.appendChild(svg("line", {
                x1: sx, y1: sy, x2: sx + dx, y2: sy + dy,
                class: "leaf-rotate-tangent",
                style: "stroke:" + color,
            }));
            return { x: sx, y: sy, heading: eh };
        }

        if (k === "wall_follow" || k === "line_follow") {
            const base = seg.base;
            if (base.kind === "straight_line") {
                const ep = base.end_pos;
                g.appendChild(svg("line", {
                    x1: sx, y1: sy, x2: ep.x, y2: ep.y,
                    class: "leaf-" + k,
                    style: "stroke:" + color,
                }));
                return { x: ep.x, y: ep.y,
                         heading: Math.atan2(ep.y - sy, ep.x - sx) };
            } else {
                // spline base
                const ep = base.end_pos;
                const eh = base.end_heading;
                const pts = hermitePoints(sx, sy, ep.x, ep.y, sh, eh);
                const d = pts.map((p, i) =>
                    (i === 0 ? "M " : "L ") + p.x + " " + p.y).join(" ");
                g.appendChild(svg("path", {
                    d: d, class: "leaf-" + k,
                    style: "stroke:" + color,
                }));
                return { x: ep.x, y: ep.y, heading: eh };
            }
        }

        if (k === "activate") {
            // Hollow diamond at current pose.
            const r = 0.35;
            const points = [
                sx + ",", (sy + r),
                ",", (sx + r), ",", sy,
                ",", sx, ",", (sy - r),
                ",", (sx - r), ",", sy,
            ].join("");
            // Build polygon as proper points string.
            const pStr = sx + "," + (sy + r) + " " +
                         (sx + r) + "," + sy + " " +
                         sx + "," + (sy - r) + " " +
                         (sx - r) + "," + sy;
            g.appendChild(svg("polygon", {
                points: pStr,
                class: "leaf-activate",
                style: "fill:none;stroke:" + color,
            }));
            // action_id label nearby.
            const lab = svg("text", {
                x: sx, y: sy - r - 0.2, "text-anchor": "middle",
                class: "leaf-activate-label",
            });
            lab.textContent = seg.action_id || "(activate)";
            g.appendChild(lab);
            return { x: sx, y: sy, heading: sh };
        }

        // Unknown kind: render a placeholder circle with the kind name.
        g.appendChild(svg("circle", {
            cx: sx, cy: sy, r: 0.2, fill: "#888",
        }));
        return { x: sx, y: sy, heading: sh };
    }

    function renderL2(board, edgeIdx) {
        if (!board || !board.edges || !board.edges[edgeIdx]) return;
        state.currentView = "L2";
        state.currentEdgeIdx = edgeIdx;
        const edge = board.edges[edgeIdx];

        const region = $("#map-region");
        region.innerHTML = "";

        // Header bar: back button + edge title + leaf-color legend
        const bar = document.createElement("div");
        bar.className = "l2-bar";
        const back = document.createElement("button");
        back.className = "back-button";
        back.textContent = "← back to topology";
        back.addEventListener("click", function () { renderBoard(board); });
        bar.appendChild(back);
        const title = document.createElement("span");
        title.className = "l2-title";
        title.textContent = edge.from + " → " + edge.to;
        bar.appendChild(title);
        region.appendChild(bar);

        // Resolve from-node coords as the start pose. heading=0 baseline;
        // a future enhancement would chain from the previous edge's
        // ending heading, but at the per-edge level 0 is the convention.
        const byName = {};
        for (const n of (board.nodes || [])) byName[n.name] = n;
        const fromNode = byName[edge.from];
        const toNode = byName[edge.to];
        if (!fromNode || !toNode) {
            const err = document.createElement("div");
            err.className = "error";
            err.textContent = "edge endpoints not found in nodes list";
            region.appendChild(err);
            return;
        }

        // Bbox over the edge's leaf endpoints + the from/to nodes.
        let minX = fromNode.x, minY = fromNode.y;
        let maxX = toNode.x,   maxY = toNode.y;
        function tally(x, y) {
            if (x < minX) minX = x;  if (y < minY) minY = y;
            if (x > maxX) maxX = x;  if (y > maxY) maxY = y;
        }
        for (const leaf of (edge.path || [])) {
            if (leaf.end_pos) tally(leaf.end_pos.x, leaf.end_pos.y);
            if (leaf.base && leaf.base.end_pos)
                tally(leaf.base.end_pos.x, leaf.base.end_pos.y);
        }
        const wd = maxX - minX, ht = maxY - minY;
        const pad = Math.max(0.5, 0.1 * Math.max(wd, ht, 1));

        const root = svg("svg", { id: "map-svg" });
        root.setAttribute("viewBox",
            (minX - pad) + " " + (minY - pad) + " " +
            (wd + 2 * pad) + " " + (ht + 2 * pad));
        root.setAttribute("preserveAspectRatio", "xMidYMid meet");

        const g = svg("g", {
            transform: "translate(0," + (minY - pad + maxY + pad) +
                       ") scale(1,-1)",
        });

        // Endpoint markers (from/to)
        for (const n of [fromNode, toNode]) {
            g.appendChild(svg("circle", {
                cx: n.x, cy: n.y, r: 0.3,
                class: "l2-endpoint",
            }));
        }

        // Walk the leaves in geometric order.
        let pose = { x: fromNode.x, y: fromNode.y, heading: 0 };
        for (const leaf of (edge.path || [])) {
            pose = renderSegment(g, leaf, pose.x, pose.y, pose.heading);
        }

        root.appendChild(g);
        region.appendChild(root);
    }

    // -- Node properties popup ------------------------------------------

    function showNodePopup(node) {
        // Remove any existing popup first.
        closePopup();

        const overlay = document.createElement("div");
        overlay.id = "node-popup-overlay";
        overlay.className = "popup-overlay";
        overlay.addEventListener("click", function (e) {
            if (e.target === overlay) closePopup();
        });

        const popup = document.createElement("div");
        popup.className = "popup";

        const close = document.createElement("button");
        close.className = "popup-close";
        close.setAttribute("aria-label", "close");
        close.textContent = "×";
        close.addEventListener("click", closePopup);
        popup.appendChild(close);

        const h = document.createElement("h3");
        h.textContent = node.name || "(unnamed node)";
        popup.appendChild(h);

        const dl = document.createElement("dl");
        function row(k, v) {
            if (v === undefined || v === null || v === "") return;
            const dt = document.createElement("dt"); dt.textContent = k;
            const dd = document.createElement("dd"); dd.textContent = String(v);
            dl.appendChild(dt); dl.appendChild(dd);
        }
        row("x", node.x);
        row("y", node.y);
        row("kb_ref", node.kb_ref);
        row("description", node.description);
        popup.appendChild(dl);

        overlay.appendChild(popup);
        document.body.appendChild(overlay);
    }

    function closePopup() {
        const ov = document.getElementById("node-popup-overlay");
        if (ov) ov.remove();
    }

    function popupOpen() {
        return document.getElementById("node-popup-overlay") !== null;
    }

    // -- Esc handler ----------------------------------------------------

    document.addEventListener("keydown", function (e) {
        if (e.key !== "Escape") return;
        if (popupOpen()) {
            closePopup();
            return;
        }
        if (state.currentView === "L2" && state.currentBoard) {
            renderBoard(state.currentBoard);
        }
    });

    // -- Wiring ---------------------------------------------------------

    function buildPicker(boards) {
        const header = document.querySelector("header");
        if (!header) return;
        const wrap = document.createElement("span");
        wrap.className = "board-picker";
        wrap.innerHTML =
            '<label for="board-select">board:</label>' +
            '<select id="board-select"></select>';
        header.appendChild(wrap);

        const sel = $("#board-select");
        const placeholder = document.createElement("option");
        placeholder.value = ""; placeholder.textContent = "(pick a board)";
        sel.appendChild(placeholder);
        for (const b of boards) {
            const opt = document.createElement("option");
            opt.value = b.name;
            opt.textContent = b.name +
                (b.size ? "  (" + b.size + " bytes)" : "");
            sel.appendChild(opt);
        }
        sel.addEventListener("change", async function () {
            if (!sel.value) return;
            showLoading("loading board " + sel.value + "...");
            try {
                const board = await loadBoard(sel.value);
                renderBoard(board);
            } catch (e) {
                showError(e.message || String(e));
            }
        });
    }

    async function init() {
        try {
            const boards = await loadBoards();
            state.boards = boards;
            if (boards.length === 0) {
                showError("no boards available; commission one via " +
                          "construction/scripts/board_dsl/compile_board.lua");
                return;
            }
            buildPicker(boards);
            showLoading("pick a board to render its topology");
        } catch (e) {
            showError("failed to load board list: " + (e.message || e));
        }
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }

    // Expose internals for testability (no-op in browser).
    if (typeof module !== "undefined") {
        module.exports = {
            bboxOfBoard: bboxOfBoard,
            hermitePoints: hermitePoints,
            LEAF_COLORS: LEAF_COLORS,
        };
    }
})();
