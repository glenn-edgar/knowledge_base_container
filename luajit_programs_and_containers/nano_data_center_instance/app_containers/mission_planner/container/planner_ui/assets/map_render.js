// planner_ui :: map renderer (Phase 5b C3).
//
// Vanilla JS L1 topology view. Loads the board list from /api/boards,
// populates a picker, and renders the selected board's region polygon
// + nodes + straight-line edges into #map-svg.
//
// L1 design (mirrors construction/scripts/board_dsl/visualizer.py):
//   - region polygon: fill + stroke, opacity 0.5
//   - nodes: passive=circle (no kb_ref), active=square (kb_ref present)
//   - edges: straight lines between connected nodes (NOT polyline detail
//     -- that's L2, lands in 5b C4)
//   - node labels: monospace, slightly offset
//
// Coordinate system: board JSON uses world coordinates. We compute a
// bounding box and apply an SVG viewBox so the renderer is
// resolution-independent. Y-axis is flipped (SVG Y increases down;
// world Y typically increases up).

(function () {
    "use strict";

    const SVG_NS = "http://www.w3.org/2000/svg";

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

    // -- Bounding box + viewBox ----------------------------------------

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

        // Padding = 5% of the larger dimension, min 1.
        const w = maxX - minX, h = maxY - minY;
        const pad = Math.max(1, 0.05 * Math.max(w, h));
        return {
            minX: minX - pad, minY: minY - pad,
            maxX: maxX + pad, maxY: maxY + pad,
            width: w + 2 * pad, height: h + 2 * pad,
        };
    }

    // -- Renderers ------------------------------------------------------

    function renderRegion(svgRoot, board) {
        if (!board.region || !Array.isArray(board.region)) return;
        const points = board.region.map(p => p.x + "," + p.y).join(" ");
        svgRoot.appendChild(svg("polygon", { points: points, class: "region" }));
    }

    function renderEdges(svgRoot, board) {
        if (!board.edges) return;
        // Index nodes by name for endpoint lookup.
        const byName = {};
        for (const n of (board.nodes || [])) byName[n.name] = n;

        for (const e of board.edges) {
            const from = byName[e.from], to = byName[e.to];
            if (!from || !to) continue;
            svgRoot.appendChild(svg("line", {
                x1: from.x, y1: from.y, x2: to.x, y2: to.y, class: "edge",
            }));
        }
    }

    function renderNodes(svgRoot, board) {
        if (!board.nodes) return;
        for (const n of board.nodes) {
            const isActive = !!(n.kb_ref && n.kb_ref !== "");
            if (isActive) {
                // square centered on (x,y), side = 0.6 in world units
                svgRoot.appendChild(svg("rect", {
                    x: n.x - 0.3, y: n.y - 0.3, width: 0.6, height: 0.6,
                    class: "node-active",
                }));
            } else {
                svgRoot.appendChild(svg("circle", {
                    cx: n.x, cy: n.y, r: 0.3, class: "node-passive",
                }));
            }
            // Label slightly above the node.
            const label = svg("text", {
                x: n.x, y: n.y - 0.6, "text-anchor": "middle",
                class: "node-label",
            });
            label.textContent = n.name || "";
            svgRoot.appendChild(label);
        }
    }

    function renderBoard(board) {
        const region = $("#map-region");
        region.innerHTML = "";

        const root = svg("svg", { id: "map-svg" });

        const bb = bboxOfBoard(board);
        // Y-flip: SVG Y grows down; world Y typically grows up. Apply
        // a transform that mirrors the viewBox vertically.
        root.setAttribute("viewBox",
            bb.minX + " " + bb.minY + " " + bb.width + " " + bb.height);
        root.setAttribute("preserveAspectRatio", "xMidYMid meet");

        const g = svg("g", {
            // Flip Y so positive world-Y points UP visually.
            transform: "translate(0," + (bb.minY + bb.maxY) + ") scale(1,-1)",
        });
        renderRegion(g, board);
        renderEdges(g, board);
        renderNodes(g, board);
        root.appendChild(g);

        region.appendChild(root);
    }

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

    // Expose internals for testability (no-op in browser; the host-side
    // smoke can require this file via a JS engine if it has one).
    if (typeof module !== "undefined") {
        module.exports = {
            bboxOfBoard: bboxOfBoard,
        };
    }
})();
