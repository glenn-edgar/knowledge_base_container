// mission-planner.js — Full-screen panel: mission route editor.
// Build multi-stop routes by clicking board nodes. Each node IS a VN.
// Nodes whose drawn circles overlap collapse into a diamond cluster;
// clicking the diamond opens a subgraph inset where the user picks a node.

import * as bg from "./board-graph.js";

var NODE_RADIUS = 30;  // Must match the SVG circle r= value below.

var _container = null;
var _ctx = null;
var _watchTokens = [];

// Data
var _board = { nodes: [], edges: [] };
var _boardName = null;
var _clusters = [];        // [{id, members, cx, cy, bbox, isSingleton}]
var _clusterById = {};     // id → cluster
var _nodeToCluster = {};   // node name → cluster id
var _edgeBuckets = { internal: {}, external: [] };
var _adj = {};         // adjacency list: name -> [{to, nav, speed, weight}]
var _registeredRobots = [];
var _robotConfigs = {};      // robot_id → { capabilities: [...] } (from /api/robots)
var _robotEnergy = {};       // robot_id → { energy_remaining, energy_max }
var _robotPositions = {};    // robot_id → current_node
var _selectedRobot = null;
var _startNode = null;
var _bookend = true;

// Steps: [{ node }]
var _steps = [];
var _radioIdx = -1;
var _expanded = {};  // step idx → bool (expanded)

// Preflight result
var _preflight = [];      // per-step: { actions:[{kb_name,supported,detail}], cost, error?, cumCost }
var _preflightSummary = null;  // { ok, fail, warn, totalCost, energyMax, energyRemaining, message }

// Inset state — id of the cluster currently expanded, or null
var _openClusterId = null;
var _escapeHandler = null;

// ---------------------------------------------------------------
// Preflight: graph + Dijkstra + per-leg action builder
// ---------------------------------------------------------------
// Edge nav field IS the kb_name directly (path_spline, path_line).

function buildAdj() {
  _adj = {};
  for (var i = 0; i < _board.nodes.length; i++) {
    _adj[_board.nodes[i].name] = [];
  }
  for (var i = 0; i < _board.edges.length; i++) {
    var e = _board.edges[i];
    if (!_adj[e.from] || !_adj[e.to]) continue;
    _adj[e.from].push({ to: e.to, nav: e.nav, speed: e.speed, weight: e.weight });
    _adj[e.to].push({ to: e.from, nav: e.nav, speed: e.speed, weight: e.weight });
  }
}

// Dijkstra: returns { path: [names], cost: number } or null if no path.
// Naive O(N^2) — board has tens of nodes, fine.
function dijkstra(from, to) {
  if (!_adj[from] || !_adj[to]) return null;
  if (from === to) return { path: [from], cost: 0 };

  var dist = {}, prev = {}, visited = {};
  for (var name in _adj) dist[name] = Infinity;
  dist[from] = 0;

  while (true) {
    // Pick unvisited with smallest dist
    var u = null, ud = Infinity;
    for (var name in dist) {
      if (!visited[name] && dist[name] < ud) { u = name; ud = dist[name]; }
    }
    if (u === null) break;
    if (u === to) break;
    visited[u] = true;

    var nbrs = _adj[u] || [];
    for (var i = 0; i < nbrs.length; i++) {
      var v = nbrs[i].to;
      if (visited[v]) continue;
      var alt = ud + (nbrs[i].weight || 0);
      if (alt < dist[v]) { dist[v] = alt; prev[v] = u; }
    }
  }

  if (dist[to] === Infinity) return null;
  var path = [];
  var cur = to;
  while (cur !== undefined) { path.unshift(cur); cur = prev[cur]; }
  return { path: path, cost: dist[to] };
}

function nodeXY(name) {
  for (var i = 0; i < _board.nodes.length; i++) {
    if (_board.nodes[i].name === name) return _board.nodes[i];
  }
  return null;
}

function findEdge(fromName, toName) {
  var nbrs = _adj[fromName] || [];
  for (var i = 0; i < nbrs.length; i++) {
    if (nbrs[i].to === toName) return nbrs[i];
  }
  return null;
}

// Mirror server-side route_builder.lua: one nav action per edge.
// Edge nav field is the kb_name directly. Returns { actions: [{kb_name, detail}] }.
function buildLegActions(nodePath) {
  var actions = [];
  for (var i = 0; i < nodePath.length - 1; i++) {
    var fromName = nodePath[i], toName = nodePath[i + 1];
    var fromN = nodeXY(fromName), toN = nodeXY(toName);
    if (!fromN || !toN) continue;
    var edge = findEdge(fromName, toName);
    if (!edge) continue;

    var kb = edge.nav || "path_spline";
    var dx = toN.x - fromN.x, dy = toN.y - fromN.y;
    var dist = Math.round(Math.sqrt(dx * dx + dy * dy));
    actions.push({
      kb_name: kb,
      detail: fromName + " → " + toName + "  (" + dist + " @ " + edge.speed + ")",
    });
  }
  return { actions: actions };
}

// Mark each action's `supported` flag against capability set.
function markSupported(actions, capSet) {
  for (var i = 0; i < actions.length; i++) {
    actions[i].supported = capSet ? !!capSet[actions[i].kb_name] : true;
  }
}

// Compute the full preflight: per-stop action list + cumulative cost,
// against selected robot's capabilities and energy budget.
function computePreflight() {
  _preflight = [];
  _preflightSummary = null;

  if (!_selectedRobot || _steps.length === 0 || _board.nodes.length === 0) {
    return;
  }

  var caps = (_robotConfigs[_selectedRobot] && _robotConfigs[_selectedRobot].capabilities) || null;
  var capSet = null;
  if (caps && caps.length) {
    capSet = {};
    for (var i = 0; i < caps.length; i++) capSet[caps[i]] = true;
  }

  // Operation types the robot supports (separate from VN capabilities)
  var opTypes = (_robotConfigs[_selectedRobot] && _robotConfigs[_selectedRobot].operation_types) || null;
  var opSet = null;
  if (opTypes && opTypes.length) {
    opSet = {};
    for (var i = 0; i < opTypes.length; i++) opSet[opTypes[i]] = true;
  }

  var energy = _robotEnergy[_selectedRobot] || {};
  var energyMax = energy.energy_max || null;
  var energyRem = (energy.energy_remaining !== undefined) ? energy.energy_remaining : energyMax;

  // Always-present bookends
  var bookendActions = [
    { kb_name: "init_check", detail: "robot self-test (mission start)" },
    { kb_name: "idle",       detail: "park (mission end)" },
  ];
  markSupported(bookendActions, capSet);

  var cumCost = 0;
  var prevNode = _startNode;
  var totalUnsupported = {};
  var anyError = false;

  for (var s = 0; s < _steps.length; s++) {
    var step = _steps[s];
    var goalNode = step.node;
    var entry = { actions: [], cost: 0, cumCost: 0, error: null };

    if (!prevNode) {
      entry.error = "no start node (robot position unknown)";
      anyError = true;
    } else if (prevNode === goalNode) {
      // Already there — no nav actions for this leg
    } else {
      var route = dijkstra(prevNode, goalNode);
      if (!route) {
        entry.error = "no path from '" + prevNode + "' to '" + goalNode + "'";
        anyError = true;
      } else {
        var leg = buildLegActions(route.path);
        markSupported(leg.actions, capSet);
        entry.actions = leg.actions;
        entry.cost = route.cost;
        cumCost += route.cost;
      }
    }

    // Stop's operation (if any) appended after nav — validated against operation_types
    if (step.action) {
      var supported = opSet ? !!opSet[step.action] : true;
      var act = { kb_name: "operation", detail: step.action + " @ " + goalNode, supported: supported };
      entry.actions.push(act);
    }

    entry.cumCost = cumCost;

    // Tally unsupported names
    for (var i = 0; i < entry.actions.length; i++) {
      if (!entry.actions[i].supported) {
        totalUnsupported[entry.actions[i].kb_name] = true;
      }
    }

    _preflight.push(entry);
    prevNode = goalNode;
  }

  // Build summary
  var unsupportedList = Object.keys(totalUnsupported);
  // Bookends contribute to unsupported list too
  for (var i = 0; i < bookendActions.length; i++) {
    if (!bookendActions[i].supported) {
      if (unsupportedList.indexOf(bookendActions[i].kb_name) === -1) {
        unsupportedList.push(bookendActions[i].kb_name);
      }
    }
  }

  var msgs = [];
  var level = "ok";
  if (anyError) { level = "fail"; msgs.push("planning errors"); }
  if (unsupportedList.length > 0) {
    level = "fail";
    msgs.push("unsupported: " + unsupportedList.join(", "));
  }
  if (energyRem !== null && energyRem !== undefined && cumCost > energyRem) {
    level = "fail";
    msgs.push("insufficient energy: need " + cumCost + ", have " + energyRem);
  } else if (!capSet) {
    if (level === "ok") level = "warn";
    msgs.push("robot capabilities unknown");
  } else if (energyRem === null || energyRem === undefined) {
    if (level === "ok") level = "warn";
    msgs.push("robot energy unknown");
  }

  if (msgs.length === 0) msgs.push("plan ok");

  _preflightSummary = {
    level: level,
    message: msgs.join("; "),
    totalCost: cumCost,
    energyRemaining: energyRem,
    energyMax: energyMax,
  };
}

function recompute() {
  computePreflight();
  renderSteps();
  renderPreflightBanner();
  renderBoard();
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------
function nodeByName(name) {
  for (var i = 0; i < _board.nodes.length; i++) {
    if (_board.nodes[i].name === name) return _board.nodes[i];
  }
  return null;
}

function buildClusters() {
  var result = bg.computeClusters(_board.nodes, NODE_RADIUS);
  _clusters = result.clusters;
  _clusterById = result.byId;
  _nodeToCluster = result.nodeToCluster;
  _edgeBuckets = bg.bucketEdges(_board.edges, _nodeToCluster);
}

// ---------------------------------------------------------------
// Render: step table
// ---------------------------------------------------------------
function renderSteps() {
  var tbody = _container.querySelector("#steps-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (_steps.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="color:#484f58;text-align:center;padding:1.5rem">Click a board node to add mission steps</td></tr>';
    return;
  }

  for (var i = 0; i < _steps.length; i++) {
    var s = _steps[i];
    var pf = _preflight[i] || null;
    var stepFails = false;
    if (pf) {
      if (pf.error) stepFails = true;
      else {
        for (var k = 0; k < pf.actions.length; k++) {
          if (!pf.actions[k].supported) { stepFails = true; break; }
        }
      }
    }

    var tr = document.createElement("tr");
    tr.className = "step-row";
    if (i === _radioIdx) tr.className += " step-selected";
    if (stepFails) tr.className += " fail";

    // Step number
    var tdNum = document.createElement("td");
    tdNum.className = "step-num";
    tdNum.textContent = i + 1;
    tr.appendChild(tdNum);

    // Radio (single select)
    var tdRadio = document.createElement("td");
    tdRadio.className = "step-radio";
    var radio = document.createElement("input");
    radio.type = "radio";
    radio.name = "step-radio";
    radio.checked = (i === _radioIdx);
    radio.onchange = (function (idx) { return function () { _radioIdx = idx; renderSteps(); renderBoard(); }; })(i);
    tdRadio.appendChild(radio);
    tr.appendChild(tdRadio);

    // Expand toggle (chevron)
    var tdToggle = document.createElement("td");
    tdToggle.className = "step-toggle" + (stepFails ? " fail" : "");
    tdToggle.textContent = _expanded[i] ? "▼" : "▶";
    tdToggle.title = "Show / hide actions";
    tdToggle.onclick = (function (idx) {
      return function () {
        _expanded[idx] = !_expanded[idx];
        renderSteps();
      };
    })(i);
    tr.appendChild(tdToggle);

    // Destination + operation
    var tdDest = document.createElement("td");
    tdDest.className = "step-dest";
    var destText = s.node.replace(/_/g, " ");
    if (s.action) destText += " [" + s.action.replace(/_/g, " ") + "]";
    tdDest.textContent = destText;
    tr.appendChild(tdDest);

    // Cost
    var tdCost = document.createElement("td");
    tdCost.className = "step-cost" + (stepFails ? " fail" : "");
    if (pf) {
      if (pf.error) {
        tdCost.textContent = "—";
      } else {
        tdCost.textContent = pf.cost > 0 ? String(pf.cost) : "0";
      }
    } else {
      tdCost.textContent = "";
    }
    tr.appendChild(tdCost);

    tbody.appendChild(tr);

    // Expansion row
    if (_expanded[i]) {
      var trEx = document.createElement("tr");
      trEx.className = "step-expanded";
      var tdEx = document.createElement("td");
      tdEx.colSpan = 5;
      tdEx.appendChild(buildExpansionContent(i, pf));
      trEx.appendChild(tdEx);
      tbody.appendChild(trEx);
    }
  }
}

function buildExpansionContent(stepIdx, pf) {
  var wrap = document.createElement("div");

  if (!pf) {
    var none = document.createElement("div");
    none.style.color = "#666";
    none.style.fontSize = "0.76rem";
    none.textContent = "(no preflight — select a robot)";
    wrap.appendChild(none);
    return wrap;
  }

  // Step-level error (no path / no start)
  if (pf.error) {
    var err = document.createElement("div");
    err.className = "preflight-fail-banner";
    err.textContent = "❌ " + pf.error;
    wrap.appendChild(err);
  }

  // Mission-level failures relevant to this step (cumulative energy check)
  var s = _preflightSummary;
  if (s) {
    if (s.energyRemaining !== null && s.energyRemaining !== undefined &&
        pf.cumCost > s.energyRemaining) {
      var eb = document.createElement("div");
      eb.className = "preflight-fail-banner";
      eb.textContent = "❌ insufficient energy: cumulative " + pf.cumCost +
        " exceeds budget " + s.energyRemaining;
      wrap.appendChild(eb);
    }
    // Per-step unsupported tally
    var unsup = [];
    for (var i = 0; i < pf.actions.length; i++) {
      if (!pf.actions[i].supported) {
        if (unsup.indexOf(pf.actions[i].kb_name) === -1) unsup.push(pf.actions[i].kb_name);
      }
    }
    if (unsup.length > 0) {
      var ub = document.createElement("div");
      ub.className = "preflight-fail-banner";
      ub.textContent = "❌ no matching virtual node on robot: " + unsup.join(", ");
      wrap.appendChild(ub);
    }
  }

  // Action list
  if (pf.actions.length === 0 && !pf.error) {
    var noAct = document.createElement("div");
    noAct.style.color = "#666";
    noAct.style.fontSize = "0.76rem";
    noAct.textContent = "(already at destination — no actions)";
    wrap.appendChild(noAct);
  } else if (pf.actions.length > 0) {
    var list = document.createElement("div");
    list.className = "action-list";
    for (var i = 0; i < pf.actions.length; i++) {
      var a = pf.actions[i];
      var item = document.createElement("div");
      item.className = "action-item" + (a.supported ? "" : " fail");
      var num = document.createElement("span"); num.className = "a-num"; num.textContent = (i + 1) + ".";
      var name = document.createElement("span"); name.className = "a-name"; name.textContent = a.kb_name;
      var det = document.createElement("span"); det.className = "a-detail"; det.textContent = a.detail || "";
      item.appendChild(num);
      item.appendChild(name);
      item.appendChild(det);
      list.appendChild(item);
    }
    wrap.appendChild(list);
  }

  return wrap;
}

function renderPreflightBanner() {
  var el = _container && _container.querySelector("#preflight-banner");
  if (!el) return;
  if (!_preflightSummary) {
    el.style.display = "none";
    el.textContent = "";
    return;
  }
  var s = _preflightSummary;
  el.className = "preflight-banner " + s.level;
  el.style.display = "flex";

  var icon = s.level === "ok" ? "✓" : (s.level === "warn" ? "⚠" : "✗");
  var energyText = "";
  if (s.energyRemaining !== null && s.energyRemaining !== undefined) {
    energyText = " (cost " + s.totalCost + " / budget " + s.energyRemaining + ")";
  } else {
    energyText = " (cost " + s.totalCost + ")";
  }

  el.innerHTML = '';
  var main = document.createElement("span");
  main.textContent = icon + " " + s.message;
  el.appendChild(main);
  var detail = document.createElement("span");
  detail.className = "pf-detail";
  detail.textContent = energyText;
  el.appendChild(detail);
}

// ---------------------------------------------------------------
// Render: board SVG (embedded, clickable)
// ---------------------------------------------------------------
function renderBoard() {
  var svg = _container.querySelector("#planner-board-svg");
  if (!svg || _board.nodes.length === 0 || _clusters.length === 0) return;

  // viewBox over all nodes (clusters might extend beyond, but members are
  // by definition within the board's bbox)
  var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (var i = 0; i < _board.nodes.length; i++) {
    var n = _board.nodes[i];
    if (n.x < minX) minX = n.x;
    if (n.y < minY) minY = n.y;
    if (n.x > maxX) maxX = n.x;
    if (n.y > maxY) maxY = n.y;
  }
  var pad = 100;
  svg.setAttribute("viewBox",
    (minX - pad) + " " + (minY - pad) + " " + (maxX - minX + pad * 2) + " " + (maxY - minY + pad * 2));
  svg.setAttribute("width", (maxX - minX + pad * 2));
  svg.setAttribute("height", (maxY - minY + pad * 2));

  var html = "";

  // Selected destination cluster (radio'd step)
  var selectedDest = (_radioIdx >= 0 && _radioIdx < _steps.length) ? _steps[_radioIdx].node : null;
  var selectedClusterId = selectedDest ? _nodeToCluster[selectedDest] : null;

  // Build set of (cluster pair) on the planned route for path highlighting
  var routeNodes = [_startNode];
  for (var i = 0; i < _steps.length; i++) routeNodes.push(_steps[i].node);
  var routeClusterPairs = {};
  for (var i = 0; i < routeNodes.length - 1; i++) {
    var ca = _nodeToCluster[routeNodes[i]];
    var cb = _nodeToCluster[routeNodes[i + 1]];
    if (!ca || !cb || ca === cb) continue;
    var lo = ca < cb ? ca : cb, hi = ca < cb ? cb : ca;
    routeClusterPairs[lo + "\u0001" + hi] = true;
  }

  // External edges between clusters — aggregated, with ×N count when many
  var externals = _edgeBuckets.external;
  for (var i = 0; i < externals.length; i++) {
    var bucket = externals[i];
    var ca = _clusterById[bucket.a];
    var cb = _clusterById[bucket.b];
    if (!ca || !cb) continue;
    var pairKey = bucket.a + "\u0001" + bucket.b;
    var isRoute = !!routeClusterPairs[pairKey];
    var cls = "edge" + (isRoute ? " path-active" : "");
    html += '<line class="' + cls + '" x1="' + ca.cx + '" y1="' + ca.cy +
      '" x2="' + cb.cx + '" y2="' + cb.cy + '"/>';
    var mx = (ca.cx + cb.cx) / 2;
    var my = (ca.cy + cb.cy) / 2;
    // Pick a representative edge for the label (nav + speed)
    var rep = bucket.edges[0];
    var label = (rep.nav || "") + " " + (rep.speed || "");
    if (bucket.edges.length > 1) label += "  \u00D7" + bucket.edges.length;
    html += '<text class="edge-label" x="' + mx + '" y="' + (my - 10) + '">' + label + '</text>';
  }

  // Cluster nodes — singletons are circles (operation) or squares (transit), multi are diamonds
  for (var i = 0; i < _clusters.length; i++) {
    var c = _clusters[i];
    if (c.isSingleton) {
      var n = c.members[0];
      if (n.type === "transit") {
        var s = NODE_RADIUS * 0.6;
        html += '<rect class="node-transit" x="' + (n.x - s) + '" y="' + (n.y - s) +
          '" width="' + (s * 2) + '" height="' + (s * 2) +
          '" data-node="' + n.name + '" data-transit="1" style="cursor:default">' +
          '<title>Transit only — not selectable as a mission stop</title></rect>';
        html += '<text class="node-label transit-label" x="' + n.x + '" y="' + (n.y + s + 12) + '">' +
          n.name.replace(/_/g, " ") + '</text>';
      } else {
        var cls = "node-circle " + (n.type || "waypoint");
        if (n.name === selectedDest) cls += " selected";
        html += '<circle class="' + cls + '" cx="' + n.x + '" cy="' + n.y + '" r="' + NODE_RADIUS + '" ' +
          'data-node="' + n.name + '" style="cursor:pointer"/>';
        html += '<text class="node-label" x="' + n.x + '" y="' + (n.y + NODE_RADIUS + 14) + '">' +
          n.name.replace(/_/g, " ") + '</text>';
      }
    } else {
      var isSelected = (c.id === selectedClusterId);
      var dcls = "node-cluster" + (isSelected ? " selected" : "");
      var cx = c.cx, cy = c.cy, r = NODE_RADIUS * 1.5;
      html += '<polygon class="' + dcls + '" ' +
        'points="' + cx + ',' + (cy - r) + ' ' + (cx + r) + ',' + cy + ' ' +
        cx + ',' + (cy + r) + ' ' + (cx - r) + ',' + cy + '" ' +
        'data-cluster="' + c.id + '" style="cursor:pointer"/>';
      html += '<text class="cluster-count" x="' + cx + '" y="' + (cy + 4) + '">' + c.members.length + '</text>';
      html += '<text class="node-label" x="' + cx + '" y="' + (cy + r + 14) + '">' +
        bg.clusterLabel(c) + '</text>';
    }
  }

  // Start marker — placed on the singleton node or on the cluster centroid
  if (_startNode) {
    var startCid = _nodeToCluster[_startNode];
    var startCluster = startCid ? _clusterById[startCid] : null;
    if (startCluster) {
      var sx = startCluster.cx, sy = startCluster.cy;
      if (startCluster.isSingleton) { sx = startCluster.members[0].x; sy = startCluster.members[0].y; }
      html += '<circle class="robot-marker" cx="' + sx + '" cy="' + (sy - NODE_RADIUS - 5) + '" r="10"/>';
      html += '<text fill="#1a1a2e" font-size="8" font-weight="bold" text-anchor="middle" ' +
        'x="' + sx + '" y="' + (sy - NODE_RADIUS - 2) + '">S</text>';
    }
  }

  svg.innerHTML = html;

  // Attach click handlers
  var circles = svg.querySelectorAll(".node-circle");
  for (var i = 0; i < circles.length; i++) {
    circles[i].addEventListener("click", function (e) {
      e.stopPropagation();
      closeInset();
      addStep(this.getAttribute("data-node"));
    });
  }

  var diamonds = svg.querySelectorAll(".node-cluster");
  for (var i = 0; i < diamonds.length; i++) {
    diamonds[i].addEventListener("click", function (e) {
      e.stopPropagation();
      var cid = this.getAttribute("data-cluster");
      if (_openClusterId === cid) { closeInset(); return; }
      showInset(cid);
    });
  }
}

// ---------------------------------------------------------------
// Subgraph inset (drill into a cluster)
// ---------------------------------------------------------------
function showInset(clusterId) {
  closeInset();
  var cluster = _clusterById[clusterId];
  if (!cluster) return;
  _openClusterId = clusterId;

  var inset = bg.buildInsetSvg({
    cluster:            cluster,
    internalEdges:      _edgeBuckets.internal[clusterId] || [],
    externalNeighbours: bg.externalForCluster(_edgeBuckets.external, clusterId),
    byId:               _clusterById,
    nodeRadius:         NODE_RADIUS,
    interactive:        true,
    onClose:            closeInset,
  });
  inset.id = "vn-inset";
  inset.addEventListener("node-select", function (ev) {
    addStep(ev.detail.nodeName);
    closeInset();
  });

  // Position near the cluster centroid (anchored right of the diamond)
  var svg = _container.querySelector("#planner-board-svg");
  var scrollBox = _container.querySelector(".planner-board-scroll");
  if (svg && scrollBox) {
    var svgRect = svg.getBoundingClientRect();
    var scrollRect = scrollBox.getBoundingClientRect();
    var viewBox = svg.getAttribute("viewBox").split(" ").map(Number);
    var scaleX = svgRect.width / viewBox[2];
    var scaleY = svgRect.height / viewBox[3];
    var px = (cluster.cx - viewBox[0]) * scaleX + svgRect.left - scrollRect.left + scrollBox.scrollLeft;
    var py = (cluster.cy - viewBox[1]) * scaleY + svgRect.top - scrollRect.top + scrollBox.scrollTop;
    inset.style.left = (px + NODE_RADIUS * 2) + "px";
    inset.style.top  = (py - 20) + "px";
  }

  scrollBox.appendChild(inset);

  // Flip horizontally if it would clip the right edge
  var box = inset.getBoundingClientRect();
  var sb = scrollBox.getBoundingClientRect();
  if (box.right > sb.right - 8) {
    var leftPx = parseFloat(inset.style.left) - (NODE_RADIUS * 4) - inset.offsetWidth;
    inset.style.left = leftPx + "px";
  }
  // Vertical clip
  box = inset.getBoundingClientRect();
  if (box.bottom > sb.bottom - 8) {
    var topPx = parseFloat(inset.style.top) - (box.bottom - sb.bottom + 16);
    inset.style.top = topPx + "px";
  }

  // Escape closes
  if (!_escapeHandler) {
    _escapeHandler = function (e) {
      if (e.key === "Escape" && _openClusterId) {
        closeInset();
      }
    };
    document.addEventListener("keydown", _escapeHandler);
  }
}

function closeInset() {
  _openClusterId = null;
  var existing = document.getElementById("vn-inset");
  if (existing) existing.remove();
  if (_escapeHandler) {
    document.removeEventListener("keydown", _escapeHandler);
    _escapeHandler = null;
  }
}

// ---------------------------------------------------------------
// Step operations
// ---------------------------------------------------------------
function addStep(nodeName) {
  // Node type IS the operation — look it up and attach as action
  var n = nodeByName(nodeName);
  var step = { node: nodeName };
  if (n && n.type && n.type !== "transit") {
    step.action = n.type;
  }
  if (_radioIdx >= 0 && _radioIdx < _steps.length) {
    _steps.splice(_radioIdx + 1, 0, step);
    // Shift expansion state
    var newExp = {};
    for (var k in _expanded) {
      var ki = parseInt(k, 10);
      newExp[ki > _radioIdx ? ki + 1 : ki] = _expanded[k];
    }
    _expanded = newExp;
    _radioIdx = _radioIdx + 1;
  } else {
    _steps.push(step);
    _radioIdx = _steps.length - 1;
  }
  recompute();
}

function moveUp() {
  if (_radioIdx <= 0) return;
  var tmp = _steps[_radioIdx];
  _steps[_radioIdx] = _steps[_radioIdx - 1];
  _steps[_radioIdx - 1] = tmp;
  var a = _expanded[_radioIdx], b = _expanded[_radioIdx - 1];
  if (a) _expanded[_radioIdx - 1] = a; else delete _expanded[_radioIdx - 1];
  if (b) _expanded[_radioIdx] = b; else delete _expanded[_radioIdx];
  _radioIdx--;
  recompute();
}

function moveDown() {
  if (_radioIdx < 0 || _radioIdx >= _steps.length - 1) return;
  var tmp = _steps[_radioIdx];
  _steps[_radioIdx] = _steps[_radioIdx + 1];
  _steps[_radioIdx + 1] = tmp;
  var a = _expanded[_radioIdx], b = _expanded[_radioIdx + 1];
  if (a) _expanded[_radioIdx + 1] = a; else delete _expanded[_radioIdx + 1];
  if (b) _expanded[_radioIdx] = b; else delete _expanded[_radioIdx];
  _radioIdx++;
  recompute();
}

function deleteStep() {
  if (_radioIdx >= 0 && _radioIdx < _steps.length) {
    _steps.splice(_radioIdx, 1);
    // Drop deleted, shift higher indices down
    var newExp = {};
    for (var k in _expanded) {
      var ki = parseInt(k, 10);
      if (ki === _radioIdx) continue;
      newExp[ki > _radioIdx ? ki - 1 : ki] = _expanded[k];
    }
    _expanded = newExp;
    if (_radioIdx >= _steps.length) _radioIdx = _steps.length - 1;
  }
  recompute();
}

function clearAll() {
  if (_steps.length === 0) return;
  if (!confirm("Clear all mission steps?")) return;
  _steps = [];
  _radioIdx = -1;
  _expanded = {};
  recompute();
}

// ---------------------------------------------------------------
// Submit mission
// ---------------------------------------------------------------
function submitMission() {
  if (!_selectedRobot) { showMsg("Select a robot first."); return; }
  if (_steps.length === 0) { showMsg("Add at least one step."); return; }
  if (!_ctx.nc) { showMsg("NATS not connected."); return; }

  var summary = _startNode;
  for (var i = 0; i < _steps.length; i++) {
    summary += " \u2192 " + _steps[i].node;
  }

  var overlay = _container.querySelector("#confirm-overlay");
  var body = _container.querySelector("#confirm-body");
  body.innerHTML =
    '<div style="margin-bottom:0.75rem">Submit mission for <span style="color:#64ffda">' + _selectedRobot + '</span>?</div>' +
    '<div style="color:#c9d1d9;font-size:0.8rem;margin-bottom:0.75rem;word-break:break-word">' + summary + '</div>' +
    '<div style="font-size:0.78rem;color:#666">Steps: ' + _steps.length + '</div>' +
    '<div style="display:flex;gap:0.5rem;margin-top:1rem;justify-content:flex-end">' +
      '<button class="btn btn-idle" id="confirm-cancel">Cancel</button>' +
      '<button class="btn btn-go" id="confirm-ok">Confirm</button>' +
    '</div>';
  overlay.style.display = "flex";

  _container.querySelector("#confirm-cancel").onclick = function () { overlay.style.display = "none"; };
  _container.querySelector("#confirm-ok").onclick = function () {
    overlay.style.display = "none";
    doSubmit();
  };
}

function generateUuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    var r = Math.random() * 16 | 0;
    return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
}

async function doSubmit() {
  var stops = _steps.map(function (s) {
    var stop = { node: s.node };
    if (s.action) stop.action = s.action;
    return stop;
  });

  var mission = {
    robot_id: _selectedRobot,
    board: _boardName,
    start: _startNode,
    stops: stops,
  };

  try {
    var kv = await _ctx.kvManager.bucket(_ctx.siteBucket + "_action_server");
    var encoder = new TextEncoder();
    var jobId = generateUuid();
    var queue = _ctx.site + ".action_server.missions";
    var priority = 5;
    var now = new Date().toISOString();

    var jobData = {
      id: jobId, queue: queue, status: "pending",
      priority: priority, max_retries: 3, retry_count: 0,
      created_at: now, started_at: "", completed_at: "",
      worker_id: "", timeout_seconds: 300, payload: mission,
    };
    await kv.put("job." + jobId, encoder.encode(JSON.stringify(jobData)));

    var prioKey = String(1000000 - priority).padStart(6, "0");
    await kv.put("queue." + queue + "." + prioKey + "." + jobId, encoder.encode(jobId));

    showMsg("Mission submitted: " + _steps.length + " steps for " + _selectedRobot);
    _steps = [];
    _radioIdx = -1;
    _expanded = {};
    recompute();
  } catch (e) {
    showMsg("Submit failed: " + e.message);
  }
}

function showMsg(msg) {
  var el = _container.querySelector("#planner-msg");
  if (el) el.textContent = msg;
}

function onRobotChange(robotId) {
  _selectedRobot = robotId;
  // Update start node from robot's current position
  var pos = _robotPositions[robotId];
  _startNode = pos || null;
  var el = _container && _container.querySelector("#planner-start-node");
  if (el) el.textContent = _startNode || "unknown";
  recompute();
}

// ---------------------------------------------------------------
// Panel lifecycle
// ---------------------------------------------------------------
export default {
  label: "Mission Planner",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _watchTokens = [];
    _steps = [];
    _radioIdx = -1;
    _expanded = {};
    _preflight = [];
    _preflightSummary = null;
    _registeredRobots = [];
    _robotConfigs = {};
    _robotEnergy = {};
    _selectedRobot = null;
    _startNode = null;
    _robotPositions = {};
    _bookend = true;
    _board = { nodes: [], edges: [] };
    _clusters = [];
    _clusterById = {};
    _nodeToCluster = {};
    _edgeBuckets = { internal: {}, external: [] };
    _adj = {};
    _openClusterId = null;

    container.innerHTML =
      '<div class="panel-planner">' +
        '<div class="planner-header">' +
          '<label>Robot:</label><select id="planner-robot"></select>' +
          '<span class="planner-start" id="planner-start">Start: <span id="planner-start-node" style="color:#64ffda">-</span></span>' +
        '</div>' +
        '<div class="steps-toolbar">' +
          '<span class="section-title" style="margin:0">Mission Steps</span>' +
          '<div class="steps-buttons">' +
            '<button class="btn btn-tool" id="btn-up" title="Move selected step up">\u25B2 Up</button>' +
            '<button class="btn btn-tool" id="btn-down" title="Move selected step down">\u25BC Down</button>' +
            '<button class="btn btn-tool btn-tool-danger" id="btn-delete" title="Delete selected step">\u2715 Delete</button>' +
            '<button class="btn btn-tool" id="btn-clear" title="Clear all steps">Clear All</button>' +
          '</div>' +
        '</div>' +
        '<div id="preflight-banner" class="preflight-banner" style="display:none"></div>' +
        '<div class="steps-scroll">' +
          '<table class="steps-table">' +
            '<thead><tr><th>#</th><th></th><th></th><th>Destination</th><th style="text-align:right">Cost</th></tr></thead>' +
            '<tbody id="steps-body"></tbody>' +
          '</table>' +
        '</div>' +
        '<div class="submit-row">' +
          '<button class="btn btn-go" id="btn-submit">Submit Mission</button>' +
          '<span class="planner-msg" id="planner-msg"></span>' +
        '</div>' +
        '<div class="section-title" style="margin-top:0.75rem">Board: <span id="board-label">loading...</span>  <span style="color:#666;font-size:0.75rem">(click node to add step)</span></div>' +
        '<div class="planner-board-scroll">' +
          '<svg id="planner-board-svg"></svg>' +
        '</div>' +
        '<div id="confirm-overlay" class="confirm-overlay" style="display:none">' +
          '<div class="confirm-box" id="confirm-body"></div>' +
        '</div>' +
      '</div>';

    container.querySelector("#btn-up").onclick = moveUp;
    container.querySelector("#btn-down").onclick = moveDown;
    container.querySelector("#btn-delete").onclick = deleteStep;
    container.querySelector("#btn-clear").onclick = clearAll;
    container.querySelector("#btn-submit").onclick = submitMission;
    container.querySelector("#planner-robot").onchange = function () { onRobotChange(this.value); };

    // Click anywhere on board scroll area outside the inset/diamond closes inset
    container.querySelector(".planner-board-scroll").addEventListener("click", function (e) {
      if (!e.target.closest(".board-inset") && !e.target.closest(".node-cluster")) {
        closeInset();
      }
    });

    // Load robot configs (capabilities) from HTTP API
    try {
      var domName = ctx.domain ? ctx.domain.name : "surface_ops";
      var resp = await fetch("/api/robots?domain=" + encodeURIComponent(domName));
      var data = await resp.json();
      var list = Array.isArray(data.robots) ? data.robots : Object.values(data.robots || {});
      for (var i = 0; i < list.length; i++) _robotConfigs[list[i].name] = list[i];
    } catch (e) { /* API unavailable, capabilities will be unknown */ }

    if (ctx.kvManager) {
      // Load board
      try {
        var boardKey = await ctx.kvManager.firstKeyWithPrefix("kb_export", ctx.site + ".boards.");
        if (boardKey) _boardName = boardKey.split(".").pop();
        var data = boardKey ? await ctx.kvManager.get("kb_export", boardKey) : null;
        if (data) {
          _board = data;
          buildClusters();
          buildAdj();
          var boardName = _boardName;
          container.querySelector("#board-label").textContent =
            boardName + " (" + _board.nodes.length + " nodes)";

        }
      } catch (e) { /* no board */ }

      // Load + watch robot energy (energy_max + energy_remaining).
      // Robot id is parsed from the key: {site}.robots.{robot_id}.status.energy
      var energyPrefix = ctx.site + ".robots.";
      var ridFromKey = function (key) {
        if (key.indexOf(energyPrefix) !== 0) return null;
        var rest = key.substring(energyPrefix.length);
        var dot = rest.indexOf(".");
        return dot > 0 ? rest.substring(0, dot) : null;
      };
      try {
        var statusBucket = ctx.siteBucket + "_robot_status";
        await ctx.kvManager.loadAll(statusBucket, function (key, val) {
          if (!val) return;
          if (key.endsWith(".status.energy")) {
            var rid = ridFromKey(key);
            if (rid) {
              _robotEnergy[rid] = {
                energy_remaining: val.energy_remaining,
                energy_max:       val.energy_max,
              };
            }
          }
        });
        var tEnergy = await ctx.kvManager.watch(statusBucket, function (key, val) {
          if (!val) return;
          if (key.endsWith(".status.energy")) {
            var rid = ridFromKey(key);
            if (rid) {
              _robotEnergy[rid] = {
                energy_remaining: val.energy_remaining,
                energy_max:       val.energy_max,
              };
              if (rid === _selectedRobot) recompute();
            }
          }
        });
        _watchTokens.push(tEnergy);
      } catch (e) { /* status bucket may not exist yet */ }

      // Load registered robots
      try {
        var summary = await ctx.kvManager.get(ctx.siteBucket + "_action_server",
          ctx.site + ".action_server.summary");
        if (summary && summary.registered_robots) {
          _registeredRobots = Array.isArray(summary.registered_robots)
            ? summary.registered_robots : Object.keys(summary.registered_robots);
        }
      } catch (e) { /* no summary */ }

      // Watch for robot changes
      try {
        var t1 = await ctx.kvManager.watch(ctx.siteBucket + "_action_server", function (key, val) {
          if (key.endsWith(".summary") && val.registered_robots) {
            _registeredRobots = Array.isArray(val.registered_robots)
              ? val.registered_robots : Object.keys(val.registered_robots);
            populateRobotDropdown();
          }
        });
        _watchTokens.push(t1);
      } catch (e) { /* action_server bucket may not exist yet */ }

      // Load robot positions from KV
      try {
        var posBucket = ctx.siteBucket + "_robot_status";
        await ctx.kvManager.loadAll(posBucket, function (key, val) {
          if (key.endsWith(".status.position") && val.current_node) {
            _robotPositions[val.robot_id] = val.current_node;
          }
        });

        // Watch for position updates
        var t2 = await ctx.kvManager.watch(posBucket, function (key, val) {
          if (key.endsWith(".status.position") && val.current_node) {
            _robotPositions[val.robot_id] = val.current_node;
            if (val.robot_id === _selectedRobot) {
              onRobotChange(_selectedRobot);
            }
          }
        });
        _watchTokens.push(t2);
      } catch (e) { /* robot_status bucket may not exist yet */ }
    }

    populateRobotDropdown();
    recompute();

    // Ensure start node is set from position data (may have loaded after dropdown populated)
    if (_selectedRobot) onRobotChange(_selectedRobot);
  },

  destroy() {
    closeInset();
    for (var i = 0; i < _watchTokens.length; i++) _ctx.kvManager.release(_watchTokens[i]);
    _watchTokens = [];
    _container = null;
  }
};

function populateRobotDropdown() {
  var sel = _container ? _container.querySelector("#planner-robot") : null;
  if (!sel) return;
  var prev = sel.value;
  sel.innerHTML = "";

  if (_registeredRobots.length === 0) {
    var opt = document.createElement("option");
    opt.value = ""; opt.textContent = "(no robots)";
    sel.appendChild(opt);
    _selectedRobot = null;
    return;
  }

  for (var i = 0; i < _registeredRobots.length; i++) {
    var opt = document.createElement("option");
    opt.value = _registeredRobots[i]; opt.textContent = _registeredRobots[i];
    sel.appendChild(opt);
  }

  if (prev && _registeredRobots.indexOf(prev) >= 0) {
    sel.value = prev; _selectedRobot = prev;
  } else {
    _selectedRobot = _registeredRobots[0]; sel.value = _selectedRobot;
  }
  // Update start from robot position
  onRobotChange(_selectedRobot);
}
