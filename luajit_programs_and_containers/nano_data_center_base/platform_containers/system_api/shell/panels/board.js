// board.js — Popup panel: SVG board viewer with robot markers.
// Loads board from kb_export, watches robot_status for markers.
// Nodes whose drawn circles overlap collapse into a diamond cluster;
// clicking the diamond opens an informational subgraph inset.

import * as bg from "./board-graph.js";

var NODE_RADIUS = 30;

var _watchTokens = [];
var _container = null;
var _ctx = null;
var _board = { nodes: [], edges: [] };
var _clusters = [];
var _clusterById = {};
var _nodeToCluster = {};
var _edgeBuckets = { internal: {}, external: [] };
var _robotPositions = {}; // robot_id -> { link_state, current_node }
var _renderTimer = null;

// Inset state
var _openClusterId = null;
var _escapeHandler = null;

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

function scheduleRender() {
  if (!_renderTimer) {
    _renderTimer = setTimeout(function () {
      _renderTimer = null;
      renderBoard();
    }, 300);
  }
}

function renderBoard() {
  var svg = _container.querySelector("#board-svg");
  if (!svg || _board.nodes.length === 0 || _clusters.length === 0) return;

  var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (var i = 0; i < _board.nodes.length; i++) {
    var n = _board.nodes[i];
    if (n.x < minX) minX = n.x;
    if (n.y < minY) minY = n.y;
    if (n.x > maxX) maxX = n.x;
    if (n.y > maxY) maxY = n.y;
  }
  var pad = 120;
  svg.setAttribute("viewBox",
    (minX - pad) + " " + (minY - pad) + " " + (maxX - minX + pad * 2) + " " + (maxY - minY + pad * 2));
  svg.style.width = "100%";
  svg.style.height = "auto";

  var html = "";

  // External edges between clusters — aggregated
  var externals = _edgeBuckets.external;
  for (var i = 0; i < externals.length; i++) {
    var bucket = externals[i];
    var ca = _clusterById[bucket.a];
    var cb = _clusterById[bucket.b];
    if (!ca || !cb) continue;
    html += '<line class="edge" x1="' + ca.cx + '" y1="' + ca.cy +
      '" x2="' + cb.cx + '" y2="' + cb.cy + '"/>';
    var mx = (ca.cx + cb.cx) / 2;
    var my = (ca.cy + cb.cy) / 2;
    var rep = bucket.edges[0];
    var label = (rep.nav || "") + " " + (rep.speed || "");
    if (bucket.edges.length > 1) label += "  \u00D7" + bucket.edges.length;
    html += '<text class="edge-label" x="' + mx + '" y="' + (my - 12) + '">' + label + '</text>';
    if (rep.weight) {
      html += '<text class="edge-label" x="' + mx + '" y="' + (my + 5) + '">w=' + rep.weight + '</text>';
    }
  }

  // Cluster nodes
  for (var i = 0; i < _clusters.length; i++) {
    var c = _clusters[i];
    if (c.isSingleton) {
      var n = c.members[0];
      if (n.type === "transit") {
        var s = NODE_RADIUS * 0.6;
        html += '<rect class="node-transit" x="' + (n.x - s) + '" y="' + (n.y - s) +
          '" width="' + (s * 2) + '" height="' + (s * 2) + '"/>';
        html += '<text class="node-label transit-label" x="' + n.x + '" y="' + (n.y + s + 12) + '">' +
          n.name.replace(/_/g, " ") + '</text>';
      } else {
        var cls = "node-circle " + (n.type || "waypoint");
        html += '<circle class="' + cls + '" cx="' + n.x + '" cy="' + n.y + '" r="' + NODE_RADIUS + '"/>';
        html += '<text class="node-label" x="' + n.x + '" y="' + (n.y + NODE_RADIUS + 14) + '">' +
          n.name.replace(/_/g, " ") + '</text>';
      }
    } else {
      var cx = c.cx, cy = c.cy, r = NODE_RADIUS * 1.5;
      var titleText = c.members.map(function (g) { return g.name; }).join("\n");
      html += '<polygon class="node-cluster" ' +
        'points="' + cx + ',' + (cy - r) + ' ' + (cx + r) + ',' + cy + ' ' +
        cx + ',' + (cy + r) + ' ' + (cx - r) + ',' + cy + '" ' +
        'data-cluster="' + c.id + '" style="cursor:pointer">' +
        '<title>' + titleText + '</title>' +
        '</polygon>';
      html += '<text class="cluster-count" x="' + cx + '" y="' + (cy + 4) + '">' + c.members.length + '</text>';
      html += '<text class="node-label" x="' + cx + '" y="' + (cy + r + 14) + '">' +
        bg.clusterLabel(c) + '</text>';
    }
  }

  // Robot markers — group by cluster so multiple robots in one cluster don't stack
  var robotsByCluster = {};
  var rids = Object.keys(_robotPositions);
  for (var i = 0; i < rids.length; i++) {
    var rp = _robotPositions[rids[i]];
    if (rp.link_state === "offline") continue;
    if (!rp.current_node) continue;
    var cid = _nodeToCluster[rp.current_node];
    if (!cid) continue;
    if (!robotsByCluster[cid]) robotsByCluster[cid] = [];
    robotsByCluster[cid].push(rids[i]);
  }
  for (var cid in robotsByCluster) {
    var cluster = _clusterById[cid];
    if (!cluster) continue;
    var rs = robotsByCluster[cid];
    var anchorX, anchorY;
    if (cluster.isSingleton) {
      anchorX = cluster.members[0].x;
      anchorY = cluster.members[0].y;
    } else {
      anchorX = cluster.cx;
      anchorY = cluster.cy;
    }
    // First robot marker
    var rid0 = rs[0];
    html += '<circle class="robot-marker" cx="' + anchorX + '" cy="' + (anchorY - NODE_RADIUS - 5) + '" r="10"/>';
    html += '<text fill="#1a1a2e" font-size="8" font-weight="bold" text-anchor="middle" ' +
      'x="' + anchorX + '" y="' + (anchorY - NODE_RADIUS - 2) + '">' +
      rid0.charAt(0).toUpperCase() + '</text>';
    // Count badge if more than one
    if (rs.length > 1) {
      html += '<circle class="cluster-count-badge" cx="' + (anchorX + 12) + '" cy="' + (anchorY - NODE_RADIUS - 12) + '" r="7"/>';
      html += '<text fill="#1a1a2e" font-size="8" font-weight="bold" text-anchor="middle" ' +
        'x="' + (anchorX + 12) + '" y="' + (anchorY - NODE_RADIUS - 9) + '">' + rs.length + '</text>';
    }
  }

  svg.innerHTML = html;

  // Wire diamond click → open inset
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
// Subgraph inset (informational only — no node-select handler)
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
    interactive:        false,
    onClose:            closeInset,
  });
  inset.id = "viewer-inset";

  // Position near the cluster centroid
  var svg = _container.querySelector("#board-svg");
  var scrollBox = _container.querySelector(".board-scroll");
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

  var box = inset.getBoundingClientRect();
  var sb = scrollBox.getBoundingClientRect();
  if (box.right > sb.right - 8) {
    var leftPx = parseFloat(inset.style.left) - (NODE_RADIUS * 4) - inset.offsetWidth;
    inset.style.left = leftPx + "px";
  }
  box = inset.getBoundingClientRect();
  if (box.bottom > sb.bottom - 8) {
    var topPx = parseFloat(inset.style.top) - (box.bottom - sb.bottom + 16);
    inset.style.top = topPx + "px";
  }

  if (!_escapeHandler) {
    _escapeHandler = function (e) {
      if (e.key === "Escape" && _openClusterId) closeInset();
    };
    document.addEventListener("keydown", _escapeHandler);
  }
}

function closeInset() {
  _openClusterId = null;
  var existing = document.getElementById("viewer-inset");
  if (existing) existing.remove();
  if (_escapeHandler) {
    document.removeEventListener("keydown", _escapeHandler);
    _escapeHandler = null;
  }
}

export default {
  label: "Board Viewer",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _watchTokens = [];
    _robotPositions = {};
    _board = { nodes: [], edges: [] };
    _clusters = [];
    _clusterById = {};
    _nodeToCluster = {};
    _edgeBuckets = { internal: {}, external: [] };
    _openClusterId = null;

    container.innerHTML =
      '<div class="board-scroll">' +
        '<svg id="board-svg"></svg>' +
      '</div>';

    container.querySelector(".board-scroll").style.cssText =
      "overflow:auto;width:100%;height:100%;padding:1rem;position:relative;";

    // Click outside the inset/diamond closes inset
    container.querySelector(".board-scroll").addEventListener("click", function (e) {
      if (!e.target.closest(".board-inset") && !e.target.closest(".node-cluster")) {
        closeInset();
      }
    });

    // Load board
    try {
      var boardKey = await ctx.kvManager.firstKeyWithPrefix("kb_export", ctx.site + ".boards.");
      var data = boardKey ? await ctx.kvManager.get("kb_export", boardKey) : null;
      if (data) {
        _board = data;
        buildClusters();
      }
    } catch (e) { /* no board */ }

    // Load current robot link + position states
    var bucket = ctx.siteBucket + "_robot_status";
    var prefix = ctx.site + ".robots.";
    await ctx.kvManager.loadAll(bucket, function (key, val) {
      if (key.endsWith(".status.link")) {
        var rest = key.substring(prefix.length);
        var rid = rest.substring(0, rest.indexOf("."));
        if (rid && val.link_state !== "offline") {
          if (!_robotPositions[rid]) _robotPositions[rid] = {};
          _robotPositions[rid].link_state = val.link_state;
        }
      } else if (key.endsWith(".status.position") && val.current_node) {
        if (!_robotPositions[val.robot_id]) _robotPositions[val.robot_id] = {};
        _robotPositions[val.robot_id].current_node = val.current_node;
      }
    });

    // Watch for robot state + position changes
    var t = await ctx.kvManager.watch(bucket, function (key, val) {
      if (key.endsWith(".status.link")) {
        var rest = key.substring(prefix.length);
        var rid = rest.substring(0, rest.indexOf("."));
        if (rid) {
          if (val.link_state === "offline") delete _robotPositions[rid];
          else {
            if (!_robotPositions[rid]) _robotPositions[rid] = {};
            _robotPositions[rid].link_state = val.link_state;
          }
          scheduleRender();
        }
      } else if (key.endsWith(".status.position") && val.current_node) {
        if (!_robotPositions[val.robot_id]) _robotPositions[val.robot_id] = {};
        _robotPositions[val.robot_id].current_node = val.current_node;
        scheduleRender();
      }
    });
    _watchTokens.push(t);

    renderBoard();
  },

  destroy() {
    closeInset();
    for (var i = 0; i < _watchTokens.length; i++) _ctx.kvManager.release(_watchTokens[i]);
    _watchTokens = [];
    if (_renderTimer) { clearTimeout(_renderTimer); _renderTimer = null; }
    _container = null;
  }
};
