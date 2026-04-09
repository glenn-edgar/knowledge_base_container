// board.js — Popup panel: SVG board viewer with robot markers.
// Loads board from kb_export, watches robot_status for markers.
// Groups co-located nodes as diamond cluster icons with info tooltip.

var _watchTokens = [];
var _container = null;
var _ctx = null;
var _board = { nodes: [], edges: [] };
var _locations = {};  // "x,y" -> [node, node, ...]
var _robotPositions = {}; // robot_id -> { link_state }
var _renderTimer = null;

function nodeByName(name) {
  for (var i = 0; i < _board.nodes.length; i++) {
    if (_board.nodes[i].name === name) return _board.nodes[i];
  }
  return null;
}

function buildLocations() {
  _locations = {};
  for (var i = 0; i < _board.nodes.length; i++) {
    var n = _board.nodes[i];
    var key = n.x + "," + n.y;
    if (!_locations[key]) _locations[key] = [];
    _locations[key].push(n);
  }
}

function locationKey(n) { return n.x + "," + n.y; }

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
  if (!svg || _board.nodes.length === 0) return;

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

  // Edges (skip zero-length between co-located nodes)
  for (var i = 0; i < _board.edges.length; i++) {
    var e = _board.edges[i];
    var a = nodeByName(e.from);
    var b = nodeByName(e.to);
    if (!a || !b) continue;
    if (a.x === b.x && a.y === b.y) continue;

    html += '<line class="edge" x1="' + a.x + '" y1="' + a.y + '" x2="' + b.x + '" y2="' + b.y + '"/>';

    var mx = (a.x + b.x) / 2;
    var my = (a.y + b.y) / 2;
    html += '<text class="edge-label" x="' + mx + '" y="' + (my - 12) + '">' + e.nav + ' ' + e.speed + '</text>';
    if (e.weight) {
      html += '<text class="edge-label" x="' + mx + '" y="' + (my + 5) + '">w=' + e.weight + '</text>';
    }
  }

  // Nodes — grouped by location
  var rendered = {};
  for (var i = 0; i < _board.nodes.length; i++) {
    var n = _board.nodes[i];
    var lk = locationKey(n);
    if (rendered[lk]) continue;
    rendered[lk] = true;

    var group = _locations[lk] || [n];

    if (group.length === 1) {
      // Single node: normal circle
      var cls = "node-circle " + (n.type || "waypoint");
      html += '<circle class="' + cls + '" cx="' + n.x + '" cy="' + n.y + '" r="30"/>';
      html += '<text class="node-label" x="' + n.x + '" y="' + (n.y + 50) + '">' +
        n.name.replace(/_/g, " ") + '</text>';
    } else {
      // Multi-VN: diamond icon with count + tooltip listing all VN names
      var cx = n.x, cy = n.y, r = 45;
      html += '<polygon class="node-cluster" ' +
        'points="' + cx + ',' + (cy - r) + ' ' + (cx + r) + ',' + cy + ' ' + cx + ',' + (cy + r) + ' ' + (cx - r) + ',' + cy + '">' +
        '<title>' + group.map(function (g) { return g.name; }).join('\n') + '</title>' +
        '</polygon>';
      html += '<text class="cluster-count" x="' + cx + '" y="' + (cy + 4) + '">' + group.length + '</text>';
      // Label: list all names
      for (var j = 0; j < group.length; j++) {
        html += '<text class="node-label" x="' + cx + '" y="' + (cy + 50 + j * 14) + '">' +
          group[j].name.replace(/_/g, " ") + '</text>';
      }
    }
  }

  // Robot markers at their current node positions
  var rids = Object.keys(_robotPositions);
  for (var i = 0; i < rids.length; i++) {
    var rp = _robotPositions[rids[i]];
    if (rp.link_state === "offline") continue;
    var posNode = rp.current_node ? nodeByName(rp.current_node) : null;
    if (!posNode) continue;
    var offset = i * 18 - 9;
    html += '<circle class="robot-marker" cx="' + (posNode.x + offset) + '" cy="' + (posNode.y - 25) + '" r="10"/>';
    html += '<text fill="#1a1a2e" font-size="8" font-weight="bold" text-anchor="middle" ' +
            'x="' + (posNode.x + offset) + '" y="' + (posNode.y - 22) + '">' +
            rids[i].charAt(0).toUpperCase() + (i + 1) + '</text>';
  }

  svg.innerHTML = html;
}

export default {
  label: "Board Viewer",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _watchTokens = [];
    _robotPositions = {};
    _board = { nodes: [], edges: [] };
    _locations = {};

    container.innerHTML =
      '<div class="board-scroll">' +
        '<svg id="board-svg"></svg>' +
      '</div>';

    container.querySelector(".board-scroll").style.cssText =
      "overflow:auto;width:100%;height:100%;padding:1rem;";

    // Load board
    try {
      var boardKey = await ctx.kvManager.firstKeyWithPrefix("kb_export", ctx.site + ".boards.");
      var data = boardKey ? await ctx.kvManager.get("kb_export", boardKey) : null;
      if (data) {
        _board = data;
        buildLocations();
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
    for (var i = 0; i < _watchTokens.length; i++) _ctx.kvManager.release(_watchTokens[i]);
    _watchTokens = [];
    if (_renderTimer) { clearTimeout(_renderTimer); _renderTimer = null; }
    _container = null;
  }
};
