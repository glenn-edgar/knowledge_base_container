// mission-planner.js — Full-screen panel: mission route editor.
// Build multi-stop routes by clicking board nodes. Each node IS a VN.
// Co-located VNs (same x,y) shown as cluster icon with popup picker.

var _container = null;
var _ctx = null;
var _watchTokens = [];

// Data
var _board = { nodes: [], edges: [] };
var _boardName = null;
var _locations = {};   // "x,y" -> [node, node, ...]
var _registeredRobots = [];
var _robotPositions = {};  // robot_id → current_node
var _selectedRobot = null;
var _startNode = null;
var _bookend = true;

// Steps: [{ node }]
var _steps = [];
var _radioIdx = -1;

// Popup picker state
var _pickerLocation = null; // "x,y" key of open picker, or null

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------
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

// ---------------------------------------------------------------
// Render: step table
// ---------------------------------------------------------------
function renderSteps() {
  var tbody = _container.querySelector("#steps-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (_steps.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="color:#484f58;text-align:center;padding:1.5rem">Click a board node to add mission steps</td></tr>';
    return;
  }

  for (var i = 0; i < _steps.length; i++) {
    var s = _steps[i];
    var tr = document.createElement("tr");
    if (i === _radioIdx) tr.className = "step-selected";

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

    // Destination
    var tdDest = document.createElement("td");
    tdDest.className = "step-dest";
    tdDest.textContent = s.node.replace(/_/g, " ");
    tr.appendChild(tdDest);

    tbody.appendChild(tr);
  }
}

// ---------------------------------------------------------------
// Render: board SVG (embedded, clickable)
// ---------------------------------------------------------------
function renderBoard() {
  var svg = _container.querySelector("#planner-board-svg");
  if (!svg || _board.nodes.length === 0) return;

  // Close picker on re-render
  _pickerLocation = null;

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

  // Build route node list for path highlighting
  var routeNodes = [_startNode];
  for (var i = 0; i < _steps.length; i++) routeNodes.push(_steps[i].node);
  var routeEdges = {};
  for (var i = 0; i < routeNodes.length - 1; i++) {
    var a = routeNodes[i], b = routeNodes[i + 1];
    routeEdges[a + ":" + b] = true;
    routeEdges[b + ":" + a] = true;
  }

  // Edges (skip zero-length edges between co-located nodes)
  for (var i = 0; i < _board.edges.length; i++) {
    var e = _board.edges[i];
    var a = nodeByName(e.from);
    var b = nodeByName(e.to);
    if (!a || !b) continue;
    if (a.x === b.x && a.y === b.y) continue; // skip zero-length
    var isRoute = routeEdges[e.from + ":" + e.to] || routeEdges[e.to + ":" + e.from];
    var cls = "edge" + (isRoute ? " path-active" : "");
    html += '<line class="' + cls + '" x1="' + a.x + '" y1="' + a.y + '" x2="' + b.x + '" y2="' + b.y + '"/>';
    var mx = (a.x + b.x) / 2;
    var my = (a.y + b.y) / 2;
    html += '<text class="edge-label" x="' + mx + '" y="' + (my - 10) + '">' + e.nav + ' ' + e.speed + '</text>';
  }

  // Render nodes grouped by location
  var selectedDest = (_radioIdx >= 0 && _radioIdx < _steps.length) ? _steps[_radioIdx].node : null;
  var rendered = {}; // track which locations we've rendered

  for (var i = 0; i < _board.nodes.length; i++) {
    var n = _board.nodes[i];
    var lk = locationKey(n);
    if (rendered[lk]) continue;
    rendered[lk] = true;

    var group = _locations[lk] || [n];

    if (group.length === 1) {
      // Single node: normal circle
      var cls = "node-circle " + (n.type || "waypoint");
      if (n.name === selectedDest) cls += " selected";
      html += '<circle class="' + cls + '" cx="' + n.x + '" cy="' + n.y + '" r="30" ' +
        'data-node="' + n.name + '" style="cursor:pointer"/>';
      html += '<text class="node-label" x="' + n.x + '" y="' + (n.y + 50) + '">' +
        n.name.replace(/_/g, " ") + '</text>';
    } else {
      // Multi-VN location: diamond icon with count
      var isSelected = group.some(function (g) { return g.name === selectedDest; });
      var cls = "node-cluster" + (isSelected ? " selected" : "");
      var cx = n.x, cy = n.y, r = 45;
      html += '<polygon class="' + cls + '" ' +
        'points="' + cx + ',' + (cy - r) + ' ' + (cx + r) + ',' + cy + ' ' + cx + ',' + (cy + r) + ' ' + (cx - r) + ',' + cy + '" ' +
        'data-location="' + lk + '" style="cursor:pointer"/>';
      html += '<text class="cluster-count" x="' + cx + '" y="' + (cy + 4) + '">' + group.length + '</text>';
      // Label: use common prefix or first node name
      var label = group[0].name.replace(/_/g, " ");
      html += '<text class="node-label" x="' + cx + '" y="' + (cy + 52) + '">' + label + '...</text>';
    }
  }

  // Start marker
  var startN = nodeByName(_startNode);
  if (startN) {
    html += '<circle class="robot-marker" cx="' + startN.x + '" cy="' + (startN.y - 25) + '" r="10"/>';
    html += '<text fill="#1a1a2e" font-size="8" font-weight="bold" text-anchor="middle" ' +
      'x="' + startN.x + '" y="' + (startN.y - 22) + '">S</text>';
  }

  svg.innerHTML = html;

  // Attach click handlers
  // Single nodes
  var circles = svg.querySelectorAll(".node-circle");
  for (var i = 0; i < circles.length; i++) {
    circles[i].addEventListener("click", function (e) {
      e.stopPropagation();
      closePicker();
      addStep(this.getAttribute("data-node"));
    });
  }

  // Cluster icons (multi-VN)
  var clusters = svg.querySelectorAll(".node-cluster");
  for (var i = 0; i < clusters.length; i++) {
    clusters[i].addEventListener("click", function (e) {
      e.stopPropagation();
      var lk = this.getAttribute("data-location");
      if (_pickerLocation === lk) { closePicker(); return; }
      showPicker(lk);
    });
  }
}

// ---------------------------------------------------------------
// VN Picker popup (for co-located nodes)
// ---------------------------------------------------------------
function showPicker(lk) {
  closePicker();
  _pickerLocation = lk;

  var group = _locations[lk];
  if (!group || group.length === 0) return;

  var picker = document.createElement("div");
  picker.className = "node-picker";
  picker.id = "vn-picker";

  var title = document.createElement("div");
  title.className = "picker-title";
  title.textContent = "Select action:";
  picker.appendChild(title);

  for (var i = 0; i < group.length; i++) {
    var item = document.createElement("div");
    item.className = "picker-item";
    item.textContent = group[i].name.replace(/_/g, " ");
    item.dataset.node = group[i].name;
    item.addEventListener("click", function () {
      addStep(this.dataset.node);
      closePicker();
    });
    picker.appendChild(item);
  }

  var cancel = document.createElement("div");
  cancel.className = "picker-cancel";
  cancel.textContent = "Cancel";
  cancel.addEventListener("click", function () { closePicker(); });
  picker.appendChild(cancel);

  // Position near the cluster
  var coords = lk.split(",");
  var svg = _container.querySelector("#planner-board-svg");
  var scrollBox = _container.querySelector(".planner-board-scroll");
  if (svg && scrollBox) {
    // Convert SVG coordinates to container coordinates
    var svgRect = svg.getBoundingClientRect();
    var scrollRect = scrollBox.getBoundingClientRect();
    var viewBox = svg.getAttribute("viewBox").split(" ").map(Number);
    var scaleX = svgRect.width / viewBox[2];
    var scaleY = svgRect.height / viewBox[3];
    var px = (Number(coords[0]) - viewBox[0]) * scaleX + svgRect.left - scrollRect.left + scrollBox.scrollLeft;
    var py = (Number(coords[1]) - viewBox[1]) * scaleY + svgRect.top - scrollRect.top + scrollBox.scrollTop;
    picker.style.left = (px + 40) + "px";
    picker.style.top = (py - 20) + "px";
  }

  scrollBox.appendChild(picker);
}

function closePicker() {
  _pickerLocation = null;
  var existing = document.getElementById("vn-picker");
  if (existing) existing.remove();
}

// ---------------------------------------------------------------
// Step operations
// ---------------------------------------------------------------
function addStep(nodeName) {
  var step = { node: nodeName };
  if (_radioIdx >= 0 && _radioIdx < _steps.length) {
    _steps.splice(_radioIdx + 1, 0, step);
    _radioIdx = _radioIdx + 1;
  } else {
    _steps.push(step);
    _radioIdx = _steps.length - 1;
  }
  renderSteps();
  renderBoard();
}

function moveUp() {
  if (_radioIdx <= 0) return;
  var tmp = _steps[_radioIdx];
  _steps[_radioIdx] = _steps[_radioIdx - 1];
  _steps[_radioIdx - 1] = tmp;
  _radioIdx--;
  renderSteps();
  renderBoard();
}

function moveDown() {
  if (_radioIdx < 0 || _radioIdx >= _steps.length - 1) return;
  var tmp = _steps[_radioIdx];
  _steps[_radioIdx] = _steps[_radioIdx + 1];
  _steps[_radioIdx + 1] = tmp;
  _radioIdx++;
  renderSteps();
  renderBoard();
}

function deleteStep() {
  if (_radioIdx >= 0 && _radioIdx < _steps.length) {
    _steps.splice(_radioIdx, 1);
    if (_radioIdx >= _steps.length) _radioIdx = _steps.length - 1;
  }
  renderSteps();
  renderBoard();
}

function clearAll() {
  if (_steps.length === 0) return;
  if (!confirm("Clear all mission steps?")) return;
  _steps = [];
  _radioIdx = -1;
  renderSteps();
  renderBoard();
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
  var stops = _steps.map(function (s) { return { node: s.node }; });

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
    renderSteps();
    renderBoard();
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
  renderBoard();
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
    _registeredRobots = [];
    _selectedRobot = null;
    _startNode = null;
    _robotPositions = {};
    _bookend = true;
    _board = { nodes: [], edges: [] };
    _locations = {};
    _pickerLocation = null;

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
        '<div class="steps-scroll">' +
          '<table class="steps-table">' +
            '<thead><tr><th>#</th><th></th><th>Destination</th></tr></thead>' +
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

    // Click anywhere on board scroll area closes picker
    container.querySelector(".planner-board-scroll").addEventListener("click", function (e) {
      if (!e.target.closest(".node-picker") && !e.target.closest(".node-cluster")) {
        closePicker();
      }
    });

    if (ctx.kvManager) {
      // Load board
      try {
        var boardKey = await ctx.kvManager.firstKeyWithPrefix("kb_export", ctx.site + ".boards.");
        if (boardKey) _boardName = boardKey.split(".").pop();
        var data = boardKey ? await ctx.kvManager.get("kb_export", boardKey) : null;
        if (data) {
          _board = data;
          buildLocations();
          var boardName = _boardName;
          container.querySelector("#board-label").textContent =
            boardName + " (" + _board.nodes.length + " nodes)";

        }
      } catch (e) { /* no board */ }

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
    renderSteps();
    renderBoard();

    // Ensure start node is set from position data (may have loaded after dropdown populated)
    if (_selectedRobot) onRobotChange(_selectedRobot);
  },

  destroy() {
    closePicker();
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
