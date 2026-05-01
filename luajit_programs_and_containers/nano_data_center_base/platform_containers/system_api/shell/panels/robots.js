// robots.js — Full-screen panel: robot status cards + mission launcher.
// Data from NATS KV: robot_status + action_server buckets.

var _watchTokens = [];
var _renderTimer = null;
var _container = null;
var _ctx = null;

var robotState = {};
var robotList = [];
var robotConfigs = {};
var selectedRobot = null;
var selectedFrom = null;
var selectedTo = null;
var _boardName = null;

var BOARD_NODES = []; // just node names for dropdowns

function _key_prefix() { return _ctx.site + ".robots."; }
function _key_summary() { return _ctx.site + ".action_server.summary"; }

function robotIdFromKey(key) {
  var prefix = _key_prefix();
  if (key.indexOf(prefix) !== 0) return null;
  var rest = key.substring(prefix.length);
  var dot = rest.indexOf(".");
  return dot > 0 ? rest.substring(0, dot) : rest;
}

function ensureRobot(id) {
  if (!robotState[id]) {
    robotState[id] = {
      link_state: "offline", energy: 0, energy_max: 10000,
      worker: "-", bitmask_fields: {}, transport: "mqtt",
      wire_format: "json", connected: false, mission_state: "-"
    };
  }
  if (robotList.indexOf(id) < 0) {
    robotList.push(id);
    robotList.sort();
  }
}

function removeRobot(id) {
  var idx = robotList.indexOf(id);
  if (idx >= 0) robotList.splice(idx, 1);
  delete robotState[id];
  if (selectedRobot === id) selectedRobot = robotList.length > 0 ? robotList[0] : null;
}

function applyKvUpdate(key, value) {
  var id = robotIdFromKey(key);
  if (!id) return false;

  if (key.endsWith(".status.link")) {
    if (value.link_state === "offline") { removeRobot(id); return true; }
    ensureRobot(id);
    var st = robotState[id];
    st.link_state = value.link_state || "offline";
    st.transport = value.transport || st.transport;
    st.wire_format = value.wire_format || st.wire_format;
    if (value.energy_remaining !== undefined) st.energy = value.energy_remaining;
    return true;
  }

  ensureRobot(id);
  var st = robotState[id];

  if (key.endsWith(".status.energy")) {
    st.energy = value.energy_remaining || 0;
    st.energy_max = value.energy_max || 10000;
  } else if (key.endsWith(".status.bitmask")) {
    st.worker = value.kb_name || "-";
    st.bitmask_fields = value.fields || {};
  } else if (key.endsWith(".status.state")) {
    st.connected = !!value.connected;
    if (value.active_worker) st.worker = value.active_worker;
  }
  return true;
}

function applySummary(value) {
  var reg = value.registered_robots || [];
  var missions = value.missions || {};

  for (var rid in missions) {
    ensureRobot(rid);
    robotState[rid].mission_state = missions[rid].state || "-";
  }
  for (var i = 0; i < robotList.length; i++) {
    if (!missions[robotList[i]]) robotState[robotList[i]].mission_state = "-";
  }
  // Prune robots not in registered list
  if (reg.length > 0 || Object.keys(missions).length === 0) {
    for (var i = robotList.length - 1; i >= 0; i--) {
      if (reg.indexOf(robotList[i]) < 0) removeRobot(robotList[i]);
    }
  }
}

function scheduleRender() {
  if (!_renderTimer) {
    _renderTimer = setTimeout(function () {
      _renderTimer = null;
      render();
    }, 250);
  }
}

function selectRobot(name) {
  selectedRobot = name;
  var st = robotState[name];
  if (st) selectedFrom = null;
  render();
  setStatus(name + " selected. Pick a destination.");
}

function setStatus(msg) {
  var el = _container.querySelector("#mission-status");
  if (el) el.textContent = msg;
}

function render() {
  renderCards();
  renderSelects();
}

function renderCards() {
  var el = _container.querySelector("#robot-cards");
  if (!el) return;
  el.innerHTML = "";

  if (robotList.length === 0) {
    el.innerHTML = '<div style="color:#484f58;font-size:0.8rem;padding:2rem;text-align:center">No robots discovered yet</div>';
    return;
  }

  for (var i = 0; i < robotList.length; i++) {
    var id = robotList[i];
    var st = robotState[id];
    var cfg = robotConfigs[id] || {};
    var energyPct = st.energy_max > 0 ? Math.round(st.energy / st.energy_max * 100) : 0;
    var energyClass = energyPct > 60 ? "high" : energyPct > 25 ? "mid" : "low";
    var isSelected = selectedRobot === id;
    var statusLabel = st.link_state;
    if (st.mission_state === "active" || st.mission_state === "executing") statusLabel = "executing";

    var card = document.createElement("div");
    card.className = "robot-card" + (isSelected ? " selected" : "");
    card.onclick = (function (rid) { return function () { selectRobot(rid); }; })(id);

    var bitmaskStr = "";
    if (st.bitmask_fields && Object.keys(st.bitmask_fields).length > 0) {
      var parts = [];
      for (var f in st.bitmask_fields) parts.push(f + "=" + (st.bitmask_fields[f] ? "1" : "0"));
      bitmaskStr = parts.join(", ");
    }

    card.innerHTML =
      '<div class="card-header">' +
        '<span class="robot-name">' + id + '</span>' +
        '<span class="status-badge ' + statusLabel + '">' + statusLabel + '</span>' +
      '</div>' +
      '<div class="card-body">' +
        '<span class="label">Transport</span><span class="value"><span class="badge">' + st.transport + '</span> <span class="badge">' + st.wire_format + '</span></span>' +
        '<span class="label">Worker</span><span class="value">' + st.worker + '</span>' +
        '<span class="label">Mission</span><span class="value">' + st.mission_state + '</span>' +
        '<span class="label">Energy</span><span class="value">' + st.energy + ' / ' + st.energy_max + ' (' + energyPct + '%)</span>' +
        (bitmaskStr ? '<span class="label">Bitmask</span><span class="value" style="font-size:0.7rem">' + bitmaskStr + '</span>' : '') +
      '</div>' +
      '<div class="energy-bar"><div class="fill ' + energyClass + '" style="width:' + energyPct + '%"></div></div>' +
      (cfg.capabilities ? '<div class="caps">' + cfg.capabilities.join(", ") + '</div>' : '');

    el.appendChild(card);
  }
}

function renderSelects() {
  var selRobot = _container.querySelector("#sel-robot");
  var selFrom = _container.querySelector("#sel-from");
  var selTo = _container.querySelector("#sel-to");
  if (!selRobot) return;

  selRobot.innerHTML = "";
  for (var i = 0; i < robotList.length; i++) {
    var opt = document.createElement("option");
    opt.value = robotList[i]; opt.textContent = robotList[i];
    if (robotList[i] === selectedRobot) opt.selected = true;
    selRobot.appendChild(opt);
  }

  selFrom.innerHTML = "";
  selTo.innerHTML = "";
  for (var i = 0; i < BOARD_NODES.length; i++) {
    var n = BOARD_NODES[i];
    var optF = document.createElement("option");
    optF.value = n; optF.textContent = n;
    if (n === selectedFrom) optF.selected = true;
    selFrom.appendChild(optF);
    var optT = document.createElement("option");
    optT.value = n; optT.textContent = n;
    if (n === selectedTo) optT.selected = true;
    selTo.appendChild(optT);
  }
}

async function launchMission() {
  if (!selectedRobot || !selectedFrom || !selectedTo) { setStatus("Select robot, origin, and destination."); return; }
  if (selectedFrom === selectedTo) { setStatus("Origin and destination must differ."); return; }
  if (!_ctx.nc) { setStatus("NATS not connected."); return; }

  var mission = {
    robot_id: selectedRobot, board: _boardName,
    stops: [{ node: selectedFrom }, { node: selectedTo }]  };
  try {
    _ctx.nc.publish(_ctx.site + ".action_server.missions",
      new TextEncoder().encode(JSON.stringify(mission)));
    setStatus("Mission submitted: " + selectedRobot + " " + selectedFrom + " \u2192 " + selectedTo);
  } catch (e) { setStatus("Publish failed: " + e.message); }
}

function stopMission() { setStatus("Stop not yet wired."); }
function sendIdle() { setStatus("Park not yet wired."); }

function sendRecharge() {
  if (!selectedRobot || !_ctx.nc) return;
  var mission = {
    robot_id: selectedRobot, board: _boardName,
    stops: [{ node: "charging_station", action: "recharge", params: { target_energy: 0 } }],
    bookend: true
  };
  try {
    _ctx.nc.publish(_ctx.site + ".action_server.missions",
      new TextEncoder().encode(JSON.stringify(mission)));
    setStatus(selectedRobot + " recharge mission submitted.");
  } catch (e) { setStatus("Publish failed: " + e.message); }
}

export default {
  label: "Robots",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _watchTokens = [];
    robotState = {};
    robotList = [];
    selectedRobot = null;
    selectedFrom = null;
    selectedTo = null;

    container.innerHTML =
      '<div class="panel-robots">' +
        '<div class="section-title">Robots</div>' +
        '<div class="robot-cards" id="robot-cards">' +
          '<div style="color:#484f58;font-size:0.8rem;padding:2rem;text-align:center">Connecting to NATS KV...</div>' +
        '</div>' +
      '</div>';

    // Load robot configs from HTTP API (capabilities)
    try {
      var resp = await fetch("/api/robots?domain=" + encodeURIComponent(ctx.domain ? ctx.domain.name : "surface_ops"));
      var data = await resp.json();
      var list = Array.isArray(data.robots) ? data.robots : Object.values(data.robots || {});
      for (var i = 0; i < list.length; i++) robotConfigs[list[i].name] = list[i];
    } catch (e) { /* API unavailable */ }

    // Load board node names for dropdowns
    try {
      var boardKey = await ctx.kvManager.firstKeyWithPrefix("kb_export", ctx.site + ".boards.");
      if (boardKey) _boardName = boardKey.split(".").pop();
      var board = boardKey ? await ctx.kvManager.get("kb_export", boardKey) : null;
      if (board && board.nodes) {
        BOARD_NODES = board.nodes.map(function (n) { return n.name; });
      }
    } catch (e) { /* no board */ }

    if (!ctx.nc) {
      _container.querySelector("#robot-cards").innerHTML =
        '<div style="color:#f85149;font-size:0.8rem;padding:2rem;text-align:center">NATS not connected</div>';
      return;
    }

    var bucket = ctx.siteBucket;

    // Load + watch robot_status
    try {
      await ctx.kvManager.loadAll(bucket + "_robot_status", function (key, val) {
        applyKvUpdate(key, val);
      });
      var t1 = await ctx.kvManager.watch(bucket + "_robot_status", function (key, val) {
        applyKvUpdate(key, val);
        scheduleRender();
      });
      _watchTokens.push(t1);
    } catch (e) { /* bucket may not exist yet */ }

    // Load + watch action_server (summary + per-robot status)
    try {
      var summaryKey = ctx.site + ".action_server.summary";
      var summaryVal = await ctx.kvManager.get(bucket + "_action_server", summaryKey);
      if (summaryVal) applySummary(summaryVal);

      var t2 = await ctx.kvManager.watch(bucket + "_action_server", function (key, val) {
        if (key.endsWith(".summary")) { applySummary(val); scheduleRender(); }
        var match = key.match(/\.action_server\.(\w+)\.status$/);
        if (match) {
          ensureRobot(match[1]);
          robotState[match[1]].mission_state = val.state || "-";
          scheduleRender();
        }
      });
      _watchTokens.push(t2);
    } catch (e) { /* bucket may not exist yet */ }

    // Merge HTTP config into discovered robots
    for (var i = 0; i < robotList.length; i++) {
      var cfg = robotConfigs[robotList[i]];
      if (cfg) {
        if (cfg.transport) robotState[robotList[i]].transport = cfg.transport;
        if (cfg.wire_format) robotState[robotList[i]].wire_format = cfg.wire_format;
      }
    }

    if (robotList.length > 0) selectedRobot = robotList[0];
    render();
  },

  destroy() {
    for (var i = 0; i < _watchTokens.length; i++) {
      _ctx.kvManager.release(_watchTokens[i]);
    }
    _watchTokens = [];
    if (_renderTimer) { clearTimeout(_renderTimer); _renderTimer = null; }
    _container = null;
  }
};
