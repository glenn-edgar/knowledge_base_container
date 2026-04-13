// mission-log.js — Full-screen panel: last 50 missions from KV history.
// Each row is expandable to show the full action breakdown with the
// failed action highlighted in red and the fault reason banner.

var _watchTokens = [];
var _container = null;
var _ctx = null;
var _missions = [];        // ordered as loaded (oldest first)
var _expanded = {};        // missionKey → bool

function formatTime(ts) {
  if (!ts) return "-";
  try { return new Date(ts).toLocaleTimeString("en-US", { hour12: false }); }
  catch (e) { return ts; }
}

// Stable identifier for an entry so expansion state survives re-renders.
function missionKey(m) {
  return (m.timestamp || "?") + "|" + (m.robot_id || "?");
}

// Backwards compat: older entries had `fault` as a plain string. New entries
// have an object {reason, detail, action_index, kb_name}. Normalize.
function faultObj(m) {
  if (!m.fault) {
    // Some old entries used `fault_reason` field at top level
    if (m.fault_reason) return { reason: m.fault_reason };
    return null;
  }
  if (typeof m.fault === "string") return { reason: m.fault };
  return m.fault;
}

function faultText(m) {
  var f = faultObj(m);
  if (!f) return "-";
  if (f.detail) return f.reason + ": " + f.detail;
  return f.reason || "-";
}

function renderTable() {
  var tbody = _container.querySelector("#log-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (_missions.length === 0) {
    tbody.innerHTML = '<tr><td colspan="8" style="color:#484f58;text-align:center;padding:2rem">No mission history yet</td></tr>';
    return;
  }

  // Newest first
  for (var i = _missions.length - 1; i >= 0; i--) {
    var m = _missions[i];
    appendRow(tbody, m, /*atEnd=*/true);
  }
}

function appendRow(tbody, m, atEnd) {
  var key = missionKey(m);
  var resultClass = m.success ? "success" : "failure";
  var resultText  = m.success ? "OK" : "FAIL";
  var elapsed     = m.elapsed_ms ? (m.elapsed_ms / 1000).toFixed(1) + "s" : "-";
  var isOpen      = !!_expanded[key];

  var tr = document.createElement("tr");
  tr.className = "log-row";
  tr.dataset.key = key;

  var tdToggle = document.createElement("td");
  tdToggle.className = "log-toggle" + (m.success ? "" : " fail");
  tdToggle.textContent = isOpen ? "▼" : "▶";
  tdToggle.title = "Show / hide actions";
  tdToggle.onclick = function () {
    _expanded[key] = !_expanded[key];
    renderTable();
  };
  tr.appendChild(tdToggle);

  function td(html, cls) {
    var c = document.createElement("td");
    if (cls) c.className = cls;
    c.innerHTML = html;
    return c;
  }
  tr.appendChild(td(formatTime(m.timestamp)));
  tr.appendChild(td(m.robot_id || "-", "log-robot"));
  tr.appendChild(td(m.board || "-"));
  tr.appendChild(td(resultText, resultClass));
  tr.appendChild(td((m.completed || 0) + "/" + (m.total || 0)));
  tr.appendChild(td(elapsed));

  // Fault summary cell — use object if available
  var ft = faultText(m);
  tr.appendChild(td(escapeHtml(ft)));

  if (atEnd) tbody.appendChild(tr);
  else tbody.insertBefore(tr, tbody.firstChild);

  if (isOpen) {
    var trEx = document.createElement("tr");
    trEx.className = "log-expanded";
    var tdEx = document.createElement("td");
    tdEx.colSpan = 8;
    tdEx.appendChild(buildExpansion(m));
    trEx.appendChild(tdEx);
    if (atEnd) tbody.appendChild(trEx);
    else tbody.insertBefore(trEx, tr.nextSibling);
  }
}

function escapeHtml(s) {
  if (s == null) return "";
  return String(s).replace(/[&<>"']/g, function (c) {
    return ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c];
  });
}

function buildExpansion(m) {
  var wrap = document.createElement("div");
  var f = faultObj(m);

  // Fault banner (mission-level reason)
  if (f) {
    var banner = document.createElement("div");
    banner.className = "preflight-fail-banner";
    var msg = "❌ " + (f.reason || "failed");
    if (f.detail) msg += ": " + f.detail;
    if (f.kb_name && f.action_index) {
      msg += "  (action " + f.action_index + " — " + f.kb_name + ")";
    }
    banner.textContent = msg;
    wrap.appendChild(banner);
  }

  // Energy detail (insufficient_energy failures)
  if (m.energy_required !== undefined && m.energy_required !== null) {
    var eb = document.createElement("div");
    eb.className = "preflight-fail-banner";
    eb.textContent = "❌ insufficient energy: needed " + m.energy_required +
      ", had " + (m.energy_remaining !== undefined ? m.energy_remaining : "?");
    // Only show as fail banner if it actually failed for energy
    if (!f || f.reason !== "insufficient_energy") {
      eb.className = "";
      eb.style.cssText = "color:#8b949e;font-size:0.76rem;margin-bottom:0.4rem";
      eb.textContent = "energy: used " + m.energy_required + " / budget " +
        (m.energy_remaining !== undefined ? m.energy_remaining : "?");
    }
    wrap.appendChild(eb);
  }

  // Unsupported actions list (planning failures)
  if (m.unsupported && m.unsupported.length > 0) {
    var ub = document.createElement("div");
    ub.className = "preflight-fail-banner";
    ub.textContent = "❌ no matching virtual node: " + m.unsupported.join("; ");
    wrap.appendChild(ub);
  }

  // Action list — render route with failed action in red
  if (m.route && m.route.length > 0) {
    var faultIdx = (f && f.action_index) ? f.action_index : null;
    var list = document.createElement("div");
    list.className = "action-list";
    for (var i = 0; i < m.route.length; i++) {
      var a = m.route[i];
      var actionIndex = i + 1;  // 1-based, matches server's action_index
      var item = document.createElement("div");
      var failed = (faultIdx !== null && actionIndex === faultIdx);
      var skipped = (faultIdx !== null && actionIndex > faultIdx);
      item.className = "action-item" + (failed ? " fail" : "");
      if (skipped) item.style.opacity = "0.45";

      var num  = document.createElement("span"); num.className = "a-num";
      num.textContent = actionIndex + ".";
      var name = document.createElement("span"); name.className = "a-name";
      name.textContent = a.kb_name;
      var det  = document.createElement("span"); det.className = "a-detail";
      var d = a.detail || "";
      if (failed) d += "  ← failed";
      else if (skipped) d += "  (not run)";
      det.textContent = d;

      item.appendChild(num);
      item.appendChild(name);
      item.appendChild(det);
      list.appendChild(item);
    }
    wrap.appendChild(list);
  } else if (!f && !m.unsupported) {
    var none = document.createElement("div");
    none.style.cssText = "color:#666;font-size:0.76rem";
    none.textContent = "(no route data — pre-upgrade entry)";
    wrap.appendChild(none);
  }

  return wrap;
}

export default {
  label: "Mission Log",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _watchTokens = [];
    _missions = [];
    _expanded = {};

    container.innerHTML =
      '<div class="panel-mission-log">' +
        '<div class="section-title">Mission Log (Last 50)</div>' +
        '<div class="log-scroll">' +
          '<table class="log-table">' +
            '<thead><tr>' +
              '<th></th>' +
              '<th>Time</th><th>Robot</th><th>Board</th><th>Result</th><th>Actions</th><th>Elapsed</th><th>Fault</th>' +
            '</tr></thead>' +
            '<tbody id="log-body">' +
              '<tr><td colspan="8" style="color:#484f58;text-align:center;padding:2rem">Loading...</td></tr>' +
            '</tbody>' +
          '</table>' +
        '</div>' +
      '</div>';

    if (!ctx.nc) {
      renderTable();
      return;
    }

    // Load mission log history
    var bucket = ctx.siteBucket + "_mission_log";
    var key = ctx.site + ".action_server.mission_log";
    try {
      _missions = await ctx.kvManager.history(bucket, key) || [];
      renderTable();
    } catch (e) {
      console.warn("Mission log load:", e.message);
      _missions = [];
      renderTable();
    }

    // Watch for new entries
    var t = await ctx.kvManager.watch(bucket, function (k, val) {
      if (k === key && val) {
        _missions.push(val);
        if (_missions.length > 100) _missions.shift();
        renderTable();
      }
    });
    _watchTokens.push(t);
  },

  destroy() {
    for (var i = 0; i < _watchTokens.length; i++) _ctx.kvManager.release(_watchTokens[i]);
    _watchTokens = [];
    _missions = [];
    _expanded = {};
    _container = null;
  }
};
