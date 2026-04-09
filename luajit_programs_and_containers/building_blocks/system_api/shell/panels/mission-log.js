// mission-log.js — Full-screen panel: last 50 missions from KV history.

var _watchTokens = [];
var _container = null;
var _ctx = null;

function formatTime(ts) {
  if (!ts) return "-";
  try { return new Date(ts).toLocaleTimeString("en-US", { hour12: false }); }
  catch (e) { return ts; }
}

function renderTable(missions) {
  var tbody = _container.querySelector("#log-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  if (missions.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" style="color:#484f58;text-align:center;padding:2rem">No mission history yet</td></tr>';
    return;
  }

  for (var i = missions.length - 1; i >= 0; i--) {
    var m = missions[i];
    var tr = document.createElement("tr");
    var resultClass = m.success ? "success" : "failure";
    var resultText = m.success ? "OK" : "FAIL";
    var elapsed = m.elapsed_ms ? (m.elapsed_ms / 1000).toFixed(1) + "s" : "-";

    tr.innerHTML =
      '<td>' + formatTime(m.timestamp) + '</td>' +
      '<td style="color:#64ffda">' + (m.robot_id || "-") + '</td>' +
      '<td>' + (m.board || "-") + '</td>' +
      '<td class="' + resultClass + '">' + resultText + '</td>' +
      '<td>' + (m.completed || 0) + '/' + (m.total || 0) + '</td>' +
      '<td>' + elapsed + '</td>' +
      '<td>' + (m.fault || "-") + '</td>';
    tbody.appendChild(tr);
  }
}

export default {
  label: "Mission Log",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _watchTokens = [];

    container.innerHTML =
      '<div class="panel-mission-log">' +
        '<div class="section-title">Mission Log (Last 50)</div>' +
        '<div class="log-scroll">' +
          '<table class="log-table">' +
            '<thead><tr>' +
              '<th>Time</th><th>Robot</th><th>Board</th><th>Result</th><th>Actions</th><th>Elapsed</th><th>Fault</th>' +
            '</tr></thead>' +
            '<tbody id="log-body">' +
              '<tr><td colspan="7" style="color:#484f58;text-align:center;padding:2rem">Loading...</td></tr>' +
            '</tbody>' +
          '</table>' +
        '</div>' +
      '</div>';

    container.querySelector(".log-scroll").style.cssText = "overflow:auto;flex:1;";

    if (!ctx.nc) {
      renderTable([]);
      return;
    }

    // Load mission log history
    var bucket = ctx.siteBucket + "_mission_log";
    var key = ctx.site + ".action_server.mission_log";
    try {
      var missions = await ctx.kvManager.history(bucket, key);
      renderTable(missions);
    } catch (e) {
      console.warn("Mission log load:", e.message);
      renderTable([]);
    }

    // Watch for new entries
    var t = await ctx.kvManager.watch(bucket, function (k, val) {
      if (k === key) {
        // Append to table
        var tbody = _container.querySelector("#log-body");
        if (!tbody) return;
        // Remove "no history" placeholder
        var placeholder = tbody.querySelector("td[colspan]");
        if (placeholder) tbody.innerHTML = "";

        var tr = document.createElement("tr");
        var resultClass = val.success ? "success" : "failure";
        var elapsed = val.elapsed_ms ? (val.elapsed_ms / 1000).toFixed(1) + "s" : "-";
        tr.innerHTML =
          '<td>' + formatTime(val.timestamp) + '</td>' +
          '<td style="color:#64ffda">' + (val.robot_id || "-") + '</td>' +
          '<td>' + (val.board || "-") + '</td>' +
          '<td class="' + resultClass + '">' + (val.success ? "OK" : "FAIL") + '</td>' +
          '<td>' + (val.completed || 0) + '/' + (val.total || 0) + '</td>' +
          '<td>' + elapsed + '</td>' +
          '<td>' + (val.fault || "-") + '</td>';
        tbody.insertBefore(tr, tbody.firstChild);
      }
    });
    _watchTokens.push(t);
  },

  destroy() {
    for (var i = 0; i < _watchTokens.length; i++) _ctx.kvManager.release(_watchTokens[i]);
    _watchTokens = [];
    _container = null;
  }
};
