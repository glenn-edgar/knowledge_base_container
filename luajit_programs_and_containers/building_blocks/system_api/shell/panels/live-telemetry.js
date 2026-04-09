// live-telemetry.js — Full-screen panel: JetStream telemetry event log.
// Subscribes to {site}.robots.*.stream.telemetry for real-time events.

var _container = null;
var _ctx = null;
var _sub = null;
var _eventCount = 0;
var _maxEvents = 500;

function typeClass(type) {
  if (type.indexOf("start") >= 0) return "start";
  if (type.indexOf("complete") >= 0) return "complete";
  if (type.indexOf("fail") >= 0) return "failed";
  if (type === "heartbeat") return "heartbeat";
  return "action";
}

function formatTime(ts) {
  if (!ts) return new Date().toLocaleTimeString("en-US", { hour12: false });
  try { return new Date(ts).toLocaleTimeString("en-US", { hour12: false }); }
  catch (e) { return ts; }
}

function addEvent(type, detail, ts) {
  var log = _container.querySelector("#event-log");
  if (!log) return;

  if (_eventCount === 0) log.innerHTML = "";
  _eventCount++;

  var countEl = _container.querySelector("#event-count");
  var lastEl = _container.querySelector("#last-event");
  if (countEl) countEl.textContent = _eventCount;
  if (lastEl) lastEl.textContent = type;

  var row = document.createElement("div");
  row.className = "event-row";
  row.innerHTML =
    '<span class="event-ts">' + formatTime(ts) + '</span>' +
    '<span class="event-type ' + typeClass(type) + '">' + type + '</span>' +
    '<span class="event-detail">' + (detail || "") + '</span>';

  log.insertBefore(row, log.firstChild);

  while (log.children.length > _maxEvents) log.removeChild(log.lastChild);
}

function formatEventDetail(data) {
  var parts = [];
  if (data.robot_id) parts.push(data.robot_id);
  if (data.kb_name) parts.push(data.kb_name);
  if (data.action_index !== undefined && data.total !== undefined)
    parts.push("[" + (data.action_index + 1) + "/" + data.total + "]");
  if (data.board) parts.push("on " + data.board);
  if (data.route_length) parts.push("(" + data.route_length + " actions)");
  if (data.reason) parts.push("reason=" + data.reason);
  if (data.success !== undefined) parts.push(data.success ? "OK" : "FAIL");
  if (data.completed !== undefined) parts.push(data.completed + "/" + (data.total || "?"));
  if (data.elapsed_ms) parts.push((data.elapsed_ms / 1000).toFixed(1) + "s");
  if (data.pose) {
    var p = data.pose;
    parts.push("x=" + (p.x || 0) + " y=" + (p.y || 0));
  }
  return parts.join(" ");
}

export default {
  label: "Live Telemetry",

  async init(container, ctx) {
    _container = container;
    _ctx = ctx;
    _sub = null;
    _eventCount = 0;

    container.innerHTML =
      '<div class="panel-telemetry">' +
        '<div class="telemetry-bar">' +
          '<span><span class="slabel">Stream:</span> <span class="svalue" id="stream-status">connecting...</span></span>' +
          '<span><span class="slabel">Events:</span> <span class="svalue" id="event-count">0</span></span>' +
          '<span><span class="slabel">Last:</span> <span class="svalue" id="last-event">-</span></span>' +
        '</div>' +
        '<div class="section-title">Live Event Stream</div>' +
        '<div class="event-log-scroll">' +
          '<div class="event-log" id="event-log">' +
            '<div style="color:#484f58;font-size:0.8rem;padding:2rem;text-align:center">Waiting for telemetry events...</div>' +
          '</div>' +
        '</div>' +
      '</div>';

    container.querySelector(".event-log-scroll").style.cssText = "overflow:auto;flex:1;";

    if (!ctx.nc) {
      _container.querySelector("#stream-status").textContent = "no NATS";
      return;
    }

    // Subscribe to telemetry stream
    var subject = ctx.site + ".robots.*.stream.telemetry";
    try {
      var js = ctx.js;
      _sub = ctx.nc.subscribe(subject);
      _container.querySelector("#stream-status").textContent = "live";

      (async function () {
        for await (var msg of _sub) {
          try {
            var data = JSON.parse(new TextDecoder().decode(msg.data));
            var type = data.type || data.event || "unknown";
            addEvent(type, formatEventDetail(data), data.timestamp);
          } catch (e) { /* skip */ }
        }
      })();
    } catch (e) {
      console.warn("Telemetry subscribe:", e.message);
      _container.querySelector("#stream-status").textContent = "error";
    }
  },

  destroy() {
    if (_sub) {
      _sub.unsubscribe();
      _sub = null;
    }
    _container = null;
  }
};
