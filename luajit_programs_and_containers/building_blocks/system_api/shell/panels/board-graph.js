// board-graph.js — Shared cluster + inset rendering for board views.
//
// Both mission-planner.js and board.js use this to:
//   1. Cluster nodes whose drawn circles overlap (distance < 2*radius)
//   2. Render the main board with diamond markers for multi-node clusters
//      and aggregated external edges between clusters
//   3. Build a popup "subgraph inset" that drills into a cluster, showing
//      its internal edges and stub edges to external neighbours
//
// No DOM dependencies in the cluster math; only buildInsetSvg touches the DOM.

// ---------------------------------------------------------------
// Cluster computation (union-find on nodes within 2*radius)
// ---------------------------------------------------------------

function dist(a, b) {
  var dx = a.x - b.x, dy = a.y - b.y;
  return Math.sqrt(dx * dx + dy * dy);
}

// Union-find
function makeDsu(n) {
  var parent = new Array(n);
  var rank = new Array(n);
  for (var i = 0; i < n; i++) { parent[i] = i; rank[i] = 0; }
  function find(x) {
    while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; }
    return x;
  }
  function union(x, y) {
    var rx = find(x), ry = find(y);
    if (rx === ry) return;
    if (rank[rx] < rank[ry]) { parent[rx] = ry; }
    else if (rank[rx] > rank[ry]) { parent[ry] = rx; }
    else { parent[ry] = rx; rank[rx]++; }
  }
  return { find: find, union: union };
}

// Compute clusters from a node array. Two nodes are in the same cluster if
// their drawn circles (radius `r`) overlap, i.e. centre distance < 2*r.
//
// Returns:
//   {
//     clusters:      [{ id, members:[node...], cx, cy, bbox, isSingleton }]
//     byId:          { id → cluster }
//     nodeToCluster: { nodeName → id }
//   }
//
// Cluster ids are derived from sorted member names so they are stable across
// re-renders even if the input node array is reordered.
export function computeClusters(nodes, radius) {
  var threshold = 2 * radius;
  var n = nodes.length;
  var dsu = makeDsu(n);
  for (var i = 0; i < n; i++) {
    for (var j = i + 1; j < n; j++) {
      if (dist(nodes[i], nodes[j]) < threshold) dsu.union(i, j);
    }
  }

  var groups = {};
  for (var i = 0; i < n; i++) {
    var root = dsu.find(i);
    if (!groups[root]) groups[root] = [];
    groups[root].push(nodes[i]);
  }

  var clusters = [];
  var byId = {};
  var nodeToCluster = {};

  for (var root in groups) {
    var members = groups[root].slice();
    members.sort(function (a, b) { return a.name < b.name ? -1 : a.name > b.name ? 1 : 0; });
    var id = members.map(function (m) { return m.name; }).join("|");

    var sumX = 0, sumY = 0;
    var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (var k = 0; k < members.length; k++) {
      var m = members[k];
      sumX += m.x; sumY += m.y;
      if (m.x < minX) minX = m.x;
      if (m.y < minY) minY = m.y;
      if (m.x > maxX) maxX = m.x;
      if (m.y > maxY) maxY = m.y;
    }

    var cluster = {
      id: id,
      members: members,
      cx: sumX / members.length,
      cy: sumY / members.length,
      bbox: { minX: minX, minY: minY, maxX: maxX, maxY: maxY },
      isSingleton: (members.length === 1),
    };
    clusters.push(cluster);
    byId[id] = cluster;
    for (var k = 0; k < members.length; k++) {
      nodeToCluster[members[k].name] = id;
    }
  }

  return { clusters: clusters, byId: byId, nodeToCluster: nodeToCluster };
}

// ---------------------------------------------------------------
// Edge bucketing
// ---------------------------------------------------------------
//
// Splits edges into:
//   internal: { clusterId: [edge, ...] }   (both endpoints in same cluster)
//   external: [{ a, b, edges:[...] }]      (a, b are clusterIds, alphabetical)
//
// Edges with an unknown endpoint are dropped.
export function bucketEdges(edges, nodeToCluster) {
  var internal = {};
  var externalMap = {}; // "a||b" → { a, b, edges }

  for (var i = 0; i < edges.length; i++) {
    var e = edges[i];
    var ca = nodeToCluster[e.from];
    var cb = nodeToCluster[e.to];
    if (!ca || !cb) continue;
    if (ca === cb) {
      if (!internal[ca]) internal[ca] = [];
      internal[ca].push(e);
    } else {
      var lo = ca < cb ? ca : cb;
      var hi = ca < cb ? cb : ca;
      var k = lo + "\u0001" + hi;
      if (!externalMap[k]) externalMap[k] = { a: lo, b: hi, edges: [] };
      externalMap[k].edges.push(e);
    }
  }

  var external = [];
  for (var k in externalMap) external.push(externalMap[k]);
  return { internal: internal, external: external };
}

// Filter external buckets that touch a given cluster, returning per-bucket
// info paired with the "other" cluster id.
export function externalForCluster(externalBuckets, clusterId) {
  var out = [];
  for (var i = 0; i < externalBuckets.length; i++) {
    var b = externalBuckets[i];
    if (b.a === clusterId) out.push({ other: b.b, edges: b.edges });
    else if (b.b === clusterId) out.push({ other: b.a, edges: b.edges });
  }
  return out;
}

// Display label for a cluster — used as a short name for stub-edge text.
export function clusterLabel(cluster) {
  if (cluster.isSingleton) return cluster.members[0].name.replace(/_/g, " ");
  var first = cluster.members[0].name.replace(/_/g, " ");
  return first + " +" + (cluster.members.length - 1);
}

// ---------------------------------------------------------------
// Inset builder (DOM)
// ---------------------------------------------------------------
//
// Returns an absolutely-positionable HTML wrapper containing an SVG that
// draws the cluster's members, internal edges, and short stub edges
// pointing toward each external neighbour.
//
// opts:
//   cluster              — the cluster to render
//   internalEdges        — edges internal to this cluster (may be undefined)
//   externalNeighbours   — output of externalForCluster(buckets, cluster.id)
//   byId                 — { id → cluster } so stubs can find neighbour positions
//   nodeRadius           — radius used for member node circles
//   interactive          — bool: clickable members emit a "node-select" event
//   onClose              — callback fired when the close button is clicked
//
// The wrapper element dispatches a CustomEvent("node-select", { detail: { nodeName } })
// when interactive is true and a member is clicked. Caller listens for it.
export function buildInsetSvg(opts) {
  var cluster = opts.cluster;
  var internalEdges = opts.internalEdges || [];
  var externals = opts.externalNeighbours || [];
  var byId = opts.byId || {};
  var nodeRadius = opts.nodeRadius || 30;
  var interactive = !!opts.interactive;
  var onClose = opts.onClose || function () { };

  // Wrapper
  var wrap = document.createElement("div");
  wrap.className = "board-inset";

  // Header strip
  var hdr = document.createElement("div");
  hdr.className = "board-inset-header";
  var title = document.createElement("span");
  title.className = "inset-title";
  title.textContent = cluster.members.length + " co-located nodes";
  hdr.appendChild(title);
  var close = document.createElement("button");
  close.className = "inset-close";
  close.type = "button";
  close.textContent = "\u00D7";
  close.title = "Close";
  close.addEventListener("click", function (e) {
    e.stopPropagation();
    onClose();
  });
  hdr.appendChild(close);
  wrap.appendChild(hdr);

  // SVG
  var svgNS = "http://www.w3.org/2000/svg";
  var svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("class", "inset-svg");

  // viewBox: cluster bbox + padding (extra room for stub edges around the perimeter)
  var pad = nodeRadius * 3.5;
  var bb = cluster.bbox;
  var w = (bb.maxX - bb.minX) + pad * 2;
  var h = (bb.maxY - bb.minY) + pad * 2;
  svg.setAttribute("viewBox", (bb.minX - pad) + " " + (bb.minY - pad) + " " + w + " " + h);

  // Internal edges
  for (var i = 0; i < internalEdges.length; i++) {
    var e = internalEdges[i];
    var a = nodeFromCluster(cluster, e.from);
    var b = nodeFromCluster(cluster, e.to);
    if (!a || !b) continue;
    if (a.x === b.x && a.y === b.y) continue;
    var line = document.createElementNS(svgNS, "line");
    line.setAttribute("class", "inset-edge");
    line.setAttribute("x1", a.x); line.setAttribute("y1", a.y);
    line.setAttribute("x2", b.x); line.setAttribute("y2", b.y);
    svg.appendChild(line);

    var mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
    var lbl = document.createElementNS(svgNS, "text");
    lbl.setAttribute("class", "inset-edge-label");
    lbl.setAttribute("x", mx);
    lbl.setAttribute("y", my - 4);
    lbl.textContent = (e.nav || "") + (e.weight ? " w=" + e.weight : "");
    svg.appendChild(lbl);
  }

  // Stub edges to external neighbours
  // For each external edge bucket, find the underlying edges where one endpoint
  // lives in this cluster — draw a short stub from that member toward the
  // direction of the neighbour cluster's centroid.
  var stubLen = nodeRadius * 1.6;
  var labelOffset = nodeRadius * 2.2;
  for (var i = 0; i < externals.length; i++) {
    var bucket = externals[i];
    var other = byId[bucket.other];
    if (!other) continue;
    var dirX = other.cx - cluster.cx;
    var dirY = other.cy - cluster.cy;
    var len = Math.sqrt(dirX * dirX + dirY * dirY) || 1;
    var ux = dirX / len, uy = dirY / len;

    // Walk underlying edges, dedupe by self-side member to avoid stub stacking
    var seen = {};
    for (var j = 0; j < bucket.edges.length; j++) {
      var ed = bucket.edges[j];
      var selfName = nodeToCluster_self(ed, cluster);
      if (!selfName || seen[selfName]) continue;
      seen[selfName] = true;
      var sn = nodeFromCluster(cluster, selfName);
      if (!sn) continue;

      var x1 = sn.x + ux * nodeRadius;
      var y1 = sn.y + uy * nodeRadius;
      var x2 = sn.x + ux * (nodeRadius + stubLen);
      var y2 = sn.y + uy * (nodeRadius + stubLen);

      var stub = document.createElementNS(svgNS, "line");
      stub.setAttribute("class", "inset-stub-edge");
      stub.setAttribute("x1", x1); stub.setAttribute("y1", y1);
      stub.setAttribute("x2", x2); stub.setAttribute("y2", y2);
      svg.appendChild(stub);

      var lbl2 = document.createElementNS(svgNS, "text");
      lbl2.setAttribute("class", "inset-stub-label");
      lbl2.setAttribute("x", sn.x + ux * (nodeRadius + labelOffset));
      lbl2.setAttribute("y", sn.y + uy * (nodeRadius + labelOffset));
      lbl2.setAttribute("text-anchor", "middle");
      lbl2.textContent = "→ " + clusterLabel(other);
      svg.appendChild(lbl2);
    }
  }

  // Member nodes (visual only — co-located members will overlap; the
  // clickable list below is the reliable picker)
  for (var i = 0; i < cluster.members.length; i++) {
    var m = cluster.members[i];
    var isTransit = (m.type === "transit");
    if (isTransit) {
      var s = nodeRadius * 0.6;
      var r = document.createElementNS(svgNS, "rect");
      r.setAttribute("class", "inset-node transit");
      r.setAttribute("x", m.x - s);
      r.setAttribute("y", m.y - s);
      r.setAttribute("width", s * 2);
      r.setAttribute("height", s * 2);
      r.setAttribute("data-node", m.name);
      svg.appendChild(r);
    } else {
      var c = document.createElementNS(svgNS, "circle");
      c.setAttribute("class", "inset-node " + (m.type || "waypoint") +
        (interactive ? " interactive" : ""));
      c.setAttribute("cx", m.x);
      c.setAttribute("cy", m.y);
      c.setAttribute("r", nodeRadius);
      c.setAttribute("data-node", m.name);
      if (interactive) {
        c.addEventListener("click", (function (name) {
          return function (e) {
            e.stopPropagation();
            wrap.dispatchEvent(new CustomEvent("node-select", { detail: { nodeName: name } }));
          };
        })(m.name));
      }
      svg.appendChild(c);
    }
  }

  wrap.appendChild(svg);

  // Member list — primary picker (always reliable, even when nodes overlap)
  var list = document.createElement("div");
  list.className = "inset-list";
  for (var i = 0; i < cluster.members.length; i++) {
    var m = cluster.members[i];
    var isTransit = (m.type === "transit");
    var item = document.createElement("div");
    item.className = "inset-list-item" +
      (isTransit ? " transit-item" : "") +
      (interactive ? " interactive" : "");
    item.dataset.node = m.name;

    var dot = document.createElement("span");
    dot.className = "inset-list-dot " + (m.type || "waypoint");
    item.appendChild(dot);

    var label = document.createElement("span");
    label.className = "inset-list-label";
    label.textContent = m.name.replace(/_/g, " ") + (isTransit ? " (transit)" : "");
    item.appendChild(label);

    if (interactive && !isTransit) {
      item.addEventListener("click", (function (name) {
        return function (e) {
          e.stopPropagation();
          wrap.dispatchEvent(new CustomEvent("node-select", { detail: { nodeName: name } }));
        };
      })(m.name));
    }
    list.appendChild(item);
  }
  wrap.appendChild(list);

  return wrap;
}

function nodeFromCluster(cluster, name) {
  for (var i = 0; i < cluster.members.length; i++) {
    if (cluster.members[i].name === name) return cluster.members[i];
  }
  return null;
}

// Given an edge and a cluster, return the endpoint name that lives in
// the cluster (or null if neither does).
function nodeToCluster_self(edge, cluster) {
  for (var i = 0; i < cluster.members.length; i++) {
    if (cluster.members[i].name === edge.from) return edge.from;
    if (cluster.members[i].name === edge.to)   return edge.to;
  }
  return null;
}
