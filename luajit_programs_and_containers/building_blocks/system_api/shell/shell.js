// shell.js — NATS WebSocket client for system API shell
//
// Connects to NATS via WebSocket, uses request/reply to talk to the
// KB sidecar (same RPC subjects the LuaJIT client uses).

import { connect, JSONCodec, StringCodec } from "./vendor/nats.js";

const NATS_WS_URL = "ws://localhost:9222";
const RPC_PREFIX = "default.system_api.kb.query";

const jc = JSONCodec();
const sc = StringCodec();

// RPC call helper — sends JSON-RPC 2.0 request, returns parsed result
async function rpcCall(nc, method, params) {
  const subject = `${RPC_PREFIX}.${method}`;
  const payload = JSON.stringify({
    id: crypto.randomUUID(),
    params: params || {},
  });

  try {
    const msg = await nc.request(subject, sc.encode(payload), { timeout: 5000 });
    const resp = JSON.parse(sc.decode(msg.data));
    // JSON-RPC 2.0 response has result field
    if (resp.result) {
      return JSON.parse(resp.result);
    }
    // Direct response (non-wrapped)
    return resp;
  } catch (e) {
    console.error(`RPC ${method} failed:`, e);
    return null;
  }
}

// Alpine.js app
window.shellApp = function () {
  return {
    // State
    connected: false,
    nc: null,
    activeTab: "system",
    domains: [],
    containers: [],
    robots: [],
    services: [],
    activeDomain: null,
    queryMethod: "domains",
    queryParams: "{}",
    queryResult: "",

    async init() {
      try {
        this.nc = await connect({ servers: NATS_WS_URL });
        this.connected = true;
        console.log("NATS connected via WebSocket");

        // Load initial data
        await this.loadDomains();
        await this.loadContainers();
      } catch (e) {
        console.error("NATS connection failed:", e);
        this.connected = false;
      }
    },

    async loadDomains() {
      const resp = await rpcCall(this.nc, "domains", {});
      if (resp && resp.domains) {
        this.domains = resp.domains;
      }
    },

    async loadContainers() {
      const resp = await rpcCall(this.nc, "containers", { cpu: "cpu_01" });
      if (resp && resp.containers) {
        this.containers = resp.containers;
      }
    },

    async selectDomain(domain) {
      this.activeTab = domain.name;
      this.activeDomain = domain;

      // Load robots for this domain
      const robotResp = await rpcCall(this.nc, "robots", { domain: domain.name });
      this.robots = (robotResp && robotResp.robots) || [];

      // Load services for this domain's container
      const svcResp = await rpcCall(this.nc, "services", {
        container_path: domain.container,
      });
      this.services = (svcResp && svcResp.services) || [];
    },

    async runQuery() {
      let params = {};
      try {
        params = JSON.parse(this.queryParams);
      } catch (e) {
        this.queryResult = "Invalid JSON: " + e.message;
        return;
      }

      const resp = await rpcCall(this.nc, this.queryMethod, params);
      this.queryResult = JSON.stringify(resp, null, 2);
    },
  };
};
