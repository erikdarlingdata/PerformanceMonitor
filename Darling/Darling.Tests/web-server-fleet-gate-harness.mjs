/* Runs the shipped app.js (its start, the sidebar's one fleet read per store behind a front door, route() and the server page with the real
   panels) against a stand-in DOM and a scripted fetch, and prints one scenario's result as one line of JSON (#8554). Each store's /api/fleet
   answer is held until the scenario releases it, so the harness can say which reads leave while a roll-up is still out.
   WebServerFleetGateTests starts it as
       node web-server-fleet-gate-harness.mjs <path to wwwroot/js> <scenario>
   Each scenario is its own process because the front-door switch and the picked store are module state. */
import { pathToFileURL } from "node:url";
import path from "node:path";

const [jsDirArg, scenario] = process.argv.slice(-2);
const jsDir = path.resolve(jsDirArg);

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; this.style = {}; this.className = "";
    this.listeners = {}; this.parentNode = null; this.hidden = false; this.value = ""; this.textContent = "";
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { if (c && typeof c === "object") c.parentNode = this; this.children.push(c); return c; }
  append(...cs) { cs.forEach((c) => this.appendChild(c)); }
  prepend(c) { this.children.unshift(c); }
  insertBefore(c, ref) { const i = ref ? this.children.indexOf(ref) : -1; if (c && typeof c === "object") c.parentNode = this; if (i < 0) this.children.push(c); else this.children.splice(i, 0, c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  replaceChildren(...cs) { this.children = []; cs.forEach((c) => this.appendChild(c)); }
  remove() {}
  setAttribute(n, v) { this.attrs[n] = String(v); }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  removeAttribute(n) { delete this.attrs[n]; }
  addEventListener(t, f) { (this.listeners[t] ||= []).push(f); }
  removeEventListener() {}
  dispatch(t) { (this.listeners[t] || []).forEach((f) => f({ target: this, preventDefault() {}, stopPropagation() {} })); }
  focus() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
  find(pred) {
    if (pred(this)) return this;
    for (const c of this.children) { const f = c && c.find ? c.find(pred) : null; if (f) return f; }
    return null;
  }
}

const byId = {};
for (const id of ["main", "server-list", "view-list", "statusbar", "refresh-hint"]) byId[id] = new FakeNode("div");
const brand = new FakeNode("div");
byId["auto-refresh-toggle"] = new FakeNode("button");
brand.appendChild(byId["auto-refresh-toggle"]);

/* Every boot's start() adds its own visibilitychange listener; a scenario fires only the newest, the live module copy's. */
const docListeners = {};
globalThis.document = {
  createElement: (t) => new FakeNode(t), createElementNS: (_n, t) => new FakeNode(t),
  createTextNode: (t) => { const n = new FakeNode("#text"); n.textContent = String(t); return n; },
  createDocumentFragment: () => new FakeNode("#frag"),
  getElementById: (id) => byId[id] || null, querySelector: () => null, querySelectorAll: () => [],
  addEventListener(t, f) { (docListeners[t] ||= []).push(f); }, removeEventListener() {}, body: new FakeNode("body"), documentElement: new FakeNode("html"),
  cookie: "", hidden: false,
};
globalThis.Node = FakeNode;
/* The in-place refresh (#6933) watches a staged panel for its loading strip to go. These scenarios do not read what lands, so the observer never fires. */
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.window = globalThis;
globalThis.location = { hash: "#/server/alpha", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const winListeners = {};
globalThis.addEventListener = (t, f) => { (winListeners[t] ||= []).push(f); };
globalThis.removeEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
globalThis.requestAnimationFrame = () => 0;
globalThis.cancelAnimationFrame = () => {};
let storage = new Map();
globalThis.localStorage = {
  getItem: (k) => (storage.has(k) ? storage.get(k) : null),
  setItem: (k, v) => { storage.set(k, String(v)); },
  removeItem: (k) => { storage.delete(k); },
};
globalThis.sessionStorage = globalThis.localStorage;
globalThis.requestAnimationFrame = () => 0;
globalThis.cancelAnimationFrame = () => {};
globalThis.setInterval = () => 1;

/* ── the scripted fetch ── */
const STORES = ["alpha", "bravo", "charlie"];
const state = { holdFleet: new Map(), fleetFail: new Set(), serverIn: "alpha" };
const log = []; /* { kind: "fleet" | "read" | "catalog" | "other", store, url } in the order they were sent */
const reply = (status, body) => ({ ok: status >= 200 && status < 300, status, url: "", headers: { get: () => null }, json: async () => body, text: async () => JSON.stringify(body) });
const card = (name) => ({ server_id: 7, server_name: name, display_name: name, is_postgres: false, band: "Healthy", band_label: "Healthy", is_online: true,
  status: "ok", last_collection: null, tags: [], is_silenced: false, metric_count: 0, measured_metric_count: 0 });
const gateOf = () => { let release; const p = new Promise((r) => { release = r; }); return { p, release }; };
globalThis.fetch = async (url, init = {}) => {
  const u = String(url);
  const path0 = u.split("?")[0];
  const store = (init.headers && init.headers["X-Darling-Store"]) || null;
  if (path0 === "/api/session") return reply(200, { can_edit: false, front_door: true });
  if (path0 === "/api/front-door") return reply(200, { stores: STORES.map((name) => ({ name, health: "ok", server_count: 1 })) });
  if (path0 === "/api/fleet") {
    log.push({ kind: "fleet", store });
    const held = state.holdFleet.get(store);
    if (held) await held.p;
    if (state.fleetFail.has(store)) return reply(500, { error: "down" });
    const cards = store === state.serverIn ? [card("SRV1")] : [card("other-" + store)];
    return reply(200, { total_servers: 1, critical_count: 0, warning_count: 0, offline_count: 0, offline_servers: 0, tags: [], worst_servers: [],
      additional_problem_count: 0, generated_at: "2026-11-11T12:00:00Z", cards });
  }
  if (path0.startsWith("/api/read/")) { log.push({ kind: "read", store, url: u }); return reply(200, {}); }
  if (path0 === "/api/catalog") { log.push({ kind: "catalog", store, url: u }); return reply(200, { tools: [] }); }
  log.push({ kind: "other", store, url: u });
  return reply(200, {});
};

const drain = async () => { for (let i = 0; i < 30; i++) await new Promise((r) => setImmediate(r)); };
const count = (kind) => log.filter((e) => e.kind === kind).length;
const filtered = () => log.filter((e) => e.kind === "read" && e.url.includes("database_name=Sales")).length;
const look = () => ({ fleet: count("fleet"), reads: count("read"), catalog: count("catalog"), filtered: filtered() });
/* What viewer-local.js stores for a server: the FNV-1a hash of the route's name (pages never store the name) and a saved database filter. */
const nameHash = (name) => { let h = 0x811c9dc5; for (let i = 0; i < name.length; i++) { h ^= name.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h; };
const saveFilter = (id) => localStorage.setItem("darling.local.dbFilter.v1", JSON.stringify({ v: 1, servers: { [id]: ["Sales"] } }));
const rememberId = (name, id) => localStorage.setItem("darling.local.serverIds.v1", JSON.stringify({ v: 1, ids: [[nameHash(name), id]] }));
const allText = (n) => (n.textContent || "") + n.children.map((c) => (c && c.children ? allText(c) : "")).join(" ");

const out = {};
const boot = async (hash) => {
  location.hash = hash;
  for (const s of STORES) state.holdFleet.set(s, gateOf());
  await import(pathToFileURL(path.join(jsDir, "app.js")).href);
  await drain();
};
const release = async (...stores) => { for (const s of stores) state.holdFleet.get(s).release(); await drain(); };

const scenarios = {
  /* Every store's roll-up is out and slow. The server's reads leave anyway, without waiting for any of the three. */
  async allFleetsHeld() {
    await boot("#/s/alpha/server/SRV1/overview");
    out.beforeFleet = look();
    await release("alpha", "bravo", "charlie");
    out.afterFleet = look();
  },
  /* A database filter is saved for this server, and this browser remembers the id the card gave it. The reads leave with the filter on
     before any roll-up answers. */
  async filterRememberedId() {
    saveFilter(7);
    rememberId("SRV1", 7);
    await boot("#/s/alpha/server/SRV1/overview");
    out.beforeFleet = look();
    await release("alpha", "bravo", "charlie");
    out.afterFleet = look();
  },
  /* A filter is saved for another server, and this browser remembers this server's id: the reads leave before any roll-up, unfiltered. */
  async filterForAnotherServer() {
    saveFilter(99);
    rememberId("SRV1", 7);
    await boot("#/s/alpha/server/SRV1/overview");
    out.beforeFleet = look();
    await release("alpha", "bravo", "charlie");
    out.afterFleet = look();
  },
  /* A filter is saved and this browser has never seen this server's card: only the card's id can say whose filter applies, so the reads wait
     for the roll-up of the server's own store. The two other stores' slow roll-ups do not hold them. */
  async filterNoRememberedId() {
    saveFilter(99);
    await boot("#/s/alpha/server/SRV1/overview");
    out.beforeFleet = look();
    await release("alpha");
    out.ownStoreAnswered = look();
    await release("bravo", "charlie");
  },
  /* The server's own store answers at once and the other two are still out: the reads are not held for them, and nothing is read twice. */
  async otherStoresHeld() {
    await boot("#/s/alpha/server/SRV1/overview");
    await release("alpha");
    out.ownStoreAnswered = look();
    await release("bravo", "charlie");
    out.allAnswered = look();
  },
  /* A server no store's roll-up carries: the page still gets its reads, and the route's name stays in the heading. */
  async missingServer() {
    state.serverIn = "nowhere";
    await boot("#/s/alpha/server/SRV1/overview");
    out.beforeFleet = look();
    await release("alpha", "bravo", "charlie");
    out.afterFleet = look();
    out.title = allText(byId["main"]).includes("SRV1");
  },
  /* The server's store is down (its roll-up fails): the reads were sent and are not sent again. */
  async storeDown() {
    state.fleetFail.add("alpha");
    await boot("#/s/alpha/server/SRV1/overview");
    out.beforeFleet = look();
    await release("alpha", "bravo", "charlie");
    out.afterFleet = look();
    out.title = allText(byId["main"]).includes("SRV1");
  },
};
if (!scenarios[scenario]) throw new Error("unknown scenario " + scenario);
await scenarios[scenario]();
console.log(JSON.stringify(out));
process.exit(0);
