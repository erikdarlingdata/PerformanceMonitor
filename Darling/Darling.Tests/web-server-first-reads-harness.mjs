/* Runs the shipped server page (pages/server.js, with the real panels.js) on a small fake DOM and a scripted fetch, and prints a scenario's
   result as one line of JSON (#8554): which panel reads leave while the fleet roll-up (the card that says which tab set a server has) is
   still outstanding, and what the card's arrival then does. WebServerFirstReadsTests starts it as
       node web-server-first-reads-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM, fetch, storage, and the tab registries (pages/server-tabs.js: a SQL Server set whose Overview reads get_sql_stub and a
   PostgreSQL set whose Overview and PgOnly read get_pg_stub, one real renderPanel panel each) are stand-ins. No clock is read: every
   step runs to the end of the event loop, then looks at what was sent. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

/* As in a browser, an observer is told only about the kinds of change it asked for: a child added or removed reaches one that asked for
   `childList`, and `hidden` or `style` set on a node reaches one that asked for `attributes` (with that name in its `attributeFilter`, when
   it gave one). An observer that watches children only never hears of a panel that hides itself. */
const observers = [];
const wants = (init, kind, name) => !!init[kind] && (kind !== "attributes" || !init.attributeFilter || init.attributeFilter.includes(name));
const notify = (kind = "childList", name = null) => queueMicrotask(() => observers.slice().forEach((o) => { if (o.inits.some((i) => wants(i, kind, name))) o.cb(); }));
globalThis.MutationObserver = class {
  constructor(cb) { this.cb = cb; this.inits = []; }
  observe(target, init) { this.inits.push(init || {}); if (!observers.includes(this)) observers.push(this); }
  disconnect() { const i = observers.indexOf(this); if (i >= 0) observers.splice(i, 1); this.inits = []; }
};

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {}; this.value = ""; this.checked = false; this.disabled = false; this._hidden = false; this.isRoot = false;
    this.style = { _display: "", get display() { return this._display; }, set display(v) { this._display = String(v); notify("attributes", "style"); } };
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get hidden() { return this._hidden; }
  set hidden(v) { this._hidden = v; notify("attributes", "hidden"); }
  get firstChild() { return this.children[0] || null; }
  get nextSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i >= 0 ? this.parent.children[i + 1] || null : null; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children.forEach((c) => (c.parent = null)); this.children = []; notify(); }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); notify(); return c; }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c); else this.children.splice(i, 0, c);
    notify();
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; notify(); }
  remove() { if (this.parent) this.parent.removeChild(this); }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  every(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.every(pred, acc); } return acc; }
  byClass(cls) { return this.every((n) => (" " + n.className + " ").includes(" " + cls + " ")); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (t) => new FakeNode(t), createElementNS: (ns, t) => new FakeNode(t), createTextNode: (t) => new FakeText(t),
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], addEventListener() {},
};
globalThis.window = globalThis;
globalThis.location = { hash: "#/server/SRV1/cpu", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost" };
globalThis.history = { pushState() {}, replaceState(a, b, url) { replaced.push(String(url)); }, back() {} };
globalThis.dispatchEvent = (e) => { events.push(e.type); return true; };
globalThis.addEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
const mem = new Map();
globalThis.localStorage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, String(v)), removeItem: (k) => mem.delete(k) };
globalThis.sessionStorage = globalThis.localStorage;
let scrolledTo = null;
globalThis.scrollTo = (x, y) => { scrolledTo = y; };

const [jsDir, scenario] = process.argv.slice(-2);

/* ── the scripted fetch: the fleet roll-up is held until the scenario releases it; every other read answers at once ── */
const state = { fleetGate: null, fleetFail: false, postgres: false, online: true, names: [{ server_name: "SRV1", display_name: "SRV1" }] };
const replaced = [];
const events = [];
const calls = [];
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });
const card = (n = { server_name: "SRV1", display_name: "SRV1" }, id = 7) => ({ server_id: id, server_name: n.server_name, display_name: n.display_name, is_postgres: state.postgres, band: "Healthy", band_label: "Healthy", is_online: state.online, status: "ok", last_collection: null, tags: [], metrics: [] });
globalThis.fetch = async (url, init) => {
  const u = String(url);
  if (u === "/api/session") return reply(200, { can_edit: false });
  if (u.startsWith("/api/fleet")) {
    calls.push({ tool: "fleet" });
    if (state.fleetGate) await state.fleetGate;
    if (state.fleetFail) return reply(500, { error: "fleet down" });
    return reply(200, { total_servers: 1, critical_count: 0, warning_count: 0, offline_count: 0, tags: [], cards: state.names.map((n, i) => card(n, 7 + i)), generated_at: "2026-01-01T00:00:00" });
  }
  const m = /^\/api\/read\/(get_(?:sql|pg)_stub)/.exec(u);
  if (m) {
    calls.push({ tool: m[1], signal: init && init.signal });
    return reply(200, { value: m[1] === "get_sql_stub" ? "sql-value" : "pg-value" });
  }
  return reply(200, {});
};
const settle = async () => { for (let i = 0; i < 6; i++) await new Promise((r) => setTimeout(r, 10)); };
const gate = () => { let release; const p = new Promise((r) => { release = r; }); return { p, release }; };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "server-first-reads-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "pages", "server-tabs.js"),
    'import { el } from "../util.js";\nimport { renderPanel } from "../panels.js";\n' +
      "const tab = (id, read) => ({ id, label: id, build(server) { return [renderPanel({ read, params: { server }, title: id + ' panel', render: (d) => el('div', { class: 'stub-result', text: d.value }) })]; } });\n" +
      "export const SERVER_TABS = [tab('overview', 'get_sql_stub')];\nexport const POSTGRES_TABS = [tab('overview', 'get_pg_stub'), tab('pgonly', 'get_pg_stub')];\n" +
      "export function serverTabsFor(card) { return card && card.is_postgres === true ? POSTGRES_TABS : SERVER_TABS; }\n" +
      "export function isPostgresTarget(card) { return !!card && card.is_postgres === true; }\n" +
      "export function findServerTab(id, tabs) { const r = tabs || SERVER_TABS; return r.find((t) => t.id === id) || r[0]; }\n" +
      "export function tabNote() { return null; }\n"
  );
  const load = (rel, query = "") => import(pathToFileURL(path.join(scratch, rel)).href + query);
  const root = () => { const n = new FakeNode("main"); n.isRoot = true; return n; };
  const count = (tool) => calls.filter((c) => c.tool === tool).length;
  const shown = (n) => n.byClass("stub-result").map((e) => e.textContent);
  const subtabBars = (n) => n.byClass("subtabs").length;
  const snapshot = (name, main) => {
    out[name] = { sql: count("get_sql_stub"), pg: count("get_pg_stub"), fleet: count("fleet"), bar: subtabBars(main), shown: shown(main), title: main.every((n) => n.tag === "h2").map((n) => n.textContent)[0] || null, badge: main.byClass("server-band").map((n) => n.textContent)[0] || null, errors: main.byClass("error").map((n) => n.textContent), replaced: replaced.slice(), events: events.slice() };
  };

  /* A first visit: the page has never seen this server, so it has no card. Holds the roll-up, looks, releases it, looks again. */
  const firstVisit = async (server, tabId) => {
    const mod = await load("pages/server.js");
    const main = root();
    const g = gate();
    state.fleetGate = g.p;
    mod.renderServer(main, server, tabId);
    await settle();
    snapshot("beforeFleet", main);
    g.release();
    await settle();
    snapshot("afterFleet", main);
  };

  const scenarios = {
    /* The SQL Server Overview's read leaves while the roll-up is outstanding, once, and the card's arrival keeps those panels and adds the tab bar. */
    async sqlServer() {
      await firstVisit("SRV1");
    },
    /* A PostgreSQL server on a first visit: the SQL Server read leaves first; when the card says PostgreSQL, the PostgreSQL panels replace it. */
    async postgresServer() {
      state.postgres = true;
      await firstVisit("SRV1");
    },
    /* The same PostgreSQL server on the next page load (the card kept from the first one is gone, the browser remembers the engine): nothing
       is read until the card says which set to show. */
    async postgresServerNextLoad() {
      state.postgres = true;
      await firstVisit("SRV1");
      const viewerLocal = await load("viewer-local.js");
      viewerLocal.reload();
      calls.length = 0;
      const mod = await load("pages/server.js", "?nextLoad");
      const main = root();
      const g = gate();
      state.fleetGate = g.p;
      mod.renderServer(main, "SRV1");
      await settle();
      snapshot("beforeFleet2", main);
      g.release();
      await settle();
      snapshot("afterFleet2", main);
    },
    /* A saved database filter is keyed by the card's server id, so nothing is read before the card: the reads would be the unfiltered ones. */
    async savedDatabaseFilter() {
      const viewerLocal = await load("viewer-local.js");
      viewerLocal.setDatabaseFilter(7, ["Sales"]);
      await firstVisit("SRV1");
    },
    /* A deep link to a tab only the PostgreSQL set has: the SQL Server set cannot start it. */
    async postgresOnlyTab() {
      state.postgres = true;
      await firstVisit("SRV1", "pgonly");
    },
    /* #8741: a route name that is only part of one server's name. The page replaces the link with the server's own name (no pushed entry) and
       asks the router to draw again; that draw takes the card the first one kept, so the header fills with no second roll-up read. */
    async partialName() {
      state.names = [{ server_name: "HOST1.corp.example", display_name: "HOST1.corp.example" }, { server_name: "OTHER2", display_name: "OTHER2" }];
      await firstVisit("host1");
      const main = root();
      calls.length = 0;
      (await load("pages/server.js")).renderServer(main, "HOST1.corp.example");
      await settle();
      snapshot("redrawn", main);
    },
    /* #8741: a name two servers contain, and a name none does: the page says so, and its panels and tab bar are gone. */
    async ambiguousName() {
      state.names = [{ server_name: "HOST1.a.example", display_name: "HOST1.a.example" }, { server_name: "HOST1.b.example", display_name: "HOST1.b.example" }];
      await firstVisit("host1");
    },
    async noMatch() {
      state.names = [{ server_name: "HOST1", display_name: "HOST1" }];
      await firstVisit("nothing-like-it");
    },
    /* A name that is exactly one server's display name is already that server's page: no link rewrite and no second draw. */
    async exactDisplayName() {
      state.names = [{ server_name: "host1.corp", display_name: "Prod One" }];
      await firstVisit("Prod One");
    },
    /* A roll-up that fails: the early panels stay, and the tab bar is drawn around them with no second read. */
    async fleetFails() {
      state.fleetFail = true;
      await firstVisit("SRV1");
    },
  };
  if (!scenarios[scenario]) throw new Error("unknown scenario " + scenario);
  await scenarios[scenario]();
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
process.exit(0);
