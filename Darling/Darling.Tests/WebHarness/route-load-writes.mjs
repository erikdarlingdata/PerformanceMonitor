import { pathToFileURL } from "node:url";

/* #7864: opening a page never writes, exports or confirms anything by itself. A person can reach a page signed in by a redirect
   from another site with no click (#6286, #7847), so a route that wrote on load would act for whoever followed the link.
   This draws EVERY route the router knows, the way a hash change does (the real route(), through the hashchange listener
   app.js registers), with fetch stubbed to answer 200 with a small valid body, and records every request each page sends
   while it loads. It exits 1 when a page sends a POST, PUT, PATCH or DELETE that is not one of the three reads that travel as
   a POST, or when the sample addresses below do not reach exactly the renderers the router has (so a new route cannot be
   left unsampled). A stub for a read the page makes before it can go on (a view, a notebook, an alert rule, a server's
   tabs) is the smallest body that lets the page get to what it sends next. */
const realSetTimeout = globalThis.setTimeout;
const failures = [];
const noop = () => {};
globalThis.Node = class {};
/* A page reads back what it just built (a field's value, the children it appended), so an element keeps those; nothing here draws. */
class FakeElement extends Node {
  constructor(tag) {
    super();
    this.tagName = String(tag).toUpperCase();
    this.attrs = {};
    this.style = {};
    this.dataset = {};
    this.children = [];
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this.hidden = false;
    this.textContent = "";
    this.className = "";
    this.innerHTML = "";
    this.options = [];
    this.selectedIndex = -1;
    this.parentNode = null;
    this.classList = { add: noop, remove: noop, toggle: noop, contains: () => false };
  }
  get childNodes() { return this.children; }
  get firstChild() { return this.children[0] || null; }
  get lastChild() { return this.children[this.children.length - 1] || null; }
  setAttribute(k, v) { this.attrs[k] = v; if (k === "value") this.value = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  hasAttribute(k) { return k in this.attrs; }
  removeAttribute(k) { delete this.attrs[k]; }
  appendChild(c) { this.children.push(c); return c; }
  append(...cs) { this.children.push(...cs); }
  prepend(...cs) { this.children.unshift(...cs); }
  insertBefore(c) { this.children.push(c); return c; }
  replaceChildren(...cs) { this.children = cs; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  replaceChild(n, o) { const i = this.children.indexOf(o); if (i >= 0) this.children[i] = n; else this.children.push(n); return o; }
  remove() {}
  replaceWith() {}
  before() {}
  after() {}
  addEventListener() {}
  removeEventListener() {}
  focus() {}
  blur() {}
  click() {}
  scrollIntoView() {}
  scrollTo() {}
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  querySelectorAll(sel) {
    /* Tag, #id, .class and [attr] / [attr=value] pieces, joined as one compound (no combinators; a list is any of its parts). */
    const parts = String(sel).split(",").map((p) => p.trim()).filter(Boolean);
    const out = [];
    const walkTree = (n) => {
      for (const c of n.children) {
        if (!(c instanceof FakeElement)) continue;
        if (parts.some((p) => c.matchesCompound(p))) out.push(c);
        walkTree(c);
      }
    };
    walkTree(this);
    return out;
  }
  matchesCompound(p) {
    const pieces = p.match(/#[\w-]+|\.[\w-]+|\[[^\]]+\]|^[a-zA-Z][\w-]*/g);
    if (!pieces || pieces.join("") !== p) return false;
    const classes = String(this.className).split(/\s+/);
    return pieces.every((x) => {
      if (x[0] === "#") return this.attrs.id === x.slice(1);
      if (x[0] === ".") return classes.includes(x.slice(1));
      if (x[0] === "[") {
        const m = x.slice(1, -1).match(/^([\w-]+)(?:="?([^"]*)"?)?$/);
        return !!m && m[1] in this.attrs && (m[2] === undefined || String(this.attrs[m[1]]) === m[2]);
      }
      return this.tagName === x.toUpperCase();
    });
  }
  getBoundingClientRect() { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0 }; }
  contains() { return false; }
  closest() { return null; }
  matches() { return false; }
  cloneNode() { return new FakeElement(this.tagName); }
}
const node = (tag) => new FakeElement(tag || "div");
const listeners = {};
globalThis.document = {
  createElement: node, createElementNS: (ns, tag) => node(tag),
  createTextNode: (t) => { const n = node("#text"); n.textContent = String(t); return n; },
  createDocumentFragment: () => node("#fragment"),
  getElementById: () => node(), querySelector: () => null, querySelectorAll: () => [], addEventListener: noop,
  removeEventListener: noop, body: node("body"), documentElement: node("html"), cookie: "", hidden: false,
};
globalThis.window = globalThis;
globalThis.location = { hash: "", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost", reload: noop };
globalThis.history = { pushState: noop, replaceState: noop, back: noop };
const store = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.localStorage = store;
globalThis.sessionStorage = store;
globalThis.addEventListener = (type, fn) => { (listeners[type] = listeners[type] || []).push(fn); };
globalThis.removeEventListener = noop;
globalThis.matchMedia = () => ({ matches: false, addEventListener: noop, removeEventListener: noop });
globalThis.requestAnimationFrame = noop;
globalThis.cancelAnimationFrame = noop;
globalThis.scrollTo = noop;
globalThis.setInterval = () => 0;
globalThis.clearInterval = noop;
globalThis.clearTimeout = noop;
/* A short timer a page sets while it loads (a debounce, a deferred first read) runs, so a request behind it is seen; a long
   one (a poll) is dropped, as no page is open for that long here. */
globalThis.setTimeout = (fn, ms) => {
  if (typeof fn === "function" && !(ms > 1000)) realSetTimeout(fn, 0);
  return 0;
};

/* The requests the page sent, with the route that was being drawn. */
const requests = [];
let drawing = "startup";
/* A page that throws on this inert DOM stops drawing there; that is listed, but it is not a failure of the rule being tested. */
const pageErrors = [];
const recordError = (e) => pageErrors.push(drawing + ": " + (e && e.stack ? e.stack.split("\n").slice(0, 2).join(" | ") : String(e)));
process.on("unhandledRejection", recordError);
process.on("uncaughtException", recordError);
let finished = false;
process.on("exit", () => {
  if (finished) return;
  console.error("the harness stopped before it finished; page errors so far: " + pageErrors.join(" || "));
  process.exitCode = 1;
});

const SERVER = "sql1";
const POSTGRES = "pg1";
const fleet = {
  window_start: "2026-01-01T00:00:00Z",
  window_end: "2026-01-01T01:00:00Z",
  cards: [
    { server_name: SERVER, display_name: SERVER, is_postgres: false, is_online: true, engine_description: "SQL Server" },
    { server_name: POSTGRES, display_name: POSTGRES, is_postgres: true, is_online: true, engine_description: "PostgreSQL" },
  ],
  worst_servers: [],
};
/* The smallest saved view, notebook, alert rule and catalog that let their pages go on to what they send next: a view with a
   composed panel and a notebook with a composed cell each send /api/compose/run, and an alert rule with a metric and a warning
   threshold sends /api/alerts/validate and /api/alerts/test. */
const composedPanel = { id: "p1", title: "Waits", source: "wait_stats", measure: "wait_time_ms", aggregate: "sum", viz: "stat" };
const dashboardView = { id: 1, name: "v", version: 1, definition: { panels: [composedPanel] } };
const notebookView = {
  id: 1, name: "n", version: 1,
  definition: { kind: "notebook", cells: [{ type: "markdown", text: "notes" }, { type: "panel", ...composedPanel }] },
};
const alertRule = {
  id: 1, name: "r", version: 1, enabled: true,
  definition: { metric: { source: "wait_stats", measure: "wait_time_ms", aggregate: "sum", hours: 1 }, predicate: { op: "gt", warnThreshold: 90 } },
};
const catalog = {
  reads: [{ name: "get_wait_stats", params: [] }],
  viz: ["table", "stat"],
  compose: { measures: [{ source: "wait_stats", name: "wait_time_ms", label: "Wait time", unit: "ms", aggregates: ["sum"] }], viz: ["stat", "table"] },
};
const alertNotebook = {
  alert: {}, status: {}, notes: [], template: { id: "t", version: 1 }, scope_server: SERVER,
  definition: { kind: "notebook", cells: [{ type: "panel", ...composedPanel }] },
};

function bodyFor(method, url) {
  const path = url.split("?")[0];
  if (path === "/api/fleet") return fleet;
  if (path === "/api/server-data-start") return { data_start_utc: "2026-01-01T00:00:00Z" };
  if (path === "/api/session") return { front_door: false, can_edit: true };
  if (path === "/api/catalog") return catalog;
  if (path === "/api/views") return [dashboardView];
  if (path.startsWith("/api/views/")) return drawing.startsWith("#/notebook") ? notebookView : dashboardView;
  if (path === "/api/alerts/validate") return { valid: true };
  if (path === "/api/alerts/test") return { rows: [] };
  if (path.startsWith("/api/alerts/")) return alertRule;
  if (path === "/api/alert-notebook") return alertNotebook;
  if (path === "/api/read/list_servers") return { servers: [{ server_name: SERVER, display_name: SERVER }] };
  return {};
}

globalThis.fetch = async (url, init) => {
  const method = ((init && init.method) || "GET").toUpperCase();
  requests.push({ route: drawing, method, url: String(url) });
  const body = JSON.stringify(bodyFor(method, String(url)));
  return new Response(body, { status: 200, headers: { "content-type": "application/json" } });
};

/* Lets what a page started settle: the stub answers asynchronously and a page chains one read after the next, so a turn
   that sends nothing new is only quiet after several of them in a row (capped). */
async function settle() {
  for (let quiet = 0, turns = 0; quiet < 5 && turns < 100; turns++) {
    const before = requests.length;
    await new Promise((r) => realSetTimeout(r, 5));
    quiet = requests.length === before ? quiet + 1 : 0;
  }
}

/* A read that travels as a POST. Each one sends nothing that changes anything; the reason is the whole entry. */
const POST_READS = [
  ["/api/compose/run", "runs a composed panel's query and returns its rows (notebooks and views); nothing is saved"],
  ["/api/alerts/validate", "checks the rule text the alert editor opens with and returns its verdict; nothing is saved"],
  ["/api/alerts/test", "evaluates the rule against recent data and returns what would fire; nothing is saved or delivered"],
];
const isRead = (r) => r.method === "GET" || r.method === "HEAD" || POST_READS.some(([p]) => r.url.split("?")[0] === p && r.method === "POST");

const app = await import(pathToFileURL(process.argv[2] + "/app.js").href);
const admin = await import(pathToFileURL(process.argv[2] + "/pages/admin.js").href);
const serverTabs = await import(pathToFileURL(process.argv[2] + "/pages/server-tabs.js").href);
const finops = await import(pathToFileURL(process.argv[2] + "/pages/finops.js").href);
await settle();

/* One sample address per renderer, plus one per server tab (SQL Server and PostgreSQL), FinOps tab and Admin tab. The tab
   lists come from the pages' own tables, so a new tab is drawn without an edit here. */
const samples = [
  "#/fleet", "#/server/" + SERVER, "#/finops/" + SERVER, "#/ag", "#/sweeps", "#/alerts", "#/investigations", "#/investigations?request=1",
  "#/proposals", "#/proposals?proposal=1", "#/alert-rules", "#/alert-rule/1", "#/alert-rule/new", "#/alert-rule/new/cpu", "#/admin",
  "#/triage?server=" + SERVER + "&metric=cpu&at=2026-01-01T00:00:00Z", "#/views", "#/view/1", "#/view/1/edit", "#/view/new", "#/notebook/1",
  "#/notebook/1/edit", "#/notebook/new", "#/notebook/new/blank", "#/mute-rules", "#/mute-rules?server_name=" + SERVER + "&metric_name=cpu",
  "#/known-patterns", "#/manage-tags", "#/manage-query-tags", "#/query-tag-usage", "#/job-history",
  ...serverTabs.SERVER_TABS.map((t) => "#/server/" + SERVER + "/" + t.id),
  ...serverTabs.POSTGRES_TABS.map((t) => "#/server/" + POSTGRES + "/" + t.id),
  ...finops.FINOPS_TABS.map((t) => "#/finops/" + SERVER + "/" + t.id),
  ...admin.TABS.map((t) => "#/admin/" + t.id),
];

/* The samples must reach EXACTLY the renderers the router has. */
const reached = new Set();
for (const s of samples) {
  location.hash = s;
  reached.add(app.currentRoute().name);
}
const known = Object.keys(app.ROUTE_RENDERERS);
for (const name of known) {
  if (!reached.has(name)) failures.push("the router has a renderer for \"" + name + "\" and no sample address reaches it: add one to samples in route-load-writes.mjs");
}
for (const name of reached) {
  if (!known.includes(name)) failures.push("a sample address reaches the route \"" + name + "\", which has no entry in ROUTE_RENDERERS (route() would draw the fleet)");
}

const listenerCount = (listeners.hashchange || []).length;
if (listenerCount === 0) failures.push("app.js registered no hashchange listener, so no route can be drawn");

const perRoute = [];
for (const s of samples) {
  drawing = s;
  const first = requests.length;
  location.hash = s;
  for (const fn of listeners.hashchange || []) fn({});
  await settle();
  const mine = requests.slice(first).filter((r) => r.route === s);
  perRoute.push({ route: s, count: mine.length });
}

const writes = requests.filter((r) => !isRead(r));
for (const w of writes) failures.push("route " + w.route + " sent " + w.method + " " + w.url + " while it loaded");
/* Each allowed read must really be sent by some page, so the allowlist stays exactly the reads that exist and the pages that send
   them (a view, a notebook, an alert editor, a triage link) were drawn far enough to reach them. */
for (const [path] of POST_READS) {
  if (!requests.some((r) => r.method === "POST" && r.url.split("?")[0] === path)) {
    failures.push("no route sent POST " + path + " while it loaded: a page stopped short of it, or it is no longer sent and its allowlist entry should go");
  }
}
const silent = perRoute.filter((p) => p.count === 0).map((p) => p.route);
for (const s of silent) failures.push("route " + s + " sent no request at all while it loaded, so it proved nothing");

for (const p of perRoute) console.log("route " + p.route + ": " + p.count + " request(s)");
if (process.env.ROUTE_LOAD_VERBOSE) for (const r of requests) console.log("  " + r.route + " " + r.method + " " + r.url);
console.log("posts allowed as reads: " + POST_READS.map(([p]) => p).join(", "));
for (const e of pageErrors) console.log("page error " + e);
console.log("sent: " +requests.filter((r) => r.method === "POST").map((r) => r.route + " " + r.url).join("; "));
finished = true;
if (failures.length > 0) {
  for (const f of failures) console.error(f);
  process.exit(1);
}
console.log("drew " + samples.length + " routes and " + requests.length + " requests; none wrote");
process.exit(0);
