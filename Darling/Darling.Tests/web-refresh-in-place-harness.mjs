/* Runs the shipped Fleet page (pages/fleet.js) and server page (pages/server.js, with the real panels.js and refresh-in-place.js)
   on a small fake DOM and a scripted fetch, and prints a scenario's result as one line of JSON (#6933). WebRefreshInPlaceTests
   starts it as
       node web-refresh-in-place-harness.mjs <path to wwwroot/js> <scenario>
   Only the DOM, fetch, storage, and the tab registry (pages/server-tabs.js: one tab with one real renderPanel panel) are
   stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const observers = [];
const notify = () => queueMicrotask(() => observers.slice().forEach((o) => o.cb()));
globalThis.MutationObserver = class {
  constructor(cb) { this.cb = cb; }
  observe() { observers.push(this); }
  disconnect() { const i = observers.indexOf(this); if (i >= 0) observers.splice(i, 1); }
};

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {}; this.value = ""; this.checked = false; this.disabled = false; this.hidden = false; this.isRoot = false;
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
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
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
globalThis.addEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
const mem = new Map();
globalThis.localStorage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, String(v)), removeItem: (k) => mem.delete(k) };
globalThis.sessionStorage = globalThis.localStorage;
let scrolledTo = null;
globalThis.scrollTo = (x, y) => { scrolledTo = y; };

const [jsDir, scenario] = process.argv.slice(-2);

/* ── the scripted fetch: each route has a version the scenario bumps, and a gate that holds the answer until released ── */
const state = { fleetVersion: 1, readVersion: 1, fleetFail: false, readFail: false, fleetGate: null, readGate: null, fleetCalls: 0, readCalls: 0 };
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });
const card = () => ({ server_id: 7, server_name: "SRV1", display_name: "SRV1", is_postgres: false, band: "Healthy", band_label: "Healthy", is_online: true, status: "ok", last_collection: null, tags: [], is_silenced: false, metric_count: 0, measured_metric_count: 0 });
globalThis.fetch = async (url) => {
  const u = String(url);
  if (u === "/api/session") return reply(200, { can_edit: false });
  if (u.startsWith("/api/fleet")) {
    state.fleetCalls++;
    if (state.fleetGate) await state.fleetGate;
    if (state.fleetFail) return reply(500, { error: "fleet down" });
    return reply(200, { total_servers: 1, critical_count: 0, warning_count: 0, offline_count: 0, tags: [], cards: [card()], generated_at: "2026-01-01T00:00:00", fleetVersion: state.fleetVersion });
  }
  if (u.startsWith("/api/read/get_stub")) {
    state.readCalls++;
    if (state.readGate) await state.readGate;
    if (state.readFail) return reply(500, { error: "read down" });
    return reply(200, { value: "value-" + state.readVersion });
  }
  return reply(200, {});
};
const settle = () => new Promise((r) => setTimeout(r, 30));
const gate = () => { let release; const p = new Promise((r) => { release = r; }); return { p, release }; };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "refresh-in-place-"));
const out = {};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "pages", "server-tabs.js"),
    'import { el } from "../util.js";\nimport { renderPanel } from "../panels.js";\n' +
      "const tab = { id: 'cpu', label: 'CPU', build(server) { return [renderPanel({ read: 'get_stub', params: { server }, title: 'Stub panel', render: (d) => el('div', { class: 'stub-result', text: d.value }) })]; } };\n" +
      "export const SERVER_TABS = [tab];\nexport const POSTGRES_TABS = [tab];\n" +
      "export function serverTabsFor(card) { return card && card.is_postgres === true ? POSTGRES_TABS : SERVER_TABS; }\n" +
      "export function isPostgresTarget(card) { return !!card && card.is_postgres === true; }\n" +
      "export function findServerTab(id, tabs) { const r = tabs || SERVER_TABS; return r.find((t) => t.id === id) || r[0]; }\n" +
      "export function tabNote() { return null; }\n"
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const root = () => { const n = new FakeNode("main"); n.isRoot = true; return n; };
  const text = (n) => n.textContent;
  const loadingStrips = (n) => n.byClass("loading").length;
  const refreshErrors = (n) => n.byClass("refresh-error").map((e) => e.textContent);

  const scenarios = {
    /* The Fleet page: a poll keeps the roll-up on screen while /api/fleet is outstanding, then swaps it in. */
    async fleetPoll() {
      const fleet = await load("pages/fleet.js");
      const main = root();
      await fleet.renderFleet(main, {});
      const first = main.firstChild;
      const before = text(main);
      const g = gate();
      state.fleetGate = g.p;
      state.fleetVersion = 2;
      const polling = fleet.renderFleet(main, { poll: true });
      await settle();
      out.sameHeadWhileReading = main.firstChild === first;
      out.loadingWhileReading = loadingStrips(main);
      out.textUnchangedWhileReading = text(main) === before;
      g.release();
      await polling;
      out.swapped = main.firstChild !== first;
      out.loadingAfter = loadingStrips(main);
      out.hasServerAfter = text(main).includes("SRV1");
    },
    /* A poll whose read failed leaves the page as it was and adds the error. */
    async fleetPollFails() {
      const fleet = await load("pages/fleet.js");
      const main = root();
      await fleet.renderFleet(main, {});
      const first = main.firstChild;
      state.fleetFail = true;
      await fleet.renderFleet(main, { poll: true });
      out.sameHead = main.firstChild === first;
      out.hasServer = text(main).includes("SRV1");
      out.errors = refreshErrors(main);
      out.loading = loadingStrips(main);
      /* the next poll succeeds: the error goes with the swap */
      state.fleetFail = false;
      await fleet.renderFleet(main, { poll: true });
      out.errorsAfterRecovery = refreshErrors(main).length;
    },
    /* The reader's own navigation still draws fresh: the loading strip shows and the old page goes. */
    async fleetNavigation() {
      const fleet = await load("pages/fleet.js");
      const main = root();
      await fleet.renderFleet(main, {});
      const g = gate();
      state.fleetGate = g.p;
      const opening = fleet.renderFleet(main, {});
      await settle();
      out.loadingWhileReading = loadingStrips(main);
      out.hasServerWhileReading = text(main).includes("SRV1");
      g.release();
      await opening;
      out.loadingAfter = loadingStrips(main);
    },
    /* The server page: a poll keeps the panel element and its content while the read runs again, then fills it in place. */
    async serverPoll() {
      const server = await load("pages/server.js");
      const main = root();
      server.renderServer(main, "SRV1", "cpu", {});
      await settle();
      const grid = main.byClass("panel-grid")[0];
      const panel = main.byClass("panel")[0];
      out.firstText = text(panel);
      const g = gate();
      state.readGate = g.p;
      state.readVersion = 2;
      server.renderServer(main, "SRV1", "cpu", { poll: true });
      await settle();
      out.sameGrid = main.byClass("panel-grid")[0] === grid;
      out.samePanel = main.byClass("panel")[0] === panel;
      out.loadingWhileReading = loadingStrips(main);
      out.textWhileReading = text(panel);
      g.release();
      await settle();
      out.samePanelAfter = main.byClass("panel")[0] === panel;
      out.panelCount = main.byClass("panel").length;
      out.textAfter = text(panel);
      out.loadingAfter = loadingStrips(main);
    },
    /* A poll whose panel read failed keeps the old content and shows the error on the panel. */
    async serverPollFails() {
      const server = await load("pages/server.js");
      const main = root();
      server.renderServer(main, "SRV1", "cpu", {});
      await settle();
      const panel = main.byClass("panel")[0];
      state.readFail = true;
      server.renderServer(main, "SRV1", "cpu", { poll: true });
      await settle();
      out.samePanel = main.byClass("panel")[0] === panel;
      out.stillShowsOld = text(panel).includes("value-1");
      out.errors = refreshErrors(panel);
      out.loading = loadingStrips(main);
      state.readFail = false;
      state.readVersion = 3;
      server.renderServer(main, "SRV1", "cpu", { poll: true });
      await settle();
      out.errorsAfterRecovery = refreshErrors(main).length;
      out.textAfterRecovery = text(panel);
    },
    /* The reader's own navigation (the same tab again, as a click or a range change does) draws a fresh grid with a loading strip. */
    async serverNavigation() {
      const server = await load("pages/server.js");
      const main = root();
      server.renderServer(main, "SRV1", "cpu", {});
      await settle();
      const panel = main.byClass("panel")[0];
      const g = gate();
      state.readGate = g.p;
      server.renderServer(main, "SRV1", "cpu", {});
      await settle();
      out.samePanel = main.byClass("panel")[0] === panel;
      out.loadingWhileReading = loadingStrips(main);
      g.release();
      await settle();
      out.loadingAfter = loadingStrips(main);
    },
  };
  if (!scenarios[scenario]) throw new Error("unknown scenario " + scenario);
  await scenarios[scenario]();
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
console.log(JSON.stringify(out));
process.exit(0);
