/* Runs the web viewer's shipped charts.js against a stand-in DOM and checks the chart menu's "at This Time" items: a
   right-click on a server-tab chart (spec.atTime = { server }) offers Show Active Queries / Blocking / Deadlocks at
   This Time, and each item sets the server's custom range to the time of the drawn point nearest the click +- 30 minutes
   and moves the hash to that tab. Prints the findings as one line of JSON.
   WebChartAtTimeBehaviourTests starts it as
       node web-chart-at-time-harness.mjs <path to wwwroot/js>
   Every .js under js/ and js/pages/ is copied into the scratch directory (a hand-kept list breaks whenever a page
   module is added). The menu checks replace the scratch pages/server.js with a recording stand-in; the range check
   loads the shipped pages/server.js and asks its own resolveCustomRange whether the pair is taken. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.listeners = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(child) { this.children.push(child); return child; }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, listener) { (this.listeners[type] = this.listeners[type] || []).push(listener); }
  removeEventListener() {}
  setPointerCapture() {}
  focus() {}
  click() {}
  get offsetWidth() { return this.attrs.role === "menu" ? 176 : 0; }
  get offsetHeight() { return this.attrs.role === "menu" ? 100 : 0; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 1000, height: 320 }; }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  body: new FakeNode("body"),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.location = { hash: "#/server/A/cpu" };
globalThis.window = globalThis;

const copyAll = (from, to) => {
  fs.mkdirSync(to, { recursive: true });
  for (const e of fs.readdirSync(from, { withFileTypes: true })) {
    if (e.isDirectory()) copyAll(path.join(from, e.name), path.join(to, e.name));
    else if (e.name.endsWith(".js")) fs.copyFileSync(path.join(from, e.name), path.join(to, e.name));
  }
};

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "chart-at-time-")));
let charts;
const out = {};
/* The browser fires no hashchange when the hash is set to the value it already holds, so goToTime raises the event itself
   on the tab already open (window.dispatchEvent). Counted here; the router that listens for it is not under test. */
globalThis.dispatchEvent = () => {
  out.dispatched = (out.dispatched || 0) + 1;
};
try {
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');

  /* The range check first, against the shipped server.js, in a copy of its own (the menu checks below swap that file). */
  try {
    const shipped = path.join(scratch, "shipped");
    fs.mkdirSync(shipped);
    fs.writeFileSync(path.join(shipped, "package.json"), '{ "type": "module" }');
    copyAll(jsDir, shipped);
    const server = await import(pathToFileURL(path.join(shipped, "pages", "server.js")).href);
    const tc = Date.UTC(2026, 0, 1, 12, 0, 0);
    const half = 30 * 60000;
    const r = server.resolveCustomRange(tc - half, tc + half, Date.UTC(2026, 0, 2));
    out.shippedRangeTaken = !r.error && r.hours === 1;
    out.shippedRangeError = r.error || null;
  } catch (e) {
    out.shippedRangeTaken = null;
    out.shippedRangeError = "could not load pages/server.js: " + (e && e.message ? e.message : e);
  }

  /* The menu checks: a copy whose pages/server.js is a recording stand-in for the module the menu items import. */
  copyAll(jsDir, scratch);
  fs.writeFileSync(
    path.join(scratch, "pages", "server.js"),
    'export const calls = []; export function applyCustomRange(server, startMs, endMs, nowMs, { redraw = true } = {}) { calls.push({ server, startMs, endMs, redraw, hash: globalThis.location.hash }); return globalThis.__rangeError || null; }\n'
  );
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  var stub = await import(pathToFileURL(path.join(scratch, "pages", "server.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}

const find = (node, pred, res = []) => {
  if (pred(node)) res.push(node);
  node.children.forEach((c) => find(c, pred, res));
  return res;
};
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const points = Array.from({ length: 11 }, (_, i) => ({ t: new Date(T0 + i * MIN).toISOString().slice(0, 19), a: i }));
const series = [{ key: "a", label: "Alpha", color: "#fff" }];
const spec = (extra) => ({ points, xKey: "t", series, windowStart: T0, windowEnd: T0 + 10 * MIN, ...extra });
const btn = (host) => find(host, (n) => n.tag === "button" && n.attrs["aria-label"] === "Chart menu")[0];
const items = (host) => find(host, (n) => n.attrs.role === "menuitem");
const labels = (host) => items(host).map((n) => n.textContent);
const chartDiv = (host) => find(host, (n) => String(n.className) === "chart")[0];
const rightClick = (host, x) => chartDiv(host).listeners.contextmenu[0]({ preventDefault() {}, clientX: x, clientY: 20 });
const closeMenu = (host) => {
  const m = find(host, (n) => n.attrs.role === "menu")[0];
  if (m) m.listeners.keydown[0]({ key: "Escape", preventDefault() {} });
};
const settle = () => new Promise((r) => setTimeout(r, 60));
const scope = charts.chartZoomScope(4);

/* x = 521 viewBox px is the middle of the plot (58 .. 984) and lands exactly on the point at T0 + 5 min (one point a
   minute), so the clicked time is T0 + 5 min. */
const a = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" } }), "t1", scope);
rightClick(a, 521);
out.rightClickItems = labels(a);
const click = async (host, label) => {
  const item = items(host).find((n) => n.textContent === label);
  if (item) item.listeners.click[0]();
  await settle();
};
const take = () => stub.calls.splice(0, stub.calls.length);

await click(a, "Show Active Queries at This Time");
out.queries = take();
rightClick(a, 521);
await click(a, "Show Blocking at This Time");
out.blocking = take();
/* The hash moved to another tab, so the router's own hashchange rebuilds the page: no event is raised for it. */
out.dispatchedOnTabChange = out.dispatched || 0;
rightClick(a, 521);
await click(a, "Show Deadlocks at This Time");
out.deadlocks = take();
/* The hash was already #/server/A/blocking: setting it again fires nothing, so the event is raised once. */
out.dispatchedOnSameTab = out.dispatched || 0;
out.hashAfter = globalThis.location.hash;
out.t0Plus5 = T0 + 5 * MIN;

/* A range the page refuses is reported on the chart and the hash stays put. */
globalThis.__rangeError = "The end cannot be in the future.";
globalThis.location.hash = "#/server/A/cpu";
rightClick(a, 521);
await click(a, "Show Blocking at This Time");
out.refusedHash = globalThis.location.hash;
out.refusedStatus = find(a, (n) => String(n.className) === "chart-menu-status")[0].textContent;
take();
globalThis.__rangeError = null;

/* The clamp: the plot's edges and beyond them hand the chart's own first and last time. */
const edge = async (host, x, id) => {
  rightClick(host, x);
  await click(host, "Show Active Queries at This Time");
  const c = take()[0];
  return c ? (c.startMs + c.endMs) / 2 : null;
};
const e1 = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" } }), "t2", scope);
out.clampLeft = await edge(e1, -300);
out.clampRight = await edge(e1, 5000);

/* The nearest drawn point: a chart with only two points, at T0 and T0 + 10 min, across the 10-minute window. The menu names
   the time of the point nearest the pointer, the one the hover tooltip names, not the raw pointer time (T0 + 5.85 min at
   x = 600, which no point holds). The exact middle is equidistant from both, so that click may land on either. */
const sparse = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" }, points: [points[0], points[10]] }), "t8", scope);
out.sparseNearLeft = await edge(sparse, 400);
out.sparseNearRight = await edge(sparse, 600);
out.sparseMiddle = await edge(sparse, 521);

/* A click inside the last half hour: a range may not end in the future, so the hour is shifted to end now and still holds the time. */
const realNow = Date.now;
Date.now = () => T0 + 7 * MIN;
const recent = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" } }), "t7", scope);
rightClick(recent, 521);
await click(recent, "Show Blocking at This Time");
Date.now = realNow;
out.nearNow = take()[0] || null;

/* The server name is URL-encoded in the hash. */
const odd = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "Prod/One #1" } }), "t3", scope);
rightClick(odd, 521);
await click(odd, "Show Active Queries at This Time");
out.encodedHash = take().length ? globalThis.location.hash : null;

/* No atTime: the menu is the same as before. */
const plain = charts.zoomableLineChart(spec({ title: "CPU" }), "t4", scope);
rightClick(plain, 521);
out.noAtTimeItems = labels(plain);

/* Opened from the button, not the pointer: no clicked time, no items. */
const viaButton = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" } }), "t5", scope);
btn(viaButton).listeners.click[0]();
out.buttonItems = labels(viaButton);
btn(viaButton).listeners.click[0]();

/* A rebuild of the chart (the 60 s poll) keeps the open menu and its clicked time. */
const k1 = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" } }), "t6", scope);
rightClick(k1, 521);
const k2 = charts.zoomableLineChart(spec({ title: "CPU", atTime: { server: "A" } }), "t6", scope);
out.rebuiltItems = labels(k2);
await click(k2, "Show Blocking at This Time");
const kc = take()[0];
out.rebuiltMiddle = kc ? (kc.startMs + kc.endMs) / 2 : null;

fs.rmSync(scratch, { recursive: true, force: true });
console.log(JSON.stringify(out));
process.exit(0);
