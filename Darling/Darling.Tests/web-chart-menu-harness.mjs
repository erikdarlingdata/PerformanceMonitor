/* Runs the web viewer's shipped charts.js (the chart menu on zoomableLineChart / renderLineChart) against a stand-in DOM,
   opens the menu, clicks its items, and prints what the menu offered as one line of JSON.
   WebChartMenuBehaviourTests starts it as
       node web-chart-menu-harness.mjs <path to wwwroot/js> */
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
  setPointerCapture() {}
  focus() {}
  click() {}
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
};
globalThis.location = { hash: "#/server/A/waits" };
let blobParts = null;
globalThis.Blob = class { constructor(parts) { blobParts = parts; } };
globalThis.URL.createObjectURL = () => "blob:stub";
globalThis.URL.revokeObjectURL = () => {};

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "chart-menu-")));
let charts;
try {
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "charts.js", "grid-tools.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  /* The CSV item loads grid-tools.js on first click; load it now, while the scratch copy exists. */
  await import(pathToFileURL(path.join(scratch, "grid-tools.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const points = Array.from({ length: 11 }, (_, i) => ({ t: new Date(T0 + i * MIN).toISOString().slice(0, 19), a: i, b: i * 2 }));
const series = [{ key: "a", label: "Alpha", color: "#fff" }, { key: "b", label: "Beta, two", color: "#0ff" }];
const spec = (extra) => ({ points, xKey: "t", series, windowStart: T0, windowEnd: T0 + 10 * MIN, ...extra });
const btn = (host) => find(host, (n) => n.tag === "button" && n.attrs["aria-label"] === "Chart menu")[0];
const items = (host) => find(host, (n) => n.attrs.role === "menuitem");
const openMenu = (host) => {
  btn(host).listeners.click[0]();
  return items(host);
};
const labels = (list) => list.map((n) => n.textContent);

const out = {};
const scope = charts.chartZoomScope(4);

const a = charts.zoomableLineChart(spec({ title: "Wait trend", source: { read: "get_wait_stats", params: { hours: 4, server: "A" } } }), "m1", scope);
out.hasButton = !!btn(a);
out.itemsUnzoomed = labels(openMenu(a));
out.expanded = btn(a).attrs["aria-expanded"];
btn(a).listeners.click[0]();
const chartDiv = find(a, (n) => String(n.className) === "chart")[0];
let prevented = false;
chartDiv.listeners.contextmenu[0]({ preventDefault: () => (prevented = true), clientX: 10, clientY: 10 });
out.contextMenuItems = items(a).length;
out.contextMenuPrevented = prevented;
btn(a).listeners.click[0]();

openMenu(a).find((n) => n.textContent === "Show Data Source").listeners.click[0]();
out.sourceText = find(a, (n) => String(n.className) === "chart-source")[0].textContent;

openMenu(a).find((n) => n.textContent.startsWith("Export Data to CSV")).listeners.click[0]();
await new Promise((r) => setTimeout(r, 50));
out.csv = blobParts ? String(blobParts[1]).split("\r\n") : null;

const z = charts.zoomableLineChart(spec({ title: "Wait trend" }), "m2", scope);
out.noSourceItems = labels(openMenu(z));
btn(z).listeners.click[0]();
charts.setChartZoom("m3", scope, T0 + 2 * MIN, T0 + 6 * MIN);
const zz = charts.zoomableLineChart(spec({ title: "Wait trend" }), "m3", scope);
out.zoomedItems = labels(openMenu(zz));
items(zz).find((n) => n.textContent === "Reset zoom").listeners.click[0]();
out.zoomAfterReset = charts.getChartZoom("m3", scope) !== null;
out.menuAfterReset = labels(openMenu(zz));

fs.rmSync(scratch, { recursive: true, force: true });
console.log(JSON.stringify(out));
process.exit(0);
