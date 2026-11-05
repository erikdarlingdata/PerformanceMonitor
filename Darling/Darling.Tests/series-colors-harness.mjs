/* #7535, #7563: which color each series of a chart gets. Scenario "fileio" runs the server page's I/O tab (File I/O Latency, a Read
   chart and a Write chart) from wwwroot/js/pages/server-tabs.js with the shipped charts.js and util.js against a scripted /api/read
   answer, and prints each chart's legend as [name, color]. Scenario "compose" runs the Custom View pivot (compose.js
   pivotTimeSeries) on the same rows in different orders and prints the series and colors of each. SeriesColorBehaviourTests starts it as
       node series-colors-harness.mjs <path to wwwroot/js> <scenario>
   The whole js tree is copied into a scratch folder first, so a module another change adds needs no edit here. Only the DOM and
   fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag) {
    this.style = {};
    this.classList = {
      add: (c) => { this.className = (this.className + " " + c).trim(); },
      remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); },
      toggle() {},
      contains: () => false,
    };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this.className = "";
  }
  get parentNode() { return this.parent; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 1000, height: 300 }; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/io" };

/* Times are hours back from now, so the 24 hour window the tab asks for always holds them. */
const ago = (h) => new Date(Date.now() - h * 3600000).toISOString().replace("Z", "0000");
const io = (line, h, read, write) => ({ time: ago(h), database_name: line, file_type: line, file_name: null, avg_read_latency_ms: read, avg_write_latency_ms: write });

/* The Read chart's top lines by peak: Sales, Orders, tempdb, Archive, (other). The Write chart's: tempdb, Orders, Archive, (other), Sales.
   Orders and tempdb sit at different places in the two lists, so a color given by a list's own order differs between the charts.
 */
const FILE_IO = [
  io("Sales", 3, 50, 5), io("Sales", 2, 20, 4),
  io("Orders", 3, 40, 60), io("Orders", 2, 10, 30),
  io("tempdb", 3, 30, 90), io("tempdb", 2, 25, 80),
  io("Archive", 3, 5, 20), io("(other)", 3, 2, 10),
];
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const body = name === "get_file_io_trend" ? { server: "a", trend: FILE_IO } : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "series-colors-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")), compose: await load("compose.js") };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
/* A legend entry is a span.item holding a span.swatch (its style carries the color) and the label. */
const legend = (host) =>
  all(host)
    .filter((n) => /\bitem\b/.test(n.className) && n.children.some((c) => /\bswatch\b/.test(c.className)))
    .map((n) => [n.textContent, (/background:(#[0-9a-fA-F]{6})/.exec(n.children[0].attrs.style || "") || [])[1] || null]);

const out = {};
if (scenario === "fileio") {
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "io");
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("a", { hours: 24, label: "last 24 hours" }));
  await flush();
  const panel = all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith("File I/O Latency")));
  /* One legend per chart: the node that holds the swatches and sits closest to them is the legend row itself. */
  const legends = panel ? all(panel).filter((n) => n.children.some((c) => /\bitem\b/.test(c.className) && c.children.some((g) => /\bswatch\b/.test(g.className)))) : [];
  out.charts = legends.map(legend);
} else if (scenario === "compose") {
  const rowsOf = (spec) => spec.map(([bucket, wait, value]) => ({ bucket, wait_type: wait, value }));
  const BASE = [
    ["2026-01-01T00:00", "LCK_M_S", 10], ["2026-01-01T00:00", "WRITELOG", 5], ["2026-01-01T00:00", "CXPACKET", 30],
    ["2026-01-01T01:00", "CXPACKET", 20], ["2026-01-01T01:00", "LCK_M_X", 5], ["2026-01-01T01:00", "SOS_SCHEDULER_YIELD", 40],
    ["2026-01-01T02:00", "LCK_M_S", 20], ["2026-01-01T02:00", "WRITELOG", 5], ["2026-01-01T02:00", "(other)", 500],
  ];
  const shape = (rows, hasResidual) => {
    const r = modules.compose.pivotTimeSeries(rowsOf(rows), ["wait_type"], "ms", hasResidual);
    return { series: r.series.map((s) => [s.label, s.color]), hidden: r.hidden };
  };
  /* The same rows read three ways: as given, reversed, and with the rows inside each bucket in another order. */
  const swapped = [BASE[2], BASE[1], BASE[0], BASE[5], BASE[4], BASE[3], BASE[8], BASE[7], BASE[6]];
  out.base = shape(BASE, true);
  out.reversed = shape([...BASE].reverse(), true);
  out.swapped = shape(swapped, true);
  /* Eleven names that tie at 10, one bigger, and the residual (bigger than all): the cut keeps eight real series by total, then
     name, however the rows arrive, and the residual rides along after them without taking a slot. */
  const TIES = ["K", "B", "J", "A", "H", "C", "F", "D", "I", "E", "G"].map((n) => ["2026-01-01T00:00", n, 10]).concat([["2026-01-01T00:00", "Z", 99], ["2026-01-01T00:00", "(other)", 1000]]);
  out.tiesForward = shape(TIES, true);
  out.tiesReversed = shape([...TIES].reverse(), true);
}
console.log(JSON.stringify(out));
