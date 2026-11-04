/* Runs the web viewer's shipped charts.js (renderLineChart and applyChartZoom) with util.js's xStartMs / setChartDataStart against a
   stand-in DOM, and prints where the first and last plotted points land on the time axis as one line of JSON (#7386).
   WebChartDataStartBehaviourTests starts it as
       node web-chart-data-start-harness.mjs <path to wwwroot/js>
   A point's place is its fraction of the plot width: 0 is the left edge, 1 the right edge. */
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
  replaceChild(next, old) {
    const i = this.children.indexOf(old);
    if (i >= 0) this.children[i] = next;
    return old;
  }
  getBoundingClientRect() {
    if (this.tag === "svg" && this.attrs.width) return { left: 0, top: 0, width: Number(this.attrs.width), height: Number(this.attrs.height) };
    return { left: 0, top: 0, width: 1000, height: 300 };
  }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.location = { hash: "#/server/A/cpu" };

/* While globalThis.__plotW is set, a watched plot box reports that width as the chart is built, as the browser does once it has laid it out. */
globalThis.ResizeObserver = class {
  constructor(cb) { this.cb = cb; }
  observe(target) {
    if (!globalThis.__plotW) return;
    target.isConnected = true;
    this.cb([{ contentRect: { width: globalThis.__plotW, height: 300 } }]);
  }
  disconnect() {}
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "chart-data-start-"));
let charts;
let util;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  util = await import(pathToFileURL(path.join(scratch, "util.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};

const HOUR = 3600000;
const MIN = 60000;
const END = Date.UTC(2026, 10, 3, 12, 0, 0);
const iso = (ms) => new Date(ms).toISOString().slice(0, 19);

/* A line of one point every 15 minutes from `fromMs` to END. */
const rowsFrom = (fromMs) => {
  const rows = [];
  for (let t = fromMs; t <= END; t += 15 * MIN) rows.push({ t: iso(t), v: ((t - fromMs) / MIN) % 90 });
  return rows;
};
const specFor = (rows, extra = {}) => ({
  points: rows,
  xKey: "t",
  series: [{ key: "v", label: "v", color: "#fff" }],
  windowStart: END - 24 * HOUR,
  windowEnd: END,
  ...extra,
});

/* The plot's inner width is the drawn width less the margins (l=58, r=16). */
const PLOT_L = 58;
const xs = (host, width) => {
  const line = find(host, (n) => n.tag === "polyline" && String(n.attrs.class).includes("series-line"))[0];
  if (!line) return null;
  const px = line.attrs.points.split(" ").map((p) => Number(p.split(",")[0]));
  const plotW = width - PLOT_L - 16;
  return { first: (px[0] - PLOT_L) / plotW, last: (px[px.length - 1] - PLOT_L) / plotW, count: px.length };
};
const draw = (spec, width = 1000) => {
  globalThis.__plotW = width === 1000 ? null : width;
  try {
    return xs(charts.renderLineChart(spec), width);
  } finally {
    globalThis.__plotW = null;
  }
};

const out = {};
const lateRows = rowsFrom(END - 3 * HOUR);

// 1. the xStartMs twin: the later of the picked start and the data start, with the floor, the run-in and the 30 minute shortest span
const S = END - 24 * HOUR;
out.pure = {
  late: (util.xStartMs(S, END, END - 3 * HOUR) - S) / MIN,
  older: util.xStartMs(S, END, S - HOUR) === S,
  none: util.xStartMs(S, END, null) === S,
  atEnd: util.xStartMs(S, END, END) === S,
  aMinuteAgo: util.xStartMs(S, END, END - MIN) === S,
};

// 2. no claim from the server: the picked window, as before (data in the right eighth)
util.setChartDataStart(null);
out.noClaim = draw(specFor(lateRows));

// 3. data that begins three hours ago under a 24 hour window: the line starts at the left edge, at 1000 and at 500 px
util.setChartDataStart(END - 3 * HOUR);
out.late = draw(specFor(lateRows));
out.lateNarrow = draw(specFor(lateRows), 500);

// 4. a server with data for the whole range draws as it did without a claim
const fullRows = rowsFrom(S);
util.setChartDataStart(S);
const fullWith = draw(specFor(fullRows));
util.setChartDataStart(null);
out.fullSame = JSON.stringify(fullWith) === JSON.stringify(draw(specFor(fullRows)));
out.fullFirst = fullWith.first;

// 5. a point older than the server's data start (another relation with its own retention) is plotted, not clipped off the left edge
util.setChartDataStart(END - 3 * HOUR);
out.olderPoint = draw(specFor(rowsFrom(END - 10 * HOUR)));

// 6. a zoomed window is exact: the chart over [4 h ago, 1 h ago] puts a point 3 h ago a third of the way in, not at the edge
const zoomRows = rowsFrom(END - 3 * HOUR);
const zoom = { scope: null, from: END - 4 * HOUR, to: END - 1 * HOUR };
const applied = charts.applyChartZoom(specFor(zoomRows), zoom);
out.zoomApplied = { exact: applied.spec.windowExact === true, zoomed: applied.zoomed };
out.zoomed = draw(applied.spec);

// 7. a zoom's window start with the claim withdrawn is the same exact window (the zoomed chart does not depend on the claim)
util.setChartDataStart(null);
out.zoomedNoClaim = draw(applied.spec);

// 8. another page: the claim is dropped, so a chart there keeps the picked window
util.setChartDataStart(END - 3 * HOUR);
util.setChartDataStart(null);
out.dropped = draw(specFor(lateRows));

console.log(JSON.stringify(out));
