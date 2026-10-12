/* Runs the web viewer's shipped charts.js (renderLineChart, with util.js) against a stand-in DOM and counts the line, area and dot
   marks drawn across a collection gap (#5644 W7), printing the counts as one line of JSON.
   WebChartGapBehaviourTests starts it as
       node web-chart-gap-harness.mjs <path to wwwroot/js>
   The DOM is a node tree of plain objects; a chart's marks are the svg children it appends. */
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
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parent = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, listener) {
    (this.listeners[type] = this.listeners[type] || []).push(listener);
  }
  setPointerCapture() {}
  focus() {}
  get offsetWidth() { return 0; }
  get offsetHeight() { return 0; }
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 1000, height: 320 };
  }
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  body: new FakeNode("body"),
};
globalThis.location = { hash: "#/server/A/cpu" };

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "chart-gap-")));
let charts;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const cls = (n) => String(n.className || n.attrs.class || "");
const marks = (host) => ({
  lines: find(host, (n) => n.tag === "polyline" && cls(n).includes("series-line") && !cls(n).includes("overlay")).length,
  overlays: find(host, (n) => n.tag === "polyline" && cls(n).includes("series-line-overlay")).length,
  areas: find(host, (n) => n.tag === "polygon" && cls(n).includes("series-area")).length,
  dots: find(host, (n) => n.tag === "circle" && cls(n).includes("series-dot")).length,
  bars: find(host, (n) => n.tag === "rect" && cls(n).includes("series-bar")).length,
});
/* The x of every vertex of every line, so a test can show no segment spans the gap. */
const lineXs = (host) =>
  find(host, (n) => n.tag === "polyline" && cls(n).includes("series-line") && !cls(n).includes("overlay")).map((n) =>
    n.attrs.points.split(" ").map((p) => Number(p.split(",")[0]))
  );

const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const iso = (ms) => new Date(ms).toISOString().slice(0, 19);
/* minutes: offsets from T0; each row carries a, b, and c (c only where `withC` says so). */
const rowsAt = (minutes) => minutes.map((m, i) => ({ t: iso(T0 + m * MIN), a: 10 + (i % 7), b: 20 + (i % 5), c: 5 + (i % 3) }));
const seq = (from, to, step = 1) => {
  const o = [];
  for (let m = from; m <= to; m += step) o.push(m);
  return o;
};
const spec = (minutes, extra) => ({
  points: rowsAt(minutes),
  xKey: "t",
  series: [
    { key: "a", label: "A", color: "#f00" },
    { key: "b", label: "B", color: "#0f0" },
  ],
  ...extra,
});
const draw = (minutes, extra) => charts.renderLineChart(spec(minutes, extra));

/* 60 one-minute buckets, then a 7 hour silence, then 60 more. */
const withGap = seq(0, 59).concat(seq(60 * 7 + 59, 60 * 7 + 118));
const noGap = seq(0, 119);

const out = {};
out.noGapLine = marks(draw(noGap));
out.gapLine = marks(draw(withGap));
out.gapLineXs = lineXs(draw(withGap, { series: [{ key: "a", label: "A", color: "#f00" }] }));
out.gapArea = marks(draw(withGap, { mode: "area" }));
out.gapStacked = marks(draw(withGap, { mode: "stacked" }));
out.noGapStacked = marks(draw(noGap, { mode: "stacked" }));
out.gapStackedBar = marks(draw(withGap, { mode: "stacked-bar" }));
out.noGapStackedBar = marks(draw(noGap, { mode: "stacked-bar" }));
out.gapOverlay = marks(draw(withGap, { series2: { key: "c", label: "C", color: "#00f" } }));
out.noGapOverlay = marks(draw(noGap, { series2: { key: "c", label: "C", color: "#00f" } }));
/* a gap that leaves one lone bucket at the end draws that bucket as a dot, and the stretch before it as a line */
out.loneAfterGap = marks(draw(seq(0, 59).concat([60 * 7]), { series: [{ key: "a", label: "A", color: "#f00" }] }));
/* a single bucket and two buckets never break */
out.single = marks(draw([0]));
out.two = marks(draw([0, 600]));
/* a 4 minute miss in one-minute buckets is under the 5 minute floor and stays connected; a 6 minute miss breaks */
out.smallMiss = marks(draw(seq(0, 30).concat(seq(34, 60)), { series: [{ key: "a", label: "A", color: "#f00" }] }));
out.bigMiss = marks(draw(seq(0, 30).concat(seq(37, 60)), { series: [{ key: "a", label: "A", color: "#f00" }] }));
/* a 15 minute collector with one missed cycle (30 minutes) stays connected: 3x the median is 45 minutes */
out.slowCollector = marks(draw(seq(0, 300, 15).concat(seq(330, 600, 15)), { series: [{ key: "a", label: "A", color: "#f00" }] }));
out.slowCollectorBreak = marks(draw(seq(0, 300, 15).concat(seq(420, 600, 15)), { series: [{ key: "a", label: "A", color: "#f00" }] }));

/* The threshold is per series (#5644 review): a 15 minute overlay on 1 minute buckets is one line when it has no gap of its own,
   and breaks when it does. The overlay c is set only at every 15th minute. */
const sparseC = (minutes) => minutes.map((m, i) => ({ t: iso(T0 + m * MIN), a: 10 + (i % 7), c: m % 15 === 0 ? 5 + (i % 3) : null }));
const drawSparse = (minutes) =>
  charts.renderLineChart({ points: sparseC(minutes), xKey: "t", series: [{ key: "a", label: "A", color: "#f00" }], series2: { key: "c", label: "C", color: "#00f" } });
out.coarseOverlayNoGap = marks(drawSparse(noGap));
out.coarseOverlayGap = marks(drawSparse(withGap));
/* a coarse PRIMARY series next to a fine one on the same rows */
out.coarsePrimary = marks(charts.renderLineChart({
  points: sparseC(noGap).map((r) => ({ ...r, b: 1 })),
  xKey: "t",
  series: [{ key: "a", label: "A", color: "#f00" }, { key: "c", label: "C", color: "#0f0" }],
}));

out.threshold = {
  empty: String(charts.gapThresholdMs([])),
  one: String(charts.gapThresholdMs([0])),
  two: String(charts.gapThresholdMs([0, 600 * MIN])),
  oneMinute: charts.gapThresholdMs(noGap.map((m) => m * MIN)),
  fifteenMinute: charts.gapThresholdMs(seq(0, 300, 15).map((m) => m * MIN)),
};
out.split = charts.splitAtGaps([0, 1, 2, 3, 4], [0, 1, 2, 100, 101], 10).map((r) => r.join(","));

fs.rmSync(scratch, { recursive: true, force: true });
console.log(JSON.stringify(out));
