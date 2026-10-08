/* Runs the web viewer's shipped charts.js (renderLineChart, renderScatterChart, renderBarChart, zoomableLineChart, with util.js) against a
   stand-in DOM that has a ResizeObserver and a frame queue, resizes the charts' boxes, and prints what was drawn as one line
   of JSON. WebChartWidthBehaviourTests starts it as
       node web-chart-width-harness.mjs <path to wwwroot/js>
   The DOM is a node tree of plain objects that keeps the listeners it is given. A chart's plot box (class chart-plot) is
   given a width by the harness; the SVG inside it reports the width and height its attributes carry, as a browser lays an
   absolutely positioned SVG with an inline size out, so a pointer x is measured against the live drawn width. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

let svgCreated = 0;
class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.listeners = {};
    this.isConnected = false;
    this.hostWidth = 0;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  replaceChild(next, old) {
    const i = this.children.indexOf(old);
    if (i >= 0) this.children[i] = next;
    return old;
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
  /* The box a reader sees: an svg reports none (it is clipped by its host), any other node its host width. */
  get clientWidth() {
    return this.tag === "svg" ? 0 : this.hostWidth;
  }
  getBoundingClientRect() {
    if (this.tag === "svg") return { left: 0, top: 0, width: Number(this.attrs.width), height: Number(this.attrs.height) };
    return { left: 0, top: 0, width: this.hostWidth, height: 300 };
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
  createElementNS: (ns, tag) => {
    if (tag === "svg") svgCreated++;
    return new FakeNode(tag);
  },
  createTextNode: (text) => new FakeNode("#text", text),
};
globalThis.location = { hash: "#/server/A/waits" };
let fetchCalls = 0;
globalThis.fetch = () => {
  fetchCalls++;
  throw new Error("a redraw must not fetch");
};

const observers = [];
globalThis.ResizeObserver = class {
  constructor(cb) {
    this.cb = cb;
    this.targets = [];
    this.disconnects = 0;
    observers.push(this);
  }
  observe(target) {
    this.targets.push(target);
  }
  disconnect() {
    this.disconnects++;
  }
};
const frames = [];
globalThis.requestAnimationFrame = (f) => {
  frames.push(f);
  return frames.length;
};
globalThis.cancelAnimationFrame = (id) => {
  frames[id - 1] = null;
};
const flush = () => {
  const queued = frames.splice(0);
  queued.forEach((f) => f && f());
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "chart-width-"));
let charts;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const MIN = 60000;
const iso = (ms) => new Date(ms).toISOString().slice(0, 19);
const points = Array.from({ length: 101 }, (_, i) => ({ t: iso(T0 + i * MIN), v: i, w: 100 - i }));
const specFor = (extra = {}) => ({
  points,
  xKey: "t",
  series: [{ key: "v", label: "v", color: "#fff" }],
  windowStart: T0,
  windowEnd: T0 + 100 * MIN,
  ...extra,
});
const DAY_H = 48;
const longPoints = Array.from({ length: 49 }, (_, i) => ({ t: iso(T0 + i * 60 * MIN), v: i }));
const longSpec = (extra = {}) => ({ points: longPoints, xKey: "t", series: [{ key: "v", label: "v", color: "#fff" }], windowStart: T0, windowEnd: T0 + DAY_H * 60 * MIN, ...extra });

const plotHostOf = (chart) => find(chart, (n) => String(n.className) === "chart-plot")[0];
const observerOf = (host) => observers.find((o) => o.targets.includes(host));
/* Puts the chart on the page with this box width: the observer reports it, as the browser does once it lays the chart out. */
const mount = (chart, width) => {
  const host = plotHostOf(chart);
  host.isConnected = true;
  host.hostWidth = width;
  observerOf(host).cb([{ contentRect: { width, height: 300 } }]);
  return host;
};
const resize = (chart, width, height = 300) => {
  const host = plotHostOf(chart);
  host.hostWidth = width;
  observerOf(host).cb([{ contentRect: { width, height } }]);
};
const rootOf = (chart) => find(chart, (n) => n.tag === "svg")[0];
const geometry = (chart) => {
  const root = rootOf(chart);
  const vb = root.attrs.viewBox.split(" ").map(Number);
  const style = root.attrs.style || "";
  const cssW = Number((/width:([\d.]+)px/.exec(style) || [])[1]);
  const cssH = Number((/height:([\d.]+)px/.exec(style) || [])[1]);
  const transformed = find(root, (n) => "transform" in n.attrs || "transform" in (n.style || {})).length;
  return {
    vbW: vb[2],
    vbH: vb[3],
    cssW,
    cssH,
    xScale: cssW / vb[2],
    yScale: cssH / vb[3],
    aspect: root.attrs.preserveAspectRatio || null,
    transformed,
    texts: find(root, (n) => n.tag === "text").length,
  };
};
/* The x-axis tick labels: the axis texts on the baseline row that are not the unit caption. */
const xLabels = (chart) =>
  find(rootOf(chart), (n) => n.tag === "text" && n.attrs.y === String(300 - 8) && n.attrs.class !== "axis-unit").map((n) => ({
    x: Number(n.attrs.x),
    anchor: n.attrs["text-anchor"],
    text: n.textContent,
  }));
/* The least gap in px between neighbouring labels. The width of a label is this test's own estimate, 11px text at 0.7 em per
   character: 17 % wider than the 0.6 em (6.6 px a character) the shipped code plans with, so a code estimate that is too tight
   fails here instead of agreeing with itself. */
const TEST_CHAR_PX = 11 * 0.7;
const minGap = (labels) => {
  const spans = labels
    .map((l) => {
      const w = l.text.length * TEST_CHAR_PX;
      const left = l.anchor === "start" ? l.x : l.anchor === "end" ? l.x - w : l.x - w / 2;
      return [left, left + w];
    })
    .sort((a, b) => a[0] - b[0]);
  let least = Infinity;
  for (let i = 1; i < spans.length; i++) least = Math.min(least, spans[i][0] - spans[i - 1][1]);
  return { least, first: spans[0][0], last: spans[spans.length - 1][1] };
};
const axisTexts = (chart) => find(rootOf(chart), (n) => n.tag === "text").map((n) => n.textContent);

const out = {};

/* 1. the drawn width equals the box width and the scale is one to one, at both widths, for both charts */
out.line = {};
out.scatter = {};
const scatterSpec = () => ({
  items: [1, 2, 3, 4, 5].map((i) => ({ label: "g" + i, x: i * 12345, y: i * 7, drill: null })),
  formatX: (v) => String(v) + " ms",
  formatY: (v) => String(v),
  unitX: "ms",
  unitY: "n",
});
for (const w of [600, 2000]) {
  const c = charts.renderLineChart(specFor());
  mount(c, w);
  out.line[w] = geometry(c);
  const s = charts.renderScatterChart(scatterSpec());
  mount(s, w);
  out.scatter[w] = geometry(s);
}
/* before it is mounted: the default */
out.unmounted = geometry(charts.renderLineChart(specFor()));
out.unmountedScatter = geometry(charts.renderScatterChart(scatterSpec()));
out.unmountedBar = geometry(charts.renderBarChart({ items: [{ label: "a", value: 1 }] }));

/* 2. a resize redraws once, keeps the zoom window, the hidden series and the annotations, and never fetches */
const scope = charts.chartZoomScope(4);
const two = { points, xKey: "t", series: [{ key: "v", label: "v", color: "#fff" }, { key: "w", label: "w", color: "#0f0" }], windowStart: T0, windowEnd: T0 + 100 * MIN };
charts.setChartHidden("r1", scope, ["w"]);
const ann = [{ source: "s", displayName: "Events", events: [{ ts: iso(T0 + 30 * MIN), label: "e" }] }];
const zc = charts.zoomableLineChart({ ...two, annotations: ann }, "r1", scope);
const polylines = (chart) => find(chart, (n) => n.tag === "polyline" && String(n.attrs.class).includes("series-line")).length;
const annLines = (chart) => find(chart, (n) => String(n.attrs.class) === "annotation-line").length;
const chipOf = (chart) => find(chart, (n) => String(n.className).includes("zoom-chip")).length;
mount(zc, 600);
const dragBetween = (chart, width, fromMin, toMin) => {
  const overlay = find(chart, (n) => (n.listeners.pointerdown || []).length)[0];
  const x = (m) => 58 + (m / 100) * (width - 74);
  overlay.listeners.pointerdown[0]({ button: 0, clientX: x(fromMin), pointerId: 1 });
  overlay.listeners.pointerup[0]({ clientX: x(toMin) });
};
dragBetween(zc, 600, 20, 40);
const zoomAt600 = charts.getChartZoom("r1", scope);
out.zoomHeldBefore = zoomAt600 ? [(zoomAt600.from - T0) / MIN, (zoomAt600.to - T0) / MIN] : null;
/* the brush redraws the zoomable host; look the chart up again, and re-mount it */
const zc2 = zc;
const hostAfterZoom = plotHostOf(zc2);
hostAfterZoom.isConnected = true;
hostAfterZoom.hostWidth = 600;
observerOf(hostAfterZoom).cb([{ contentRect: { width: 600, height: 300 } }]);
const before = { labels: axisTexts(zc2), polylines: polylines(zc2), ann: annLines(zc2), chip: chipOf(zc2), svgs: svgCreated };
const xl600 = xLabels(zc2);
resize(zc2, 2000);
const queuedAfterResize = frames.filter(Boolean).length;
resize(zc2, 2000.4); // under a pixel from the pending width: nothing more is queued
resize(zc2, 2000, 640); // a height-only change: nothing more is queued
const queuedAfterNoise = frames.filter(Boolean).length;
const svgsBeforeFlush = svgCreated;
flush();
const xl2000 = xLabels(zc2);
const zoomAfter = charts.getChartZoom("r1", scope);
out.resize = {
  queuedAfterResize,
  queuedAfterNoise,
  redrawsOnResize: svgCreated - svgsBeforeFlush,
  drawnOnlyAtFlush: svgsBeforeFlush === before.svgs,
  geometry: geometry(zc2),
  polylines: [before.polylines, polylines(zc2)],
  annotations: [before.ann, annLines(zc2)],
  chip: [before.chip, chipOf(zc2)],
  zoomKept: !!zoomAfter && zoomAfter.from === zoomAt600.from && zoomAfter.to === zoomAt600.to,
  firstLabel: [xl600[0].text, xl2000[0].text],
  lastLabel: [xl600[xl600.length - 1].text, xl2000[xl2000.length - 1].text],
  ticks: [xl600.length, xl2000.length],
  fetchCalls,
};
/* a second resize of the same size draws nothing */
resize(zc2, 2000);
flush();
out.resize.redrawsOnSameSize = svgCreated - svgsBeforeFlush - 1;

/* 3. the observer lets go when the chart leaves the page */
const gone = charts.renderLineChart(specFor());
const goneHost = mount(gone, 800);
const goneObs = observerOf(goneHost);
resize(gone, 900); // queued
goneHost.isConnected = false; // the panel re-rendered: the chart is out of the page
const svgsBeforeGone = svgCreated;
flush();
out.removed = { disconnects: goneObs.disconnects, redrawsAfterRemoval: svgCreated - svgsBeforeGone };
goneObs.cb([{ contentRect: { width: 0, height: 0 } }]);
out.removed.disconnectsAfterFinalReport = goneObs.disconnects;
const refreshed = [];
for (let i = 0; i < 5; i++) {
  const c = charts.zoomableLineChart(specFor(), "leak" + i, scope);
  mount(c, 700);
  refreshed.push(c);
}
refreshed.forEach((c) => {
  plotHostOf(c).isConnected = false;
  observerOf(plotHostOf(c)).cb([{ contentRect: { width: 0, height: 0 } }]);
});
out.leak = refreshed.map((c) => observerOf(plotHostOf(c)).disconnects);

/* 4. hover and the zoom drag land on the same data point before and after a resize */
const hover = (chart, width, fraction) => {
  const overlay = find(chart, (n) => (n.listeners.mousemove || []).length)[0];
  overlay.listeners.mousemove[0]({ clientX: 58 + fraction * (width - 74) });
  const tip = find(chart, (n) => String(n.className) === "chart-tooltip")[0];
  return tip.children[0].textContent;
};
const hc = charts.zoomableLineChart(specFor(), "h1", scope);
mount(hc, 600);
const h600 = hover(hc, 600, 0.372);
resize(hc, 2000);
flush();
const h2000 = hover(hc, 2000, 0.372);
out.hover = [h600, h2000, hover(hc, 2000, 0.372 + 0.004)];
const dc = charts.zoomableLineChart(specFor(), "d1", scope);
mount(dc, 600);
resize(dc, 2000);
flush();
dragBetween(dc, 2000, 20, 40);
const dz = charts.getChartZoom("d1", scope);
out.dragAfterResize = dz ? [(dz.from - T0) / MIN, (dz.to - T0) / MIN] : null;
const sc = charts.zoomableLineChart(specFor(), "d2", scope);
mount(sc, 2000);
{
  const overlay = find(sc, (n) => (n.listeners.pointerdown || []).length)[0];
  const x0 = 58 + (20 / 100) * (2000 - 74);
  overlay.listeners.pointerdown[0]({ button: 0, clientX: x0, pointerId: 1 });
  overlay.listeners.pointerup[0]({ clientX: x0 + 7 });
  out.sevenPx = charts.getChartZoom("d2", scope) !== null;
  overlay.listeners.pointerdown[0]({ button: 0, clientX: x0, pointerId: 1 });
  overlay.listeners.pointerup[0]({ clientX: x0 + 9 });
  out.ninePx = charts.getChartZoom("d2", scope) !== null;
}

/* 5. the x tick labels do not overlap at 360 and 600 px, and a wide panel gets more of them */
out.ticks = {};
for (const w of [360, 600, 2000]) {
  const shortDay = charts.renderLineChart(specFor());
  mount(shortDay, w);
  const dated = charts.renderLineChart(longSpec());
  mount(dated, w);
  const overlay = charts.renderLineChart(longSpec({ series2: { key: "v", label: "o", color: "#0f0" } }));
  mount(overlay, w);
  const a = xLabels(shortDay);
  const b = xLabels(dated);
  const c = xLabels(overlay);
  out.ticks[w] = {
    shortCount: a.length,
    datedCount: b.length,
    overlayCount: c.length,
    shortGap: minGap(a).least,
    datedGap: minGap(b).least,
    overlayGap: minGap(c).least,
    datedLabel: b[0].text,
    datedFirst: minGap(b).first,
    datedLast: minGap(b).last,
    plotRight: w - 16,
  };
}
const sw = {};
for (const w of [360, 600]) {
  const s = charts.renderScatterChart(scatterSpec());
  mount(s, w);
  const labels = xLabels(s).filter((l) => l.text.endsWith(" ms"));
  sw[w] = { count: labels.length, gap: minGap(labels).least };
}
out.scatterTicks = sw;

/* 6. no two x labels read the same, at any width and window (a brush-zoomed span can be a few minutes or seconds wide) */
const windowChart = (startMs, spanMs, width) => {
  const c = charts.renderLineChart({
    points: [0, 1].map((i) => ({ t: iso(startMs + i * (spanMs - 1000)), v: i })),
    xKey: "t",
    series: [{ key: "v", label: "v", color: "#fff" }],
    windowStart: startMs,
    windowEnd: startMs + spanMs,
  });
  mount(c, width);
  return xLabels(c).map((l) => l.text);
};
const THIRTY_DAYS = 30 * 24 * 60 * MIN;
const monthTicks = windowChart(T0 + 40000, THIRTY_DAYS, 2000);
const evenly = Array.from({ length: 11 }, (_, i) => {
  const t = T0 + 40000 + (THIRTY_DAYS * i) / 10;
  return new Date(t).toLocaleString([], { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" });
});
out.distinctLabels = {
  // the start is 40 s past the minute, so the end of each window is not on a whole minute either
  sixMinutes2000: windowChart(T0 + 40000, 6 * MIN, 2000),
  sixMinutes1000: windowChart(T0 + 40000, 6 * MIN, 1000),
  twoMinutes2000: windowChart(T0 + 40000, 2 * MIN, 2000),
  ninetySeconds600: windowChart(T0 + 40000, 90000, 600),
  ninetySeconds2000: windowChart(T0 + 40000, 90000, 2000),
  fortyFiveSeconds2000: windowChart(T0 + 40000, 45000, 2000),
  thirtyDays: monthTicks,
  thirtyDaysEvenly: evenly,
};

/* 7. the tooltip is clamped to the visible box: in a 260 px host the svg is 300 px wide and clipped */
{
  const c = charts.zoomableLineChart(specFor(), "tip1", scope);
  mount(c, 260);
  const tip = find(c, (n) => String(n.className) === "chart-tooltip")[0];
  tip.offsetWidth = 120;
  const overlay = find(c, (n) => (n.listeners.mousemove || []).length)[0];
  overlay.listeners.mousemove[0]({ clientX: 270 });
  out.tooltip = { left: parseFloat(tip.style.left), visibleWidth: 260, tooltipWidth: 120 };
}

/* 8. the scatter keeps every gridline and labels fewer ticks on a narrow plot, always the first and the last */
{
  const verticalGrid = (c) => find(rootOf(c), (n) => n.tag === "line" && n.attrs.class === "grid-line" && n.attrs.x1 === n.attrs.x2).length;
  const out8 = {};
  for (const w of [360, 2000]) {
    const s = charts.renderScatterChart(scatterSpec());
    mount(s, w);
    const labels = xLabels(s).filter((l) => l.text.endsWith(" ms"));
    out8[w] = { gridlines: verticalGrid(s), labels: labels.map((l) => l.text) };
  }
  /* six ticks (0, 2 ... 10) with a stride of two: the tick before the last gives way to the last one */
  const six = charts.renderScatterChart({ ...scatterSpec(), items: [1, 2, 3].map((i) => ({ label: "g" + i, x: i * 3.3, y: i, drill: null })) });
  mount(six, 360);
  out8.six = { gridlines: verticalGrid(six), labels: xLabels(six).filter((l) => l.text.endsWith(" ms")).map((l) => l.text) };
  out.scatterGrid = out8;
}

/* 9. a poll builds a chart once: the second build of a chart already measured is drawn at that width and is not redrawn */
{
  const buildsFor = (make) => {
    const first = make();
    mount(first, 1234);
    const before = svgCreated;
    const second = make();
    mount(second, 1234);
    return { builds: svgCreated - before, vbW: geometry(second).vbW };
  };
  out.poll = {
    keyedLine: buildsFor(() => charts.zoomableLineChart(specFor(), "poll1", scope)),
    widthKeyLine: buildsFor(() => charts.renderLineChart(specFor({ widthKey: "composed|0|p" }))),
    keyedScatter: buildsFor(() => charts.renderScatterChart({ ...scatterSpec(), widthKey: "composed|1|s" })),
    unkeyedLine: buildsFor(() => charts.renderLineChart(specFor())),
  };
}

/* 10. the ranked bar chart is drawn at its box's width like the line and scatter charts: one unit per pixel at 600 and 2,000 px,
   a height that follows the bar count alone, labels that fit their gutter and values that fit the right edge. A glyph is
   BAR_TEST_CHAR_PX wide (12px text at 0.6 em); the browser check measures the real font. */
{
  const BAR_TEST_CHAR_PX = 12 * 0.6;
  const barItems = [
    { label: "a_rather_long_database_name_that_is_cut_at_thirty", value: 1234567, drill: [{ dimension: "d", value: "x" }] },
    { label: "master", value: 900000 },
    { label: "tempdb", value: 450000 },
    { label: "msdb", value: 12 },
    { label: "", value: 3 },
  ];
  const barSpec = (extra = {}) => ({ items: barItems, formatValue: (v) => v.toLocaleString("en-US") + " ms", thresholds: [600000], onSelect: () => {}, ...extra });
  const barGeometry = (chart) => {
    const root = rootOf(chart);
    const g = geometry(chart);
    const labels = find(root, (n) => n.tag === "text" && n.attrs.class === "bar-label");
    const values = find(root, (n) => n.tag === "text" && n.attrs.class === "bar-value");
    const tracks = find(root, (n) => n.tag === "rect" && String(n.attrs.class).startsWith("bar-track"));
    const textW = (n) => n.textContent.length * BAR_TEST_CHAR_PX;
    return {
      ...g,
      rows: labels.length,
      labelLeftMin: Math.min(...labels.map((n) => Number(n.attrs.x) - textW(n))),
      labelRight: Math.max(...labels.map((n) => Number(n.attrs.x))),
      trackLeft: Number(tracks[0].attrs.x),
      trackRight: Number(tracks[0].attrs.x) + Number(tracks[0].attrs.width),
      valueRightMax: Math.max(...values.map((n) => Number(n.attrs.x) + textW(n))),
      longestLabel: Math.max(...labels.map((n) => n.textContent.length)),
      firstLabel: labels[0].textContent,
      thresholds: find(root, (n) => String(n.attrs.class) === "threshold-line").length,
    };
  };
  out.bar = {};
  for (const w of [360, 600, 2000]) {
    const c = charts.renderBarChart(barSpec());
    mount(c, w);
    out.bar[w] = barGeometry(c);
  }
  const rc = charts.renderBarChart(barSpec());
  mount(rc, 600);
  const barBuilds = svgCreated;
  resize(rc, 2000);
  const queued = frames.filter(Boolean).length;
  flush();
  out.barResize = { queued, redraws: svgCreated - barBuilds, vbW: geometry(rc).vbW };
  /* the number of bars alone sets the height */
  const few = charts.renderBarChart(barSpec({ items: barItems.slice(0, 2) }));
  mount(few, 1500);
  out.barFew = geometry(few);
}

/* 11. a poll builds a bar chart once, like the other two */
{
  const buildsFor = (make) => {
    const first = make();
    mount(first, 1234);
    const before = svgCreated;
    const second = make();
    mount(second, 1234);
    return { builds: svgCreated - before, vbW: geometry(second).vbW };
  };
  out.poll.keyedBar = buildsFor(() => charts.renderBarChart({ items: [{ label: "a", value: 2 }, { label: "b", value: 1 }], widthKey: "composed|2|b" }));
  out.poll.unkeyedBar = buildsFor(() => charts.renderBarChart({ items: [{ label: "a", value: 2 }, { label: "b", value: 1 }] }));
}

/* 12. the scatter's end labels stay inside the plot and its x unit caption is clear of the label row */
{
  out.scatterEdges = {};
  for (const w of [600, 2000]) {
    const c = charts.renderScatterChart(scatterSpec());
    mount(c, w);
    const texts = find(rootOf(c), (n) => n.tag === "text");
    const labels = texts.filter((n) => n.attrs.y === String(300 - 8) && n.attrs.class !== "axis-unit");
    const last = labels[labels.length - 1];
    const cap = texts.find((n) => n.attrs.class === "axis-unit" && n.textContent === "ms");
    out.scatterEdges[w] = {
      plotRight: w - 16,
      firstAnchor: labels[0].attrs["text-anchor"],
      firstX: Number(labels[0].attrs.x),
      lastAnchor: last.attrs["text-anchor"],
      lastX: Number(last.attrs.x),
      captionY: Number(cap.attrs.y),
      labelRowY: 300 - 8,
    };
  }
}
console.log(JSON.stringify(out));
