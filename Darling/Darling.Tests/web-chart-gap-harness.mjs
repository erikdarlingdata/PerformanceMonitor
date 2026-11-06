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
  get parentNode() {
    return this.parent || null;
  }
  closest(tag) {
    for (let n = this; n; n = n.parent) if (n.tag === tag) return n;
    return null;
  }
  appendChild(child) {
    if (child.parent) child.parent.children = child.parent.children.filter((x) => x !== child);
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
let slicer;
let panels;
let util;
let tabs;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  slicer = await import(pathToFileURL(path.join(scratch, "time-slicer.js")).href);
  panels = await import(pathToFileURL(path.join(scratch, "panels.js")).href);
  util = await import(pathToFileURL(path.join(scratch, "util.js")).href);
  tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
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

/* #5644 H4b: a series of EVENTS has no cadence. Clusters of rows with minutes to hours between them have a tiny median
   interval, so the 5 minute floor used to cut them into lone dots. They draw one line; a sampled series that went silent
   (above) still breaks. */
const events = [0, 1, 2, 9, 10, 31, 70, 150, 151, 152, 400, 405, 410, 600, 640, 660];
out.eventsLine = marks(draw(events, { series: [{ key: "a", label: "A", color: "#f00" }] }));
out.eventsArea = marks(draw(events, { mode: "area", series: [{ key: "a", label: "A", color: "#f00" }] }));
out.eventsThreshold = String(charts.gapThresholdMs(events.map((m) => m * MIN)));
/* a 1 minute series that is missing a quarter of its samples at random is still sampled: it breaks at the 7 hour silence */
out.jitterGap = marks(draw(withGap.filter((m, i) => i % 5 !== 3), { series: [{ key: "a", label: "A", color: "#f00" }] }));

/* #7508: a series that joins two grains. The Query Store Duration Trend is one point per hour, then one point per interval from the
   seam (routing.raw_from): 22 hourly points, then 90 one-minute points. One threshold for the whole series reads every hourly step as a
   gap once the minute points outnumber them; with the seam each side is judged against its own spacing. */
const seamAt = 22 * 60;
const oneSeries = [{ key: "a", label: "A", color: "#f00" }];
const hourly = seq(0, 21 * 60, 60);
const raw = seq(seamAt, seamAt + 89);
const seamExtra = { series: oneSeries, gapSeamMs: T0 + seamAt * MIN };
out.seamJoined = marks(draw(hourly.concat(raw), seamExtra));
out.seamAbsent = marks(draw(hourly.concat(raw), { series: oneSeries }));
out.seamArea = marks(draw(hourly.concat(raw), { ...seamExtra, mode: "area" }));
/* a 6 hour outage inside the hourly side, and a 20 minute one inside the raw side, each still break the line */
out.seamHourlyOutage = marks(draw(seq(0, 10 * 60, 60).concat(seq(16 * 60, 21 * 60, 60), raw), seamExtra));
out.seamRawOutage = marks(draw(hourly.concat(seq(seamAt, seamAt + 29), seq(seamAt + 50, seamAt + 89)), seamExtra));
/* a seam with every point on one side of it, or a bad one, leaves the plain rule */
out.seamAllRaw = marks(draw(seq(0, 119), { series: oneSeries, gapSeamMs: T0 + 500 * MIN }));
out.seamAllRawOutage = marks(draw(withGap, { series: oneSeries, gapSeamMs: T0 }));
out.seamBad = marks(draw(withGap, { series: oneSeries, gapSeamMs: NaN }));
/* a side too short for a spacing of its own borrows the other side's, so an outage next to it still breaks. Two hourly points, a
   19 hour silence, then one-minute points. And 22 hourly points, then 2 raw points 19 hours apart. */
const silenceEnd = 20 * 60;
out.seamShortHourlyOutage = marks(draw([0, 60].concat(seq(silenceEnd, silenceEnd + 59)), { series: oneSeries, gapSeamMs: T0 + silenceEnd * MIN }));
out.seamShortRawOutage = marks(draw(hourly.concat([seamAt, seamAt + 19 * 60]), seamExtra));
/* neither side can judge: the seam adds nothing */
const bothShort = [0, 60, silenceEnd, silenceEnd + 1].map((m) => m * MIN);
out.seamBothShortIsPlain = charts.gapThresholdMs(bothShort, silenceEnd * MIN) === charts.gapThresholdMs(bothShort);
/* a seamed threshold is a function of a pair's two times; a plain one is a number */
const at = (t, x, y) => (typeof t === "function" ? t(x, y) : t);
out.seamThreshold = {
  plain: charts.gapThresholdMs(noGap.map((m) => m * MIN), null),
  oneSide: charts.gapThresholdMs(noGap.map((m) => m * MIN), 500 * MIN),
  hourlySide: at(charts.gapThresholdMs(hourly.concat(raw).map((m) => m * MIN), seamAt * MIN), 0, 60 * MIN),
  rawSide: at(charts.gapThresholdMs(hourly.concat(raw).map((m) => m * MIN), seamAt * MIN), (seamAt + 5) * MIN, (seamAt + 6) * MIN),
  crossing: at(charts.gapThresholdMs(hourly.concat(raw).map((m) => m * MIN), seamAt * MIN), (seamAt - 60) * MIN, seamAt * MIN),
};

out.threshold = {
  empty: String(charts.gapThresholdMs([])),
  one: String(charts.gapThresholdMs([0])),
  two: String(charts.gapThresholdMs([0, 600 * MIN])),
  oneMinute: charts.gapThresholdMs(noGap.map((m) => m * MIN)),
  fifteenMinute: charts.gapThresholdMs(seq(0, 300, 15).map((m) => m * MIN)),
};
/* #7572: a trend whose reply states its bucket. A quiet file's 15 minute buckets with readings at 5:45 PM, 6:00 PM, 7:30 PM and 11:45 PM: the
   steps (15 min, 90 min, 255 min) have no cadence of their own, so the series read as events and never broke. With the bucket the line breaks
   at every step longer than one bucket: one line for 5:45-6:00, and a dot each for 7:30 and 11:45. */
const quiet = [0, 15, 105, 360];
out.bucketQuiet = marks(draw(quiet, { series: oneSeries, bucketMs: 15 * MIN }));
out.bucketQuietNoBucket = marks(draw(quiet, { series: oneSeries }));
out.bucketQuietArea = marks(draw(quiet, { series: oneSeries, bucketMs: 15 * MIN, mode: "area" }));
out.bucketQuietStacked = marks(draw(quiet, { series: oneSeries, bucketMs: 15 * MIN, mode: "stacked" }));
/* a series with every bucket present stays one line, and a null bucket in the middle (a figure-less row) breaks it the same way */
out.bucketFull = marks(draw(seq(0, 90, 15), { series: oneSeries, bucketMs: 15 * MIN }));
out.bucketNullMiddle = marks(charts.renderLineChart({
  points: [0, 15, 30, 45, 60].map((m, i) => ({ t: iso(T0 + m * MIN), a: i === 2 ? null : 10 + i })),
  xKey: "t", series: oneSeries, bucketMs: 15 * MIN,
}));
/* A collector scheduled every 5 minutes on a chart of 1 minute buckets: rows 5 minutes apart (some with no figure, which still mark a collection).
   The rows' own cadence is the step, so it stays one line; the same rows judged by the bucket alone are all dots. */
const slowTimes = seq(0, 120, 5);
const slowStep = charts.bucketMsOf(1, slowTimes.map((m) => T0 + m * MIN));
out.slowCollector = marks(draw(slowTimes, { series: oneSeries, bucketMs: slowStep }));
out.slowCollectorStep = slowStep;
/* ...and an outage (no rows from minute 60 to 90) still breaks it */
const slowWithOutage = slowTimes.filter((m) => m < 60 || m > 90);
out.slowCollectorOutage = marks(draw(slowWithOutage, { series: oneSeries, bucketMs: charts.bucketMsOf(1, slowWithOutage.map((m) => T0 + m * MIN)) }));
/* a quiet file at 15 minute buckets whose rows (figure-less ones included) are every bucket keeps the bucket */
out.quietRowsStep = charts.bucketMsOf(15, seq(0, 360, 15).map((m) => T0 + m * MIN));
out.bucketThreshold = {
  stated: charts.gapThresholdMs(quiet.map((m) => m * MIN), null, 15 * MIN),
  ignoredWhenAbsent: String(charts.gapThresholdMs(quiet.map((m) => m * MIN), null, null)),
  bad: String(charts.gapThresholdMs(quiet.map((m) => m * MIN), null, NaN)),
  minutes: [charts.bucketMsOf(15), String(charts.bucketMsOf(null)), String(charts.bucketMsOf(0)), String(charts.bucketMsOf("x"))],
};

/* #7544: the range strips. 5 minute points for an hour, a 3 hour hole, then an hour more. */
const stripWin = { startMs: T0, endMs: T0 + 8 * 60 * MIN };
const stripAt = (minutes) => minutes.map((m, i) => ({ t: T0 + m * MIN, v: 1 + (i % 4) }));
const holey = seq(0, 55, 5).concat(seq(240, 295, 5));
const subpaths = (d) => (d.match(/M/g) || []).length;
const shape = (p) => ({ lineRuns: subpaths(p.line), areaRuns: subpaths(p.area), dots: p.dots.length });
out.stripHole = shape(slicer.stripPaths(stripAt(holey), stripWin, null, 5 * MIN));
out.stripHoleDerived = shape(slicer.stripPaths(stripAt(holey), stripWin));
out.stripNoHole = shape(slicer.stripPaths(stripAt(seq(0, 115, 5)), stripWin, null, 5 * MIN));
/* a burst, then nothing for hours, then a lone mark (the Blocking strip's day): per-minute buckets, so a 3 hour step breaks it */
out.stripBurst = shape(slicer.stripPaths(stripAt([0, 1, 2, 3, 4, 5, 240]), stripWin, null, MIN));
out.stripOne = shape(slicer.stripPaths(stripAt([30]), stripWin, null, MIN));
out.stripNever = shape(slicer.stripPaths(stripAt(holey), stripWin, null, Infinity));
/* the seam of a two-grain strip (Query Store): hourly points, then minute points */
const twoGrains = seq(0, 3 * 60, 60).concat(seq(4 * 60, 4 * 60 + 30));
out.stripSeam = shape(slicer.stripPaths(stripAt(twoGrains), stripWin, null, { seamMs: T0 + 4 * 60 * MIN }));
out.stripValues = [null, undefined, "", 0, "3.5", 7].map((v) => String(slicer.stripValue(v)));
/* a query strip on a collector every 5 minutes, whose reply states a 1 minute bucket: still one line */
out.stripSlowCollector = shape(slicer.stripPaths(stripAt(seq(0, 115, 5)), stripWin, null, slicer.stripGap({}, { bucket_minutes: 1 })));
out.stripGaps = {
  blocking: slicer.stripGap({ stepMs: 60000 }, { bucket_minutes: 10 }),
  bucket: String(slicer.stripGap({}, { bucket_minutes: 10 })),
  none: String(slicer.stripGap({}, {})),
  seam: slicer.stripGap({ bucketed: false, seam: (d) => d.routing.raw_from }, { bucket_minutes: 60, routing: { raw_from: "2026-01-01T04:00:00" } }),
};

/* #7692: Query Duration Trend on a quiet server. The reply holds rows only where queries ran: steps of 12, 1, 23, 1, 1, 256 and 9 minutes on a 1 minute bucket.
   Those steps are uneven, so the chart's own cadence guess reads them as events and never breaks; the panel names the reply's bucket (bucketKey) and the
   rows' own cadence (median 9 minutes) sets the break, so the 23 minute and 256 minute steps break and no segment crosses the 256 minute one. The strip
   above Top Queries takes the same step through stripGap. */
const quietServer = [0, 12, 13, 36, 37, 38, 294, 303];
const quietReply = { bucket_minutes: 1, trend: quietServer.map((m, i) => ({ time: iso(T0 + m * MIN), value: 5 + (i % 3) })) };
const quietDesc = { viz: "line", rowsKey: "trend", xKey: "time", series: [{ key: "value", label: "Duration" }], format: "ms", emptyText: "none" };
const vertexCounts = (host) => lineXs(host).map((xs) => xs.length);
out.quietPanel = { runs: vertexCounts(panels.VIZ.line(quietReply, { ...quietDesc, bucketKey: "bucket_minutes" })), dots: marks(panels.VIZ.line(quietReply, { ...quietDesc, bucketKey: "bucket_minutes" })).dots };
out.quietPanelNoKey = { runs: vertexCounts(panels.VIZ.line(quietReply, quietDesc)) };
/* a busy server (a row every minute, one 4 hour hole) breaks at the hole and nowhere else */
const busyReply = { bucket_minutes: 1, trend: seq(0, 60).concat(seq(300, 360)).map((m) => ({ time: iso(T0 + m * MIN), value: 5 })) };
out.busyPanel = { runs: vertexCounts(panels.VIZ.line(busyReply, { ...quietDesc, bucketKey: "bucket_minutes" })) };
const quietPoints = quietServer.map((m, i) => ({ t: T0 + m * MIN, v: 5 + (i % 3) }));
const quietStrip = { bucketed: true };
out.quietStrip = shape(slicer.stripPaths(quietPoints, stripWin, null, slicer.stripGap(quietStrip, { bucket_minutes: 1 }, quietPoints.map((p) => p.t))));
out.quietStripNoBucketed = shape(slicer.stripPaths(quietPoints, stripWin, null, slicer.stripGap({}, { bucket_minutes: 1 }, quietPoints.map((p) => p.t))));
out.quietStripGaps = {
  noBucketInReply: String(slicer.stripGap(quietStrip, {}, quietPoints.map((p) => p.t))),
  stepWins: String(slicer.stripGap({ bucketed: true, stepMs: 60000 }, { bucket_minutes: 10 }, quietPoints.map((p) => p.t))),
};

/* #7692, through the page's own wiring. The cases above hand VIZ.line a bucketKey and slicer.stripGap a strip object, so a dropped `bucketKey` descriptor
   in server-tabs.js, a dropped `bucketed: true` in stripOf, or a rankedSlicer that stops passing the rows' times would pass them all. Here the Queries tab
   is built from SERVER_TABS and every read it makes is answered with the quiet server's reply (a 256 minute hole), so the line runs are those of the
   charts and strips the page really draws. */
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
const allNodes = (n, o = []) => { o.push(n); n.children.forEach((c) => allNodes(c, o)); return o; };
const quietRows = quietServer.map((m, i) => ({ time: iso(T0 + m * MIN), value: 5 + (i % 3), execution_count: 60, executions_per_second: 1 + (i % 3) }));
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const body = name === "get_query_duration_trend" || name === "get_procedure_duration_trend"
    ? { bucket_minutes: 1, trend: quietRows }
    : { status: "empty", message: "none" };
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});
const pageCtx = { hours: 24, label: "last 24 hours", startMs: stripWin.startMs, endMs: stripWin.endMs, rangeKey: "preset:24" };
const queriesTab = tabs.SERVER_TABS.find((t) => t.id === "queries");
const tabHost = new FakeNode("main");
util.mount(tabHost, queriesTab.build("quiet", pageCtx));
await flush();
const panelRuns = (title) => allNodes(tabHost).filter((n) => n.attrs["data-title"] === title).map((p) => vertexCounts(p))[0] || null;
const stripRuns = (title) => allNodes(tabHost).filter((n) => cls(n).split(" ").includes("slicer") && n.attrs["data-slicer"] === title).map((s) => {
  const d = allNodes(s).filter((n) => cls(n).includes("slicer-line")).map((n) => n.attrs.d || "")[0] || "";
  return subpaths(d);
})[0] ?? null;
out.pageWiring = {
  queryDuration: panelRuns("Query Duration Trend"),
  executionsPerSecond: panelRuns("Executions per Second"),
  procedureDuration: panelRuns("Procedure Duration Trend"),
  queryStrip: stripRuns("Query duration"),
  procedureStrip: stripRuns("Procedure duration"),
};

out.split = charts.splitAtGaps([0, 1, 2, 3, 4], [0, 1, 2, 100, 101], 10).map((r) => r.join(","));

/* #7850: the Blocking and Deadlock trends fill the buckets inside the reply's `covered` ranges with 0 and leave the rest out. The chart's own spacing
   guess reads three events hours apart as events and joined them into one slope across time nobody observed. With a `covered` list the panel hands the
   chart the fill's bucket, so any two neighbouring points more than one bucket apart break the line, and a lone point is a dot. A 6 hour window ending
   at T0 + 6 hours, 5 minute buckets. A reply without `covered` keeps the whole-window fill and the chart's own rule. */
const realNow = Date.now;
Date.now = () => T0 + 360 * MIN;
const trendPanelDesc = { viz: "line", rowsKey: "trend", xKey: "time", series: [{ key: "count", label: "Reports" }], format: "int", zeroFill: true, params: { server: "srv-a", hours: 6 }, emptyText: "none" };
const coveredDrawn = (events, covered) => {
  const host = panels.VIZ.line({ bucket_minutes: 5, trend: events.map((m, i) => ({ time: iso(T0 + m * MIN), count: 3 - (i % 3) })), ...(covered ? { covered } : {}) }, trendPanelDesc);
  return { runs: vertexCounts(host), dots: marks(host).dots };
};
const spanOf = (from, to) => ({ start: iso(T0 + from * MIN) + "Z", end: iso(T0 + to * MIN) + "Z" });
/* a 90 minute hole (120 to 210) inside covered time, events at 60, 65 and 250: two runs, one each side */
out.coveredHole = coveredDrawn([60, 65, 250], [spanOf(0, 120), spanOf(210, 360)]);
/* no covered field: the whole window is filled, one line of 73 points, no dots */
out.coveredAbsent = coveredDrawn([60, 65, 250], null);
/* covered is an empty list and three events hours apart: three separate dots, no line joins two of them */
out.coveredEmptyEvents = coveredDrawn([30, 180, 270], []);
/* an event in the hole between two short covered ranges (55 to 65 and 245 to 255) is a dot of its own and joins neither run */
out.coveredEventInHole = coveredDrawn([60, 150, 250], [spanOf(55, 65), spanOf(245, 255)]);
/* two events a bucket apart with nothing covered are one short line, not two dots */
out.coveredEmptyAdjacent = coveredDrawn([30, 35], []);
Date.now = realNow;

fs.rmSync(scratch, { recursive: true, force: true });
console.log(JSON.stringify(out));
