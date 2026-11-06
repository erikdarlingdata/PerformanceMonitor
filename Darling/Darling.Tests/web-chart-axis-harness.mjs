/* Runs the web viewer's shipped chart axis code (#6158): util.js's formatters, compose.js's formatComposedValue and charts.js's
   renderLineChart with its niceScale / axisTickLabels, against a stand-in DOM, and prints what an axis would read as one line of
   JSON. WebChartAxisBehaviourTests starts it as
       node web-chart-axis-harness.mjs <path to wwwroot/js>
   The DOM is a node tree of plain objects; the y tick labels are the svg text nodes a chart draws left of its plot. */
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

const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "chart-axis-")));
let charts, util, compose, panels, findings, shared, serverPage;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  util = await import(pathToFileURL(path.join(scratch, "util.js")).href);
  compose = await import(pathToFileURL(path.join(scratch, "compose.js")).href);
  panels = await import(pathToFileURL(path.join(scratch, "panels.js")).href);
  findings = await import(pathToFileURL(path.join(scratch, "pages", "analysis-findings.js")).href);
  shared = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs", "shared.js")).href);
  serverPage = await import(pathToFileURL(path.join(scratch, "pages", "server.js")).href);
} catch (e) {
  fs.rmSync(scratch, { recursive: true, force: true });
  throw e;
}
process.on("exit", () => fs.rmSync(scratch, { recursive: true, force: true }));

const find = (node, pred, out = []) => {
  if (pred(node)) out.push(node);
  node.children.forEach((c) => find(c, pred, out));
  return out;
};
const cls = (n) => String(n.className || n.attrs.class || "");

const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
const iso = (ms) => new Date(ms).toISOString().slice(0, 19);
const rows = (values, key = "a", values2 = null) =>
  values.map((v, i) => ({ t: iso(T0 + i * 60000), [key]: v, ...(values2 ? { b: values2[i] } : {}) }));

/* The y axis a chart draws: tick labels on the left (anchor "end", left of the plot), and on the right when series2 is given
   (anchor "start"); the plot's left edge and right edge come from its gridlines, and the widest label's estimated width in px. */
function axes(spec) {
  return axesOf(charts.renderLineChart({ xKey: "t", series: [{ key: "a", label: "A", color: "#f00" }], ...spec }));
}
/* The same reading, taken off any node tree that holds a drawn chart (a panel's or a composed result's). */
function axesOf(host) {
  const texts = find(host, (n) => n.tag === "text" && !cls(n).includes("axis-unit"));
  const grid = find(host, (n) => n.tag === "line" && cls(n).includes("grid-line") && n.attrs.x2 !== n.attrs.x1);
  const left = Number(grid[0].attrs.x1);
  const right = Number(grid[0].attrs.x2);
  const leftLabels = texts.filter((n) => n.attrs["text-anchor"] === "end" && Number(n.attrs.x) === left - 8).map((n) => n.text);
  const rightLabels = texts.filter((n) => n.attrs["text-anchor"] === "start" && Number(n.attrs.x) === right + 8).map((n) => n.text);
  const svg = find(host, (n) => n.tag === "svg")[0];
  const width = Number(svg.attrs.width);
  const px = (labels) => Math.max(0, ...labels.map((l) => String(l).length)) * 6.6;
  return { leftLabels, rightLabels, left, right, width, leftPx: px(leftLabels), rightPx: px(rightLabels) };
}

const out = {};

/* The three charts the issue names: tiny values on a millisecond, a rate and a percent axis. */
const small = rows([0.1, 0.4, 0.2, 0.9, 0.3, 0.6, 0.05]);
const ms = (v, o) => util.applyFormat("ms", v, o);
const pct = (v, o) => util.applyFormat("pct", v, o);
out.msAxis = axes({ points: small, formatValue: ms }).leftLabels;
out.pctAxis = axes({ points: small, formatValue: pct, clampMax: 100 }).leftLabels;
out.plainAxis = axes({ points: small, formatValue: (v, o) => (o && o.step > 0 && util.tickDecimals(o.step) ? Number(v).toFixed(util.tickDecimals(o.step)) : String(Math.round(v))) }).leftLabels;
/* a formatter that ignores the step still never repeats a label (the repeat is left blank) */
out.roundingAxis = axes({ points: small, formatValue: (v) => Math.round(v) + " ms" }).leftLabels;
/* the same formatters on a range that was already whole: unchanged */
out.wholeMs = axes({ points: rows([10, 250, 900, 40]), formatValue: ms }).leftLabels;
out.wholePct = axes({ points: rows([10, 55, 96, 40]), formatValue: pct, clampMax: 100 }).leftLabels;

/* Any maximum from 0.0007 to 5,000,000 reads as distinct tick labels, for the formatters panels use. */
const formats = { ms, pct, int: (v, o) => util.applyFormat("int", v, o), num1: (v, o) => util.applyFormat("num1", v, o), mb: (v, o) => util.applyFormat("mb", v, o) };
const repeats = [];
let checked = 0;
for (const [name, fmt] of Object.entries(formats)) {
  for (let e = -4; e <= 6; e++) {
    for (const m of [1, 1.3, 2, 2.5, 3, 4.7, 5, 7, 9.9]) {
      const max = m * Math.pow(10, e);
      const scale = charts.niceScale(0, max, 4, name === "pct" ? 100 : null);
      const labels = charts.axisTickLabels(scale.ticks, scale.step, fmt);
      checked++;
      if (labels.some((l) => l === "") || new Set(labels).size !== labels.length) repeats.push({ name, max, labels });
    }
  }
}
out.sweep = { checked, repeats: repeats.slice(0, 5), repeatCount: repeats.length };

/* Custom View sizes: 80,000 MB left and 80,000 on the right must fit, with the unit written MB / GB. */
const big = rows([60000, 70000, 80000, 76000], "a", [60000, 72000, 80000, 79000]);
const bigMb = (v, o) => compose.formatComposedValue(v, "mb", o);
const bigCount = (v, o) => compose.formatComposedValue(v, "", o);
const wide = axes({ points: big, formatValue: bigMb, series2: { key: "b", label: "B", color: "#00f", formatValue: bigCount } });
out.bigAxis = { left: wide.leftLabels, right: wide.rightLabels, leftRoom: wide.left - 8, leftNeed: wide.leftPx, rightRoom: wide.width - wide.right - 8, rightNeed: wide.rightPx };
/* The same chart without a right axis reserves nothing extra on the right. */
const lone = axes({ points: big, formatValue: bigMb });
out.loneAxis = { left: lone.leftLabels, leftRoom: lone.left - 8, leftNeed: lone.leftPx, right: lone.right, width: lone.width };
/* A small-number chart keeps the original 58 px margin and 56 px gutter. */
const tiny = axes({ points: rows([1, 2, 3, 4]), series2: { key: "b", label: "B", color: "#00f" }, formatValue: (v) => String(v) });
out.tinyAxis = { left: tiny.left, rightGutter: tiny.width - tiny.right };

/* #7488: a size axis whose labels step up to GB picks its ticks in GB. The expected words are built here with the page's own number
   formatter, so the test holds in any locale. A chart whose largest value stays under 1,024 MB keeps the ticks it always had. */
const GB = (n) => util.fmtInt(n) + " GB";
const mbFmt = (v, o) => util.applyFormat("mb", v, o);
const sized = (values, extra = {}) => axes({ points: rows(values), formatValue: mbFmt, sizeUnit: "mb", ...extra });
const unsized = (values) => axes({ points: rows(values), formatValue: mbFmt });
const composedBoth = axes({
  points: rows([42000, 51000, 60000, 58000], "a", [10000, 20000, 30000, 24000]),
  formatValue: (v, o) => compose.formatComposedValue(v, "mb", o),
  sizeUnit: "mb",
  series2: { key: "b", label: "B", color: "#00f", formatValue: (v, o) => compose.formatComposedValue(v, "mb", o), sizeUnit: "mb" },
});
out.sizeAxis = {
  big: sized([42000, 51000, 60000, 58000]).leftLabels,
  bigExpected: [GB(0), GB(20), GB(40), GB(60)],
  bigWithout: unsized([42000, 51000, 60000, 58000]).leftLabels,
  small: sized([120, 340, 500, 410]).leftLabels,
  smallWithout: unsized([120, 340, 500, 410]).leftLabels,
  composedLeft: composedBoth.leftLabels,
  composedRight: composedBoth.rightLabels,
  composedLeftExpected: [GB(0), GB(20), GB(40), GB(60)],
  composedRightExpected: [GB(0), GB(10), GB(20), GB(30)],
};

/* #7555: each gridline of a one-point chart, as the value it stands at and the value its label says. The one point is drawn as a dot at a known
   value, and the zero line is the lowest gridline, so a gridline's value is the dot's value scaled by its height above zero over the dot's height.
   The label's value is its number times its unit (KB, MB, GB, 1,024 apart) in bytes or MB. */
const UNIT_STEP = { B: 1, KB: 1024, MB: 1024 * 1024, GB: 1024 * 1024 * 1024 };
function gridValues(host, peak, anchor, dotAt, unitBase) {
  const texts = find(host, (n) => n.tag === "text" && !cls(n).includes("axis-unit") && n.attrs["text-anchor"] === anchor);
  const dot = find(host, (n) => n.tag === "circle" && cls(n).includes("series-dot"))[dotAt];
  const zeroY = Math.max(...texts.map((n) => Number(n.attrs.y)));
  const dotUp = zeroY - 4 - Number(dot.attrs.cy);
  return texts.filter((n) => n.text !== "").map((n) => {
    const [num, unit] = n.text.replace(/,/g, "").split(" ");
    const per = unitBase === "bytes" ? UNIT_STEP[unit] : { MB: 1, GB: 1024 }[unit];
    return { label: n.text, tick: (peak * (zeroY - Number(n.attrs.y))) / dotUp, said: Number(num) * per };
  });
}
const onePoint = (v, extra) => charts.renderLineChart({ xKey: "t", series: [{ key: "a", label: "A", color: "#f00" }], points: rows([v], "a", extra && extra.v2 != null ? [extra.v2] : null), ...(extra && extra.spec) });
const bytesFmt = (v, o) => util.applyFormat("bytes", v, o);

/* #7515: the same axes, drawn the way the pages draw them. A composed panel goes through renderComposedResult (the left axis and the
   overlay axis each pass their own size unit), and a "format: mb" panel through the panel table's line viz (VIZ.line, the public way
   into vizLine). Nothing here sets sizeUnit by hand: a panel that stops passing it reads 19.5, 39.1, 58.6 GB again. Expected words are
   built with the page's own formatter, so the test holds in any locale. */
const composedRows = (values, values2) => values.map((v, i) => ({ bucket: iso(T0 + i * 60000), value: v, ...(values2 ? { value2: values2[i] } : {}) }));
const composedAxes = (panel, values, values2) => {
  const nodes = compose.renderComposedResult({ sql: "", rows: composedRows(values, values2) }, panel);
  const holder = new FakeNode("div");
  nodes.filter(Boolean).forEach((n) => holder.appendChild(n));
  return axesOf(holder);
};
const composedPlain = composedAxes({ viz: "line", unit: "mb", measure: "memory" }, [42000, 51000, 60000, 58000]);
const composedOverlay = composedAxes({ viz: "line", unit: "mb", measure: "memory", overlay: { measure: "other", unit: "mb" } }, [42000, 51000, 60000, 58000], [10000, 20000, 30000, 24000]);
const mbPanel = (values) => {
  const holder = new FakeNode("div");
  holder.appendChild(panels.VIZ.line({ trend: rows(values) }, { rowsKey: "trend", xKey: "t", series: [{ key: "a", label: "A" }], format: "mb" }));
  return axesOf(holder);
};
out.panelAxis = {
  composedLeft: composedPlain.leftLabels,
  composedOverlayLeft: composedOverlay.leftLabels,
  composedOverlayRight: composedOverlay.rightLabels,
  mbPanel: mbPanel([42000, 51000, 60000, 58000]).leftLabels,
  mbPanelSmall: mbPanel([120, 340, 500, 410]).leftLabels,
  mbPanelSmallWithout: unsized([120, 340, 500, 410]).leftLabels,
  expectedLeft: [GB(0), GB(20), GB(40), GB(60)],
  expectedRight: [GB(0), GB(10), GB(20), GB(30)],
};

/* #7555: a "format: bytes" chart picks its ticks in the unit its labels print (20, 40, 60 KB, not 19.5, 39.1, 58.6), drawn through the public
   way into a panel (VIZ.line), and every tick of a size axis prints in the unit of its largest tick (a 2 GB Memory Trend reads 0 GB,
   0.5 GB, 1.0 GB, 1.5 GB, not 0 MB, 512 MB, 1.0 GB). Expected words are built with the page's own formatters, so the test holds in any locale. */
const sizePanel = (values, format) => {
  const holder = new FakeNode("div");
  holder.appendChild(panels.VIZ.line({ trend: rows(values) }, { rowsKey: "trend", xKey: "t", series: [{ key: "a", label: "A" }], format, unit: format === "bytes" ? "/s" : undefined }));
  return axesOf(holder);
};
const KB = (n) => util.fmtInt(n) + " KB";
const MBw = (n) => util.fmtInt(n) + " MB";
const unitsOf = (labels) => [...new Set(labels.filter((l) => l !== "").map((l) => l.split(" ").pop()))];
const memory = sizePanel([300, 900, 1500, 1900, 1700], "mb");
out.byteAxis = {
  kb: sizePanel([20000, 41000, 60000, 52000], "bytes").leftLabels,
  kbExpected: ["0 KB", KB(20), KB(40), KB(60)],
  mb: sizePanel([20000000, 41000000, 60000000, 52000000], "bytes").leftLabels,
  mbExpected: ["0 MB", MBw(20), MBw(40), MBw(60)],
  small: sizePanel([120, 340, 500, 410], "bytes").leftLabels,
  cell: [0, 900, 1536, 2 * 1024 * 1024, 3 * 1024 * 1024 * 1024].map((v) => util.fmtBytes(v)),
  mapIndex: [900, 2048].map(util.fmtBytes),
};
out.memoryAxis = {
  labels: memory.leftLabels,
  units: unitsOf(memory.leftLabels),
  expected: ["0 GB", util.fmtNum(0.5, 1) + " GB", util.fmtNum(1, 1) + " GB", util.fmtNum(1.5, 1) + " GB"],
  tooltipCell: [512, 1536].map((v) => util.fmtMb(v)),
};

/* #7555: the gridlines of a size axis whose data max sits just under a unit step (1,023 MB rounds up to a 1,500 MB top tick) say what they stand
   for. Each is the value the gridline is drawn at, next to the value its label states. */
const mbEdge = (v) => gridValues(onePoint(v, { spec: { formatValue: mbFmt, sizeUnit: "mb" } }), v, "end", 0, "mb");
out.gridAxis = {
  mb1023: mbEdge(1023),
  mb1010: mbEdge(1010),
  mb386: mbEdge(386),
  bytes1030000: gridValues(onePoint(1030000, { spec: { formatValue: bytesFmt, sizeUnit: "bytes" } }), 1030000, "end", 0, "bytes"),
  right386: gridValues(onePoint(5, { v2: 386, spec: { formatValue: (v) => String(v), series2: { key: "b", label: "B", color: "#00f", formatValue: mbFmt, sizeUnit: "mb" } } }), 386, "start", 1, "mb"),
  right1023: gridValues(onePoint(5, { v2: 1023, spec: { formatValue: (v) => String(v), series2: { key: "b", label: "B", color: "#00f", formatValue: mbFmt, sizeUnit: "mb" } } }), 1023, "start", 1, "mb"),
};

/* A chart of counts keeps every tick on a whole number (integerTicks), where the same range without it steps by 0.2. */
out.integerAxis = {
  withOption: [1, 2, 3, 7, 40].map((max) => ({ max, ticks: charts.niceScale(0, max, 4, null, true).ticks })),
  without: charts.niceScale(0, 1, 4, null, false).ticks,
  drawn: axes({ points: rows([0, 1, 0, 1]), formatValue: (v) => util.fmtInt(v), integerTicks: true }).leftLabels,
};

/* #7587: a small-range rate axis. Six gridlines 0 ... 0.01 (step 0.002) read 0, 0, 0, 0.01, 0.01, 0.01 through a formatter that rounds to two places,
   so the 0.006 gridline kept the "0.01" label. The page formatter now takes its decimals from the step. */
const rateTicks = [0, 0.002, 0.004, 0.006, 0.008, 0.01];
const mbps = (v, o) => util.fmtPlain(v, o) + " MB/s";
/* a formatter that is a decimal short of what the step needs: formatted again with a finer step, it grows the decimals instead of blanking */
const oneShort = (v, o) => Number(v).toFixed(Math.max(0, util.tickDecimals(o.step) - 1));
const rateAxis = axes({ points: rows([0.01, 0.004, 0.0042, 0.002]), formatValue: mbps });
out.smallRate = {
  labels: charts.axisTickLabels(rateTicks, 0.002, mbps),
  scale: charts.niceScale(0, 0.01, 5, null),
  drawn: rateAxis.leftLabels,
  tooltip: [0.0042, 1234.5678, 0].map((v) => util.fmtPlain(v)),
  oneShort: charts.axisTickLabels(rateTicks, 0.002, oneShort),
  blind: charts.axisTickLabels(rateTicks, 0.002, (v) => (Math.round(v * 100) / 100).toLocaleString()),
};
/* #7586: a duration axis (the per-query history chart) prints one unit, taken from its largest tick, and picks its ticks in that unit. */
const msUnits = (labels) => [...new Set(labels.filter((l) => l !== "").map((l) => l.split(" ").pop()))];
const msAxis30s = axes({ points: rows([20000, 33700, 31000, 28000]), formatValue: ms, sizeUnit: "ms" });
const msAxis5m = axes({ points: rows([100000, 240000, 300000]), formatValue: ms, sizeUnit: "ms" });
out.durationAxis = {
  s30: msAxis30s.leftLabels,
  s30Units: msUnits(msAxis30s.leftLabels),
  m5: msAxis5m.leftLabels,
  m5Units: msUnits(msAxis5m.leftLabels),
  small: axes({ points: rows([120, 340, 500, 410]), formatValue: ms, sizeUnit: "ms" }).leftLabels,
  cell: [999, 1500, 90000, 5400000].map((v) => util.fmtMs(v)),
};

out.composed = {
  mb: [0.5, 512, 1023, 1024, 1536, 80000, 2000000].map((v) => compose.formatComposedValue(v, "mb")),
  kb: compose.formatComposedValue(2048, "kb"),
  gb: compose.formatComposedValue(80, "GB"),
  percent: compose.formatComposedValue(12.34, "percent"),
  count: compose.formatComposedValue(1234, "count"),
  bare: compose.formatComposedValue(0.25, "ms/s"),
  mbStep: [0, 100, 200, 300].map((v) => compose.formatComposedValue(v, "mb", { step: 100 })),
  mbStepSmall: [0, 0.2, 0.4].map((v) => compose.formatComposedValue(v, "mb", { step: 0.2 })),
  gbStep: [0, 20000, 40000, 60000, 80000].map((v) => compose.formatComposedValue(v, "mb", { step: 20000 })),
};
out.formatters = {
  msNoStep: [0.4, 1.6, 999, 1500].map((v) => util.fmtMs(v)),
  /* #7726: a positive figure under 1 ms is whole microseconds; zero stays "0 ms"; 0.9996 rounds to 1000 us and reads "1 ms". */
  msMicro: [0.03235, 0.0254, 0.0003, 0.5, 0.9996, 0].map((v) => util.fmtMs(v)),
  /* #7726: a trace flag is an identifier, copied into DBCC TRACEON, so the id format prints 3895 with no separator. */
  traceFlag: [3895, 1117, 12345].map((v) => util.applyFormat("id", v)),
  /* #7726: the unreachable-since sentence has no seconds ("2026-11-05 16:52 UTC"). Only a caller that opts in with { minutes: true } shifts it:
     with the reply's own `since` in the same minute it prints that instant (seconds included, the tile's time); without one it prints the minute. */
  unreachable: "Server unreachable since 2026-11-05 16:52 UTC: refused. Next retry at 2026-11-05 17:51:35 UTC.",
  localSentence: util.localizeIsoText("Server unreachable since 2026-11-05 16:52 UTC: refused. Next retry at 2026-11-05 17:51:35 UTC.", { minutes: true }),
  localWithSince: util.localizeIsoText("Server unreachable since 2026-11-05 16:52 UTC: refused.", { minutes: true, since: "2026-11-05T16:52:26.1230000" }),
  localOtherMinute: util.localizeIsoText("Server unreachable since 2026-11-05 16:52 UTC: refused.", { minutes: true, since: "2026-11-05T16:53:26" }),
  localDefault: util.localizeIsoText("Server unreachable since 2026-11-05 16:52 UTC: refused. Next retry at 2026-11-05 17:51:35 UTC."),
  localExpected: [new Date("2026-11-05T16:52:00Z").toLocaleString(undefined, { year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit" }), new Date("2026-11-05T17:51:35Z").toLocaleString(), new Date("2026-11-05T16:52:26Z").toLocaleString()],
  noZoneKept: util.localizeIsoText("saw 2026-11-05 16:52 here and 2026-11-05T16:52 there", { minutes: true }),
  /* #7726: backtick spans in advice text become code elements whose text is the span as text (never markup). */
  adviceSpans: findings.adviceNodes("Set `max server memory` (see `sp_configure 'max server memory (MB)', 1000`) and a stray ` tick <b>x</b>.").map((n) => (typeof n === "string" ? "t:" + n : n.tag + ":" + n.text)),
  msStep: [0, 0.2, 0.4].map((v) => util.fmtMs(v, { step: 0.2 })),
  msSeconds: [1000, 1050, 1100].map((v) => util.fmtMs(v, { step: 50 })),
  pctStep: [0, 0.5, 1].map((v) => util.fmtPct(v, { step: 0.5 })),
  pctNoStep: util.fmtPct(0.6),
  intStep: [0, 0.2].map((v) => util.fmtInt(v, { step: 0.2 })),
  mapIndex: [5, 7].map(util.fmtMs),
  mb: [200, 1023, 1024, 2048].map((v) => util.fmtMb(v)),
};

/* #7726: the wiring, not just localizeIsoText. The real Server Connection descriptor drawn by the real stat viz, and the real server head
   (the strip's dot, badge tooltip and sentence), each over a reply whose sentence has no seconds and whose own `since` has them. A descriptor
   that lost its `sinceKey`, or a head that stopped passing `connect_down_since`, prints the bare minute instead of the tile's time. */
const sinceSentence = "Server unreachable since 2026-11-05 16:52 UTC: refused.";
const connectionPanel = panels.VIZ.stat({ server_connection: { message: sinceSentence, since: "2026-11-05T16:52:26.1230000", status: "Unreachable" } }, shared.SERVER_CONNECTION_PANEL);
const connectionNoSince = panels.VIZ.stat({ server_connection: { message: sinceSentence, status: "Unreachable" } }, shared.SERVER_CONNECTION_PANEL);
const headSlots = () => ({ title: new FakeNode("h1"), dot: new FakeNode("span"), badge: new FakeNode("span"), engine: new FakeNode("span"), why: new FakeNode("div") });
function serverHead(card) {
  const h = headSlots();
  serverPage.fillServerHead(h.title, h.dot, h.badge, h.engine, h.why, card, sinceSentence);
  return {
    badgeTitle: find(h.badge, (n) => n.tag === "span" && n.attrs.title != null).map((n) => n.attrs.title)[0] ?? null,
    reason: find(h.why, (n) => cls(n).includes("server-reason")).map((n) => n.textContent)[0] ?? null,
  };
}
out.wiring = {
  panelSentence: find(connectionPanel, (n) => cls(n).includes("stat-sentence")).map((n) => n.textContent)[0] ?? null,
  panelSentenceNoSince: find(connectionNoSince, (n) => cls(n).includes("stat-sentence")).map((n) => n.textContent)[0] ?? null,
  head: serverHead({ display_name: "SRV1", band: "red", status: "Unreachable", connect_down_since: "2026-11-05T16:52:26.1230000" }),
  headNoSince: serverHead({ display_name: "SRV1", band: "red", status: "Unreachable" }),
};

/* #7586: the FinOps Version Store (PVS) tab's size trend, drawn by the real tab. Its read is answered by a stand-in fetch; the tab
   builds its own chart, so the axis labels below come from the formatter the page ships (not a copy of it). */
async function pvsAxis(sizes) {
  const now = Date.now();
  const points = sizes.map((v, i) => ({ collection_time: new Date(now - (sizes.length - i) * 60000).toISOString().slice(0, 19), pvs_size_mb: v }));
  const realFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    const raw = JSON.stringify({ as_of: iso(now), databases: [], trend: [{ database_name: "AppDb", points }] });
    return { status: 200, ok: true, text: async () => raw };
  };
  try {
    const pvs = await import(pathToFileURL(path.join(scratch, "pages", "finops", "version-store.js")).href);
    const root = pvs.tab.build("SRV1", {});
    for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r));
    return axesOf(root).leftLabels;
  } finally {
    globalThis.fetch = realFetch;
  }
}
out.pvsSmall = await pvsAxis([0.001, 0.004, 0.002, 0.009, 0.003, 0.006, 0.0005]);
out.pvsLarge = await pvsAxis([1200, 4800, 2500, 9100, 3300, 6000, 700]);

/* #7578: a unit is printed once. A caption over the axis shows only when the tick labels do not carry the unit already. */
const captionsOf = (spec) => find(charts.renderLineChart({ xKey: "t", series: [{ key: "a", label: "A", color: "#f00" }], ...spec }), (n) => n.tag === "text" && cls(n).includes("axis-unit")).map((n) => n.text);
out.unitCaptions = {
  ticksWithUnit: captionsOf({ points: rows([100, 200, 300, 400]), formatValue: (v, o) => util.applyFormat("ms", v, o), unit: "ms" }),
  ticksWithRateUnit: captionsOf({ points: rows([0.1, 0.2, 0.3]), formatValue: (v) => v.toFixed(1) + " MB/s", unit: "MB/s" }),
  bareTicks: captionsOf({ points: rows([100, 200, 300, 400]), formatValue: (v) => util.fmtInt(v), unit: "MB" }),
  secondAxisWithUnit: captionsOf({ points: rows([1, 2, 3], "a", [10, 20, 30]), formatValue: (v) => String(v), unit: "n", series2: { key: "b", label: "B", color: "#00f", formatValue: (v) => v + " ms", unit: "ms" } }),
};

console.log(JSON.stringify(out));
