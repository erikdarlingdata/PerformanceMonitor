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
let charts, util, compose;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);
  util = await import(pathToFileURL(path.join(scratch, "util.js")).href);
  compose = await import(pathToFileURL(path.join(scratch, "compose.js")).href);
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
  const host = charts.renderLineChart({ xKey: "t", series: [{ key: "a", label: "A", color: "#f00" }], ...spec });
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
  msStep: [0, 0.2, 0.4].map((v) => util.fmtMs(v, { step: 0.2 })),
  msSeconds: [1000, 1050, 1100].map((v) => util.fmtMs(v, { step: 50 })),
  pctStep: [0, 0.5, 1].map((v) => util.fmtPct(v, { step: 0.5 })),
  pctNoStep: util.fmtPct(0.6),
  intStep: [0, 0.2].map((v) => util.fmtInt(v, { step: 0.2 })),
  mapIndex: [5, 7].map(util.fmtMs),
  mb: [200, 1023, 1024, 2048].map((v) => util.fmtMb(v)),
};

console.log(JSON.stringify(out));
