/* Runs the web viewer's shipped charts.js (zoomableLineChart and its helpers, with util.js) against a stand-in DOM,
   drives a brush across the chart's plot, and prints what was drawn as one line of JSON.
   WebChartZoomBehaviourTests starts it as
       node web-chart-zoom-harness.mjs <path to wwwroot/js>
   The DOM is a node tree of plain objects that keeps the listeners it is given. */
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
};
globalThis.location = { hash: "#/server/A/waits" };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "chart-zoom-"));
let charts;
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert. */
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
const points = Array.from({ length: 101 }, (_, i) => ({ t: new Date(T0 + i * MIN).toISOString().slice(0, 19), v: i }));
const specFor = () => ({ points, xKey: "t", series: [{ key: "v", label: "v", color: "#fff" }], windowStart: T0, windowEnd: T0 + 100 * MIN });

/* The plot spans viewBox x 58..984 of 1000 (margins l=58, r=16); the stand-in is 1000 px wide, so a client x is a viewBox x. */
const PLOT_L = 58;
const PLOT_W = 1000 - 58 - 16;
const xAt = (minute) => PLOT_L + (minute / 100) * PLOT_W;
const brush = (host, fromMin, toMin) => {
  const overlay = find(host, (n) => (n.listeners.pointerdown || []).length)[0];
  overlay.listeners.pointerdown[0]({ button: 0, clientX: xAt(fromMin), pointerId: 1 });
  overlay.listeners.pointerup[0]({ clientX: xAt(toMin) });
};
const shown = (host) => {
  const chips = find(host, (n) => String(n.className || n.attrs.class || "").includes("zoom-chip"));
  const polys = find(host, (n) => n.tag === "polyline" || n.tag === "path");
  const labels = find(host, (n) => n.tag === "text").map((n) => n.textContent);
  return { chip: chips.length > 0, chipText: chips.length ? chips[0].textContent : null, shapes: polys.length, labels };
};
const hostChip = (host) => find(host, (n) => n.tag === "button" && n.attrs["aria-label"] === "Reset zoom")[0];

const out = {};
const scope = charts.chartZoomScope(4);

// 1. a brush maps to the right domain
const a = charts.zoomableLineChart(specFor(), "c1", scope);
out.before = shown(a);
brush(a, 20, 40);
const z = charts.getChartZoom("c1", scope);
out.zoom = z ? { from: (z.from - T0) / MIN, to: (z.to - T0) / MIN } : null;
out.zoomed = shown(a);
const applied = charts.applyChartZoom(specFor(), z);
out.appliedPoints = applied.spec.points.length;
out.appliedWindow = [(applied.spec.windowStart - T0) / MIN, (applied.spec.windowEnd - T0) / MIN];

// 2. a rebuild under the same key re-applies it
const rebuilt = charts.zoomableLineChart(specFor(), "c1", scope);
out.rebuilt = shown(rebuilt);

// 3. a different server (page address) or range, or a different chart, is not zoomed
globalThis.location.hash = "#/server/B/waits";
out.otherServer = shown(charts.zoomableLineChart(specFor(), "c1", charts.chartZoomScope(4)));
out.otherServerHeld = charts.getChartZoom("c1", charts.chartZoomScope(4)) !== null;
globalThis.location.hash = "#/server/A/waits";
out.otherChart = shown(charts.zoomableLineChart(specFor(), "c2", scope));

// 4. reset restores the full domain
charts.setChartZoom("c1", scope, z.from, z.to);
const b = charts.zoomableLineChart(specFor(), "c1", scope);
out.preReset = shown(b);
hostChip(b).listeners.click[0]();
out.postReset = shown(b);
out.postResetHeld = charts.getChartZoom("c1", scope) !== null;

// 5. a click (under 8 px) is not a brush
const c = charts.zoomableLineChart(specFor(), "c3", scope);
brush(c, 50, 50.5);
out.clickHeld = charts.getChartZoom("c3", scope) !== null;

// 6. a range change drops it: same server, other preset hours
brush(charts.zoomableLineChart(specFor(), "c4", scope), 10, 30);
out.otherRange = shown(charts.zoomableLineChart(specFor(), "c4", charts.chartZoomScope(24)));

// 7. a brush over a span with no loaded point stores nothing, and a later rebuild with a point there does not zoom
const sparse = (pts) => ({ points: pts, xKey: "t", series: [{ key: "v", label: "v", color: "#fff" }], windowStart: T0, windowEnd: T0 + 100 * MIN });
const mk = (minutes) => minutes.map((m) => ({ t: new Date(T0 + m * MIN).toISOString().slice(0, 19), v: m }));
const e = charts.zoomableLineChart(sparse(mk([0, 5, 90, 100])), "c5", scope);
brush(e, 30, 50);
out.emptyHeld = charts.getChartZoom("c5", scope) !== null;
out.emptyChip = shown(e).chip;
const late = charts.zoomableLineChart(sparse(mk([0, 5, 40, 90, 100])), "c5", scope);
out.emptyLater = shown(late);
out.emptyLaterBaseline = shown(charts.zoomableLineChart(sparse(mk([0, 5, 40, 90, 100])), "c5b", scope));

// 8. a held zoom whose span has no point any more is dropped when the chart is drawn
charts.setChartZoom("c6", scope, T0 + 30 * MIN, T0 + 50 * MIN);
const f = charts.zoomableLineChart(sparse(mk([0, 5, 90, 100])), "c6", scope);
out.agedOutHeld = charts.getChartZoom("c6", scope) !== null;
out.agedOutChip = shown(f).chip;

// 9. the zoomed line keeps one neighbour point each side and the plot clips at the edge
const g = charts.applyChartZoom(specFor(), { scope, from: T0 + 20.5 * MIN, to: T0 + 40.5 * MIN });
out.edgeMinutes = g.spec.points.map((r) => (Date.parse(r.t + "Z") - T0) / MIN);
const gh = charts.zoomableLineChart(specFor(), "c7", scope);
brush(gh, 20.5, 40.5);
const line = find(gh, (n) => n.tag === "polyline" && String(n.attrs.class).includes("series-line"))[0];
out.lineClipped = !!line && /^url\(#.+\)$/.test(line.attrs["clip-path"] || "");
const clipRect = find(gh, (n) => n.tag === "clipPath").map((n) => n.children[0].attrs);
out.clipRect = clipRect.length ? { x: Number(clipRect[0].x), w: Number(clipRect[0].width) } : null;
out.lineXs = line ? line.attrs.points.split(" ").map((p) => Number(p.split(",")[0])) : [];

// 10. a query-only change of the page address keeps the zoom scope
globalThis.location.hash = "#/server/A/waits?x=1";
out.scopeQueryStripped = charts.chartZoomScope(4) === scope;
globalThis.location.hash = "#/server/A/waits";

console.log(JSON.stringify(out));
