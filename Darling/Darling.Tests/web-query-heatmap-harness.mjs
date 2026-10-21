/* Runs the shipped Query Heatmap grid (wwwroot/js/pages/query-heatmap.js and query-heatmap-model.js, #6293) with the real util.js on a
   small fake DOM and prints one line of JSON. WebQueryHeatmapBehaviourTests starts it as
       node web-query-heatmap-harness.mjs <path to wwwroot/js> <scenario>
   Stand-ins: the DOM, panels.js (its table is a marker, its renderPanel is unused here) and charts.js (openServerTabAt records its calls). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.dataset = {}; this.hidden = false; this.disabled = false; this.style = {}; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t) { for (const fn of this.listeners[t] || []) fn({}); }
  every(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.every(pred, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/SRV1/queries" };

const [jsDir, scenario] = process.argv.slice(-2);
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "query-heatmap-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(path.join(scratch, "panels.js"),
    'import { el } from "./util.js";\n' +
    'export const renderPanel = () => null; export const getPanelSignal = () => null; export const setPanelSignal = () => {};\n' +
    'export const gridTable = (rows) => el("div", { class: "stub-table", "data-rows": String(rows.length) });\n');
  fs.writeFileSync(path.join(scratch, "charts.js"),
    'export const calls = []; export const CHART_MENU_LABELS = { atQueries: "Show Active Queries at This Time" };\n' +
    'export async function openServerTabAt(server, tMs, item) { calls.push({ server, tMs, item }); return null; }\n');

  const model = await import(pathToFileURL(path.join(scratch, "pages", "query-heatmap-model.js")).href);
  const page = await import(pathToFileURL(path.join(scratch, "pages", "query-heatmap.js")).href);
  const charts = await import(pathToFileURL(path.join(scratch, "charts.js")).href);

  const cell = (time, bucket, count) => ({ time_bucket: time + ":00.0000000", bucket_index: bucket, bucket_label: "b" + bucket, query_count: count, top_query_hash: "0xAB", top_query_text: "SELECT 1" });
  const labels = ["0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s"].map((label, bucket_index) => ({ bucket_index, label }));
  /* 14:00, 14:05 and 14:20: the 14:10 and 14:15 bins hold nothing. */
  const data = { metric: "duration", bucket_minutes: 5, magnitude_buckets: labels, cell_count: 3, truncated: false,
    cells: [cell("2026-10-21T14:00", 1, 1), cell("2026-10-21T14:05", 3, 40), cell("2026-10-21T14:20", 3, 5)] };

  const out = {};
  if (scenario === "model") {
    const g = model.buildHeatmap(data);
    out.columns = g.columns; out.rows = g.rows.length; out.cellCount = g.cellCount; out.max = g.max;
    out.filled = g.rows.flat().filter((x) => x).length;
    out.gapColumnsEmpty = g.rows.every((r) => r[2] === null && r[3] === null);
    out.shadeLow = g.rows[1][0].shade; out.shadeHigh = g.rows[3][1].shade; out.shadeMid = g.rows[3][4].shade;
    out.shadeOfZero = model.heatmapShade(0, 40); out.shadeOfNull = model.heatmapShade(null, 40);
    out.shadeOfOneOfOne = model.heatmapShade(1, 1);
    let monotone = true, prev = 0;
    for (let c = 1; c <= 500; c++) { const s = model.heatmapShade(c, 500); if (s < prev || s < 1 || s > 8) monotone = false; prev = s; }
    out.monotone = monotone;
    out.bins = [1, 12, 24, 168, 4000].map(model.heatmapBinMinutes);
    out.metrics = model.HEATMAP_METRICS.map((m) => m.value);
    out.empty = model.buildHeatmap({ cells: [] }).empty;
    out.firstMs = g.startMs;
  } else if (scenario === "render") {
    const body = page.renderHeatmapBody(data, { server: "SRV1", columns: [], desc: { emptyText: "none" } });
    const tds = body.every((n) => n.tag === "td");
    out.cells = tds.length;
    out.emptyCells = tds.filter((t) => t.className.includes("qh-empty")).length;
    out.shaded = tds.filter((t) => /qh-band-\d/.test(t.className)).length;
    const high = tds.find((t) => t.getAttribute("data-shade") === "8");
    const detailText = body.every((n) => n.className === "qh-detail-text")[0];
    const open = body.every((n) => n.tag === "button" && n.className.includes("qh-open"))[0];
    const toggle = body.every((n) => n.tag === "button" && n.className.includes("qh-toggle"))[0];
    const gridView = body.every((n) => n.className === "qh-grid-view")[0];
    const tableView = body.every((n) => n.className === "qh-table")[0];
    out.tooltip = high.getAttribute("title");
    out.openHiddenAtStart = open.hidden;
    high.fire("mouseenter"); out.hoverText = detailText.textContent; out.openHiddenOnHover = open.hidden;
    high.fire("mouseleave"); out.afterLeave = detailText.textContent.startsWith("Hover");
    high.fire("click"); out.selectedText = detailText.textContent; out.openHiddenAfterClick = open.hidden;
    open.fire("click"); await new Promise((r) => setTimeout(r, 5));
    out.calls = charts.calls;
    out.gridShownAtStart = !gridView.hidden; out.tableHiddenAtStart = tableView.hidden;
    toggle.fire("click");
    out.tableShownAfterToggle = !tableView.hidden; out.gridHiddenAfterToggle = gridView.hidden;
    out.pressed = toggle.getAttribute("aria-pressed");
    out.tableRows = tableView.every((n) => n.className === "stub-table")[0].getAttribute("data-rows");
    toggle.fire("click");
    out.gridBack = !gridView.hidden;
  } else if (scenario === "renderEmpty") {
    const body = page.renderHeatmapBody({ metric: "duration", bucket_minutes: 5, cells: [] }, { server: "SRV1", columns: [], desc: { emptyText: "none" } });
    out.tds = body.every((n) => n.tag === "td").length;
    out.toggle = body.every((n) => n.tag === "button" && n.className.includes("qh-toggle")).length;
    out.tableShown = !body.every((n) => n.className === "qh-table")[0].hidden;
  } else {
    throw new Error("unknown scenario " + scenario);
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
