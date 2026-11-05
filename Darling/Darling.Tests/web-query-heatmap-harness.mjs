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
  } else if (scenario === "caption") {
    /* #6353: the legend's order sentence, per metric the control offers. */
    out.captions = {};
    for (const m of model.HEATMAP_METRICS) {
      const body = page.renderHeatmapBody({ ...data, metric: m.value }, { server: "SRV1", columns: [], desc: { emptyText: "none" } });
      out.captions[m.value] = body.every((n) => n.className && n.className.includes("qh-legend"))[0].textContent;
    }
  } else if (scenario === "layout") {
    /* #7542: the grid fills its panel. For a day of 10 minute bins and a week of 70 minute bins, the grid's cell count per row, the label row's
       column count (the sum of its colspans), the minimum width the table asks for, and whether any cell is given a fixed width. */
    const draw = (binMinutes, firstTime, lastTime) => {
      const d = { metric: "duration", bucket_minutes: binMinutes, magnitude_buckets: labels, cell_count: 2, truncated: false,
        cells: [cell(firstTime, 1, 3), cell(lastTime, 3, 9)] };
      const body = page.renderHeatmapBody(d, { server: "SRV1", columns: [], desc: { emptyText: "none" } });
      const table = body.every((n) => n.tag === "table")[0];
      const rows = table.children;
      const labelRow = rows[0];
      const dataRow = rows[1];
      const cellsInRow = dataRow.children.filter((c) => c.tag === "td").length;
      const labelColumns = labelRow.children.filter((c) => c.className === "qh-tick").reduce((n, c) => n + Number(c.getAttribute("colspan")), 0);
      const cells = table.every((n) => n.tag === "td");
      return { cellsInRow, labelColumns, minWidth: table.style.minWidth || "", fixedCellWidth: cells.some((c) => c.style.width || c.style.minWidth || c.getAttribute("width")) };
    };
    out.day = draw(10, "2026-10-21T00:00", "2026-10-21T23:50");
    out.week = draw(70, "2026-10-15T00:00", "2026-10-21T22:50");
  } else if (scenario === "window") {
    /* #7683: the columns run across the reply's window, not from the first bin to the last. A day of 10 minute bins (the window starts at
       23:05 and ends at 23:05, so the first and last bins are partial and the day is 145 columns): one bin, and bins only in the last hour. */
    const win = { window_start: "2026-11-04T23:05:00.0000000Z", window_end: "2026-11-05T23:05:00.0000000Z" };
    const draw = (cells, truncated) => {
      const d = { metric: "duration", bucket_minutes: 10, ...win, magnitude_buckets: labels, cell_count: cells.length, truncated, cells };
      const g = model.buildHeatmap(d);
      const filledColumns = [];
      for (let c = 0; c < g.columns; c++) if (g.rows.some((r) => r[c])) filledColumns.push(c);
      const body = page.renderHeatmapBody(d, { server: "SRV1", columns: [], desc: { emptyText: "none" } });
      const table = body.every((n) => n.tag === "table")[0];
      const ticks = table.children[0].children.filter((c) => c.className === "qh-tick");
      return { columns: g.columns, filledColumns, startMs: g.startMs, cellsInRow: table.children[1].children.filter((c) => c.tag === "td").length,
        minWidth: table.style.minWidth || "", lastTickSpan: Number(ticks[ticks.length - 1].getAttribute("colspan")),
        lastTickText: ticks[ticks.length - 1].textContent, tickBeforeLastText: ticks[ticks.length - 2].textContent };
    };
    out.one = draw([cell("2026-11-05T11:00", 1, 4)], false);
    out.lastHour = draw([cell("2026-11-05T22:10", 1, 4), cell("2026-11-05T22:40", 3, 9), cell("2026-11-05T23:00", 2, 2)], false);
    /* A cut read (the oldest bins dropped) starts at the first bin it returned. */
    out.cut = draw([cell("2026-11-05T20:00", 1, 4), cell("2026-11-05T23:00", 2, 2)], true);
    out.windowStartMs = Date.parse("2026-11-04T23:00:00Z");
    out.cutStartMs = Date.parse("2026-11-05T20:00:00Z");
  } else if (scenario === "retention") {
    /* #7683: a 7 day window (70 minute bins) whose oldest 4 days the store no longer holds: window_truncated is true and effective_start is 4
       days into the window. The grid starts at effective_start's bin, so no column before the data is drawn as "no query". The same reply
       without window_truncated still starts at window_start. */
    const binMs = 70 * 60000;
    const binStart = (ms) => Math.floor(ms / binMs) * binMs;
    const iso = (ms) => new Date(ms).toISOString().slice(0, 16);
    const winStart = Date.parse("2026-10-29T23:05:00Z");
    const winEnd = Date.parse("2026-11-05T23:05:00Z");
    const effStart = Date.parse("2026-11-02T23:05:00Z");
    const first = binStart(effStart);
    const cells = [cell(iso(first), 1, 4), cell(iso(first + 10 * binMs), 3, 9), cell(iso(binStart(winEnd)), 2, 2)];
    const draw = (extra) => {
      const d = { metric: "duration", bucket_minutes: 70, window_start: "2026-10-29T23:05:00.0000000Z", window_end: "2026-11-05T23:05:00.0000000Z",
        magnitude_buckets: labels, cell_count: cells.length, truncated: false, cells, ...extra };
      const g = model.buildHeatmap(d);
      const filledColumns = [];
      for (let c = 0; c < g.columns; c++) if (g.rows.some((r) => r[c])) filledColumns.push(c);
      const body = page.renderHeatmapBody(d, { server: "SRV1", columns: [], desc: { emptyText: "none" } });
      const table = body.every((n) => n.tag === "table")[0];
      return { columns: g.columns, filledColumns, startMs: g.startMs, cellsInRow: table.children[1].children.filter((c) => c.tag === "td").length };
    };
    out.retentionCut = draw({ window_truncated: true, effective_start: "2026-11-02T23:05:00.0000000Z" });
    out.notTruncated = draw({ window_truncated: false, effective_start: "2026-11-02T23:05:00.0000000Z" });
    out.noFlag = draw({});
    out.effectiveStartBinMs = first;
    out.windowStartBinMs = binStart(winStart);
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
