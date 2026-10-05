/* #4843: runs the shipped composed-panel table (compose.js), the alert-rule test table (alert-editor.js) and the Fleet
   Sweeps tables (pages/sweeps.js) on a small fake DOM, through the shared grid in panels.js, and prints what a scenario did
   as one line of JSON. GridAdoptionBehaviourTests starts it as
       node web-grid-adoption-harness.mjs <path to the js folder> <scenario> */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) { this.style = {}; this.classList = { add: (c) => { this.className = (this.className + " " + c).trim(); }, remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); } }; this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = ""; this.className = ""; this.value = ""; }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  click() { this.fire("click", { target: this }); }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
  focus() { globalThis.document.activeElement = this; this.fire("focus"); }
  contains(n) { for (; n; n = n.parent) if (n === this) return true; return false; }
  find(tag) { for (const c of this.children) { if (c.tag === tag) return c; const f = c.find && c.find(tag); if (f) return f; } return null; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
const body = new FakeNode("body");
let downloads = [];
globalThis.document = { activeElement: null, body, createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
const origCreate = URL.createObjectURL;
URL.createObjectURL = (blob) => { downloads.push({ blob }); return "blob:fake"; };
URL.revokeObjectURL = () => {};
const origAppend = body.appendChild.bind(body);
body.appendChild = (a) => { if (a.tag === "a") a.click = () => { downloads[downloads.length - 1].name = a.download; }; return origAppend(a); };
const setNav = (v) => Object.defineProperty(globalThis, "navigator", { value: v, configurable: true, writable: true });
globalThis.location = { hash: "#/server/a/queries" };

const root = process.argv[2];
const imp = (f) => import(pathToFileURL(root + "/" + f).href);
const { renderComposedResult } = await imp("compose.js");
const { renderTestResult } = await imp("alert-editor.js");
const { renderSweeps } = await imp("pages/sweeps.js");
const { VIZ, gridTable } = await imp("panels.js");

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const tableOf = (w) => all(w).find((n) => n.tag === "table");
const ths = (w) => tableOf(w).find("thead").find("tr").children;
const tbodyOf = (w) => tableOf(w).children[1];
const cellsOf = (w, i) => tbodyOf(w).children.filter((t) => t.style.display !== "none").map((t) => t.children[i].textContent);
const filterBtn = (w, i) => ths(w)[i].children.find((c) => c.tag === "button");
const box = (w) => all(w).find((n) => n.className === "grid-filter-input");
const filterBy = (w, i, text) => { filterBtn(w, i).click(); const b = box(w); b.value = text; b.fire("input"); };
const tool = (w, label) => all(w).find((n) => n.tag === "button" && n.textContent === label);
const flush = () => new Promise((r) => setTimeout(r, 0));
const clip = [];
Object.defineProperty(globalThis, "navigator", { value: { clipboard: { writeText: async (t) => { clip.push(t); } } }, configurable: true, writable: true });
const csvText = async () => { const d = downloads[downloads.length - 1]; return d ? await d.blob.text() : null; };
const out = {};
const wrapOf = (nodes) => (Array.isArray(nodes) ? nodes : [nodes]).filter(Boolean).find((n) => all(n).some((x) => x.tag === "table"));

const composedRows = [
  { bucket: "2026-01-02T10:00:00", db: "alpha", value: 1500 },
  { bucket: "2026-01-02T11:00:00", db: "beta", value: 20 },
  { bucket: "2026-01-02T12:00:00", db: "Alpha two", value: 300 },
];
const composed = (server) => wrapOf(renderComposedResult({ rows: composedRows }, { viz: "table", title: "Db load", measure: "m", unit: "count" }, { scope: { server } }));

const alertRes = { kind: "data", data: { results: [
  { server: "srv-a", current_value: 90, breaching: true, severity: "Critical" },
  { server: "srv-b", current_value: 5, breaching: false },
  { server: "srv-c", no_data: true },
  { server: "srv-d", current_value: 40, breaching: true },
] } };
const alertTable = (key) => wrapOf(renderTestResult(alertRes, "percent", key));

const sweepDoc = {
  swept_at: "2026-01-02T10:00:00", span_start: "2026-01-02T09:00:00", span_end: "2026-01-02T10:00:00", alerts_enabled: true, instruments_alive: true,
  previous_sweep_id: "1", entry_bar_sweeps: 2, exit_bar_sweeps: 2,
  report: { changes: { band_transitions: [
    { server: "web-one", from: "Healthy", to: "Critical", reason: "disk" },
    { server: "web-two", from: "Warning", to: "Healthy", reason: "recovered" },
    { server: "api-one", from: "Healthy", to: "Warning", reason: "cpu" },
  ] } },
  verdicts: [], liveness: {},
};
const watch = { entry_bar_sweeps: 2, exit_bar_sweeps: 2, items: [
  { server: "web-one", item: "disk", state: "open", consecutive_hits: 3, consecutive_misses: 0, first_seen_at: "2026-01-02T08:00:00", last_seen_at: "2026-01-02T10:00:00", condition: "disk low", evidence: { free: 1 } },
  { server: "api-one", item: "cpu", state: "carried", consecutive_hits: 1, consecutive_misses: 1, first_seen_at: "2026-01-02T09:00:00", last_seen_at: "2026-01-02T10:00:00", condition: "cpu high" },
  { server: null, server_id: 0, fleet_scope: true, item: "fleet", state: "pending", consecutive_hits: 1, consecutive_misses: 0, first_seen_at: "2026-01-02T09:30:00", last_seen_at: "2026-01-02T10:00:00", condition: "fleet wide" },
  { server: null, server_id: 9, item: "orphan", state: "open", consecutive_hits: 12, consecutive_misses: 0, first_seen_at: "2026-01-02T09:30:00", last_seen_at: "2026-01-02T10:00:00", condition: "no name" },
] };
globalThis.fetch = async (url) => {
  const u = String(url);
  const body = u.includes("watch-items") ? watch : u.includes("/api/sweeps/latest") || /\/api\/sweeps\/\d/.test(u) ? sweepDoc : u.includes("/api/sweeps") ? { sweeps: [] } : {};
  const raw = JSON.stringify(body);
  return { ok: true, status: 200, text: async () => raw, json: async () => body };
};
const sweepPage = async () => {
  const main = new FakeNode("main");
  await renderSweeps(main, {});
  await flush(); await flush(); await flush();
  return main;
};
const sectionTable = (main, headText) => all(main).filter((n) => n.className === "grid-box").find((w) => ths(w).map((h) => h.textContent).join("|").includes(headText));

const scenarios = {
  async viz() {
    globalThis.location = { hash: "#/server/a/viz" };
    const cols = [
      { key: "name", label: "Name" },
      { key: "a", label: "A", format: "int", group: "G1" },
      { key: "b", label: "B", format: "int", group: "G2" },
      { key: "pick", label: "", copy: false, render: () => new FakeNode("input") },
    ];
    const rows = [{ name: "Alpha", a: 3, b: 30 }, { name: "beta", a: 1, b: 10 }, { name: "ALPHA two", a: 2, b: 20 }];
    const desc = { rowsKey: "rows", columns: cols, title: "V", groups: ["G1", "G2"], defaultGroups: ["G1"] };
    const w = VIZ.table({ rows }, desc);
    out.heads = ths(w).map((h) => h.textContent.replace(/[▲▼ ▾]/g, ""));
    out.controlFilter = !!filterBtn(w, 3);
    out.hiddenB = tbodyOf(w).children[0].children[2].style.display;
    ths(w)[1].fire("click"); ths(w)[1].fire("click");
    filterBy(w, 0, "alpha");
    out.shown = cellsOf(w, 0);
    tool(w, "Copy all").click(); await flush();
    out.copy = clip[clip.length - 1];
    tool(w, "Export CSV").click();
    out.csv = await csvText();
    out.rebuilt = cellsOf(VIZ.table({ rows }, desc), 0);
    const g = gridTable(rows, { ...desc, title: "V2" });
    out.sameHeads = ths(g).map((h) => h.textContent.replace(/[▲▼ ▾]/g, "")).join("|") === out.heads.join("|");
    out.sameRows = cellsOf(g, 0).join("|") === cellsOf(VIZ.table({ rows }, { ...desc, title: "V3" }), 0).join("|");
    out.emptyViz = VIZ.table({ rows: [] }, desc).textContent;
    out.noFields = VIZ.table({ rows }, { rowsKey: "rows", columns: [] }).textContent;
  },
  async composed() {
    globalThis.location = { hash: "#/server/a/queries" };
    const w = composed("srv-a");
    out.heads = ths(w).map((h) => h.textContent.replace(/[▲▼ ▾]/g, ""));
    out.before = cellsOf(w, 2);
    ths(w)[2].fire("click");
    out.sortedAsc = cellsOf(w, 2);
    ths(w)[2].fire("click");
    out.sortedDesc = cellsOf(w, 2);
    filterBy(w, 1, "alpha");
    out.filtered = cellsOf(w, 1);
    tool(w, "Copy all").click(); await flush();
    out.copy = clip[clip.length - 1];
    tool(w, "Export CSV").click();
    out.csv = await csvText();
    const again = composed("srv-a");
    out.rebuilt = cellsOf(again, 1);
    out.rebuiltSortInd = ths(again)[2].getAttribute("aria-sort");
    out.rebuiltBox = !!box(again);
    const other = composed("srv-b");
    out.otherServer = cellsOf(other, 1);
    out.otherSort = ths(other)[2].getAttribute("aria-sort");
  },
  async alert() {
    globalThis.location = { hash: "#/alert-rule/7" };
    const w = alertTable("rule-7");
    out.heads = ths(w).map((h) => h.textContent.replace(/[▲▼ ▾]/g, ""));
    out.before = cellsOf(w, 0);
    ths(w)[1].fire("click"); ths(w)[1].fire("click");
    out.byValueDesc = cellsOf(w, 0);
    filterBy(w, 2, "yes");
    out.fired = cellsOf(w, 0);
    tool(w, "Copy all").click(); await flush();
    out.copy = clip[clip.length - 1];
    tool(w, "Export CSV").click();
    out.csv = await csvText();
    const again = alertTable("rule-7");
    out.rebuilt = cellsOf(again, 0);
    out.rebuiltSort = ths(again)[1].getAttribute("aria-sort");
    globalThis.location = { hash: "#/alert-rule/8" };
    const other = alertTable("rule-8");
    out.otherRule = cellsOf(other, 0);
    out.otherRuleSort = ths(other)[1].getAttribute("aria-sort");
    out.otherRuleFiltered = cellsOf(other, 0).length;
    globalThis.location = { hash: "#/alert-rule/new" };
    const d1 = alertTable("draft-1");
    ths(d1)[1].fire("click");
    out.draftSort = ths(d1)[1].getAttribute("aria-sort");
    out.nextDraftSort = ths(alertTable("draft-2"))[1].getAttribute("aria-sort") || "";
  },
  async sweeps() {
    globalThis.location = { hash: "#/sweeps" };
    const main = await sweepPage();
    const t = sectionTable(main, "From");
    out.found = !!t;
    out.heads = ths(t).map((h) => h.textContent.replace(/[▲▼ ▾]/g, ""));
    out.before = cellsOf(t, 0);
    ths(t)[0].fire("click");
    out.sorted = cellsOf(t, 0);
    filterBy(t, 3, "c");
    out.filtered = cellsOf(t, 0);
    tool(t, "Copy all").click(); await flush();
    out.copy = clip[clip.length - 1];
    const main2 = await sweepPage();
    const t2 = sectionTable(main2, "From");
    out.rebuilt = cellsOf(t2, 0);
    out.rebuiltSort = ths(t2)[0].getAttribute("aria-sort");
    const watchT = sectionTable(main2, "Condition");
    out.watchRows = cellsOf(watchT, 0);
    out.watchHeads = ths(watchT).map((h) => h.textContent.replace(/[▲▼ ▾]/g, ""));
    tool(watchT, "Export CSV").click();
    out.watchCsv = await csvText();
    ths(watchT)[3].fire("click");
    out.posAsc = cellsOf(watchT, 1);
    ths(watchT)[3].fire("click");
    out.posDesc = cellsOf(watchT, 1);
    ths(watchT)[0].fire("click");
    out.serverAsc = cellsOf(watchT, 0);
    out.serverCsv = (() => { tool(watchT, "Export CSV").click(); return csvText(); })();
    out.serverCsv = await out.serverCsv;
  },
  async twoPanels() {
    globalThis.location = { hash: "#/view/dash" };
    const spec = { viz: "table", title: "", measure: "m", unit: "count" };
    const mk = (slot) => wrapOf(renderComposedResult({ rows: composedRows }, spec, { scope: { server: "srv-a" }, panelSlot: slot }));
    const a = mk(0);
    ths(a)[2].fire("click"); ths(a)[2].fire("click");
    filterBy(a, 1, "alpha");
    out.aSort = ths(a)[2].getAttribute("aria-sort");
    out.aRows = cellsOf(a, 1);
    const b = mk(1);
    out.bSort = ths(b)[2].getAttribute("aria-sort") || "";
    out.bRows = cellsOf(b, 1);
    const a2 = mk(0);
    out.a2Sort = ths(a2)[2].getAttribute("aria-sort");
    out.a2Rows = cellsOf(a2, 1);
  },
  async composedInto() {
    globalThis.location = { hash: "#/view/dash" };
    globalThis.fetch = async () => {
      const b = { sql: "select 1", rows: composedRows };
      const raw = JSON.stringify(b);
      return { ok: true, status: 200, text: async () => raw, json: async () => b };
    };
    const { renderComposedInto } = await imp("compose.js");
    const body = new FakeNode("div");
    await renderComposedInto(body, { viz: "table", title: "", measure: "m", unit: "count" }, { server: "srv-a" }, { panelSlot: 3 });
    const t = wrapOf(body.children);
    out.found = !!t;
    out.rows = t ? cellsOf(t, 1) : [];
    if (t) { ths(t)[2].fire("click"); out.sorted = cellsOf(t, 1); }
    const body2 = new FakeNode("div");
    await renderComposedInto(body2, { viz: "table", title: "", measure: "m", unit: "count" }, { server: "srv-a" }, { panelSlot: 3 });
    out.sameSlotSort = ths(wrapOf(body2.children))[2].getAttribute("aria-sort");
    const body3 = new FakeNode("div");
    await renderComposedInto(body3, { viz: "table", title: "", measure: "m", unit: "count" }, { server: "srv-a" }, { panelSlot: 4 });
    out.otherSlotSort = ths(wrapOf(body3.children))[2].getAttribute("aria-sort") || "";
  },
  async vizEdges() {
    globalThis.location = { hash: "#/server/a/viz-edges" };
    const rows = [{ name: "Alpha", n: 3 }, { name: "beta", n: 1 }];
    const one = VIZ.table({ rows }, { rowsKey: "rows", columns: [{ key: "name", label: "Name" }], title: "One" });
    out.oneHeads = ths(one).map((h) => h.textContent.replace(/[▲▼ ▾]/g, ""));
    ths(one)[0].fire("click");
    out.oneSorted = cellsOf(one, 0);
    const hidden = VIZ.table({ rows }, { rowsKey: "rows", title: "Hidden", columns: [{ key: "name", label: "Name", group: "G" }, { key: "n", label: "N", group: "G" }], groups: ["G"], defaultGroups: [] });
    out.hiddenShown = tbodyOf(hidden).children[0].children.map((c) => c.style.display);
    out.hiddenCopy = (() => { tool(hidden, "Copy all").click(); return flush(); })();
    await out.hiddenCopy;
    out.hiddenCopy = clip[clip.length - 1];
    const dot = VIZ.table({ name: "solo", n: 7 }, { rowsKey: ".", columns: [{ key: "name", label: "Name" }, { key: "n", label: "N", format: "int" }], title: "Dot" });
    out.dotRows = cellsOf(dot, 0);
    out.dotN = cellsOf(dot, 1);
    const noTools = VIZ.table({ rows }, { rowsKey: "rows", columns: [{ key: "name", label: "Name" }], title: "NoTools", tools: false });
    out.noToolsCopy = !!tool(noTools, "Copy all");
    out.noToolsFilter = !!filterBtn(noTools, 0);
    const noFilter = VIZ.table({ rows }, { rowsKey: "rows", columns: [{ key: "name", label: "Name" }], title: "NoFilter", filter: false });
    out.noFilterBtn = !!filterBtn(noFilter, 0);
    out.noFilterTools = !!tool(noFilter, "Copy all");
    out.noFilterSort = (ths(noFilter)[0].className || "").includes("sortable");
    const emptyCls = VIZ.table({ rows }, { rowsKey: "rows", columns: [{ key: "name", label: "Name", cellClass: () => undefined }], title: "Cls" });
    out.cls = tbodyOf(emptyCls).children[0].children[0].className;
  },
};
await scenarios[process.argv[3]]();
console.log(JSON.stringify(out));
