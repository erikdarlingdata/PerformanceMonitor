/* #5987: runs the by-tag page (wwwroot/js/pages/query-tag-usage.js) and the CPU tab's Top Queries grid (pages/server-tabs.js) with the shipped
   panels.js and util.js against a scripted /api/read, and prints as one line of JSON what each asked for and drew. QueryTagUsageBehaviourTests starts it as
       node query-tag-usage-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = text == null ? "" : String(text);
    this.className = "";
    this.value = "";
  }
  get parentNode() { return this.parent; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot === true; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.location = { hash: "#/server/a/cpu" };

const ROWS = [
  { query_hash: "0x1", database_name: "db1", query_text: "select 1", tag: "Prod / Web", total_cpu_ms: 5, execution_count: 2 },
  { query_hash: "0x2", database_name: "db1", query_text: "select 2", total_cpu_ms: 4, execution_count: 1 },
  { query_hash: "0x3", database_name: "db1", query_text: "select 3", tag: "Batch", total_cpu_ms: 3, execution_count: 1 },
];
const fig = (cpu, ex) => ({ cpu_ms: cpu, duration_ms: cpu * 2, logical_reads: cpu * 10, logical_writes: 0, executions: ex });
const planCache = (serverName, tagOnly) => {
  const b = {
    section: "plan_cache", status: "ok", source: "collect.query_stats",
    tag_rows: tagOnly
      ? [{ tag_id: 7, tag: "Web", path: "Prod / Web", own: fig(5, 2), subtree: fig(5, 2) }]
      : [{ tag_id: 3, tag: "Prod", path: "Prod", own: fig(0, 0), subtree: fig(5, 2) }, { tag_id: 7, tag: "Web", path: "Prod / Web", own: fig(5, 2), subtree: fig(5, 2) }],
    total: fig(tagOnly ? 5 : 9, tagOnly ? 2 : 4),
  };
  if (!tagOnly) b.untagged = fig(4, 2);
  else b.total_scope = "tag_subtree";
  if (serverName) b.server = serverName;
  return b;
};
const deadlocks = () => ({ section: "deadlocks", status: "empty", message: "No deadlock graphs in the window.", tag_rows: [], total: { deadlocks: 0, processes: 0 }, untagged: { deadlocks: 0, processes: 0 } });

/* #6794: what a fleet plan-cache section carries when the hourly rollup served it. */
const HOURLY_NOTE = "the hourly rollup is materialized only to 2026-10-26T21:00:00Z; nothing after it was read";
/* #6800: what the fleet Query Store section carries when the hourly table served it: the start moved up to the hour. */
const QS_NOTE = "hourly buckets start on the hour: no bucket before 2026-10-26T23:00:00.0000000Z: the data from 2026-10-26T22:17:00.0000000Z to 2026-10-26T23:00:00.0000000Z is not included";
const asks = [];
const answerFor = (name, q) => {
  if (name === "list_servers") return { servers: [{ server_name: "a", display_name: "Server A", engine_kind: "sqlserver" }, { server_name: "b" }] };
  if (name === "list_query_tags") return { status: "ok", tags: [{ tag_id: 3, name: "Prod", path: "Prod", colour: "#4E79A7" }, { tag_id: 7, name: "Web", path: "Prod / Web", colour: "#59A14F" }, { tag_id: 8, name: "Batch", path: "Batch", colour: null }] };
  if (name === "get_resource_use_by_tag") {
    const tagOnly = !!q.get("tag");
    const fleet = !q.get("server");
    const sections = fleet
      ? [{ section: "plan_cache", status: "ok", source: "collect.query_stats_io_hourly (hourly rollup)", tier: "hourly", window_end: "2026-10-26T21:00:00Z", window_edges_note: HOURLY_NOTE, served_start: "2026-10-26T13:00:00.0000000Z", start_dropped_from: "2026-10-26T12:50:01.0234751Z", materialized_to: "2026-10-26T21:00:00Z", end_hour_counted_whole: false, servers: [planCache("a", tagOnly), planCache("b", tagOnly)], fleet_total: fig(18, 8) }, { section: "query_store", status: "empty", source: "collect.query_store_hash_daily + collect.query_store_hash_hourly + collect.query_store_interval_wide", tier: "daily+hourly+tail", window_edges_note: QS_NOTE, served_start: "2026-10-26T23:00:00.0000000Z", start_dropped_from: "2026-10-26T22:17:00.0000000Z" }, deadlocks()]
      : [planCache(null, tagOnly), deadlocks()];
    return { scope: fleet ? "fleet" : "server", hours_back: 24, sections_additive: false, sections };
  }
  /* #6831: the empty Query Store regressions answer, with the shared cut-window sentence the service sends (the test passes it in). */
  if (name === "get_query_store_regressions") return { status: "empty", message: process.env.HARNESS_CUT_WINDOW_MESSAGE, hints: { effective_start: "2026-10-22T20:02:00Z" } };
  if (name === "get_top_queries_by_cpu") {
    const tag = q.get("tag");
    const rows = !tag ? ROWS : tag === "untagged" ? ROWS.filter((r) => !r.tag) : ROWS.filter((r) => r.tag === tag || (r.tag || "").startsWith(tag + " / "));
    return { server: "a", hours_back: 24, tier_used: "raw", queries: rows };
  }
  return {};
};
/* #6800: how many of the next by-tag reads the service refuses busy (HTTP 429 with Retry-After: 1), as admission answers a long read
   when every slot is held. */
let busyReads = 0;
const BUSY_TEXT = "The service is running as many long reads as it allows (2), and 16 more are waiting. Try again in about 1 seconds.";
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  asks.push({ name, q: Object.fromEntries(u.searchParams) });
  if (name === "get_resource_use_by_tag" && busyReads > 0) {
    busyReads--;
    return { status: 429, ok: false, headers: { get: (h) => (h.toLowerCase() === "retry-after" ? "1" : null) }, text: async () => JSON.stringify({ error: BUSY_TEXT }) };
  }
  return { status: 200, ok: true, headers: { get: () => null }, text: async () => JSON.stringify(answerFor(name, u.searchParams)) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "query-tag-usage-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")), page: await load(path.join("pages", "query-tag-usage.js")), ag: await load(path.join("pages", "ag.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 30; i++) await new Promise((r) => setTimeout(r, 0)); };
const readsOf = (name) => asks.filter((a) => a.name === name).map((a) => a.q);
const out = {};

/* The by-tag page. */
{
  const main = new FakeNode("main");
  main.isRoot = true;
  modules.page.renderQueryTagUsage(main);
  await flush();
  const cardOfSection = (s) => all(main).find((n) => n.attrs["data-section"] === s);
  const rowsOf = (card) => all(card).filter((n) => n.tag === "tr").map((tr) => tr.children.map((td) => td.textContent));
  out.firstRead = readsOf("get_resource_use_by_tag")[0];
  out.deadlockText = cardOfSection("deadlocks").textContent;
  const sel = all(main).find((n) => n.tag === "select" && n.attrs["aria-label"] === "Server");
  out.serverOptions = all(sel).filter((n) => n.tag === "option").map((o) => o.attrs.value);
  /* The page opens on Fleet: no server in the read, and a block per server. */
  out.fleetRead = out.firstRead;
  out.fleetText = cardOfSection("plan_cache").textContent;
  /* #6794: the hourly fleet section shows the read's own edges note. */
  out.fleetPageText = main.textContent;
  out.fleetHourlyNote = all(cardOfSection("plan_cache")).filter((n) => n.className === "muted").map((n) => n.textContent);
  out.fleetQueryStoreNote = all(cardOfSection("query_store")).filter((n) => n.className === "muted").map((n) => n.textContent);
  /* #6157: a server's heading is its display name; a server with none keeps its key. */
  out.fleetHeadings = all(cardOfSection("plan_cache")).filter((n) => n.tag === "h4").map((n) => n.textContent);

  /* One server: its own table, with each tag, untagged and the total. */
  asks.length = 0;
  sel.value = "a";
  sel.fire("change");
  await flush();
  out.serverRead = readsOf("get_resource_use_by_tag")[0];
  out.planRows = rowsOf(cardOfSection("plan_cache"));
  /* #6794: a raw section sends no note, so the page shows none. */
  out.rawMutedNotes = all(cardOfSection("plan_cache")).filter((n) => n.className === "muted").map((n) => n.textContent);

  /* A click on a tag opens its subtree: the read is sent that tag's id. */
  asks.length = 0;
  const link = all(main).find((n) => n.className === "tag-link" && n.textContent === "Prod / Web");
  link.fire("click");
  await flush();
  out.tagRead = readsOf("get_resource_use_by_tag")[0];
  out.tagRows = rowsOf(cardOfSection("plan_cache"));
}

/* #6865: the Query Store section in the backlog state draws the note with the gap and the backlog hours as two numbers, from the shipped sectionCard.
   Without backlog fields (or with the count 0) it draws none. */
{
  const qs = { name: "query_store", title: "Query Store" };
  const mutedOf = (block) => all(modules.page.sectionCard(qs, block, () => {})).filter((n) => n.className === "muted").map((n) => n.textContent);
  const base = { section: "query_store", status: "empty", source: "collect.query_store_interval_wide", effective_start: "2026-10-26T20:00:00.0000000Z" };
  out.backlogStart = modules.util.localTime(base.effective_start);
  out.backlogNotes = mutedOf({ ...base, window_truncated: true, backlog_hours_waiting: 4, hours_left_out: 8 });
  out.backlogOneNotes = mutedOf({ ...base, window_truncated: true, backlog_hours_waiting: 1, hours_left_out: 1 });
  out.noBacklogNotes = mutedOf({ ...base, window_truncated: false });
  out.zeroBacklogNotes = mutedOf({ ...base, window_truncated: true, backlog_hours_waiting: 0, hours_left_out: 0 });
}

/* #6831: the other edge cases, said from the fields. */
out.edgeCases = [
  { tier: "hourly", window_edges_note: "n", served_start: "2026-10-26T13:00:00Z", window_end: "2026-10-27T11:00:00Z", start_dropped_from: "2026-10-26T12:59:00Z", materialized_to: null, end_hour_counted_whole: true },
  { tier: "hourly", window_edges_note: "n", served_start: "2026-10-26T13:00:00Z", window_end: null, start_dropped_from: null, materialized_to: null, end_hour_counted_whole: false },
  { tier: "hourly", window_edges_note: null, window_end: "2026-10-27T11:00:00Z" },
  { tier: "hourly", window_edges_note: "n", served_start: "2026-10-26T13:00:00Z", window_end: "2026-10-27T11:00:00Z", start_dropped_from: "2026-10-26T12:59:40Z", materialized_to: null, end_hour_counted_whole: false },
  { tier: "hourly", window_edges_note: "n", served_start: "2026-10-26T18:00:00Z", window_end: "2026-10-27T11:00:00Z", start_dropped_from: "2026-10-26T12:30:00Z", materialized_to: null, end_hour_counted_whole: false },
].map((b) => modules.page.edgesText(b));
out.sources = ["collect.query_stats", "collect.query_stats_io_hourly (hourly rollup)", "collect.query_store_interval_wide", "collect.query_store_hash_hourly + collect.query_store_interval_wide", "collect.dmv_blocking_snapshots"].map((x) => modules.page.sourceText(x));

/* #6831: the page's empty-panel sentence names the time the store's data starts, not the field and the hints it came from. */
{
  const res = await modules.util.readTool("get_query_store_regressions", { server: "a", hours: 24 });
  out.regressionsEmptyKind = res.kind;
  out.regressionsEmptyMessage = res.message;
}

/* #6800: a by-tag read refused busy shows a "retrying" note, not an error, and is sent once more after the Retry-After it names.
   A second refusal is shown as the error it is, and the page does not ask a third time. */
for (const [label, refusals] of [["busyOnce", 1], ["busyTwice", 2]]) {
  const main = new FakeNode("main");
  main.isRoot = true;
  asks.length = 0;
  busyReads = refusals;
  modules.page.renderQueryTagUsage(main);
  await flush();
  const strips = () => all(main).filter((n) => /\bstrip\b/.test(n.attrs.class || n.className || "")).map((n) => ({ cls: n.attrs.class || n.className, text: n.textContent }));
  const seen = { readsBefore: readsOf("get_resource_use_by_tag").length, stripsBefore: strips() };
  await new Promise((r) => setTimeout(r, 1300));
  await flush();
  seen.readsAfter = readsOf("get_resource_use_by_tag").length;
  seen.stripsAfter = strips();
  seen.planCardAfter = all(main).some((n) => n.attrs["data-section"] === "plan_cache");
  busyReads = 0;
  out[label] = seen;
}

/* #6157: the Availability Groups card names the reporting server by its display name, and falls back to the key. */
{
  const cards = [{ server_name: "a", display_name: "Server A" }, { server_name: "b" }];
  const sub = (name, fleet) => modules.ag.viewSubtitle({ server_name: name, collection_time: new Date().toISOString() }, fleet);
  out.agSubtitleNamed = sub("a", cards);
  out.agSubtitleNoDisplayName = sub("b", cards);
  out.agSubtitleUnknown = sub("zz", cards);
  out.agSubtitleNoFleet = sub("a", []);
}

/* The grid. */
{
  const cpu = modules.tabs.SERVER_TABS.find((t) => t.id === "cpu");
  const root = new FakeNode("main");
  root.isRoot = true;
  modules.util.mount(root, cpu.build("a", { hours: 24, label: "last 24 hours" }));
  await flush();
  const gridRows = () => {
    const card = all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith("Top Queries by")));
    return all(card).filter((n) => n.tag === "tbody").flatMap((tb) => tb.children.map((tr) => ({ text: tr.textContent, pills: all(tr).filter((c) => c.className === "tag-pill").map((p) => p.textContent) })));
  };
  out.gridFirstAsk = readsOf("get_top_queries_by_cpu")[0];
  out.gridFirst = gridRows();
  const sel = all(root).find((n) => n.tag === "select" && n.attrs["aria-label"] === "Tag" && all(n).some((o) => o.attrs.value === "Prod / Web"));
  out.tagOptions = all(sel).filter((n) => n.tag === "option").map((o) => o.attrs.value);
  asks.length = 0;
  const pick = all(root).filter((n) => n.tag === "select" && n.attrs["aria-label"] === "Tag")[0];
  pick.value = "Prod / Web";
  pick.fire("change");
  await flush();
  out.gridTagAsk = readsOf("get_top_queries_by_cpu").at(-1);
  out.gridTagged = gridRows();
  const pick2 = all(root).filter((n) => n.tag === "select" && n.attrs["aria-label"] === "Tag")[0];
  asks.length = 0;
  pick2.value = "untagged";
  pick2.fire("change");
  await flush();
  out.gridUntaggedAsk = readsOf("get_top_queries_by_cpu").at(-1);
  out.gridUntagged = gridRows();
}
console.log(JSON.stringify(out));
