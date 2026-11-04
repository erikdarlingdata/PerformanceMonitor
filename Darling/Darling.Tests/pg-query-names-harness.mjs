/* #7428: runs the PostgreSQL Activity, Waits and I/O tabs (wwwroot/js/pages/server-tabs.js with the shipped panels.js, util.js and
   pages/server-tabs/*.js) against a scripted /api/read answer, and prints as one line of JSON what the page drew: each panel's
   heading (title and subtitle), the notes and table cells under it. PgQueryNamesPageBehaviourTests starts it as
       node pg-query-names-harness.mjs <path to wwwroot/js> <scenario>
   The whole js tree is copied into a scratch folder first, so a module another change adds needs no edit here. Only the DOM and
   fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag) {
    this.style = {};
    this.classList = {
      add: (c) => { this.className = (this.className + " " + c).trim(); },
      remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); },
      toggle() {},
      contains: () => false,
    };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this.className = "";
  }
  get parentNode() { return this.parent; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/pg/activity" };

const LONG_STATEMENT = "SELECT o.id, o.total, c.name FROM orders AS o JOIN customers AS c ON c.id = o.customer_id WHERE o.created_at > $1 AND o.status = ANY($2) ORDER BY o.created_at DESC LIMIT 500";
const AUTO = "chosen automatically: ";
const T = "2026-10-21T12:00:00.0000000";

/* One answer per tool the three tabs read; any tool not listed answers an empty object, which every panel draws as its own empty state. */
const answers = {
  get_pg_wait_trend: { wait_event: "MessageQueueReceive", wait_event_source: AUTO + "the most-sampled event in this window", points: [{ collection_time: T, estimated_wait_ms_per_second: 5 }] },
  get_pg_io_trend: { backend_type: "client backend", context: "normal", subject_source: AUTO + "the busiest pair in this window", points: [{ collection_time: T, reads_per_second: 5 }] },
  get_pg_query_duration_trend: { queryid: "1247446479259998619", queryid_source: AUTO + "the statement with the most execution time in this window", points: [{ collection_time: T, mean_exec_ms: 5, calls_per_second: 2 }] },
  get_pg_database_trend: { database: "orders_db", database_source: AUTO + "the biggest temp-file spiller in this window", points: [{ collection_time: T, cache_hit_pct: 99 }] },
  get_pg_wait_sampling: {
    instrument: "extension_sampled",
    waits: [
      { event_type: "CPU", wait_event: "Running", queryid: "0", samples: 59, estimated_wait_ms: 590, backends: 3, pct_of_samples: 59.0, query_text: null },
      { event_type: "IO", wait_event: "DataFileRead", queryid: "1247446479259998619", samples: 30, estimated_wait_ms: 300, backends: 2, pct_of_samples: 30.0, query_text: LONG_STATEMENT },
      { event_type: "Lock", wait_event: "relation", queryid: null, samples: 11, estimated_wait_ms: 110, backends: 1, pct_of_samples: 11.0, query_text: null },
    ],
  },
  get_pg_kernel_stats: {
    queries: [
      { queryid: "0", database_name: "db1", cpu_ms: 10, query_text: null },
      { queryid: "1247446479259998619", database_name: "db1", cpu_ms: 9, query_text: LONG_STATEMENT },
    ],
  },
  get_pg_write_stats: { checkpoints_timed: 2, checkpoints_requested: null, buffers_checkpoint: null, buffers_backend: null, postmaster_restarted: true, buffers_backend_availability: "not_collected: PostgreSQL 17 removed buffers_backend from pg_stat_bgwriter" },
};
if (scenario === "plainWrite") {
  answers.get_pg_write_stats = { checkpoints_timed: 2, checkpoints_requested: 1, buffers_checkpoint: 100, buffers_backend: 4, postmaster_restarted: false };
}
if (scenario === "plainTrends") {
  /* No source sentence: a subject somebody asked for by name is not labelled as a guess. */
  answers.get_pg_wait_trend.wait_event_source = "as requested";
  answers.get_pg_io_trend.subject_source = AUTO.slice(0, -2);
  delete answers.get_pg_query_duration_trend.queryid_source;
  answers.get_pg_database_trend.database_source = "as requested";
}
if (scenario === "noSubject") {
  /* A trend that found nothing to follow names nothing, and keeps the static sentence it was drawn with. */
  answers.get_pg_wait_trend = { points: [] };
  answers.get_pg_io_trend = { points: [] };
  answers.get_pg_query_duration_trend = { points: [] };
  answers.get_pg_database_trend = { points: [] };
}
if (scenario === "partialIo") {
  answers.get_pg_io_trend.subject_source = "backend_type as requested; context chosen automatically within it: the busiest";
}

globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const body = answers[name] || {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pg-query-names-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    tabs: await load(path.join("pages", "server-tabs.js")),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
const draw = async (tabId) => {
  const tab = modules.tabs.POSTGRES_TABS.find((t) => t.id === tabId);
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("pg", { hours: 1, label: "last 1 hour" }));
  await flush();
  return root;
};
const isPanel = (n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3");

/* Each panel: its title and subtitle as the heading draws them, every line of text under the heading, and a table's rows keyed by header. */
const report = (root) => {
  const panels = {};
  for (const p of all(root).filter(isPanel)) {
    const h3 = all(p).find((c) => c.tag === "h3");
    const sub = all(h3).find((c) => c.className && /\bpanel-sub\b/.test(c.className));
    const title = h3.children[0] ? h3.children[0].textContent : "";
    const table = all(p).find((c) => c.tag === "table");
    let rows = [];
    if (table) {
      const heads = all(table).filter((c) => c.tag === "th").map((c) => c.textContent.trim());
      rows = all(table).filter((c) => c.tag === "tr").map((tr) => {
        const cells = tr.children.filter((c) => c.tag === "td").map((c) => c.textContent.trim());
        return Object.fromEntries(cells.map((v, i) => [heads[i] || "#" + i, v]));
      }).filter((r) => Object.keys(r).length > 0);
    }
    panels[title.trim()] = { subtitle: sub ? sub.textContent.trim() : null, text: p.textContent, rows };
  }
  return panels;
};

const out = {};
for (const id of ["activity", "waits", "io"]) out[id] = report(await draw(id));
console.log(JSON.stringify(out));
