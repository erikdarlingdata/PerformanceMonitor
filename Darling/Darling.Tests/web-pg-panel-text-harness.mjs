/* #7548: runs the shipped PostgreSQL Activity and Storage tabs (wwwroot/js/pages/server-tabs.js) against scripted replies for a healthy server and
   prints, as one line of JSON, what the Deadlocks, Deadlock Graphs, Column Statistics and Index Bloat (answered ranking) panels drew: the line
   a person reads, and the long text behind "More detail". Each reply carries the text the MCP tool really sends, so the page's line is what
   has to differ from it.
       node web-pg-panel-text-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.hidden = false;
    this.parentNode = null;
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    child.parentNode = null;
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
    if (name === "class") this.className = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, fn) {
    (this.listeners ||= {})[type] = fn;
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

/* One scripted answer per read name. A scenario replaces the table before it builds a tab. */
let answers = {};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.split("/").pop();
  const body = answers[name] || {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const DEADLOCKS_TEXT =
  "No deadlock was reported on pg-a in the last 24 hour(s). FOUR different things produce that and they are worth telling apart: the server had no deadlocks, which is the healthy answer; " +
  "the log could not be read; the log was read and PostgreSQL did not write it in English; or, on a target that logs in the stderr format, the log was read but its line prefix is one the reader cannot parse. " +
  "The pg_plan_capture_readiness collector judges all four, because plan capture reads the same file the same way - the English one as its message_locale facet (#3061) and the prefix one as its " +
  "log_line_prefix_readable facet (#4735) - and get_pg_plan_capture_readiness is the read that returns those facets with the remedy for each. get_pg_server_config carries the target's lc_messages. " +
  "pg_stat_database's cumulative deadlock counter, in get_pg_database_stats, is the independent check for all four.";
const GRAPH_TEXT =
  "No deadlock graph is stored for pg-a. Either the server had no deadlocks, which is the healthy answer, or its log could not be read, or it was read in a language this does not match - PostgreSQL translates its own " +
  "messages under a non-English lc_messages, severity label included (#3061) - or it was read on a stderr log target whose log_line_prefix the reader cannot parse, so every line was dropped " +
  "(the log_line_prefix_readable facet of get_pg_plan_capture_readiness, #4735). get_pg_database_stats carries pg_stat_database's cumulative deadlock counter, which tells the healthy case from the others.";
const COLUMN_TEXT =
  "No column statistics for pg-a in the last 24 hour(s). This collector runs DAILY, so a window shorter than a day can be empty on a perfectly healthy server - widen it before concluding anything. " +
  "Tables at or above the 1,000 page floor (last 24 hours): 12. Of those, with column statistics this monitoring login can read: 0. Column statistic rows returned (over the window you asked for): 0. " +
  "CAUSE: the privilege filter, not the size floor. Tables clear the floor and not one of them has column statistics confirmed readable by this monitoring login. " +
  "Remedy: grant the monitoring login read access to the tables - GRANT pg_read_all_data TO <login> on PostgreSQL 14 and above, or GRANT SELECT on the tables themselves on 13.";
const BLOAT_TEXT =
  "answered_only excluded every row: pg-a has 40 candidate btree index(es) in this window and NOT ONE of them has a trusted bloat answer. This is not a clean bill of health and it is not an empty server - " +
  "the suppression breakdown below says why each index has no answer and which reasons are remediable. Re-run without answered_only to see the rows and their reasons. " +
  "NO COVERAGE AT ALL: every candidate index on this server carries a reason instead of an answer.";
const bloatBuckets = (...buckets) => ({
  status: "no_answers",
  message: BLOAT_TEXT,
  hints: { arm: "NothingTrusted", candidate_index_count: 40, suppressed_by_reason: buckets },
});
const bloatReply = (reason) => bloatBuckets({ reason, index_count: 40, index_bytes: 1000000 });
/* The list is sorted by bytes and then count, so the grant bucket can come first or second whatever its share of the indexes. */
const GRANT_BUCKET = { reason: "ColumnWidthsNotVisible", index_count: 30, index_bytes: 4000000000 };
const PARTIAL_BUCKET = { reason: "Partial", index_count: 10, index_bytes: 1000000000 };

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pg-panel-text-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
  const find = (node, pred) => (pred(node) ? node : node.children.map((c) => find(c, pred)).find(Boolean) || null);
  const collect = (node, pred, out = []) => {
    if (pred(node)) out.push(node);
    node.children.forEach((c) => collect(c, pred, out));
    return out;
  };
  const { plainText } = await import(pathToFileURL(path.join(scratch, "plain-text.js")).href);
  const tabOf = (id) => tabs.POSTGRES_TABS.find((t) => t.id === id);
  const draw = async (id, scripted) => {
    answers = scripted;
    const panels = tabOf(id).build("srv-a", { hours: 24, label: "24h" }).flat();
    await new Promise((r) => setTimeout(r, 150));
    return panels.filter((p) => p && p.tag);
  };
  const panelTitled = (panels, title) => panels.find((p) => find(p, (n) => n.tag === "h3" && n.textContent.startsWith(title)));
  /* What the panel's strip shows: the line a person reads (its own text, not the disclosure's), and the text behind "More detail". */
  const strip = (panel) => {
    const s = panel ? find(panel, (n) => String(n.className).includes("strip")) : null;
    if (!s) return null;
    const details = collect(s, (n) => n.tag === "details");
    const bodies = collect(s, (n) => String(n.className).includes("disc-body"));
    return {
      line: s.children.filter((c) => c.tag === "#text").map((c) => c.text).join("") || (details.length ? "" : s.textContent),
      hasDetails: details.length,
      summary: details.length ? collect(s, (n) => n.tag === "summary")[0].textContent : null,
      detail: bodies.length ? bodies[0].textContent : null,
    };
  };

  const activity = await draw("activity", {
    get_pg_deadlocks: { status: "empty", message: DEADLOCKS_TEXT, hints: { window_start: "2026-11-04T00:00:00Z" } },
    get_pg_deadlock_detail: { status: "empty", message: GRAPH_TEXT },
    get_pg_column_stats: { status: "empty", message: COLUMN_TEXT, hints: { reason: "pg_stats_unreadable" } },
  });
  /* The same column read when the cause is the size floor: no reason is sent, so the page keeps the tool's text. */
  const columnFloor = await draw("activity", {
    get_pg_column_stats: { status: "empty", message: "No column statistics for pg-a in the last 24 hour(s). CAUSE: the size floor. Nothing to fix." },
  });
  const storage = await draw("storage", { get_pg_index_bloat: bloatReply("ColumnWidthsNotVisible") });
  const storageOther = await draw("storage", { get_pg_index_bloat: bloatReply("Partial") });
  const storageGrantFirst = await draw("storage", { get_pg_index_bloat: bloatBuckets(GRANT_BUCKET, PARTIAL_BUCKET) });
  const storageGrantSecond = await draw("storage", { get_pg_index_bloat: bloatBuckets({ ...PARTIAL_BUCKET, index_count: 2, index_bytes: 5000000000 }, { ...GRANT_BUCKET, index_count: 38, index_bytes: 1000000000 }) });

  /* #7548: the stored withheld marker reads as words in a statement cell and a plan, and only the exact marker does. */
  const shared = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs", "shared.js")).href);
  const planViewer = await import(pathToFileURL(path.join(scratch, "pages", "plan-viewer.js")).href);
  const MARKER = "-- statement text withheld (#4348)";
  const cellText = (v) => {
    const n = shared.codeDisclosure(v);
    const pre = n.tag === "#text" ? null : find(n, (x) => x.tag === "pre");
    return n.tag === "#text" ? n.text : pre ? pre.textContent : n.textContent;
  };
  /* The other sites that show a stored statement or plan: the generic grid cell, the block chain node, the deadlock graph card and its side
     panel, and the PostgreSQL plan cell. Each is fed the exact marker, and a statement that only holds the marker among other text. */
  const panelsMod = await import(pathToFileURL(path.join(scratch, "panels.js")).href);
  const blockChain = await import(pathToFileURL(path.join(scratch, "pages", "block-chain.js")).href);
  const deadlockGraph = await import(pathToFileURL(path.join(scratch, "pages", "deadlock-graph.js")).href);
  const pgPlanViewer = await import(pathToFileURL(path.join(scratch, "pages", "pg-plan-viewer.js")).href);
  const CONTAINING = "SELECT 1 /* see " + MARKER + " */";
  const gridCells = (text) => {
    const t = panelsMod.gridTable([{ sql: text }], { columns: [{ key: "sql", label: "Statement" }], sortable: false, filter: false, tools: false });
    return collect(t, (n) => n.tag === "td").map((n) => n.textContent);
  };
  const chainText = (text) => {
    const host = blockChain.drawBlockChain({ chain: { is_apex: true, session_id: 51, sql_text: text } });
    return find(host, (n) => n.tag === "pre").textContent;
  };
  const graphOf = (text) => ({
    width: 600, height: 300, node_width: 240, node_height: 150,
    processes: [{ id: "p1", spid: 51, x: 10, y: 10, victim: true, sql_text: text }],
    edges: [],
  });
  const deadlockText = (text) => {
    const g = deadlockGraph.drawDeadlockGraph(graphOf(text), null);
    const card = find(g, (n) => String(n.className).includes("dlg-node"));
    return {
      card: collect(card, (n) => String(n.className).includes("dlg-line")).map((n) => n.textContent),
      side: find(g, (n) => n.tag === "pre" && String(n.className).includes("code")).textContent,
    };
  };
  let planKey = 0;
  const pgPlanPanel = (plan) => {
    /* A new queryid each time: an opened plan is remembered by its row identity, and a second click on the same row would close it. */
    const host = pgPlanViewer.pgPlanCell("srv-a", { queryid: String(++planKey), plan_hash: "2", plan });
    find(host, (n) => n.tag === "button").listeners.click();
    return host.textContent;
  };
  const withheld = {
    grid: { marker: gridCells(MARKER), padded: gridCells("  " + MARKER + "\n"), containing: gridCells(CONTAINING) },
    chain: { marker: chainText(MARKER), containing: chainText(CONTAINING) },
    deadlock: { marker: deadlockText(MARKER), containing: deadlockText(CONTAINING) },
    pgPlan: { marker: pgPlanPanel(MARKER), real: pgPlanPanel({ "Node Type": "Seq Scan" }) },
    marker: cellText(MARKER),
    padded: cellText("  " + MARKER + "\n"),
    containing: cellText("SELECT 1 /* see " + MARKER + " */"),
    comment: cellText("SELECT 1 -- fixed in (#123), then (#4046, #6866)"),
    plan: planViewer.classifyPlanRead({ kind: "data", data: { plan_xml: MARKER, truncated: false } }),
    realPlan: planViewer.classifyPlanRead({ kind: "data", data: { plan_xml: "<ShowPlanXML/>", truncated: false } }),
  };

  console.log(JSON.stringify({
    withheld,
    deadlocks: strip(panelTitled(activity, "Deadlocks")),
    graphs: strip(panelTitled(activity, "Deadlock Graphs")),
    columns: strip(panelTitled(activity, "Column Statistics")),
    columnsFloor: strip(panelTitled(columnFloor, "Column Statistics")),
    bloat: strip(panelTitled(storage, "Index Bloat (answered ranking)")),
    bloatOther: strip(panelTitled(storageOther, "Index Bloat (answered ranking)")),
    bloatGrantFirst: strip(panelTitled(storageGrantFirst, "Index Bloat (answered ranking)")),
    bloatGrantSecond: strip(panelTitled(storageGrantSecond, "Index Bloat (answered ranking)")),
    /* A parenthesized issue reference in a service sentence is dropped from any text the page shows. */
    refs: [
      plainText("The read went on without them (#4046, #6866). The next sentence stays."),
      plainText("Older ones were not collected (#4699)."),
      plainText("It reads the facet (the setting of the Activity tab, #4735). It stays."),
      plainText("A real color #416FA6 and a count of 3 stay."),
    ],
  }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
