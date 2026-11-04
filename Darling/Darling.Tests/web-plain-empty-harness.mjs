/* #7410: runs the web dashboard's real empty-reply path (wwwroot/js/panels.js panelEmptyStrip, VIZ.stat, util.js plainEmptyStrip and
   fmtMbFit) on a small fake DOM and prints what it drew as one line of JSON. WebPlainEmptyMessageBehaviourTests starts it as
       node web-plain-empty-harness.mjs <path to the js folder>
   Only the DOM is a stand-in. */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.parent = null;
    this._text = "";
    this.className = "";
    this.dataset = {};
    this.style = {};
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  get firstChild() { return this.children[0] || null; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener() {}
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/a/vacuum" };

const root = process.argv[2];
const { panelEmptyStrip, VIZ } = await import(pathToFileURL(root + "/panels.js").href);
const { applyFormat } = await import(pathToFileURL(root + "/util.js").href);
const { fanout } = await import(pathToFileURL(root + "/pages/server-tabs/shared.js").href);

const longMessage =
  "The pg_buffer_usage collector is running against example-pg-01 but the PostgreSQL extension it reads is not installed there: " +
  "its last run was recorded as a named non-fatal skip, so nothing from the buffer cache is being stored. relation \"pg_buffercache\" does not exist (SQLSTATE 42P01). " +
  "The stored sentence names the extension and the database to create it in.";

const describe = (strip) => {
  const details = strip.findAll((n) => n.tag === "details");
  const summaries = strip.findAll((n) => n.tag === "summary");
  const bodies = strip.findAll((n) => String(n.className).includes("disc-body"));
  return {
    cls: strip.className,
    text: strip.textContent,
    hasDetails: details.length,
    summary: summaries.length ? summaries[0].textContent : null,
    detailBody: bodies.length ? bodies[0].textContent : null,
  };
};

const out = {};
out.message = longMessage;
out.extension = describe(panelEmptyStrip({}, {
  kind: "empty", status: "precondition", message: longMessage,
  hints: { reason: "extension_missing", collector: "pg_buffer_usage", extensions: ["pg_buffercache"], restart_required: false },
}));
out.extensionRestart = describe(panelEmptyStrip({}, {
  kind: "empty", status: "precondition", message: longMessage,
  hints: { reason: "extension_missing", extensions: ["pg_qualstats"], restart_required: true },
}));
out.panelOwnLine = describe(panelEmptyStrip({ extensionMissingLine: "pg_stat_kcache is not installed on this server, so per-query OS CPU is not collected." }, {
  kind: "empty", status: "precondition", message: longMessage,
  hints: { reason: "extension_missing", extensions: ["pg_stat_kcache"], restart_required: true },
}));
out.sourceUnreadable = describe(panelEmptyStrip({}, {
  kind: "empty", status: "precondition", message: longMessage, hints: { reason: "source_unreadable", collector: "pg_column_stats" },
}));
out.planCapture = describe(panelEmptyStrip({}, {
  kind: "empty", status: "precondition", message: "This server cannot capture execution plans yet. Unsatisfied precondition(s): capture_threshold = -1 | extension_available = false",
  hints: { reason: "plan_capture_not_ready", unsatisfied_count: 2 },
}));
/* A reply with no structured reason keeps its own text, with nothing behind a click. */
out.noReason = describe(panelEmptyStrip({}, { kind: "empty", status: "empty", message: "No query statistics in this window.", hints: null }));
out.unknownReason = describe(panelEmptyStrip({}, { kind: "empty", status: "empty", message: "Plain text.", hints: { reason: "something_new" } }));
/* A panel's agreed sentence (emptyWins) still replaces an empty reply's message. */
out.emptyWins = describe(panelEmptyStrip({ emptyWins: true, emptyText: "No deadlocks." }, { kind: "empty", status: "empty", message: "Long tool text about deadlocks.", hints: null }));

/* #7410: `emptyLine: true` says the panel's own empty sentence for a true negative and keeps the tool's text behind a click. A reply that is not a true negative keeps the tool's text. */
out.emptyLineTrue = describe(panelEmptyStrip({ emptyLine: true, emptyText: "No lock activity was sampled in this window." }, { kind: "empty", status: "empty", message: longMessage, hints: null }));
out.emptyLineUnavailable = describe(panelEmptyStrip({ emptyLine: true, emptyText: "No lock activity was sampled in this window." }, { kind: "empty", status: "unavailable", message: longMessage, hints: null }));

/* The page-cut flag is a sentence under the tiles, not a tile. */
const stats = [
  { key: "database_count", label: "Databases active", format: "int" },
  { key: "truncated", label: "Window held more", pageCut: "The window held more databases than the grid below lists.", format: "bool" },
];
const tiles = (data) => {
  const node = VIZ.stat(data, { stats, title: "Database Activity" });
  const labels = node.findAll((n) => n.className === "label").map((n) => n.textContent);
  const notices = node.findAll((n) => String(n.className).includes("notice")).map((n) => n.textContent);
  return { labels, notices };
};
out.cut = tiles({ database_count: 30, truncated: true });
out.notCut = tiles({ database_count: 3, truncated: false });

out.fit = { small: applyFormat("mbfit", 0.14), whole: applyFormat("mbfit", 2), large: applyFormat("mbfit", 4096), none: applyFormat("mbfit", null), zero: applyFormat("mbfit", 0) };

/* fanout (server-tabs/shared.js): several panels over one read. The canned reply is the wire body the page would get; only fetch is a stand-in. */
let fanoutRead = 0;
const runFanout = async (specs, reply) => {
  globalThis.fetch = async () => ({ ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify(reply) });
  const panels = fanout("get_fanout_probe_" + ++fanoutRead, { hours: 24 }, specs);
  await new Promise((resolve) => setTimeout(resolve, 50));
  return panels.map((p) => p.findAll((n) => String(n.className).includes("panel-body"))[0].textContent);
};
const table = (title, extra) => ({ title, viz: "table", emptyText: "Nothing for " + title + ".", ...extra });
const fanoutMessage = "No rows came back for this read in the window.";
const emptyReply = { status: "empty", message: fanoutMessage, hints: null };
out.fanoutThreeEmpty = await runFanout([table("A"), table("B"), table("C")], emptyReply);
/* A panel with its own agreed sentence (emptyWins) does not say the read's message, so it neither stands in for it nor is it shortened. */
out.fanoutEmptyWinsFirst = await runFanout([table("A", { emptyWins: true }), table("B"), table("C")], emptyReply);
out.fanoutEmptyWinsBetween = await runFanout([table("A"), table("B", { emptyWins: true }), table("C")], emptyReply);
/* A reply that is not a true negative keeps the read's message even on an emptyWins panel, so that panel is the one that says it. */
out.fanoutNotEmptyStatus = await runFanout([table("A", { emptyWins: true }), table("B")], { status: "unavailable", message: fanoutMessage, hints: null });
out.fanoutMessage = fanoutMessage;
console.log(JSON.stringify(out));
