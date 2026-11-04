/* #7416 items 7, 8, 12, 13, 17, 23, 24: runs the web page's real notice, range strip, restart strip, tab note, Query Store reason and
   Run live now code (wwwroot/js) on a small fake DOM against a scripted /api answer, and prints one line of JSON with what each drew.
   WebNoticesAndRangeStripBehaviourTests starts it as
       node web-notices-truths-harness.mjs <path to wwwroot/js>
   The js tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];

class FakeNode {
  constructor(tag) {
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
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
  getBoundingClientRect() { return { left: 0, width: 1000 }; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/blocking" };

const END = Date.UTC(2026, 9, 21, 12, 0, 0);
const HOUR = 3600000;
const START = END - 24 * HOUR;
const iso = (ms) => new Date(ms).toISOString().replace("Z", "");
const RESTART = { at: iso(END - 5 * HOUR), reason: "restart", detail: "every delta baseline for this server was forgotten and re-established on the next pass (observed by wait_stats, cpu_utilization)" };

const answerFor = (name, params) => {
  const server = params.get("server");
  if (name === "get_blocking_trend" || name === "get_deadlock_trend") {
    if (server === "single") return name === "get_blocking_trend" ? { trend: [{ time: iso(END - 6 * HOUR), count: 3 }] } : { status: "empty", message: "No deadlocks in this window." };
    if (server === "none") return { status: "empty", message: "No rows in this window." };
    return { trend: [1, 7, 13, 19].map((h, i) => ({ time: iso(START + h * HOUR), count: 2 + i })) };
  }
  /* any other trend read: a few level rows, and the restart entry the payload carries */
  return {
    trend: [3, 9, 15].map((h) => ({ time: iso(START + h * HOUR), memory_mb: 50, allocated_mb: 40, avg_read_latency_ms: 2, avg_write_latency_ms: 3, line: "db1 data" })),
    series: [{ clerk_type: "MEMORYCLERK_SQLBUFFERPOOL", trend: [3, 9, 15].map((h) => ({ time: iso(START + h * HOUR), memory_mb: 50 })) }],
    discontinuities: [RESTART],
  };
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  if (u.pathname === "/api/session") return { status: 200, ok: true, text: async () => JSON.stringify({ can_edit: true }) };
  if (u.pathname === "/api/active-queries/live/available") return { status: 200, ok: true, text: async () => JSON.stringify({ can_queue: true }) };
  const name = u.pathname.replace("/api/read/", "");
  return { status: 200, ok: true, text: async () => JSON.stringify(answerFor(name, u.searchParams)) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "notices-truths-"));
let M;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  M = {
    util: await load("util.js"),
    tabs: await load(path.join("pages", "server-tabs.js")),
    shared: await load(path.join("pages", "server-tabs", "shared.js")),
    queries: await load(path.join("pages", "server-tabs", "sql-queries.js")),
    live: await load(path.join("pages", "active-live.js")),
    slicer: await load("time-slicer.js"),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
const cls = (n) => n.className || n.attrs.class || "";
const byClass = (root, c) => all(root).filter((n) => new RegExp("(^|\\s)" + c + "(\\s|$)").test(cls(n)));
const ctxOf = () => ({ hours: 24, label: "last 24 hours", startMs: START, endMs: END, rangeKey: "preset:24" });
const draw = async (tabId, server) => {
  const tab = M.tabs.SERVER_TABS.find((t) => t.id === tabId);
  const root = new FakeNode("main");
  M.util.mount(root, tab.build(server, ctxOf()));
  await flush();
  return root;
};
const out = {};

/* 7. The partial window sentence: capital P, and every date said once. */
const OLDEST = iso(END - 3 * HOUR);
const floor = {
  window_truncated: true, oldest_shown_utc: OLDEST, window_start_utc: iso(START), window_end_utc: iso(END),
  truncation_note: "partial window: this grid shows only the newest rows, back to X, because it stops at its row limit. The window started at S. The grid covers X to E.",
};
const capped = M.util.windowNoteText(floor);
const oldestLocal = M.util.localTime(OLDEST);
out.capped = { text: capped, oldestCount: capped.split(oldestLocal).length - 1, startCount: capped.split(M.util.localTime(iso(START))).length - 1 };
const dataStart = M.util.windowNoteText({ window_truncated: true, data_start_utc: OLDEST, window_start_utc: iso(START), window_end_utc: iso(END), truncation_note: "partial window: this panel's data starts at X, after the window's start at S. The panel covers X to E." });
out.dataStart = { text: dataStart, firstCount: dataStart.split(oldestLocal).length - 1 };
out.sentOnly = M.util.noticeStrip("partial window: this panel read the hourly tier, which reaches back about 7 days.").textContent;

/* 12. The restart strip: plain words, the tool's text behind a disclosure, nothing on a chart of levels. */
const strip = M.shared.discontinuityStrip({ discontinuities: [RESTART] });
out.restart = {
  text: strip.children[0].textContent,
  details: byClass(strip, "strip-detail").length,
  full: byClass(strip, "strip-detail")[0].textContent,
  levels: M.shared.discontinuityStrip({ discontinuities: [RESTART] }, { delta: false }),
};
const memory = await draw("memory", "mem");
out.memoryTab = { drewClerks: /MEMORYCLERK_SQLBUFFERPOOL|Clerk/.test(memory.textContent), restartText: /discontinuity|restarted/i.test(memory.textContent) };
const io = await draw("io", "io");
out.ioTab = { restartText: /SQL Server restarted at/.test(io.textContent), full: /baseline discontinuity at/.test(io.textContent) };

/* 13. A tab note describes the page: quiet text, not the yellow warning. */
const eventsTab = M.tabs.SERVER_TABS.find((t) => t.id === "events");
const note = M.tabs.tabNote(eventsTab, 168);
out.tabNote = { cls: cls(note), text: note.textContent };

/* 8 and 17. The Blocking tab's strips. */
const single = await draw("blocking", "single");
const sliceOf = (root, title) => byClass(root, "slicer").find((n) => n.attrs["data-slicer"] === title);
const reports = sliceOf(single, "Blocked process reports");
out.singleSample = { dots: byClass(reports, "slicer-dot").length, stem: (byClass(reports, "slicer-line")[0].attrs.d || "").split("L").length };
out.multiSample = { dots: byClass(sliceOf(await draw("blocking", "many"), "Blocked process reports"), "slicer-dot").length };
const deadlocks = sliceOf(single, "Deadlocks");
out.emptyStrip = { hidden: deadlocks.parentNode.style.display === "none" };
const none = await draw("blocking", "none");
const noneStrips = byClass(none, "slicer");
out.noRows = { strips: noneStrips.length, shown: noneStrips.filter((n) => n.parentNode.style.display !== "none").length };

/* 23. Query Store reasons in words; Unknown says what it is. */
out.qs = {
  off: M.queries.qsReasonsText({ verdict: "Unknown", verdict_reasons: ["qs_off"] }),
  none: M.queries.qsReasonsText({ verdict: "Unknown", verdict_reasons: [] }),
  graded: M.queries.qsReasonsText({ verdict: "Warning", verdict_reasons: ["plan_churn_high", "qs_read_only"] }),
};

/* 24. Run live now: the button, then its sentence on a line of its own. */
const bar = M.live.activeQueriesLiveBar("a", []);
await flush();
const block = bar.children[0];
out.live = { first: block ? block.children.map((c) => c.tag) : [], second: bar.children[1] ? bar.children[1].tag : null, noteText: bar.children[1] ? bar.children[1].textContent : "" };

console.log(JSON.stringify(out));
process.exit(0);
