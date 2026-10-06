/* #5299 (F9): runs the server page's four Top Queries and Top Procedures cards (the CPU tab's two grids, the Queries tab's composite and its
   procedures grid, all in wwwroot/js/pages/server-tabs.js with the shipped panels.js and util.js) against a scripted /api/read answer, picks
   the Reads ranking on the CPU tab's cards, and prints as one line of JSON the notice lines each card drew. TopRankingNoticeBehaviourTests
   starts it as
       node top-ranking-notice-harness.mjs <path to wwwroot/js> <scenario>
   with HARNESS_INPUT set to {"floor": <the window-floor sentence>, "retention": <the retention notice>}. The whole js tree is copied into a
   scratch folder first (#5279), so a module another change adds needs no edit here. Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);
const INPUT = JSON.parse(process.env.HARNESS_INPUT);

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
  click() { this.fire("click", { target: this }); }
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
globalThis.location = { hash: "#/server/a/cpu" };

/* What the two reads answer: a payload that carries the window floor's note and the reads ranking's retention notice, in the shape each
   scenario names. A read this harness does not script answers an empty object. */
const MARKUP = 'partial window: <b>bold</b> <img src="x" onerror="alert(1)"> older points are not included.';
const NOTES = {
  both: { window_truncated: true, truncation_note: INPUT.floor, retention_notice: INPUT.retention },
  floorOnly: { window_truncated: true, truncation_note: INPUT.floor, retention_notice: null },
  retentionOnly: { window_truncated: false, truncation_note: null, retention_notice: INPUT.retention },
  neither: { window_truncated: false, truncation_note: null, retention_notice: null },
  markup: { window_truncated: true, truncation_note: null, retention_notice: MARKUP },
}[scenario];

const asks = [];
const answerFor = (name) => {
  if (name === "get_top_queries_by_cpu") {
    return {
      server: "a", hours_back: 168, tier_used: "raw", ...NOTES,
      queries: [{ query_hash: "0x1", database_name: "db1", query_text: "select 1", total_cpu_ms: 5, total_logical_reads: 7, execution_count: 2 }],
    };
  }
  if (name === "get_top_procedures_by_cpu") {
    return {
      server: "a", hours_back: 168, tier_used: "raw", ...NOTES,
      procedures: [{ database_name: "db1", full_name: "dbo.p1", execution_count: 3, total_cpu_ms: 4, total_logical_reads: 9 }],
    };
  }
  return {};
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  if (name === "get_top_queries_by_cpu" || name === "get_top_procedures_by_cpu") {
    asks.push(name + ":" + (u.searchParams.get("order_by") ?? "-"));
  }
  return { status: 200, ok: true, text: async () => JSON.stringify(answerFor(name)) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "topranking-notice-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
const draw = async (tabId) => {
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === tabId);
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("a", { hours: 168, label: "last 7 days" }));
  await flush();
  return root;
};
const cardOf = (root, title) =>
  all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith(title))) || null;
const noticesOf = (card) => all(card).filter((n) => n.tag === "div" && /\bstrip\b/.test(n.className) && /\bnotice\b/.test(n.className));
const pickReads = async (root, title) => {
  const select = all(cardOf(root, title)).find((n) => n.tag === "select" && n.attrs["aria-label"] === "Rank by");
  select.value = "reads";
  select.fire("change");
  await flush();
};
/* What a card drew as notices: each line's text, and whether it is text alone (a text node inside the strip, never an element). */
const report = (root, title) => {
  const card = cardOf(root, title);
  if (!card) return { found: false };
  const strips = noticesOf(card);
  return {
    found: true,
    title: all(card).find((n) => n.tag === "h3").textContent.split(" last ")[0],
    notices: strips.map((s) => s.textContent),
    textOnly: strips.every((s) => all(s).slice(1).every((c) => c.tag === "#text")),
  };
};

const out = { cards: {} };
const cpu = await draw("cpu");
await pickReads(cpu, "Top Queries by");
await pickReads(cpu, "Top Procedures by");
out.cards.cpuQueries = report(cpu, "Top Queries by");
out.cards.cpuProcedures = report(cpu, "Top Procedures by");
/* The pick lives at module scope (the page's 60 s rebuild keeps it), so the Queries tab's cards open on Reads without a pick of their own. */
const queries = await draw("queries");
out.cards.queriesQueries = report(queries, "Top Queries by");
out.cards.queriesProcedures = report(queries, "Top Procedures by");
out.asks = asks;
console.log(JSON.stringify(out));
