/* #7688: runs the System Events tab's Scheduler Issues card (wwwroot/js/pages/server-tabs.js with the shipped panels.js and util.js) against a
   scripted /api/read answer for a SQL Server on Linux, where the read clears Other-Process CPU and Idle and says why in system_cpu_note.
   Prints as one line of JSON the notice lines the card drew and the text of each cell in its CPU columns. SchedulerIssuesNoteBehaviourTests
   starts it as
       node scheduler-issues-note-harness.mjs <path to wwwroot/js>
   with HARNESS_INPUT set to {"note": <the system_cpu_note sentence>}. Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[process.argv.length - 1];
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
  insertBefore(c, ref) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; const i = this.children.indexOf(ref); if (i < 0) this.children.push(c); else this.children.splice(i, 0, c); return c; }
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

const issues = [
  { event_time: "2026-11-04T10:00:00.000Z", sql_cpu_utilization: 94, other_process_cpu: null, system_idle: null, memory_utilization: 100, page_faults: 5, working_set_delta_mb: 0 },
];
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const answer = name === "get_health_parser_scheduler_issues"
    ? { server: "a", hours_back: 24, issue_count: 1, shown: 1, system_cpu_note: INPUT.note, issues }
    : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(answer) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "scheduler-note-"));
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
const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "events");
const root = new FakeNode("main");
modules.util.mount(root, tab.build("a", { hours: 24, label: "last 24 hours" }));
await flush();
const card = all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith("Scheduler Issues")));
if (!card) { console.log(JSON.stringify({ found: false })); process.exit(0); }
const headers = all(card).filter((n) => n.tag === "th").map((n) => n.textContent);
const row = all(card).find((n) => n.tag === "tr" && n.children.some((c) => c.tag === "td"));
const cells = row ? row.children.filter((c) => c.tag === "td").map((c) => c.textContent) : [];
const cell = (label) => { const i = headers.findIndex((h) => h.startsWith(label)); return i < 0 ? null : cells[i]; };
console.log(JSON.stringify({
  found: true,
  notices: all(card).filter((n) => n.tag === "div" && /\bstrip\b/.test(n.className) && /\bnotice\b/.test(n.className)).map((n) => n.textContent),
  otherProcess: cell("Other-Process CPU"),
  idle: cell("Idle"),
  sqlCpu: cell("SQL CPU"),
}));
