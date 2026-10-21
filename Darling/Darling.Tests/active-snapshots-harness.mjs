/* #6302: runs the server page's Queries tab (wwwroot/js/pages/server-tabs.js with the shipped panels.js, util.js and
   pages/active-snapshots.js) against a scripted /api/read answer, and prints as one line of JSON what the Active Queries card asked
   for and drew: the picker's options and choice, and every get_active_queries request the card made. ActiveQueriesSnapshotPickerBehaviourTests
   starts it as
       node active-snapshots-harness.mjs <path to wwwroot/js> <scenario>
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
globalThis.location = { hash: "#/server/a/queries" };

/* Three stored snapshots, newest first. The snapshot list says how many rows each holds; a snapshot read answers that many rows. */
const SNAPSHOTS = [
  { collection_time: "2026-10-21T12:03:00.0000000", row_count: 3 },
  { collection_time: "2026-10-21T12:02:00.0000000", row_count: 2 },
  { collection_time: "2026-10-21T12:01:00.0000000", row_count: 1 },
];
const rowsAt = (time, n) =>
  Array.from({ length: n }, (_, i) => ({ collection_time: time, session_id: 50 + i, database_name: "db1", status: "running", query_text: "select " + i }));

const asks = [];
const answerFor = (name, q) => {
  if (name !== "get_active_queries") return {};
  const ask = { limit: q.get("limit"), collection_time: q.get("collection_time"), snapshots_only: q.get("snapshots_only"), wait_type: q.get("wait_type"), hours: q.get("hours") };
  asks.push(ask);
  /* "kept": the server keeps 168 hours, so a longer window is refused the way every ranged read is (util.js readWithinKeptHistory). */
  if (scenario === "kept" && Number(q.get("hours")) > 168) {
    return { __status: 400, status: "invalid", message: "hours_back value '" + q.get("hours") + "' exceeds maximum of 168 hours (7 days). Use a smaller value." };
  }
  if (q.get("snapshots_only") === "true") {
    if (scenario === "listEmpty") return { status: "empty", message: "No active query snapshots found in the requested time range." };
    return { server: "a", hours_back: 1, total_snapshot_times: 3, snapshot_times_returned: 3, truncated: false, snapshots: SNAPSHOTS };
  }
  const one = q.get("collection_time");
  if (one) {
    const hit = SNAPSHOTS.find((s) => s.collection_time === one);
    return { server: "a", hours_back: 1, collection_time: one, queries: rowsAt(one, hit ? hit.row_count : 0) };
  }
  return { server: "a", hours_back: 1, queries: SNAPSHOTS.flatMap((s) => rowsAt(s.collection_time, s.row_count)) };
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const body = answerFor(name, u.searchParams);
  const status = body && body.__status ? body.__status : 200;
  return { status, ok: status === 200, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "active-snapshots-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = {
    util: await load("util.js"),
    tabs: await load(path.join("pages", "server-tabs.js")),
    snap: await load(path.join("pages", "active-snapshots.js")),
  };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
const draw = async () => {
  const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "queries");
  const root = new FakeNode("main");
  modules.util.mount(root, tab.build("a", scenario === "kept" ? { hours: 720, label: "last 30 days" } : { hours: 1, label: "last 1 hour" }));
  await flush();
  return root;
};
const cardOf = (root) =>
  all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith("Active Queries"))) || null;
const selectOf = (root) => all(root).find((n) => n.tag === "select" && n.attrs["aria-label"] === "Snapshot") || null;
const report = (root) => {
  const select = selectOf(root);
  const card = cardOf(root);
  return {
    hasCard: card !== null,
    hasSelect: select !== null,
    options: select ? all(select).filter((n) => n.tag === "option").map((o) => ({ value: o.attrs.value, label: o.textContent })) : [],
    selected: select ? select.value : null,
    subtitle: card ? card.textContent : "",
    asks: asks.splice(0),
  };
};
const choose = async (root, value) => {
  const select = selectOf(root);
  select.value = value;
  select.fire("change");
  await flush();
};

const out = {};
if (scenario === "default" || scenario === "listEmpty" || scenario === "kept") {
  out.first = report(await draw());
} else if (scenario === "pick") {
  const root = await draw();
  out.first = report(root);
  await choose(root, SNAPSHOTS[1].collection_time);
  out.picked = report(root);
  await choose(root, "all");
  out.all = report(root);
  await choose(root, "latest");
  out.latest = report(root);
} else if (scenario === "jump") {
  /* A jump to a moment (a chart's Active Queries item, a wait link) picks the snapshot nearest it: 12:02:20 is nearer 12:02:00 than 12:03:00. */
  modules.snap.focusSnapshotAt("a", Date.parse("2026-10-21T12:02:20Z"));
  const root = await draw();
  out.first = report(root);
  /* The jump's pick is kept as that snapshot, so the tab's 60 s rebuild draws it again rather than the latest. */
  out.again = report(await draw());
} else if (scenario === "wait") {
  modules.util.setQueryWaitFilter("a", "PAGEIOLATCH_SH");
  out.first = report(await draw());
}
console.log(JSON.stringify(out));
