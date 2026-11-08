/* #8096: draws three shipped pieces of the dashboard under Node on a small fake DOM and prints, as one line of JSON, the detail a reader
   needs from each: the Custom Views list (wwwroot/js/pages/views.js) and the Alert Rules list (pages/alert-rules.js), whose card names are
   clipped to one line by the stylesheet and so must carry the whole name as their title; and the Collection Log grid (COLLECTION_LOG_COLUMNS
   in pages/server-tabs/shared.js through the shared renderer in panels.js), whose Error and Run note cells must stay one line tall with the
   whole text in the cell. WebLayoutDetails8096Tests starts it as
       node web-layout-details-8096-harness.mjs <path to wwwroot/js> <piece>
   where <piece> is views, alert-rules or collection-log. Only the DOM and fetch are stand-ins (the same fake DOM as the list harnesses). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag, text = "") {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = text;
    this.className = ""; this.dataset = {}; this.style = {}; this.value = ""; this.checked = false; this.open = false; this.hidden = false;
    this.disabled = false; this.isRoot = false;
    this.classList = {
      add: (c) => { this.className = (this.className + " " + c).trim(); },
      remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); },
      toggle: (c, on) => { if (on === undefined) on = !this.classList.contains(c); on ? this.classList.add(c) : this.classList.remove(c); },
      contains: (c) => this.className.split(" ").includes(c),
    };
  }
  get parentNode() { return this.parent; }
  get parentElement() { return this.parent; }
  get firstChild() { return this.children[0] || null; }
  get nextSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i >= 0 ? this.parent.children[i + 1] || null : null; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children.forEach((c) => (c.parent = null)); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  removeAttribute(k) { delete this.attrs[k]; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c); else this.children.splice(i, 0, c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  remove() { if (this.parent) this.parent.removeChild(this); }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener(t, fn) { this.listeners[t] = (this.listeners[t] || []).filter((f) => f !== fn); }
  getBoundingClientRect() { return { left: 0, top: 0, right: 1000, bottom: 300, width: 1000, height: 300 }; }
  closest() { return null; }
  contains(n) { for (let x = n; x; x = x.parent) if (x === this) return true; return false; }
  focus() {}
  querySelectorAll() { return []; }
  querySelector() { return null; }
  all(pred, acc = []) { for (const c of this.children) { if (pred(c)) acc.push(c); c.all(pred, acc); } return acc; }
}
globalThis.Node = FakeNode;
globalThis.document = {
  activeElement: null,
  documentElement: { offsetHeight: 0 },
  createElement: (t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  querySelectorAll: () => [],
  querySelector: () => null,
};
globalThis.window = globalThis;
globalThis.scrollY = 0;
globalThis.scrollTo = () => {};
const stored = {};
const storage = {
  getItem: (k) => (Object.prototype.hasOwnProperty.call(stored, k) ? stored[k] : null),
  setItem: (k, v) => { stored[k] = String(v); },
  removeItem: (k) => { delete stored[k]; },
};
globalThis.localStorage = storage;
globalThis.sessionStorage = storage;
process.on("unhandledRejection", () => {});

const jsDir = process.argv[2];
const piece = process.argv[3];

/* A name a little over what a 260px card holds at 1280, the width the list's grid tracks start from. */
export const LONG_NAME = "Blocking and lock waits by database, hourly, for the reporting replicas";
/* A run note of the length a successful run leaves when it names several counters, the shape that wrapped to five lines. */
export const LONG_NOTE = "shred gated: 0 (the XML was read), statement scrub named: 0, plan rows kept: 412, plan rows skipped as duplicates: 18, "
  + "oversized plans deferred: 2, deferred fetch round trips: 3, watermark advanced to 2026-11-06T10:05:00Z";
const LONG_ERROR = "Login failed for user 'perfmon_collector'. Reason: The account is currently locked out. The system administrator can unlock it. "
  + "(Microsoft SQL Server, Error: 18486) after 3 attempts over 00:01:30";

const withClass = (node, cls) => node.all((n) => (" " + n.className + " ").includes(" " + cls + " "));
const settle = () => new Promise((r) => setTimeout(r, 20));
const reply = (body, status = 200) => ({ ok: status < 400, status, text: async () => JSON.stringify(body) });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-layout-details-8096-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  const main = new FakeNode("main");
  main.isRoot = true;
  let out;

  if (piece === "views") {
    globalThis.location = { hash: "#/views", pathname: "/", search: "", href: "http://localhost/#/views", origin: "http://localhost" };
    const view = { id: 1, name: LONG_NAME, kind: "dashboard", version: 1, folder: null, personal: false, created_by: "alice", updated_by: "alice", updated_at: "2026-11-06T10:00:00Z", mine: true };
    globalThis.fetch = async (url) => {
      const u = String(url).split("?")[0];
      if (u === "/api/session") return reply({ can_edit: true, can_own: true });
      if (u === "/api/fleet") return reply({ cards: [] });
      if (u === "/api/views") return reply([view]);
      if (u === "/api/views/1") return reply({ ...view, definition: { panels: [{ viz: "line" }] } });
      return reply([]);
    };
    const page = await load(path.join("pages", "views.js"));
    await page.renderViewList(main);
    await settle();
    out = { names: withClass(main, "vc-name-text").map((n) => ({ text: n.textContent, title: n.getAttribute("title") })) };
  } else if (piece === "alert-rules") {
    globalThis.location = { hash: "#/alert-rules", pathname: "/", search: "", href: "http://localhost/#/alert-rules", origin: "http://localhost" };
    const rule = { id: 1, name: LONG_NAME, enabled: true, version: 1, description: null, updated_at: "2026-11-06T10:00:00Z", updated_by: "alice", last_fired: null, expires_at_utc: null, expired: false };
    globalThis.fetch = async (url) => {
      const u = String(url).split("?")[0];
      if (u === "/api/session") return reply({ can_edit: true, can_own: true });
      if (u === "/api/alert-templates") return reply({ templates: [] });
      if (u === "/api/alerts") return reply([rule]);
      if (u === "/api/alerts/1") return reply({ ...rule, definition: { metric: { measure: "cpu_utilization", aggregate: "max" }, predicate: { op: "ge", warnThreshold: 80 }, scope: { mode: "all" } } });
      return reply([]);
    };
    const page = await load(path.join("pages", "alert-rules.js"));
    await page.renderAlertRuleList(main);
    await settle();
    out = { names: withClass(main, "vc-name-text").map((n) => ({ text: n.textContent, title: n.getAttribute("title") })) };
  } else if (piece === "collection-log") {
    globalThis.location = { hash: "#/server/a/health", pathname: "/", search: "", href: "http://localhost/#/server/a/health", origin: "http://localhost" };
    globalThis.fetch = async () => reply({});
    const panels = await load("panels.js");
    const shared = await load(path.join("pages", "server-tabs", "shared.js"));
    const runs = [
      { collection_time: "2026-11-06T10:05:00Z", collector: "query_store", status: "SUCCESS", duration_ms: 124972, sql_duration_ms: 107334, sql_store_ms: 90000, store_duration_ms: 1200, rows_collected: 412, error_message: null, run_note: LONG_NOTE },
      { collection_time: "2026-11-06T10:00:00Z", collector: "wait_stats", status: "ERROR", duration_ms: 90000, sql_duration_ms: 0, sql_store_ms: null, store_duration_ms: 0, rows_collected: 0, error_message: LONG_ERROR, run_note: null },
    ];
    const table = panels.VIZ.table({ runs }, { rowsKey: "runs", columns: shared.COLLECTION_LOG_COLUMNS, title: "Collection Log", tools: false });
    const headers = table.all((n) => n.tag === "th").map((th) => th.textContent);
    const rowsDrawn = table.all((n) => n.tag === "tr" && n.parent && n.parent.tag === "tbody");
    /* What a long-text cell drew: its classes, and the one-line clamp node inside it when there is one, with its text and title. */
    const cell = (tr, label) => {
      const td = tr.children[headers.indexOf(label)];
      const clamp = td ? withClass(td, "clamp-text")[0] || null : null;
      return td ? { tdClass: td.className, text: td.textContent, clamp: clamp ? { text: clamp.textContent, title: clamp.getAttribute("title") } : null } : null;
    };
    out = {
      headers,
      runNote: cell(rowsDrawn[0], "Run note"),
      error: cell(rowsDrawn[1], "Error"),
    };
  } else {
    throw new Error("unknown piece " + piece);
  }

  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
