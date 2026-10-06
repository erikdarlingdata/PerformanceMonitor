/* Runs the shipped Locking & Contention tab (wwwroot/js/pages/finops/locking.js) against a scripted get_object_locking answer and a
   node-tree DOM, and clicks its rows (#5311): the index detail pane. Prints one line of JSON: what the pane said while it waited,
   the request each click made (the detail_* selector), the four counters it drew, which click won when two overlapped, what Escape
   and the Close button did, and what a failed or empty answer showed. Every value in the pane must be a text node.
       node finops-locking-detail-harness.mjs <path to wwwroot/js> */
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
    this.value = "";
    this.checked = false;
    this.handlers = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  removeChild(child) {
    const i = this.children.indexOf(child);
    if (i >= 0) this.children.splice(i, 1);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  getAttribute(name) {
    return name in this.attrs ? this.attrs[name] : null;
  }
  addEventListener(type, fn) {
    this.handlers[type] = fn;
  }
  get isConnected() {
    return true;
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
const docHandlers = {};
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  addEventListener: (type, fn) => (docHandlers[type] = fn),
  removeEventListener: (type, fn) => {
    if (docHandlers[type] === fn) delete docHandlers[type];
  },
};


const row = (name, indexName) => ({
  database_name: "Db",
  schema_name: "dbo",
  table_name: name,
  index_name: indexName === undefined ? "IX_" + name : indexName,
  index_type: "NONCLUSTERED",
  reserved_mb: 1,
  total_rows: 1,
  row_lock_wait_count: 1,
  row_lock_wait_ms: 5,
  page_lock_wait_count: 1,
  page_lock_wait_ms: 5,
  lock_escalations: 0,
  page_latch_wait_ms: 5,
  page_io_latch_wait_ms: 5,
});
const objects = [row("one"), row("slow"), row("fast"), row("bad"), row("gone"), row("html", "<img src=x onerror=alert(1)>")];

const detailCalls = [];
let listCalls = 0;
const counters = (n) => ({ server: "x", detail: { row_lock_count: n, page_lock_count: n + 1, page_latch_wait_count: n + 2, page_io_latch_wait_count: n + 3 } });
globalThis.fetch = async (url) => {
  const u = String(url);
  const respond = (status, payload) => ({ status, ok: status < 400, text: async () => JSON.stringify(payload) });
  if (u.startsWith("/api/server-databases")) return respond(200, { server: "x", databases: [], truncated: false });
  if (!u.includes("detail_table=")) {
    listCalls++;
    return respond(200, { objects });
  }
  detailCalls.push(u);
  const table = new URLSearchParams(u.split("?")[1]).get("detail_table");
  if (table === "slow") {
    await new Promise((r) => setTimeout(r, 150));
    return respond(200, counters(1000));
  }
  if (table === "fast") return respond(200, counters(2000));
  if (table === "bad") return respond(500, { error: "boom: secret detail" });
  if (table === "gone") return respond(200, { status: "empty", message: "No such index in the latest snapshot of x." });
  return respond(200, counters(10));
};

const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "locking-detail-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const { tab } = await import(pathToFileURL(path.join(scratch, "pages", "finops", "locking.js")).href);
  const root = tab.build("srv-a", {});
  await wait(60);

  const pane = root.children[1];
  const bodyRows = () => findAll(root, (n) => n.tag === "tr").filter((tr) => tr.children.length && tr.children.every((c) => c.tag === "td"));
  const click = (table) => {
    const tr = bodyRows().find((r) => r.children[2].textContent === table);
    tr.handlers.click({ type: "click" });
  };
  const out = { paneBefore: pane.textContent };

  click("one");
  out.loading = pane.textContent;
  await wait(60);
  out.afterOne = pane.textContent;
  out.labels = findAll(pane, (n) => n.tag === "dt").map((n) => n.textContent);
  out.values = findAll(pane, (n) => n.tag === "dd").map((n) => n.textContent);
  out.oneCalls = detailCalls.length;
  out.oneUrl = detailCalls[0];

  // Two overlapping clicks: the slow one first, the fast one second. The newest click wins; the slow answer is dropped.
  click("slow");
  click("fast");
  await wait(300);
  out.newest = pane.textContent;
  out.newestValues = findAll(pane, (n) => n.tag === "dd").map((n) => n.textContent);

  click("bad");
  await wait(60);
  out.failure = pane.textContent;
  click("gone");
  await wait(60);
  out.empty = pane.textContent;

  click("html");
  await wait(60);
  out.html = pane.textContent;
  out.htmlElements = findAll(pane, (n) => n.tag === "img" || n.tag === "b").length;

  out.escapeListener = typeof docHandlers.keydown;
  docHandlers.keydown({ key: "a" });
  out.afterOtherKey = pane.textContent === "" ? "closed" : "open";
  docHandlers.keydown({ key: "Escape" });
  out.afterEscape = pane.textContent;
  out.listenerAfterEscape = typeof docHandlers.keydown;

  click("one");
  await wait(60);
  const close = findAll(pane, (n) => n.tag === "button")[0];
  out.closeLabel = close.textContent;
  close.handlers.click({ type: "click" });
  out.afterClose = pane.textContent;
  out.listCalls = listCalls;
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
