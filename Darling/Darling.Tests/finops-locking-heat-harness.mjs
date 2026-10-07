/* Runs the shipped Locking & Contention tab (wwwroot/js/pages/finops/locking.js) against a scripted get_object_locking answer and a
   node-tree DOM, and prints as one line of JSON the class of each of the four heat-shaded wait cells per row (#5311). The scripted
   rows carry a `heat` list whose bands do NOT follow their wait values (the smallest value carries the top band), so a cell that took
   its class from the response shows it and one that computed a band from the numbers would not. A row with no `heat` and a row
   with a null band show no band class.
       node finops-locking-heat-harness.mjs <path to wwwroot/js> */
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
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

const row = (name, waits, heat) => {
  const r = {
    database_name: "Db",
    schema_name: "dbo",
    table_name: name,
    index_name: "IX_" + name,
    index_type: "NONCLUSTERED",
    reserved_mb: 1,
    total_rows: 1,
    row_lock_wait_count: 1,
    row_lock_wait_ms: waits[0],
    page_lock_wait_count: 1,
    page_lock_wait_ms: waits[1],
    lock_escalations: 0,
    page_latch_wait_ms: waits[2],
    page_io_latch_wait_ms: waits[3],
  };
  if (heat !== undefined) r.heat = heat;
  return r;
};
const objects = [
  // The smallest waits carry the top bands and the largest carry none: the class can only have come from `heat`.
  row("small", [1, 2, 3, 4], [7, 6, 5, 4]),
  row("large", [9000, 9000, 9000, 9000], [0, 1, 2, 3]),
  row("nulls", [0, 0, 5, 0], [null, null, 3, null]),
  row("noheat", [5, 5, 5, 5]),
];

globalThis.fetch = async (url) => {
  const u = String(url);
  const payload = u.startsWith("/api/server-databases") ? { server: "x", databases: [], truncated: false } : { objects };
  return { status: 200, ok: true, text: async () => JSON.stringify(payload) };
};

const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "locking-heat-"));
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279); every stand-in below is
     written AFTER the copy, so it still replaces the real file. */
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

  const headers = findAll(root, (n) => n.tag === "th").map((n) => n.textContent);
  const heatColumns = ["Row lock wait", "Page lock wait", "Page latch", "Page IO latch"].map((h) => headers.indexOf(h));
  const bodyRows = findAll(root, (n) => n.tag === "tr").filter((tr) => tr.children.length && tr.children.every((c) => c.tag === "td"));
  const out = { headers: headers.length, rows: {} };
  for (const tr of bodyRows) {
    const name = tr.children[2].textContent;
    out.rows[name] = heatColumns.map((i) => tr.children[i].className);
    out.rows[name + "#others"] = tr.children.filter((_, i) => !heatColumns.includes(i)).map((c) => c.className).filter((c) => c.includes("heat-band"));
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
