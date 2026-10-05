/* Runs the shipped Storage Growth tab (wwwroot/js/pages/finops/storage-growth.js) against a recording fetch and a node-tree DOM,
   and prints as one line of JSON what it asked for: the databases read, the objects read after a click on a database, a pick of
   each window, a rebuild for the same server (the 60 s poll) and a build for another server.
       node finops-storage-growth-window-harness.mjs <path to wwwroot/js> */
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

const fetches = [];
let body = "{}";
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  return { status: 200, ok: true, text: async () => body };
};

const find = (node, tag) => (node.tag === tag ? node : node.children.map((c) => find(c, tag)).find(Boolean) || null);
const paramsOf = (url) => Object.fromEntries(new URL(url, "http://viewer.test").searchParams.entries());
const findAll = (node, tag, out = []) => {
  if (node.tag === tag) out.push(node);
  node.children.forEach((c) => findAll(c, tag, out));
  return out;
};
const findText = (node, tag, text) => findAll(node, tag).find((n) => n.textContent === text) || null;

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "storage-growth-window-"));
try {
  fs.mkdirSync(path.join(scratch, "pages", "finops"), { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "grid-tools.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  fs.copyFileSync(path.join(jsDir, "pages", "finops", "storage-growth.js"), path.join(scratch, "pages", "finops", "storage-growth.js"));
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const { tab } = await import(pathToFileURL(path.join(scratch, "pages", "finops", "storage-growth.js")).href);
  const settle = () => new Promise((r) => setTimeout(r, 20));
  const dbBody = { databases: { status: "ok", database_count: 1, truncated: false, rows: [{ database_name: "Alpha", current_size_mb: 10 }] } };
  const objBody = (days) => ({ database: { status: "ok" }, objects: { status: "ok", window_days: days, object_count: 0, truncated: false, days: [], rows: [] } });
  const seen = () => fetches.map(paramsOf).map((p) => ({ view: p.view, hours: p.hours, database_name: p.database_name ?? null, limit: p.limit ?? null }));
  const out = {};

  // The databases level: no window picker, 24 hours.
  body = JSON.stringify(dbBody);
  fetches.length = 0;
  let root = tab.build("srv-a", {});
  await settle();
  out.databases = { reads: seen(), selects: findAll(root, "select").length };

  // Open the database: the objects read at the default window (30 days = 720 hours), with the picker.
  body = JSON.stringify(objBody(30));
  fetches.length = 0;
  findText(root, "button", "Show objects").handlers.click();
  await settle();
  let select = findAll(root, "select")[0];
  out.objects = { reads: seen(), options: select.children.map((o) => o.attrs.value), value: select.value };

  // Pick 90 days, then 7 days: each re-reads with days * 24.
  for (const days of [90, 7]) {
    fetches.length = 0;
    body = JSON.stringify(objBody(days));
    select = findAll(root, "select")[0];
    select.value = String(days);
    select.handlers.change();
    await settle();
    out["picked" + days] = { reads: seen(), value: findAll(root, "select")[0].value };
  }

  // The poll rebuilds the tab for the same server: one read with the picked window, picker still on it.
  fetches.length = 0;
  root = tab.build("srv-a", {});
  await settle();
  out.rebuilt = { reads: seen(), value: findAll(root, "select")[0].value };

  // Another server starts at the databases level.
  body = JSON.stringify(dbBody);
  fetches.length = 0;
  root = tab.build("srv-b", {});
  await settle();
  out.other = { reads: seen(), selects: findAll(root, "select").length };
  // ... and its own objects read starts at 30 days.
  body = JSON.stringify(objBody(30));
  fetches.length = 0;
  findText(root, "button", "Show objects").handlers.click();
  await settle();
  out.otherObjects = { reads: seen(), value: findAll(root, "select")[0].value };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
