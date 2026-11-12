/* #7936: runs the shipped notebook composer (notebook.js renderNotebookEditor) on a saved notebook with several panel cells against a
   scripted service and a stand-in DOM, then adds, moves and removes cells and changes the scope, and prints as one line of JSON
   what each click rebuilt and what it read. WebNotebookEditorCellsBehaviourTests starts it as
       node web-notebook-editor-cells-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM, focus and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

/* Focus as a browser keeps it: focus() makes a node the active element, and a node taken out of the document (or a node holding
   it) loses it. */
let activeElement = null;
const within = (node, root) => { for (let n = node; n; n = n.parent) if (n === root) return true; return false; };
const detached = (node) => { if (activeElement && within(activeElement, node)) activeElement = null; };

class FakeNode {
  constructor(tag, text) {
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = text == null ? "" : String(text);
    this.className = "";
    this.value = "";
    this.disabled = false;
  }
  get parentNode() { return this.parent; }
  get isConnected() { return true; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this.children.forEach((c) => detached(c)); this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  removeAttribute(k) { delete this.attrs[k]; }
  appendChild(c) {
    if (c.parent) { detached(c); c.parent.children = c.parent.children.filter((x) => x !== c); }
    c.parent = this;
    this.children.push(c);
    return c;
  }
  insertBefore(c, ref) {
    if (c.parent) { detached(c); c.parent.children = c.parent.children.filter((x) => x !== c); }
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c);
    else this.children.splice(i, 0, c);
    return c;
  }
  replaceChildren(...nodes) { this.children.forEach((c) => detached(c)); this.children = []; this._text = ""; for (const n of nodes) this.appendChild(n); }
  removeChild(c) { detached(c); this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener() {}
  focus() { activeElement = this; }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.window = globalThis;
globalThis.document = {
  body: new FakeNode("body"),
  get activeElement() { return activeElement || this.body; },
  querySelectorAll: () => [],
  querySelector: () => null,
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.MutationObserver = class { observe() {} disconnect() {} };
globalThis.location = { hash: "#/notebook/n1/edit", pathname: "/", search: "", href: "http://viewer.test/", origin: "http://viewer.test" };
const store = () => { const d = {}; return { getItem: (k) => (k in d ? d[k] : null), setItem: (k, v) => { d[k] = String(v); }, removeItem: (k) => { delete d[k]; } }; };
globalThis.localStorage = store();
globalThis.sessionStorage = store();

/* The scripted service: one saved notebook with four composed panel cells between two markdown cells, and every compose run
   answered at once with one row. `runs` is every compose run the page sent, in order. */
const PANELS = 4;
const panelCell = (i) => ({ type: "panel", title: "Panel " + i, source: "wait_stats", measure: "wait_ms", aggregate: "sum", viz: "line", timeBucket: "auto" });
const notebook = {
  id: "n1", name: "Cells", description: "", version: 3, folder: null, personal: false,
  definition: { kind: "notebook", range: { hours: 24 }, cells: [{ type: "markdown", text: "# Top" }, ...Array.from({ length: PANELS }, (_, i) => panelCell(i + 1)), { type: "markdown", text: "# Bottom" }] },
};
const runs = [];
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, headers: { get: () => null }, text: async () => JSON.stringify(body) });
globalThis.fetch = async (url, init = {}) => {
  const u = new URL(String(url), "http://viewer.test");
  const method = (init.method || "GET").toUpperCase();
  if (u.pathname === "/api/session") return reply(200, { can_edit: true });
  if (u.pathname === "/api/catalog") {
    return reply(200, {
      reads: [], viz: [],
      compose: {
        measures: [{ source: "wait_stats", key: "wait_ms", measure: "wait_ms", displayName: "Wait time", label: "Wait time", kind: "measure", validAggs: ["sum"], defaultAgg: "sum", unitFamily: "duration", category: "Waits" }],
        dimensions: [], annotationSources: [],
      },
    });
  }
  if (u.pathname === "/api/fleet") return reply(200, { cards: [] });
  if (u.pathname === "/api/views" && method === "GET") return reply(200, [notebook]);
  if (u.pathname === "/api/views/n1" && method === "GET") return reply(200, notebook);
  if (u.pathname === "/api/compose/run" && method === "POST") {
    const body = JSON.parse(init.body);
    runs.push({ measure: body.panel && (body.panel.ratio || body.panel.measure || null), hours: body.hours ?? null });
    return reply(200, { sql: "select 1", rows: [{ bucket: "2026-01-01T00:00:00", value: 1 }] });
  }
  return reply(404, { error: "not scripted" });
};
process.on("unhandledRejection", (e) => { if (process.env.HARNESS_DEBUG) console.error(String(e && e.stack)); });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-notebook-editor-cells-"));
let notebookModule;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  notebookModule = await import(pathToFileURL(path.join(scratch, "notebook.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
/* The composer debounces its previews (350 ms): wait past that, so every preview a click caused has been sent. */
const settle = async () => { await flush(); await new Promise((r) => setTimeout(r, 600)); await flush(); };
const cls = (n) => n.attrs.class || n.className || "";
const cellsBox = (main) => all(main).find((n) => cls(n) === "cells-box");
const cellNodes = (main) => Array.from(cellsBox(main).children);
const titles = (main) => cellNodes(main).map((c) => all(c).find((n) => cls(n) === "cell-editor-title").textContent);
const kinds = (main) => cellNodes(main).map((c) => (cls(c).includes("md-cell") ? "md" : "panel"));
const headButtons = (cell, label) => all(cell).find((n) => n.tag === "button" && n.attrs["aria-label"] === label);
const byText = (root, tag, text) => all(root).find((n) => n.tag === tag && n.textContent === text);
const sameNodes = (before, after) => before.every((n) => after.includes(n));
const mdLabels = (main) => all(main).filter((n) => n.tag === "textarea").map((n) => n.attrs["aria-label"]);
const runsSince = (mark) => runs.slice(mark).map((r) => r.measure);

const main = new FakeNode("main");
await notebookModule.renderNotebookEditor(main, "n1");
await settle();
const out = {};
out.openCells = kinds(main);
out.openRuns = runsSince(0);

/* The author is typing in the first panel cell's title when they add a markdown cell at the end. */
let before = cellNodes(main);
const firstPanel = before[1];
const titleInput = all(firstPanel).find((n) => n.tag === "input" && n.attrs["aria-label"] === "Panel title");
titleInput.focus();
let mark = runs.length;
byText(main, "button", "+ Markdown cell").fire("click");
await settle();
out.addMd = {
  runs: runsSince(mark),
  cells: kinds(main),
  titles: titles(main),
  keptNodes: sameNodes(before, cellNodes(main)),
  newNodes: cellNodes(main).filter((n) => !before.includes(n)).length,
  focusKept: document.activeElement === titleInput,
};

/* "+ Panel cell": only the new cell's preview runs. */
before = cellNodes(main);
mark = runs.length;
byText(main, "button", "+ Panel cell").fire("click");
await settle();
out.addPanel = { runs: runsSince(mark), cells: kinds(main), keptNodes: sameNodes(before, cellNodes(main)), newNodes: cellNodes(main).filter((n) => !before.includes(n)).length };

/* Move the last panel cell up: nothing is read, the cells renumber, and the two swapped nodes are the same nodes in the other order. */
before = cellNodes(main);
const lastPanel = before[before.length - 1];
mark = runs.length;
headButtons(lastPanel, "Move cell up").fire("click");
await settle();
let after = cellNodes(main);
out.moveUp = {
  runs: runsSince(mark),
  titles: titles(main),
  mdLabels: mdLabels(main),
  keptNodes: sameNodes(before, after),
  swapped: after[after.length - 1] === before[before.length - 2] && after[after.length - 2] === lastPanel,
  upDisabledOnFirst: headButtons(after[0], "Move cell up").disabled === true,
  downDisabledOnLast: headButtons(after[after.length - 1], "Move cell down").disabled === true,
  downEnabledOnMoved: headButtons(lastPanel, "Move cell down").disabled === false,
};

/* Remove the first cell (the top markdown): nothing is read, the other nodes stay, and the cells renumber from 1. */
before = cellNodes(main);
mark = runs.length;
headButtons(before[0], "Remove cell").fire("click");
await settle();
after = cellNodes(main);
out.remove = { runs: runsSince(mark), titles: titles(main), keptNodes: sameNodes(before.slice(1), after), count: after.length, firstIsPanel: after[0] === firstPanel, focusKept: document.activeElement === titleInput };

/* A scope change (a new variable): every panel's preview runs again, once each, and nothing else. */
before = cellNodes(main);
mark = runs.length;
byText(main, "button", "+ Add variable").fire("click");
await settle();
out.scopeChange = { runs: runsSince(mark), panelCount: kinds(main).filter((k) => k === "panel").length };

console.log(JSON.stringify(out));
