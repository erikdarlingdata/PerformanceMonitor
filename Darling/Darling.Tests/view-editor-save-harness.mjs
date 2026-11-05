/* #7565: runs the shipped notebook composer (notebook.js renderNotebookEditor) and dashboard composer (editor.js renderEditor)
   against a scripted /api/views and a stand-in DOM. The author fills the form, presses Create, the service answers 201 with a
   warning, and the author presses Save again. The harness prints as one line of JSON every write the page sent. Both composers
   must turn the second press into a PUT to the saved view's id, never a second POST (a twin view). ViewEditorSaveBehaviourTests
   starts it as
       node view-editor-save-harness.mjs <path to wwwroot/js>
   The whole js tree is copied into a scratch folder first (#5279). Only the DOM and fetch are stand-ins. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];

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
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  removeAttribute(k) { delete this.attrs[k]; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c);
    else this.children.splice(i, 0, c);
    return c;
  }
  replaceChildren(...nodes) { this.children = []; this._text = ""; for (const n of nodes) this.appendChild(n); }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  removeEventListener() {}
  focus() {}
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, stopPropagation() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.window = globalThis;
globalThis.document = {
  body: new FakeNode("body"),
  querySelectorAll: () => [],
  querySelector: () => null,
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  createDocumentFragment: () => new FakeNode("#fragment"),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.location = { hash: "#/views/new", pathname: "/", search: "", href: "http://viewer.test/", origin: "http://viewer.test" };
const store = () => { const d = {}; return { getItem: (k) => (k in d ? d[k] : null), setItem: (k, v) => { d[k] = String(v); }, removeItem: (k) => { delete d[k]; } }; };
globalThis.localStorage = store();
globalThis.sessionStorage = store();

/* The scripted service: a create answers 201 with a view that carries one warning, an update answers 200 with the bumped version. */
const writes = [];
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, headers: { get: () => null }, text: async () => JSON.stringify(body) });
const WARNING = "panel 0: the measure 'perfmon_value' charts a running total.";
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
  if (u.pathname === "/api/views" && method === "GET") return reply(200, []);
  if (u.pathname === "/api/views" && method === "POST") {
    writes.push({ method, path: u.pathname });
    return reply(201, { id: 41, name: JSON.parse(init.body).name, version: 1, warnings: [WARNING] });
  }
  if (u.pathname.startsWith("/api/views/") && method === "PUT") {
    const body = JSON.parse(init.body);
    writes.push({ method, path: u.pathname, version: body.version });
    return reply(200, { id: 41, name: body.name, version: (body.version || 1) + 1 });
  }
  return reply(404, { error: "not scripted" });
};
process.on("unhandledRejection", (e) => { if (process.env.HARNESS_DEBUG) console.error(String(e && e.stack)); });

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "view-editor-save-"));
let notebook;
let editor;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  notebook = await load("notebook.js");
  editor = await load("editor.js");
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setTimeout(r, 0)); };
const byLabel = (root, label) => all(root).find((n) => n.attrs["aria-label"] === label);
const byText = (root, tag, text) => all(root).find((n) => n.tag === tag && n.textContent === text);

async function press(main, text) {
  const btn = byText(main, "button", text);
  if (!btn) throw new Error("no button " + text);
  btn.fire("click");
  await flush();
}

/* The notebook: a name, and the blank panel cell removed so the markdown cell alone is a valid notebook. */
async function notebookRun() {
  writes.length = 0;
  const main = new FakeNode("main");
  await notebook.renderNotebookEditor(main, "new");
  const name = byLabel(main, "Notebook name");
  name.value = "Rate notes";
  name.fire("input");
  const remove = all(main).filter((n) => n.tag === "button" && n.textContent === "Remove");
  remove[remove.length - 1].fire("click");
  await flush();
  await press(main, "Create notebook");
  const afterFirst = writes.map((w) => `${w.method} ${w.path}`);
  /* After the first (warned) save the view has an id, so the button must no longer say it creates one. */
  const labelAfterFirst = all(main).find((n) => n.tag === "button" && ["Create notebook", "Save changes"].includes(n.textContent)).textContent;
  await press(main, labelAfterFirst);
  return { labelAfterFirst, afterFirst, all: writes.map((w) => `${w.method} ${w.path}`), versions: writes.map((w) => w.version ?? null) };
}

/* The dashboard: a name and one composed panel (a metric, an aggregate and a chart type) is a valid view. */
async function editorRun() {
  writes.length = 0;
  const main = new FakeNode("main");
  await editor.renderEditor(main, "new");
  const name = byLabel(main, "View name");
  name.value = "Rate view";
  name.fire("input");
  for (const [label, value] of [["Metric", "wait_ms"], ["Aggregate", "sum"], ["Chart type", "line"]]) {
    const select = byLabel(main, label);
    select.value = value;
    select.fire("change");
    await flush();
  }
  await press(main, "Create view");
  const afterFirst = writes.map((w) => `${w.method} ${w.path}`);
  const labelAfterFirst = all(main).find((n) => n.tag === "button" && ["Create view", "Save changes"].includes(n.textContent)).textContent;
  await press(main, labelAfterFirst);
  return { labelAfterFirst, afterFirst, all: writes.map((w) => `${w.method} ${w.path}`), versions: writes.map((w) => w.version ?? null) };
}

const result = { notebook: await notebookRun(), editor: await editorRun() };
console.log(JSON.stringify(result));
