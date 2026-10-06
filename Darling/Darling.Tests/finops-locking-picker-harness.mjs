/* Runs the shipped Locking & Contention tab (wwwroot/js/pages/finops/locking.js and its shared Database box, database-box.js)
   against a scripted fetch and a node-tree DOM, and prints as one line of JSON what it asked for (#5231, #5244): the first reads, the
   database_name each typed value sent (the six awkward names as ONE name each, blank as none, "salesdb" as "SalesDb"), what a typed
   name did against a list the route cut (nothing until the pause, one read for the newest text, the newest answer shown) and
   that an uncut list reads nothing while typing. Index Analysis shares the box, so its own case runs here too.
       node finops-locking-picker-harness.mjs <path to wwwroot/js> */
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

const fetches = [];
// Scripted answers: databases(url) -> { databases, truncated, delay } for /api/server-databases; any other URL is a tool read.
let databases = () => ({ databases: [], truncated: false });
let toolBody = () => ({ objects: [] });
globalThis.fetch = async (url) => {
  const u = String(url);
  fetches.push(u);
  let payload = toolBody();
  let delay = 0;
  if (u.startsWith("/api/server-databases")) {
    const a = databases(u);
    payload = { server: "x", databases: a.databases, truncated: a.truncated };
    delay = a.delay || 0;
  }
  if (delay) await new Promise((r) => setTimeout(r, delay));
  return { status: 200, ok: true, text: async () => JSON.stringify(payload) };
};

const paramsOf = (url) => new URL(url, "http://viewer.test").searchParams;
const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const byTag = (tag) => (n) => n.tag === tag;
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const isRoute = (u) => u.startsWith("/api/server-databases");
const isRead = (u) => u.startsWith("/api/read/");
const readNames = () => fetches.filter(isRead).map((u) => paramsOf(u).getAll("database_name"));
const options = (root) => findAll(root, byTag("option")).map((o) => o.attrs.value);
const boxOf = (root) => findAll(root, byTag("input")).find((n) => n.attrs.type === "text");
const type = (box, text) => {
  box.value = text;
  box.handlers.input();
};
const commit = async (box, text) => {
  type(box, text);
  box.handlers.change();
  await wait(30);
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "locking-picker-"));
try {
  /* Copy the whole js tree (js/, js/pages/ and every subdirectory) rather than a hand-kept list (#5279): a page module that
     another PR adds then needs no edit here. Only imported files load, so the rest are inert; every stand-in below is
     written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const { tab } = await import(pathToFileURL(path.join(scratch, "pages", "finops", "locking.js")).href);
  const { tab: indexAnalysis } = await import(pathToFileURL(path.join(scratch, "pages", "finops", "index-analysis.js")).href);
  const AWKWARD = ["A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>"];
  const out = {};

  // The first reads: the list from the route, then the table with no database_name; the box lists the names as option values.
  databases = () => ({ databases: ["SalesDb", ...AWKWARD], truncated: false });
  fetches.length = 0;
  let root = tab.build("srv-a", {});
  await wait(40);
  out.first = {
    route: fetches.filter(isRoute).map((u) => ({ server: paramsOf(u).get("server"), search: paramsOf(u).get("search") })),
    reads: readNames(),
    options: options(root),
    images: findAll(root, byTag("img")).length,
  };

  // L8c: a typed "salesdb" is sent as the stored "SalesDb"; the box shows the stored spelling.
  let box = boxOf(root);
  fetches.length = 0;
  await commit(box, "salesdb");
  out.lowercase = { reads: readNames(), shown: box.value };

  // Blank and whitespace-only are All databases: no database_name.
  fetches.length = 0;
  await commit(box, "   ");
  out.blank = { reads: readNames() };

  // A name that matches no suggestion goes as typed.
  fetches.length = 0;
  await commit(box, "Nope");
  out.unknown = { reads: readNames() };

  // The six awkward names, each in the list: each goes as ONE name, exactly as typed.
  out.awkward = {};
  for (const name of AWKWARD) {
    fetches.length = 0;
    await commit(box, name);
    out.awkward[name] = { reads: readNames(), images: findAll(root, byTag("img")).length };
  }

  // The same six names when the list holds none of them: still one name each, and a leading space is kept.
  databases = () => ({ databases: ["SalesDb"], truncated: false });
  root = tab.build("srv-b", {});
  await wait(40);
  box = boxOf(root);
  out.awkwardNotListed = {};
  for (const name of AWKWARD) {
    fetches.length = 0;
    await commit(box, name);
    out.awkwardNotListed[name] = readNames();
  }

  // Two suggestions that differ only by case: the typed text goes as typed unless it equals one exactly.
  databases = () => ({ databases: ["Foo", "foo"], truncated: false });
  root = tab.build("srv-c", {});
  await wait(40);
  box = boxOf(root);
  out.ambiguous = {};
  for (const name of ["FOO", "foo", "Foo"]) {
    fetches.length = 0;
    await commit(box, name);
    out.ambiguous[name] = readNames();
  }

  // An uncut list reads nothing while the reader types.
  fetches.length = 0;
  type(box, "f");
  type(box, "fo");
  await wait(400);
  out.uncutTyping = { routeReads: fetches.filter(isRoute).length };

  // A cut list asks again with the typed text, after a pause, once; the newest answer wins; a name past the cut is matched from it.
  databases = (u) => {
    const search = paramsOf(u).get("search");
    if (search == null) return { databases: ["Alpha", "Beta"], truncated: true };
    if (search === "a") return { databases: ["STALE"], truncated: false, delay: 400 };
    if (search === "sale") return { databases: ["SalesDb", "SalesEU"], truncated: false };
    if (search === "ab") return { databases: ["Abacus"], truncated: false };
    return { databases: [], truncated: false };
  };
  fetches.length = 0;
  root = tab.build("srv-cut", {});
  await wait(40);
  box = boxOf(root);
  const hint = () => findAll(root, (n) => n.className === "finops-note").map((n) => n.textContent);
  out.cut = { options: options(root), hint: hint(), routeReads: fetches.filter(isRoute).length };
  fetches.length = 0;
  type(box, "s");
  type(box, "sa");
  type(box, "sal");
  type(box, "sale");
  const early = fetches.filter(isRoute).length;
  await wait(400);
  out.cutSearch = {
    early,
    searches: fetches.filter(isRoute).map((u) => paramsOf(u).get("search")),
    options: options(root),
    hint: hint(),
  };
  // The name past the cut is matched from the search answer, in its stored spelling.
  fetches.length = 0;
  await commit(box, "SALESDB");
  out.cutMatch = { reads: readNames() };
  // An empty box drops the answer and lists the first page again.
  type(box, "");
  out.cutCleared = { options: options(root) };

  // Newest answer wins: "a" answers late, "ab" answers at once; the late one is dropped.
  fetches.length = 0;
  type(box, "a");
  await wait(300);
  type(box, "ab");
  await wait(300);
  const afterNewest = options(root);
  await wait(300);
  out.newestWins = { afterNewest, afterStale: options(root), searches: fetches.filter(isRoute).map((u) => paramsOf(u).get("search")) };

  // Index Analysis shares the box: a typed "salesdb" goes as "SalesDb" (its list comes from its own read).
  toolBody = () => ({ databases: [{ database_name: "SalesDb" }, { database_name: "Other" }], recommendations: [], recommendation_count: 0 });
  const ia = indexAnalysis.build("srv-ia", {});
  await wait(40);
  const iaBox = boxOf(ia);
  fetches.length = 0;
  await commit(iaBox, "salesdb");
  out.indexAnalysis = { database_name: paramsOf(fetches[0]).getAll("database_name"), shown: iaBox.value, options: options(ia) };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
