/* Runs the shipped Optimization, High Impact and Application Connections tabs (wwwroot/js/pages/finops) against a
   recording fetch and a node-tree DOM, and prints as one line of JSON what each tab asked for: the first build, a change
   of the Window picker, a rebuild for the same server (the 60 s poll) and a build for another server.
       node finops-window-picker-harness.mjs <path to wwwroot/js> */
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
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  return { status: 200, ok: true, text: async () => "{}" };
};

const find = (node, tag) => (node.tag === tag ? node : node.children.map((c) => find(c, tag)).find(Boolean) || null);
const hoursOf = (url) => new URL(url, "http://viewer.test").searchParams.get("hours") ?? new URL(url, "http://viewer.test").searchParams.get("hours_back");

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "finops-window-"));
try {
  fs.mkdirSync(path.join(scratch, "pages", "finops"), { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  for (const f of ["optimization.js", "high-impact.js", "application-connections.js"]) {
    fs.copyFileSync(path.join(jsDir, "pages", "finops", f), path.join(scratch, "pages", "finops", f));
  }
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n'
  );
  const out = {};
  for (const name of ["optimization", "high-impact", "application-connections"]) {
    const { tab } = await import(pathToFileURL(path.join(scratch, "pages", "finops", name + ".js")).href);
    const run = async (server, pick) => {
      fetches.length = 0;
      const root = tab.build(server, {});
      const select = find(root, "select");
      if (pick != null) {
        select.value = String(pick);
        select.handlers.change();
      }
      await new Promise((r) => setTimeout(r, 20));
      return { select: select.value, hours: fetches.map(hoursOf), options: select.children.map((o) => o.attrs.value) };
    };
    out[name] = {
      first: await run("srv-a"),
      picked: await run("srv-a", 4),
      rebuilt: await run("srv-a"),
      other: await run("srv-b"),
    };
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
