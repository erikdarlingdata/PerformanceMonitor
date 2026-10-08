/* Runs the shipped FinOps page under Node for the PostgreSQL-target rules (Darling web click-through, FinOps):
   the page opens on a SQL Server target, the server list says which targets are PostgreSQL, and on a PostgreSQL target a
   panel that does not apply says one short line instead of the gate paragraph. FinOpsPostgresTargetTests starts it as
       node finops-postgres-target-harness.mjs <path to wwwroot/js> <scenario>
   with HARNESS_REGISTRY (the list_servers rows) and HARNESS_READS (tool name -> answer body) in the environment.
   `fetch` and the DOM are stand-ins; everything else is the shipped code. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.value = "";
    this.listeners = {};
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
  addEventListener(type, listener) {
    (this.listeners[type] = this.listeners[type] || []).push(listener);
  }
  focus() {
    globalThis.document.activeElement = this;
  }
  setSelectionRange(start, end) {
    this.selectionStart = start;
    this.selectionEnd = end;
  }
  /* Enough of querySelector for ".class": the first descendant that carries the class. */
  querySelector(selector) {
    const wanted = String(selector).replace(/^\./, "");
    const walk = (n) => {
      for (const c of n.children || []) {
        if (String(c.className).split(" ").includes(wanted)) return c;
        const hit = walk(c);
        if (hit) return hit;
      }
      return null;
    };
    return walk(this);
  }
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 1000, height: 320 };
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
  activeElement: null,
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
};

const REGISTRY = JSON.parse(process.env.HARNESS_REGISTRY || "[]");
const READS = JSON.parse(process.env.HARNESS_READS || "{}");
globalThis.fetch = async (url) => {
  await new Promise((r) => setTimeout(r, 2));
  const u = String(url);
  const tool = (u.match(/\/api\/read\/([a-z_]+)/) || [])[1];
  const body = tool === "list_servers" ? { server_count: REGISTRY.length, servers: REGISTRY } : READS[tool] || {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
globalThis.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "finops-pg-target-"));
let finops;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  finops = await import(pathToFileURL(path.join(scratch, "pages", "finops.js")).href);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (node, pred, found = []) => {
  if (!node || typeof node !== "object") return found;
  if (pred(node)) found.push(node);
  (node.children || []).forEach((c) => all(c, pred, found));
  return found;
};
const settle = async () => { for (let i = 0; i < 80; i++) await new Promise((r) => setTimeout(r, 2)); };
const picker = (root) => all(root, (n) => n.tag === "select" && n.attrs["aria-label"] === "Server")[0];

const render = async (server, tab) => {
  const main = new FakeNode("main");
  finops.renderFinops(main, server, tab);
  await settle();
  return main;
};

const scenarios = {
  /* A bare #/finops visit: which server the picker lands on, and what each option says. */
  open: async () => {
    const main = await render("", "utilization");
    const sel = picker(main);
    return { selected: sel.value, options: sel.children.map((o) => o.text) };
  },
  /* A tab opened on a named server: the text of every empty strip and notice strip on the page. */
  strips: async () => {
    const main = await render(process.env.HARNESS_SERVER, process.env.HARNESS_TAB);
    const strips = all(main, (n) => /\bstrip\b/.test(String(n.className))).map((n) => n.textContent);
    return { strips };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
const result = await chosen();
console.log(JSON.stringify({ ...result, rejections }));
