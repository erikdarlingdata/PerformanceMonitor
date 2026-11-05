/* Runs the shipped "Run all collectors now" bar (wwwroot/js/pages/collector-run.js) against a scripted /api/session and
   /api/fleet answer (#7590): the button is off with the offline tip for a server whose fleet card is Offline or carries a
   stored failed connect, and on for a reachable one. Prints one line of JSON.
       node web-collector-run-offline-harness.mjs <path to wwwroot/js> */
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
    this.handlers = {};
    this.style = {};
    this.className = "";
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  contains(n) {
    return n === this || this.children.some((c) => c.contains(n));
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
  addEventListener(t, fn) {
    (this.handlers[t] = this.handlers[t] || []).push(fn);
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

/* The page drops a button that has left the document (isConnected is false); every node here is on the page. */
FakeNode.prototype.isConnected = true;

const CARDS = [
  { server_name: "srv-down", display_name: "srv-down", band: "Offline", connect_down_since: "2026-11-04T10:00:00", last_connect_error: "no route" },
  { server_name: "srv-stored", display_name: "srv-stored", band: "Warning", connect_down_since: "2026-11-04T10:00:00", last_connect_error: "no route" },
  { server_name: "srv-up", display_name: "srv-up", band: "Healthy", connect_down_since: null },
];
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = u.pathname === "/api/session" ? { can_edit: true } : u.pathname === "/api/fleet" ? { cards: CARDS } : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "collector-run-offline-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const run = await import(pathToFileURL(path.join(scratch, "pages", "collector-run.js")).href);
  const collect = (node, pred, out = []) => {
    if (pred(node)) out.push(node);
    node.children.forEach((c) => collect(c, pred, out));
    return out;
  };
  const settle = () => new Promise((r) => setTimeout(r, 50));
  const result = {};
  for (const server of ["srv-down", "srv-stored", "srv-up"]) {
    const bar = run.collectorRunBar(server);
    await settle();
    const button = collect(bar, (n) => n.tag === "button" && n.textContent === "Run all collectors now")[0];
    result[server] = button ? { disabled: !!button.disabled, title: button.title ?? button.attrs.title ?? null } : null;
  }
  console.log(JSON.stringify(result));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
