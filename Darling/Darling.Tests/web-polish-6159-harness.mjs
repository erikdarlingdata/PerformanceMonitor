/* #6159: runs the small web fixes (alert badge name, scroll reset on a route change, update level on the Level tile, secondary Availability Group note)
   with the shipped modules and prints one line of JSON. WebPolish6159BehaviourTests starts it as
       node web-polish-6159-harness.mjs <path to wwwroot/js>
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
  }
  get parentNode() { return this.parent; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot === true; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, target: this, ...e }); }
}
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeNode("#text", t),
  addEventListener() {},
  removeEventListener() {},
};
globalThis.location = { hash: "#/server/a/cpu" };


process.on("unhandledRejection", () => {});
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-polish-6159-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { ag: await load(path.join("pages", "ag.js")), scroll: await load("scroll-reset.js"), localUi: await load("viewer-local-ui.js"), panels: await load("panels.js") };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const out = {};
out.badgeOne = modules.localUi.alertBadgeLabel(1);
out.badgeMany = modules.localUi.alertBadgeLabel(3);
let jumps = 0;
const reset = modules.scroll.makeScrollReset(() => { jumps++; });
const server = (tab) => ({ name: "server", param: "a", tab });
reset(server("cpu"), false); // the first route has nowhere to come from
reset(server("cpu"), true); // the poll re-renders the same route
reset(server("config"), true); // a poll never moves the reader
reset(server("memory"), false); // a tab change starts at the top
reset(server("memory"), false); // the same tab again does not
out.scrollJumps = jumps;
const stat = (data) => all(modules.panels.VIZ.stat(data, { stats: [{ key: "product_level", label: "Level", format: "text", appendKey: "product_update_level" }] }))
  .filter((n) => /(^|\s)value(\s|$)/.test(n.className)).map((n) => n.textContent);
out.levelWithUpdate = stat({ product_level: "RTM", product_update_level: "CU20" });
out.levelWithoutUpdate = stat({ product_level: "RTM", product_update_level: null });
out.primaryNoteHidden = modules.ag.primaryNote({ primary_not_visible: true });
out.primaryNoteNone = modules.ag.primaryNote({});
console.log(JSON.stringify(out));
