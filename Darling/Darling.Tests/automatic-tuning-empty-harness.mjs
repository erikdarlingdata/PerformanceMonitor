/* #7813: what the Queries tab prints when get_plan_corrections answers empty. Plan Corrections and Automatic Tuning come from one fanout
   (pages/server-tabs.js), so Plan Corrections prints the tool's sentence whole and Automatic Tuning points at it. Runs the shipped page
   scripts through the real Queries tab on a small fake DOM, with fetch as the only stand-in, and prints as one line of JSON what each of the
   two panel bodies shows for each sentence given. AutomaticTuningEmptyHarnessTests starts it as
       node automatic-tuning-empty-harness.mjs <path to the js folder> <sentence> [<sentence> ...] */
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.parent = null;
    this._text = "";
    this.className = "";
    this.dataset = {};
    this.style = {};
    const set = new Set();
    this.classList = { add: (c) => set.add(c), remove: (c) => set.delete(c), toggle: (c, on) => (on ? set.add(c) : set.delete(c)), contains: (c) => set.has(c) };
  }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); if (k === "class") this.className = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  get firstChild() { return this.children[0] || null; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; return c; }
  addEventListener() {}
  findAll(pred, out = []) { if (pred(this)) out.push(this); this.children.forEach((c) => c.findAll(pred, out)); return out; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createElementNS: (_ns, t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/server/example-sql-01/queries" };

const root = process.argv[2];
const sentences = process.argv.slice(3);
const { SERVER_TABS } = await import(pathToFileURL(root + "/pages/server-tabs.js").href);
const tab = SERVER_TABS.find((t) => t.id === "queries");

/* One fetch answers every panel of the tab: the plan corrections read gets the empty reply with the sentence, every other read is unavailable. */
const out = [];
for (const message of sentences) {
  globalThis.fetch = async (url) => {
    const body = String(url).includes("get_plan_corrections")
      ? { status: "empty", server: "example-sql-01", message, hints: null }
      : { status: "unavailable", message: "n/a" };
    return { ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify(body) };
  };
  const panels = tab.build("example-sql-01", { hours: 6, label: "last 6 hours", server: "example-sql-01" });
  await new Promise((resolve) => setTimeout(resolve, 100));
  const flat = [];
  const walk = (n) => { if (!n) return; if (Array.isArray(n)) return n.forEach(walk); flat.push(n); };
  walk(panels);
  const bodyOf = (title) => {
    const p = flat.flatMap((n) => n.findAll((x) => x.attrs && x.attrs["data-title"] === title))[0];
    if (!p) return null;
    const b = p.findAll((x) => String(x.className).includes("panel-body"))[0];
    return { text: b.textContent, details: b.findAll((x) => x.tag === "details").length };
  };
  out.push({ planCorrections: bodyOf("Plan Corrections"), automaticTuning: bodyOf("Automatic Tuning") });
}
console.log(JSON.stringify(out));
