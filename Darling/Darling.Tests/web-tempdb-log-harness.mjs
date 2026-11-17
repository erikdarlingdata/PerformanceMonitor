/* #8881: what the File I/O tab's tempdb chart draws for the log. Runs the shipped page scripts through the real tab on a small fake DOM, with fetch as the
   only stand-in, and prints as one line of JSON, for each get_tempdb_trend reply given, the panel body's text and how many "Log used" legend entries it has.
   WebTempDbLogTests starts it as
       node web-tempdb-log-harness.mjs <path to the js folder> <reply JSON> [<reply JSON> ...] */
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
const replies = process.argv.slice(3).map((r) => JSON.parse(r));
const { SERVER_TABS } = await import(pathToFileURL(root + "/pages/server-tabs.js").href);
const tab = SERVER_TABS.find((t) => t.id === "io");

const out = [];
for (const reply of replies) {
  globalThis.fetch = async (url) => {
    const body = String(url).includes("get_tempdb_trend") ? reply : { status: "unavailable", message: "n/a" };
    return { ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify(body) };
  };
  const panels = tab.build("example-sql-01", { hours: 6, label: "last 6 hours", server: "example-sql-01" });
  await new Promise((resolve) => setTimeout(resolve, 150));
  const flat = [];
  const walk = (n) => { if (!n) return; if (Array.isArray(n)) return n.forEach(walk); flat.push(n); };
  walk(panels);
  const panel = flat.flatMap((n) => n.findAll((x) => x.attrs && x.attrs["data-title"] === "tempdb"))[0];
  const bodyNode = panel ? panel.findAll((x) => String(x.className).includes("panel-body"))[0] : null;
  const text = bodyNode ? bodyNode.textContent : null;
  out.push({ found: !!panel, text, logUsedMentions: text ? text.split("Log used").length - 1 : 0 });
}
console.log(JSON.stringify(out));
