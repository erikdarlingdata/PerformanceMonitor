/* #8082: draws the shipped Mute Rules page (wwwroot/js/pages/mute-rules.js) or Known Patterns page (pages/known-patterns.js) with the real
   shared renderer (util.js, alerts-api.js) on a small fake DOM and a fixture fetch, and prints as one line of JSON where the rules table
   sits: the class of every box between the table and the page's main element, and the cells a reader has to reach on the right.
   ListPageTableScrollTests starts it as
       node web-list-table-wrap-harness.mjs <path to wwwroot/js> <page>
   where <page> is mute-rules or known-patterns. Only the DOM and fetch are stand-ins (the same fake DOM as known-patterns-harness.mjs). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

class FakeNode {
  constructor(tag) {
    this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.parent = null; this._text = "";
    this.className = ""; this.dataset = {}; this.value = ""; this.checked = false; this.open = false; this.isRoot = false;
  }
  get parentNode() { return this.parent; }
  get firstChild() { return this.children[0] || null; }
  get nextSibling() { const i = this.parent ? this.parent.children.indexOf(this) : -1; return i >= 0 ? this.parent.children[i + 1] || null : null; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n.isRoot; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children.forEach((c) => (c.parent = null)); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  appendChild(c) { if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c); c.parent = this; this.children.push(c); return c; }
  insertBefore(c, ref) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    const i = ref ? this.children.indexOf(ref) : -1;
    if (i < 0) this.children.push(c); else this.children.splice(i, 0, c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  remove() { if (this.parent) this.parent.removeChild(this); }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  all(tag, acc = []) { for (const c of this.children) { if (c.tag === tag) acc.push(c); c.all(tag, acc); } return acc; }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = { createElement: (t) => new FakeNode(t), createTextNode: (t) => new FakeText(t) };
globalThis.location = { hash: "#/mute-rules" };
globalThis.window = { confirm: () => true, location: globalThis.location, history: { replaceState() {} } };

const jsDir = process.argv[2];
const pageName = process.argv[3];

/* One rule and one pattern whose scope and free text run long, the shape that pushes the right-hand columns past a laptop-width page. */
const rule = {
  id: "r1", enabled: true, action: "mute", server_id: 7,
  match_summary: "server srv-prod-01, metric Client Timeouts, database Sales*, app matches Contoso.Billing.Worker*",
  external_source: null,
  reason: "The billing worker's nightly reconciliation times out by design while the warehouse load runs; the vendor's fix lands next quarter.",
  schedule: null, starts_at_utc: null, expires_at_utc: "2030-01-02T03:04:00.0000000Z", created_at_utc: "2026-05-01T00:00:00.0000000Z",
};
const pattern = {
  id: 5, server_id: 7, metric_name: "High CPU", time_of_day: "02:00", time_zone: "America/New_York", window_minutes: 45,
  note: "The nightly index maintenance and the statistics refresh run back to back from two in the morning; CPU sits near the line for the whole window and comes down by itself.",
  created_by: "web", created_at_utc: "2026-05-01T00:00:00.0000000Z", expires_at_utc: null, expired: false,
};

globalThis.fetch = async (url) => {
  const u = String(url);
  let body = {};
  if (u.startsWith("/api/session")) body = { can_edit: true };
  else if (u.startsWith("/api/admin/servers")) body = { servers: [{ server_id: 7, server_name: "srv-prod-01", display_name: "Production 01" }] };
  else if (u.includes("/get_mute_rules")) body = { rules_returned: 1, mute_rules: [rule] };
  else if (u.includes("/get_known_patterns")) body = { entries_returned: 1, known_patterns: [pattern] };
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "list-table-wrap-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return null; }\n'
  );
  const main = new FakeNode("main");
  main.isRoot = true;
  if (pageName === "mute-rules") {
    const page = await import(pathToFileURL(path.join(scratch, "pages", "mute-rules.js")).href);
    await page.renderMuteRules(main, "");
  } else if (pageName === "known-patterns") {
    globalThis.location.hash = "#/known-patterns";
    const page = await import(pathToFileURL(path.join(scratch, "pages", "known-patterns.js")).href);
    await page.renderKnownPatterns(main);
  } else {
    throw new Error("unknown page " + pageName);
  }

  const tables = main.all("table");
  const table = tables[0];
  /* The class of each box from the table's parent up to, not including, main. The nearest one is what the browser scrolls. */
  const boxes = [];
  for (let n = table ? table.parent : null; n && n !== main; n = n.parent) boxes.push(n.className);
  const firstRow = table ? table.children[1].children[0] : null;
  const lastCell = firstRow ? firstRow.children[firstRow.children.length - 1] : null;
  const out = {
    tables: tables.length,
    tableClass: table ? table.className : null,
    boxes,
    headers: table ? table.children[0].children[0].children.map((th) => th.textContent) : [],
    actionButtons: lastCell ? lastCell.all("button").map((b) => b.textContent) : [],
  };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
