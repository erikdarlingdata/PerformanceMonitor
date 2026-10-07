/* Runs the shipped Index Analysis tab (wwwroot/js/pages/finops/index-analysis.js) against a recording fetch and a node-tree DOM,
   and prints as one line of JSON what it asked for and which notices it drew (#5238): the first read's limit, the notice for a
   list the read cut, for a cut list of one database, for a list that fits, for a list of exactly the limit that cuts nothing and for a list
   cut one row short of whole, and the stored spelling the shared Database box sends for a typed name that differs only by case.
       node finops-index-analysis-limit-harness.mjs <path to wwwroot/js> */
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
let body = "{}";
globalThis.fetch = async (url) => {
  fetches.push(String(url));
  return { status: 200, ok: true, text: async () => body };
};

const paramsOf = (url) => Object.fromEntries(new URL(url, "http://viewer.test").searchParams.entries());
const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const byTag = (tag) => (n) => n.tag === tag;
const noticeTexts = (root) => findAll(root, (n) => String(n.className).split(" ").includes("notice")).map((n) => n.textContent);

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "index-analysis-limit-"));
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
  const { tab } = await import(pathToFileURL(path.join(scratch, "pages", "finops", "index-analysis.js")).href);
  const settle = () => new Promise((r) => setTimeout(r, 30));

  const rec = (i) => ({ action: "DISABLE", database_name: "Alpha", schema_name: "dbo", table_name: "T" + i, index_name: "IX" + i, index_size_gb: 1 });
  // The read's envelope, with `shown` recommendations out of `total`, cut when total is more than the limit it was asked for.
  const payload = (shown, total, limit) => ({
    uptime_warning: false,
    dedupe_only_applied: false,
    notes: [],
    overall: { total_max_savings_gb: 1 },
    database_count: 1,
    databases_truncated: false,
    databases: [{ database_name: "Alpha", total_max_savings_gb: 1 }],
    recommendation_count: total,
    truncated: total > shown,
    limit,
    recommendations: Array.from({ length: shown }, (_, i) => rec(i)),
  });
  const seen = () => fetches.map(paramsOf).map((p) => ({ view: p.view, limit: p.limit ?? null, database_name: p.database_name ?? null }));
  const out = {};

  // A server with more findings than the read lists: 500 of 612 come back.
  body = JSON.stringify(payload(500, 612, 500));
  fetches.length = 0;
  let root = tab.build("srv-a", {});
  await settle();
  out.cut = { reads: seen(), notices: noticeTexts(root) };

  // The reader picks a database that also has more findings than the limit lists: the notice does not send them to the box again.
  body = JSON.stringify(payload(500, 503, 500));
  fetches.length = 0;
  const box = findAll(root, byTag("input")).find((n) => n.attrs.type === "text");
  box.value = "Alpha";
  box.handlers.change();
  await settle();
  out.filtered = { reads: seen(), notices: noticeTexts(root) };

  // A list that fits.
  body = JSON.stringify(payload(3, 3, 500));
  fetches.length = 0;
  root = tab.build("srv-b", {});
  await settle();
  out.fits = { reads: seen(), notices: noticeTexts(root) };

  // Exactly as many findings as the limit lists: nothing is below the cut, so nothing says there is.
  body = JSON.stringify(payload(500, 500, 500));
  fetches.length = 0;
  root = tab.build("srv-c", {});
  await settle();
  out.exact = { reads: seen(), notices: noticeTexts(root) };

  // One finding more than the limit lists: the notice counts the one left over in the singular.
  body = JSON.stringify(payload(500, 501, 500));
  fetches.length = 0;
  root = tab.build("srv-d", {});
  await settle();
  out.oneOver = { reads: seen(), notices: noticeTexts(root) };

  // The shared Database box sends the stored spelling (#5244 L8c): a typed "alpha" goes as the listed "Alpha"; a name no list holds goes as typed.
  body = JSON.stringify(payload(3, 3, 500));
  root = tab.build("srv-e", {});
  await settle();
  const stored = findAll(root, byTag("input")).find((n) => n.attrs.type === "text");
  fetches.length = 0;
  stored.value = "alpha";
  stored.handlers.input();
  stored.handlers.change();
  await settle();
  const lower = seen().map((r) => r.database_name);
  fetches.length = 0;
  stored.value = "Gamma";
  stored.handlers.input();
  stored.handlers.change();
  await settle();
  out.storedSpelling = { typedLower: lower, typedUnknown: seen().map((r) => r.database_name), shown: stored.value };
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
