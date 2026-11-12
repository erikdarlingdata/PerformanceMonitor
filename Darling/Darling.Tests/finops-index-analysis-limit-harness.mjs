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
// #7402: a plain count is the quiet "source" caption and a cut list is the yellow "notice"; both are the tab's one-line count note.
const noticeTexts = (root) => findAll(root, (n) => String(n.className).split(" ").some((c) => c === "notice" || c === "source")).map((n) => n.textContent);

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

  // #7701 / #7702: a 2 MB size reads in MB (not 0.00), a left-over database carries its mark, and Collected sits right after Database.
  body = JSON.stringify({
    ...payload(1, 1, 500),
    overall: { total_size_gb: 0.0021, total_max_savings_gb: 0.001 },
    databases: [
      { database_name: "Alpha", total_size_gb: 0.002, unused_size_gb: 0.0004, captured_at: "2026-01-15T12:00:00.0000000Z" },
      { database_name: "Alpha", total_size_gb: 0.5, left_over: true, captured_at: "2026-01-05T12:00:00.0000000Z" },
    ],
    // 0.000391 GB is how the read sends an index of 0.4 MB (six places); it must print 0.4 MB, not 0.400384 MB.
    recommendations: [{ ...rec(1), index_size_gb: 0.000391 }],
  });
  root = tab.build("srv-f", {});
  await settle();
  const headerRow = findAll(root, byTag("tr"))[0];
  out.sizes = {
    text: root.textContent,
    header: findAll(headerRow, byTag("th")).map((n) => n.textContent),
    // every grid as header + body rows, so a test can read one cell of one row
    tables: findAll(root, byTag("table")).map((t) => ({
      header: findAll(t, byTag("th")).map((n) => n.textContent),
      rows: findAll(t, byTag("tr")).map((r) => findAll(r, byTag("td")).map((n) => n.textContent)).filter((r) => r.length),
    })),
  };
  // #7702 item 1: the size helper at its edges. Each database carries one Size, sent in GB as the read sends it.
  const mbToGb = (mb) => mb / 1024;
  const edges = {
    Exact: mbToGb(0.005), Below: mbToGb(0.004999), Zero: 0, Negative: mbToGb(-0.4), NegTiny: mbToGb(-0.001),
    Under1: mbToGb(0.994), RoundsTo1: mbToGb(0.999), Over1: mbToGb(1.001), Over1Half: mbToGb(1.5),
  };
  body = JSON.stringify({
    ...payload(1, 1, 500),
    overall: { total_size_gb: 0 },
    databases: Object.keys(edges).map((k) => ({ database_name: k, total_size_gb: edges[k], captured_at: "2026-01-15T12:00:00.0000000Z" })),
    recommendations: [rec(1)],
  });
  root = tab.build("srv-g", {});
  await settle();
  const edgeGrid = findAll(root, byTag("table")).find((t) => findAll(t, byTag("th")).some((n) => n.textContent === "Size"));
  const edgeHeader = findAll(edgeGrid, byTag("th")).map((n) => n.textContent);
  out.edges = {};
  for (const r of findAll(edgeGrid, byTag("tr"))) {
    const cells = findAll(r, byTag("td")).map((n) => n.textContent);
    if (cells.length && edges[cells[edgeHeader.indexOf("Database")]] !== undefined) out.edges[cells[edgeHeader.indexOf("Database")]] = cells[edgeHeader.indexOf("Size")];
  }
  // #7538, #7651: a read that left databases out names them on the empty page and on the page with rows. The empty page's own sentence no
  // longer repeats the count, so the page must draw the left-out strip itself. Each step is every strip under the tab, in the order drawn.
  const leftOutFields = {
    databases_left_out: ["a_old", "b_old"],
    databases_left_out_count: 2,
    databases_left_out_note: "These databases were left out of this read because the server's newest database snapshot no longer lists them. Name one in database_name to read its history.",
  };
  const stripsOf = (node) => findAll(node, (n) => String(n.className).split(" ").some((c) => c === "notice" || c === "source" || c === "empty")).map((n) => n.textContent);
  body = JSON.stringify({ status: "empty", message: "No index statistics for a database this server still has in the latest snapshots.", ...leftOutFields });
  root = tab.build("srv-h", {});
  await settle();
  out.leftOutEmpty = stripsOf(root);
  body = JSON.stringify({ ...payload(3, 3, 500), ...leftOutFields });
  root = tab.build("srv-i", {});
  await settle();
  out.leftOutData = stripsOf(root);
  body = JSON.stringify(payload(3, 3, 500));
  root = tab.build("srv-j", {});
  await settle();
  out.leftOutNone = stripsOf(root);
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
