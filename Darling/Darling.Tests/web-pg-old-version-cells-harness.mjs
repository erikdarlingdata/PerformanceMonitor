/* Runs the shipped PostgreSQL Activity and Waits tabs (wwwroot/js/pages/server-tabs.js) against scripted get_pg_top_queries and
   get_pg_wait_sampling answers and prints, as one line of JSON, what the two grids drew: per row, header to cell text. A target
   that does not report a figure (#6836: wal_bytes on PostgreSQL 12 and older, query_id on 13 and older) sends null, and the
   page must draw the not-applicable dash, never 0 or a blank, while a real 0 stays "0".
       node web-pg-old-version-cells-harness.mjs <path to wwwroot/js> */
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
  addEventListener() {}
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

const answers = {
  get_pg_top_queries: {
    queries: [
      { queryid: "101", calls: 5, wal_bytes: null },
      { queryid: "102", calls: 5, wal_bytes: 0 },
      { queryid: "103", calls: 5, wal_bytes: 2048 },
    ],
    evictions: {},
  },
  get_pg_wait_sampling: {
    waits: [
      { event_type: "IO", wait_event: "DataFileRead", queryid: null, samples: 3, backends: 1 },
      { event_type: "CPU", wait_event: "Running", queryid: "0", samples: 2, backends: 1 },
      { event_type: "Lock", wait_event: "relation", queryid: "555", samples: 1, backends: 1 },
    ],
  },
};
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const body = answers[u.pathname.split("/").pop()] || {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pg-old-version-cells-"));
try {
  /* The whole js tree is copied (a page module another PR adds then needs no edit here), and charts.js is replaced by a stub. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
  const find = (node, pred) => (pred(node) ? node : node.children.map((c) => find(c, pred)).find(Boolean) || null);
  const collect = (node, tag, out = []) => {
    if (node.tag === tag) out.push(node);
    node.children.forEach((c) => collect(c, tag, out));
    return out;
  };

  const grid = async (tabId, title) => {
    const tab = tabs.POSTGRES_TABS.find((t) => t.id === tabId);
    const panels = tab.build("srv-a", { hours: 24, label: "24h" }).flat();
    await new Promise((r) => setTimeout(r, 100));
    const panel = panels.find((p) => p && p.tag && find(p, (n) => n.tag === "h3" && n.textContent.startsWith(title)));
    if (!panel) return { present: false, rows: [] };
    const heads = collect(panel, "th").map((th) => th.textContent);
    const rows = collect(panel, "tr").slice(1).map((tr) => {
      const cells = collect(tr, "td").map((td) => td.textContent);
      return Object.fromEntries(heads.map((h, i) => [h, cells[i]]));
    });
    return { present: true, rows };
  };

  console.log(JSON.stringify({
    topQueries: await grid("activity", "Top Query Shapes"),
    sampledWaits: await grid("waits", "Sampled Waits"),
  }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
