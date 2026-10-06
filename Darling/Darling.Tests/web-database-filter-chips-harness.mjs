/* Runs the database-scope chips on the web viewer's panels (#5244, #5245): wwwroot/js/panels.js (renderPanel),
   pages/server-tabs.js (panelShell, fanout and the table/stat/line helpers) and util.js (dbScopeChip), with a stand-in DOM and
   a stub fetch, and prints what every panel heading drew as one line of JSON. WebDatabaseFilterChipsBehaviourTests starts it as
       node web-database-filter-chips-harness.mjs <path to wwwroot/js> <scenario>
   Every SQL Server tab is built (the way the filter harness's census does) and the h3 of every panel is read back: its title,
   and the database-scope chip in it, if any. The whole js tree is copied into a scratch folder first, then the stand-ins are
   written over it: charts.js (the SVG renderer needs a real browser) and database-filter-reads.js, which wraps the real module
   and moves a few reads into FILTERED (the shipped list is empty until the pages that wire a read to the filter land). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
const scenario = process.argv[3];

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
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
globalThis.location = { hash: "#/server/SRV1" };

/* Every request answers 200 with an empty object: the panels settle as an empty or error strip, which does not matter here, the
   headings are drawn before any read returns. */
globalThis.fetch = async () => ({ status: 200, ok: true, text: async () => "{}" });

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));

const AWKWARD = ["A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>"];
/* Reads moved into FILTERED for this run, so a mixed tab has all four states: the Queries tab's Top Queries by CPU and the Query
   Store panels, and the Top Queries panel (a composite built by panelShell). */
const SEED = ["get_top_queries_by_cpu", "get_active_queries", "get_query_store_top"];

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "database-filter-chips-"));
let util;
let panels;
let tabs;
try {
  /* The whole js tree (js/, js/pages/ and every subdirectory) is copied rather than a hand-kept list (#5279); every stand-in
     below is written AFTER the copy, so it still replaces the real file. */
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    "export const SERIES_COLORS = ['#111', '#222', '#333', '#444', '#555', '#666'];\n" +
      "export const CATEGORICAL_COLORS = SERIES_COLORS;\n" +
      "export function normalizeColor(color) { return color; }\n" +
      "export function renderLineChart() { return document.createElement('div'); }\n" +
      "export function zoomableLineChart() { return document.createElement('div'); }\n" +
      "export function chartZoomScope() { return ''; }\n"
  );
  fs.copyFileSync(path.join(scratch, "database-filter-reads.js"), path.join(scratch, "database-filter-reads-real.js"));
  fs.writeFileSync(
    path.join(scratch, "database-filter-reads.js"),
    'import * as real from "./database-filter-reads-real.js";\n' +
      "const seed = " + JSON.stringify(SEED) + ";\n" +
      "export const FILTERED = new Set([...real.FILTERED, ...seed]);\n" +
      "export const UNFILTERED = new Set([...real.UNFILTERED].filter((t) => !FILTERED.has(t)));\n" +
      "export const IDENTITY = real.IDENTITY;\n" +
      "export const SERVER_WIDE = real.SERVER_WIDE;\n" +
      "export function readScope(tool) {\n" +
      "  return FILTERED.has(tool) ? 'filtered' : UNFILTERED.has(tool) ? 'unfiltered' : IDENTITY.has(tool) ? 'identity' : SERVER_WIDE.has(tool) ? 'server' : null;\n" +
      "}\n"
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  util = await load("util.js");
  panels = await load("panels.js");
  tabs = await load(path.join("pages", "server-tabs.js"));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const settle = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setImmediate(r));
};
const walk = (node, visit) => {
  visit(node);
  for (const c of node.children) walk(c, visit);
};
const countTag = (node, tag) => {
  let n = 0;
  walk(node, (x) => {
    if (x.tag === tag) n++;
  });
  return n;
};

/* What a panel heading drew: its title (the h3's first child, a text node) and the chip in it, or null. */
const headings = (root) => {
  const found = [];
  walk(root, (node) => {
    if (node.tag !== "h3") return;
    const chips = node.children.filter((c) => c.dataset && c.dataset.dbScope);
    found.push({
      title: node.children[0] ? node.children[0].textContent : "",
      chips: chips.length,
      state: chips[0] ? chips[0].dataset.dbScope : null,
      text: chips[0] ? chips[0].textContent : null,
      chipTitle: chips[0] ? chips[0].getAttribute("title") : null,
      className: chips[0] ? chips[0].className : null,
      chipChildren: chips[0] ? chips[0].children.length : null,
    });
  });
  return found;
};

const buildTabs = async (names) => {
  globalThis.location.hash = "#/server/SRV1";
  util.setActiveDatabaseFilter(names === null ? null : { server: "SRV1", databases: names });
  const out = {};
  let imgs = 0;
  for (const tab of tabs.SERVER_TABS) {
    const holder = new FakeNode("div");
    util.mount(holder, tab.build("SRV1", { hours: 24, label: "last 24 hours" }));
    await settle();
    out[tab.id] = headings(holder);
    imgs += countTag(holder, "img");
  }
  return { tabs: out, imgs };
};

const out = {};
const scenarios = {
  /* Every SQL Server tab with a filter active: the chip on every panel heading, tab by tab. */
  mixed: async () => {
    const r = await buildTabs(["SalesDb", "Orders"]);
    out.tabs = r.tabs;
    out.tabIds = tabs.SERVER_TABS.map((t) => t.id);
  },
  /* The same tabs with no filter, and with an emptied one: no chip anywhere. */
  none: async () => {
    const none = await buildTabs(null);
    out.noFilter = Object.values(none.tabs).flat().filter((h) => h.chips > 0).length;
    out.noFilterPanels = Object.values(none.tabs).flat().length;
    const emptied = await buildTabs([]);
    out.emptied = Object.values(emptied.tabs).flat().filter((h) => h.chips > 0).length;
    globalThis.location.hash = "#/fleet";
    util.setActiveDatabaseFilter({ server: "SRV1", databases: ["SalesDb"] });
    const off = new FakeNode("div");
    util.mount(off, panels.renderPanel({ title: "T", read: "get_blocking", params: { server: "SRV1" }, viz: "table", rowsKey: "rows", columns: [], emptyText: "none" }));
    await settle();
    out.offPage = headings(off);
  },
  /* renderPanel straight: each class, an override, and an identity read, which draws no chip. */
  renderPanel: async () => {
    globalThis.location.hash = "#/server/SRV1";
    util.setActiveDatabaseFilter({ server: "SRV1", databases: ["SalesDb"] });
    const one = async (desc) => {
      const holder = new FakeNode("div");
      util.mount(holder, panels.renderPanel({ viz: "table", rowsKey: "rows", columns: [], emptyText: "none", params: { server: "SRV1" }, ...desc }));
      await settle();
      return headings(holder)[0];
    };
    out.filtered = await one({ title: "F", read: "get_top_queries_by_cpu" });
    out.unfiltered = await one({ title: "U", read: "get_blocking" });
    out.server = await one({ title: "S", read: "get_cpu_utilization" });
    out.identity = await one({ title: "I", read: "get_query_trend" });
    out.plan = await one({ title: "P", read: "get_plan_xml" });
    out.noRead = await one({ title: "N", path: "/api/x" });
    out.overrideServer = await one({ title: "O1", read: "get_current_waits_trend", dbScope: "server" });
    out.overrideUnfiltered = await one({ title: "O2", read: "get_blocking_stats", dbScope: "unfiltered" });
    out.overrideProcessRows = await one({ title: "O3", read: "get_deadlock_detail", dbScope: "process-rows" });
  },
  /* One database at a time, each of the six awkward names: the Top Queries panel's chip names it as text, and no markup was read. */
  awkward: async () => {
    out.names = [];
    for (const name of AWKWARD) {
      const r = await buildTabs([name]);
      const top = r.tabs.queries.find((h) => h.title === "Top Queries by CPU");
      out.names.push({ name, state: top.state, text: top.text, chipTitle: top.chipTitle, chipChildren: top.chipChildren, imgs: r.imgs });
    }
    const all = await buildTabs(AWKWARD);
    const top = all.tabs.queries.find((h) => h.title === "Top Queries by CPU");
    out.all = { state: top.state, text: top.text, chipTitle: top.chipTitle, lines: top.chipTitle.split("\n"), imgs: all.imgs };
  },
};

const chosen = scenarios[scenario];
if (!chosen) throw new Error("unknown scenario " + scenario);
await chosen();
console.log(JSON.stringify({ ...out, rejections }));
