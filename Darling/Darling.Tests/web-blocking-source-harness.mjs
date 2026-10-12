/* Runs the blocking panels' source line on the web viewer (#5244): wwwroot/js/panels.js (renderPanel), pages/server-tabs.js (line()
   and fanout) and util.js (sourceStrip), with a stand-in DOM and a stub fetch that answers get_blocking_trend and get_blocking_stats
   with the scenario's body. It prints, for every panel of every SQL Server tab, the source lines drawn and their text.
   WebBlockingSourceBehaviourTests starts it as
       node web-blocking-source-harness.mjs <path to wwwroot/js> <scenario>
   The whole js tree is copied into a scratch folder first, then charts.js (the SVG renderer needs a real browser) is replaced. */
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

const rejections = [];
process.on("unhandledRejection", (e) => rejections.push(String(e && e.stack ? e.stack : e)));


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
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  util = await load("util.js");
  panels = await load("panels.js");
  tabs = await load(path.join("pages", "server-tabs.js"));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}


const ROWS = [{ time: "2026-01-01T00:00:00.0000000Z", count: 2 }];
const STAT_ROWS = [{ time: "2026-01-01T00:00:00.0000000Z", event_count: 2, total_duration_ms: 500, max_duration_ms: 300, avg_duration_ms: 250 }];
const bodiesFor = (source) => ({
  trend: { source, trend: ROWS },
  stats: { source, blocking_duration: STAT_ROWS, deadlock_severity: [] },
});
/* What each scenario's two reads answer: the trend (rows under `trend`) and the stats (rows under `blocking_duration`). */
const BODIES = {
  xe: bodiesFor("blocked-process-report"),
  dmv: bodiesFor("DMV snapshot"),
  /* Rows, but no source on the answer: nothing to name. */
  noSource: bodiesFor(null),
  /* A value that is neither tag is not drawn, and is never read as markup. */
  other: bodiesFor("<b>x</b>"),
  /* The read found nothing: zero rows with a null source, and the {status, message} envelope. */
  zeroRows: {
    trend: { source: null, trend: [] },
    stats: { source: null, blocking_duration: [], deadlock_severity: [] },
  },
  envelope: {
    trend: { status: "empty", message: "No blocking." },
    stats: { status: "empty", message: "No blocking." },
  },
};

const settle = async () => {
  for (let i = 0; i < 200; i++) await new Promise((r) => setImmediate(r));
};
const walk = (node, visit) => {
  visit(node);
  for (const c of node.children) walk(c, visit);
};
const hasClass = (node, name) => String(node.className).split(" ").includes(name);

/* Every panel under the root: its title (the h3's first child) and the source lines inside it. */
const panelsOf = (root) => {
  const found = [];
  walk(root, (node) => {
    if (!hasClass(node, "panel")) return;
    const h3 = node.children.find((c) => c.tag === "h3");
    const strips = [];
    walk(node, (x) => {
      if (hasClass(x, "source")) strips.push(x.textContent);
    });
    found.push({ title: h3 && h3.children[0] ? h3.children[0].textContent : "", strips });
  });
  return found;
};

const body = BODIES[scenario];
if (!body) throw new Error("unknown scenario " + scenario);
globalThis.fetch = async (url) => {
  const u = String(url);
  const answer = u.includes("/get_blocking_trend") ? body.trend : u.includes("/get_blocking_stats") ? body.stats : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(answer) };
};

const out = [];
for (const tab of tabs.SERVER_TABS) {
  const holder = new FakeNode("div");
  util.mount(holder, tab.build("SRV1", { hours: 24, label: "last 24 hours" }));
  await settle();
  for (const p of panelsOf(holder)) out.push({ tab: tab.id, ...p });
}
console.log(JSON.stringify({ panels: out, rejections }));
