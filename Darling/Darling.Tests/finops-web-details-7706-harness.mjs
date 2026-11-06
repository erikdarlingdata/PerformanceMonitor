/* #7706: the FinOps page details. Runs the shipped Optimization, Recommendations, Version Store (PVS) and Storage Growth tabs against a canned fetch and a
   node-tree DOM, and prints as one line of JSON what each shows. FinOpsWebDetails7706Tests starts it as
       node finops-web-details-7706-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { withDocumentListeners } from "./web-picker-driver.mjs";

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
  set textContent(value) {
    this.children = [];
    this.text = String(value);
  }
  get textContent() {
    return (this.text || "") + this.children.map((c) => c.textContent).join("");
  }
}

globalThis.Node = FakeNode;
globalThis.document = withDocumentListeners({
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
});

const reads = {};
globalThis.fetch = async (url) => {
  const u = String(url);
  const name = Object.keys(reads).find((k) => u.includes("/" + k + "?"));
  return { status: 200, ok: true, text: async () => JSON.stringify(name ? reads[name] : {}) };
};

const settle = () => new Promise((r) => setTimeout(r, 40));
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "finops-web-details-7706-"));
const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const count = (text, part) => text.split(part).length - 1;

try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = async (...p) => import(pathToFileURL(path.join(scratch, ...p)).href);
  const optimization = (await load("pages", "finops", "optimization.js")).tab;
  const recommendations = (await load("pages", "finops", "recommendations.js")).tab;
  const versionStore = (await load("pages", "finops", "version-store.js")).tab;
  const storageGrowth = (await load("pages", "finops", "storage-growth.js")).tab;

  const build = async (tab, name, payload) => {
    reads[name] = payload;
    const root = tab.build("srv-a", {});
    await settle();
    return root;
  };

  const out = {};

  // 1. The Expensive Queries heading carries no count: the caption under it says how many rows came back.
  const opt = await build(optimization, "get_finops", {
    status: "ok", server: "srv-a", view: "optimization", hours_back: 24, monthly_cost_usd: 1000,
    expensive_queries: { status: "ok", window_hours: 24, rows: [1, 2, 3, 4, 5].map((i) => ({ query_hash: "0x0" + i, database_name: "Sales", total_cpu_ms: 100 * i })) },
  });
  out.headings = all(opt).filter((n) => n.tag === "h3").map((n) => n.textContent);
  out.queryCaption = all(opt).map((n) => n.textContent).find((t) => /^Top 5 by CPU/.test(t)) || null;

  // 2. An empty Recommendations tab says "No recommendations" once, in the table. With rows, the count caption stays.
  const none = await build(recommendations, "get_finops_recommendations", { server: "srv-a", monthly_cost_usd: null, recommendations: [] });
  out.emptyRecommendationsMentions = count(none.textContent, "No recommendations");
  const some = await build(recommendations, "get_finops_recommendations", {
    server: "srv-a", monthly_cost_usd: 1000,
    recommendations: [{ category: "Storage", severity: "Low", confidence: "Medium", finding: "f", detail: "d", est_savings_usd_month: null }],
  });
  out.recommendationsCaption = some.textContent.includes("1 recommendation, High severity first.");

  // 6. A secondary copy with no PVS row is said, in the service's words.
  const pvs = (note) => ({
    server: "srv-a", as_of: "2026-11-05T10:00:00.0000000Z", secondary_replica_note: note, trend: [],
    databases: [{ database_name: "master", is_adr_on: false, pvs_size_mb: 0, pvs_measured: true, cleanup_state: "Idle" }],
  });
  const note = "1 database has no row here: this server holds a secondary copy of it in an availability group. Check the primary replica for its persistent version store.";
  out.pvsNoted = (await build(versionStore, "get_pvs_stats", pvs(note))).textContent.includes(note);
  out.pvsNoNote = (await build(versionStore, "get_pvs_stats", pvs(null))).textContent.includes("availability group");

  // 7. Storage Growth: sizes read as mbfit, a blank baseline is a dash with the sentence as its hint, the button column does not sort.
  const sg = await build(storageGrowth, "get_finops", {
    status: "ok", server: "srv-a", view: "storage_growth",
    databases: {
      status: "ok", database_count: 1, truncated: false,
      rows: [{
        database_name: "Sales", current_size_mb: 648, size_7d_ago_mb: 256, size_7d_ago_note: null,
        size_30d_ago_mb: null, size_30d_ago_note: "No sample from 30 days ago",
        growth_7d_mb: 392, growth_30d_mb: null, daily_growth_rate_mb: 0.5, growth_pct_30d: null,
      }],
    },
  });
  const nodes = all(sg);
  out.cells = nodes.filter((n) => n.tag === "td").map((n) => n.textContent);
  const dash = nodes.find((n) => n.tag === "span" && n.attrs.title === "No sample from 30 days ago");
  out.dashHint = dash ? dash.textContent : null;
  // A sortable header carries the sort arrow span (sort-ind); the button column's header must not.
  out.headers = nodes.filter((n) => n.tag === "th").map((n) => [n.textContent, n.children.some((c) => /\bsort-ind\b/.test(c.className))]);

  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
