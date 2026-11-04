/* #7402: runs the shipped web scripts under a node-tree DOM and prints one line of JSON for WebLayoutWordingBehaviourTests:
   the Query Tag Usage server order, the Optimization cost line, the FinOps caption-or-caveat choice, and a line chart whose points hold no number.
   The whole js tree is copied to a scratch folder first, with charts.js replaced by a stand-in, so only the DOM and fetch are fake.
       node web-layout-wording-harness.mjs <path to wwwroot/js> */
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

let payload = {};
globalThis.fetch = async (url) => {
  const u = String(url);
  const body = u.startsWith("/api/server-databases") ? { server: "x", databases: [], truncated: false } : payload;
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};

const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const wait = (ms) => new Promise((r) => setTimeout(r, ms));
/* The strips of a rendered tab: the class each carries and its text. */
const strips = (root) => findAll(root, (n) => /(^| )strip( |$)/.test(n.className || "")).map((n) => ({ cls: n.className, text: n.textContent }));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-layout-wording-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", { class: "chart" }); }\n' +
      'export function zoomableLineChart() { return el("div", { class: "chart" }); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = (...p) => import(pathToFileURL(path.join(scratch, ...p)).href);
  const out = {};

  /* Query Tag Usage: the sections run in display-name order, not connection-key order. */
  const usage = await load("pages", "query-tag-usage.js");
  const block = { status: "ok", servers: ["k3", "k1", "k2"].map((s) => ({ server: s, status: "empty" })) };
  const names = [{ server_name: "k1", display_name: "Charlie" }, { server_name: "k2", display_name: "Bravo" }, { server_name: "k3", display_name: "Alpha" }];
  const card = usage.sectionCard({ name: "plan_cache", title: "Plan cache" }, block, () => {}, "this time range", names);
  out.serverHeadings = findAll(card, (n) => n.tag === "h4").map((n) => n.textContent);

  /* Optimization: the cost line starts with a capital whatever cost_reason says. */
  const optimization = await load("pages", "finops", "optimization.js");
  out.costLines = [undefined, null, "monthly cost not set", "no monthly cost is set for this server"].map((reason) =>
    optimization.costLine(reason === undefined ? { monthly_cost_usd: null } : { monthly_cost_usd: null, cost_reason: reason })
  );

  /* A plain count is a quiet caption, a caveat stays yellow. */
  const recommendations = await load("pages", "finops", "recommendations.js");
  payload = {
    recommendations: [{ category: "Licensing", severity: "Medium", finding: "f", detail: "d", est_savings_usd_month: 1 }],
    monthly_cost_usd: null,
    skipped_checks: ["check_a"],
  };
  const recRoot = recommendations.tab.build("srv-a", {});
  await wait(60);
  out.recommendationStrips = strips(recRoot);

  const resources = await load("pages", "finops", "database-resources.js");
  const resourceStrips = async (truncated) => {
    payload = { rows: [{ database_name: "a" }], truncated, database_count: 9, hours_back: 24 };
    const root = resources.tab.build("srv-a", {});
    await wait(60);
    return strips(root);
  };
  out.resourcesWhole = await resourceStrips(false);
  out.resourcesCut = await resourceStrips(true);

  /* A line read whose only point has no number: the panel says what happened instead of "No numeric values to chart.". */
  const panels = await load("panels.js");
  const desc = { series: [{ key: "value", label: "Value" }], rowsKey: "trend", xKey: "time", emptyText: "No samples." };
  const point = { time: "2026-11-03T10:00:00Z", value: null, execution_count: null };
  const text = (node) => node.textContent;
  const noteForTool = "1 collection(s) had no knowable rate: the first collection after a restart has nothing to difference against; call get_query_duration_trend with a wider window.";
  out.noRate = text(panels.VIZ.line({ trend: [point], unrated_points: 1, unrated_note: noteForTool }, desc));
  out.noRateWithMessage = text(panels.VIZ.line({ trend: [point], unrated_points: 1, unrated_note: noteForTool, message: "The server restarted a moment ago, so no rate is known yet." }, desc));
  out.noValues = text(panels.VIZ.line({ trend: [point] }, desc));
  out.withValues = panels.VIZ.line({ trend: [{ ...point, value: 3 }] }, desc).className;

  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
