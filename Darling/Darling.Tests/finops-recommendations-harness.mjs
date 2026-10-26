/* Runs the shipped FinOps Recommendations tab (wwwroot/js/pages/finops/recommendations.js) against a scripted
   get_finops_recommendations answer and a node-tree DOM, and prints one line of JSON: the class of every cell in the first row,
   keyed by the column's header. The Finding and Detail prose cells must carry the rec-text class, which gives them a minimum width
   in app.css so a phone-width page scrolls the grid sideways instead of squeezing the Detail text into a one-word-wide column.
       node finops-recommendations-harness.mjs <path to wwwroot/js> */
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

const recommendations = [
  {
    category: "Licensing",
    severity: "Medium",
    confidence: "High",
    finding: "Enterprise Edition with no Enterprise-only features in use",
    detail: "No Enterprise-only feature is in use on this server, so Standard Edition would cover the same workload at a lower licence cost.",
    est_savings_usd_month: 1200,
  },
];

globalThis.fetch = async (url) => {
  const u = String(url);
  const payload = u.startsWith("/api/server-databases") ? { server: "x", databases: [], truncated: false } : { recommendations, monthly_cost_usd: 5000 };
  return { status: 200, ok: true, text: async () => JSON.stringify(payload) };
};

const findAll = (node, test, out = []) => {
  if (test(node)) out.push(node);
  node.children.forEach((c) => findAll(c, test, out));
  return out;
};
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "finops-recommendations-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const { tab } = await import(pathToFileURL(path.join(scratch, "pages", "finops", "recommendations.js")).href);
  const root = tab.build("srv-a", {});
  await wait(60);

  const headers = findAll(root, (n) => n.tag === "th").map((n) => n.textContent);
  const bodyRows = findAll(root, (n) => n.tag === "tr").filter((tr) => tr.children.length && tr.children.every((c) => c.tag === "td"));
  const out = { rows: bodyRows.length, classes: {} };
  if (bodyRows.length) {
    const cells = bodyRows[0].children;
    for (const h of ["Category", "Finding", "Detail"]) {
      const i = headers.findIndex((t) => t.startsWith(h));
      out.classes[h] = i >= 0 ? cells[i].className : null;
    }
  }
  console.log(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
