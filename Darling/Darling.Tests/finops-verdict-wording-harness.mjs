/* Runs the shipped FinOps Utilization and Server Inventory tabs (wwwroot/js/pages/finops/utilization.js, server-inventory.js) against a
   canned fetch and a node-tree DOM, and prints as one line of JSON the text each shows for the verdicts the service can send (#6139):
   too few CPU samples, a high stolen-memory share, and a verdict with neither.
       node finops-verdict-wording-harness.mjs <path to wwwroot/js> */
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

const settle = () => new Promise((r) => setTimeout(r, 30));
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "finops-verdict-wording-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const utilization = (await import(pathToFileURL(path.join(scratch, "pages", "finops", "utilization.js")).href)).tab;
  const inventory = (await import(pathToFileURL(path.join(scratch, "pages", "finops", "server-inventory.js")).href)).tab;

  const util = (verdict, samples, stolen, note, reason) => ({
    status: "ok", server: "srv-a", view: "utilization", verdict, verdict_reason: reason, health_score: 50, health_band: "poor",
    cpu: { avg_cpu_pct: 5, p95_cpu_pct: 8, max_cpu_pct: 9, cpu_samples: samples, cpu_count: 4, cpu_count_unit: "cpus", max_workers: 512, current_workers: 40 },
    memory: { memory_basis: "physical", stolen_memory_pct: stolen, stolen_memory_note: note, buffer_pool_pct: 14, physical_memory_mb: 1024, total_memory_mb: 800, buffer_pool_mb: 220 },
    provisioning_trend: [{ day: "2026-10-18", avg_cpu_pct: 5, p95_cpu_pct: 8, max_cpu_pct: 9, memory_ratio: 1, verdict: "NOT_ENOUGH_DATA" }],
  });
  const out = {};
  const utilizationText = async (payload) => {
    reads["get_finops"] = payload;
    const root = utilization.build("srv-a", {});
    await settle();
    return root.textContent;
  };
  out.lowSamples = await utilizationText(util("NOT_ENOUGH_DATA", 6, 20, null, "The last 24 hours hold 6 CPU samples; a verdict needs at least 12."));
  out.oneSample = await utilizationText(util("NOT_ENOUGH_DATA", 1, 20, null, "x"));
  out.highStolen = await utilizationText(util("UNDER_PROVISIONED", 288, 72.5, "73% of the memory SQL Server holds is outside the buffer pool.", "Memory is under pressure: stolen memory is 73%."));
  out.plain = await utilizationText(util("OVER_PROVISIONED", 288, 20, null, "Low CPU."));

  const inventoryText = async (status) => {
    reads["get_finops_inventory"] = { status: "ok", total_servers: 1, truncated: false, servers: [{ server: "srv-a", provisioning_status: status, health_band: "good" }] };
    const root = inventory.build("srv-a", {});
    await settle();
    return root.textContent;
  };
  out.inventoryLowSamples = await inventoryText("NOT_ENOUGH_DATA");
  out.inventoryOver = await inventoryText("OVER_PROVISIONED");
  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
