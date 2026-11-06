/* #7562: the display details on FinOps, Fleet Sweeps and Availability Groups. Runs the shipped FinOps Utilization, Database Sizes, High Impact and
   Server Inventory tabs, the Fleet Sweeps text helpers and the Availability Groups primary name against a canned fetch and a node-tree DOM, and prints
   as one line of JSON what each shows. FinOpsDisplayDetailsBehaviourTests starts it as
       node finops-display-details-harness.mjs <path to wwwroot/js> */
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
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "finops-display-details-"));
const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
/* The tile whose label reads `label`: [the value's classes, the value's text]. */
const tile = (root, label) => {
  const t = all(root).find((n) => /\bstat\b/.test(n.className) && n.children.length === 2 && n.children[1].textContent === label);
  return t ? [t.children[0].className, t.children[0].textContent] : null;
};
/* The classes of the first node whose whole text is `text` and that carries a severity class. */
const sevOf = (root, text) => {
  const n = all(root).find((x) => x.textContent === text && /\bsev-/.test(x.className));
  return n ? n.className : "";
};
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = async (...p) => import(pathToFileURL(path.join(scratch, ...p)).href);
  const utilization = (await load("pages", "finops", "utilization.js")).tab;
  const inventory = (await load("pages", "finops", "server-inventory.js")).tab;
  const dbSizes = (await load("pages", "finops", "database-sizes.js")).tab;
  const highImpact = (await load("pages", "finops", "high-impact.js")).tab;
  const sweeps = await load("pages", "sweeps.js");
  const ag = await load("pages", "ag.js");

  const util = (verdict, stolen, note) => ({
    status: "ok", server: "srv-a", view: "utilization", verdict, verdict_reason: "x", health_score: 50, health_band: "poor",
    cpu: { avg_cpu_pct: 5, p95_cpu_pct: 8, max_cpu_pct: 9, cpu_samples: 288, cpu_count: 4, cpu_count_unit: "cpus", max_workers: 512, current_workers: 40 },
    memory: { memory_basis: "physical", stolen_memory_pct: stolen, stolen_memory_note: note, buffer_pool_pct: 14, physical_memory_mb: 1024, total_memory_mb: 800, buffer_pool_mb: 220 },
    provisioning_trend: [],
  });
  const build = async (tab, name, payload) => {
    reads[name] = payload;
    const root = tab.build("srv-a", {});
    await settle();
    return root;
  };

  const out = {};
  const under = await build(utilization, "get_finops", util("UNDER_PROVISIONED", 20, null));
  out.verdictTile = tile(under, "Verdict");
  out.stolenPlain = tile(under, "Stolen memory %");
  out.stolenAmber = tile(await build(utilization, "get_finops", util("RIGHT_SIZED", 72.5, "73% of the memory SQL Server holds is outside the buffer pool.")), "Stolen memory %");
  out.stolenJustUnderRed = tile(await build(utilization, "get_finops", util("RIGHT_SIZED", 84.8, "84.8% of the memory SQL Server holds is outside the buffer pool.")), "Stolen memory %");
  out.stolenRed = tile(await build(utilization, "get_finops", util("RIGHT_SIZED", 88, "88% of the memory SQL Server holds is outside the buffer pool.")), "Stolen memory %");
  out.stolenOnTheLine = tile(await build(utilization, "get_finops", util("RIGHT_SIZED", 85, "85% of the memory SQL Server holds is outside the buffer pool.")), "Stolen memory %");

  const files = [
    { database_name: "Sales", file_name: "Sales.mdf", file_type: "ROWS", total_size_mb: 4096, used_size_mb: 3280, free_space_mb: 816, used_pct: 80, max_size_mb: 2097152, auto_growth_mb: 64, is_percent_growth: false },
    { database_name: "Sales", file_name: "Sales.ldf", file_type: "LOG", total_size_mb: 2048, used_size_mb: 200, free_space_mb: 1848, used_pct: 10, max_size_mb: 2048, auto_growth_mb: 64, is_percent_growth: false },
  ];
  const sizes = await build(dbSizes, "get_finops", { status: "ok", server: "srv-a", view: "database_sizes", rows: files });
  out.dbSizeHeaders = all(sizes).filter((n) => n.tag === "th").map((n) => n.textContent);
  out.maxSizeCells = ["2.0 TB", "2.0 GB", "2,048.0 GB"].filter((t) => all(sizes).some((n) => n.textContent === t));

  const impact = await build(highImpact, "get_finops", {
    status: "ok", server: "srv-a", view: "high_impact", hours_back: 24,
    rows: ["high", "medium", "low"].map((b, i) => ({ impact_score: 90 - i * 30, impact_band: b, database_name: "Sales", query_hash: "0x0" + i, total_executions: 1 })),
  });
  out.band = { high: sevOf(impact, "High"), medium: sevOf(impact, "Medium"), low: sevOf(impact, "Low") };

  const osOf = async (text) => {
    const root = await build(inventory, "get_finops_inventory", { status: "ok", total_servers: 1, truncated: false, servers: [{ server: "srv-a", host_os_version: text, health_band: "good" }] });
    return root.textContent;
  };
  out.hostOsFallback = (await osOf("Windows Server 2016 Datacenter 10.0 <X64> (Build 14393: ) (Hypervisor)")).includes("Windows Server 2016 Datacenter 10.0") ? "raw" : "short";
  out.hostOsFallbackText = (await osOf("Windows Server 2016 Datacenter 10.0 <X64> (Build 14393: ) (Hypervisor)")).includes("Windows Server 2016 Datacenter") ? "named" : "missing";
  out.hostOsLinux = (await osOf("Ubuntu 20.04.6 LTS")).includes("Ubuntu 20.04.6 LTS");

  out.ram = sweeps.ramText({ effective_bytes: 4 * 1024 ** 3, total_bytes: 4 * 1024 ** 3, source: "GlobalMemoryStatusEx" });
  const rowText = (rows) => rows.map((r) => r[0] + ": " + r[1]).join(" | ");
  out.livenessReset = rowText(sweeps.livenessRows({ instruments_alive: true }, { baseline_reset: true, alert_passes_total: 9, alert_passes_advancing: null, alert_read_failures_total: 0 }));

  const view = (name, primary, replicas) => ({ ag_name: "AG1", group_id: "g1", server_name: name, primary_replica: primary, replicas });
  const groups = [
    view("sql01", "SQL01", [{ replica_server_name: "SQL01", is_primary: true }]),
    view("sql02", "sql01", [{ replica_server_name: "SQL02", is_primary: false }]),
  ];
  out.primaries = groups.map((g) => ag.primaryText(g, groups));
  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
