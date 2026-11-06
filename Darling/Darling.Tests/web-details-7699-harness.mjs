/* #7699: the small details of the web pages whose text comes from a column descriptor or a page function. Loads the shipped page scripts and
   prints as one line of JSON what a cell would show: the Latch Stats Severity and Avg/req columns, the Memory Grant Efficiency figures, the
   Memory Broker whole numbers and the Query Store Duration Trend caption. WebDetails7699HarnessTests starts it as
       node web-details-7699-harness.mjs <path to wwwroot/js> */
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
    this.handlers = {};
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  appendChild(child) {
    this.children.push(child);
    return child;
  }
  setAttribute(name, value) {
    this.attrs[name] = String(value);
  }
  addEventListener(type, fn) {
    this.handlers[type] = fn;
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
  addEventListener() {},
  removeEventListener() {},
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-details-7699-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = async (...p) => import(pathToFileURL(path.join(scratch, ...p)).href);
  const { applyFormat } = await load("util.js");
  const { statusToSev } = await load("panels.js");
  const waits = await load("pages", "server-tabs", "sql-waits.js");
  const events = await load("pages", "server-tabs", "sql-events.js");
  const optimization = await load("pages", "finops", "optimization.js");
  const serverTabs = await load("pages", "server-tabs.js");

  const col = (cols, key) => cols.find((c) => c.key === key);
  /* What a cell prints: the column's own display function, else its named format, else the value as is (the order panels.js draws them in). */
  const shown = (c, row) => (typeof c.display === "function" ? c.display(row) : c.format ? applyFormat(c.format, row[c.key]) : String(row[c.key]));

  const out = {};
  const severity = col(waits.LATCH_COLUMNS, "severity");
  out.latchSeverityHeader = severity.label;
  out.latchSeverity = ["LOW", "MEDIUM", "HIGH", null].map((s) => [shown(severity, { severity: s }), severity.statusSev ? statusToSev(s) : null]);
  out.latchAvgHeader = col(waits.LATCH_COLUMNS, "avg_wait_ms_per_request").label;

  const grant = { avg_granted_mb: 0.03, avg_used_mb: 0.0026, efficiency_pct: 9.1, wasted_mb: 0.03, peak_granted_mb: 1536 };
  out.grant = Object.fromEntries(Object.keys(grant).map((k) => [k, shown(col(optimization.GRANT_COLUMNS, k), grant)]));
  out.grantHeaders = optimization.GRANT_COLUMNS.slice(1, 6).map((c) => c.label);

  const broker = { memory_ratio: 99, rate: -13012 };
  out.broker = Object.fromEntries(Object.keys(broker).map((k) => [k, shown(col(events.MEMORY_BROKER_COLUMNS, k), broker)]));

  const sameDay = new Date();
  sameDay.setHours(7, 0, 0, 0);
  const earlier = new Date(sameDay.getTime() - 3 * 86400000);
  out.captionToday = serverTabs.queryStoreSeamCaption(sameDay);
  out.captionEarlier = serverTabs.queryStoreSeamCaption(earlier);
  out.captionNone = serverTabs.queryStoreSeamCaption(null);
  out.captionToday7 = sameDay.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  process.stdout.write(JSON.stringify(out));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
