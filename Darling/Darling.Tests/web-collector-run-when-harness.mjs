/* Runs the shipped "Run all collectors now" bar (wwwroot/js/pages/collector-run.js) through a scripted run (#7930): the
   run is queued and finishes at once, and get_collection_log answers one run whose collection_time is the bare stamp the
   service writes (ToString("o") of a timestamp column, no zone). Prints the "When" cell beside what localTime makes of the
   same stamp and what a local-time parse makes of it. The caller pins the process zone off UTC with TZ.
       TZ=Asia/Kolkata node web-collector-run-when-harness.mjs <path to wwwroot/js> */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const jsDir = process.argv[2];
const STAMP = "2026-11-05T12:00:00.0000000";

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.handlers = {};
    this.style = {};
    this.className = "";
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() {
    return this.children[0] || null;
  }
  contains(n) {
    return n === this || this.children.some((c) => c.contains(n));
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
  addEventListener(t, fn) {
    (this.handlers[t] = this.handlers[t] || []).push(fn);
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
FakeNode.prototype.isConnected = true;

const CARDS = [{ server_name: "srv-up", display_name: "srv-up", band: "Healthy", connect_down_since: null }];
const RUNS = [{ collector: "wait_stats", collection_time: STAMP, status: "SUCCESS", rows_collected: 3, duration_ms: 120 }];
globalThis.fetch = async (url, init) => {
  const u = new URL(String(url), "http://viewer.test");
  const method = (init && init.method) || "GET";
  let status = 200;
  let body = {};
  if (u.pathname === "/api/session") body = { can_edit: true };
  else if (u.pathname === "/api/fleet") body = { cards: CARDS };
  else if (u.pathname === "/api/collectors/run" && method === "POST") { status = 202; body = { command_id: 7 }; }
  else if (u.pathname === "/api/collectors/run/7") body = { finished: true, status: "succeeded", result: { collectorsRun: 1, rows: 3, skipped: [] } };
  else if (u.pathname === "/api/read/get_collection_log") body = { server: "srv-up", runs: RUNS, run_count: 1, truncated: false };
  return { status, ok: status < 300, headers: { get: () => null }, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "collector-run-when-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const util = await import(pathToFileURL(path.join(scratch, "util.js")).href);
  const run = await import(pathToFileURL(path.join(scratch, "pages", "collector-run.js")).href);
  const collect = (node, pred, out = []) => {
    if (pred(node)) out.push(node);
    node.children.forEach((c) => collect(c, pred, out));
    return out;
  };
  const settle = (ms) => new Promise((r) => setTimeout(r, ms));

  const bar = run.collectorRunBar("srv-up");
  await settle(50);
  const button = collect(bar, (n) => n.tag === "button" && n.textContent === "Run all collectors now")[0];
  for (const fn of button.handlers.click || []) fn({});
  /* The page polls the queued run every POLL_MS (2 s) before it reads the log, so the table lands a little after that. */
  let table = null;
  for (let waited = 0; waited < 10000 && !table; waited += 100) {
    await settle(100);
    table = collect(bar, (n) => n.tag === "table")[0] || null;
  }
  const row = table ? collect(table, (n) => n.tag === "tbody")[0].children[0] : null;
  const cells = row ? row.children.map((c) => c.textContent) : [];
  console.log(
    JSON.stringify({
      tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
      offsetMinutes: new Date(STAMP + "Z").getTimezoneOffset(),
      localHour: new Date(STAMP + "Z").getHours(),
      headers: table ? collect(table, (n) => n.tag === "th").map((n) => n.textContent) : [],
      collector: cells[0] ?? null,
      when: cells[1] ?? null,
      expected: util.localTime(STAMP),
      bare: new Date(STAMP).toLocaleString(),
    })
  );
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
