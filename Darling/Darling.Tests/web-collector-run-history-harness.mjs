/* Runs the shipped SQL Server Collection Health tab (wwwroot/js/pages/server-tabs.js) against a scripted /api/read
   answer (#5227): clicking a collector row re-requests get_collection_log for that collector over 168 hours, the pick
   survives a rebuild of the tab, and the chip clears it. Prints one line of JSON.
       node web-collector-run-history-harness.mjs <path to wwwroot/js> */
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

const fetches = [];
const healthBody = { collectors: [{ collector: "wait_stats", status: "HEALTHY", total_runs: 5 }, { collector: "query_store", status: "HEALTHY", total_runs: 7 }], sweep_pressure: {} };
const healthHangs = false;
const logSignals = [];
globalThis.fetch = async (url, opts) => {
  const u = new URL(String(url), "http://viewer.test");
  fetches.push(u.pathname.replace("/api/read/", "") + "?" + u.searchParams.toString());
  if (u.pathname.endsWith("/get_collection_log") && opts && opts.signal) logSignals.push({ collector: u.searchParams.get("collector_name"), signal: opts.signal });
  if (healthHangs && u.pathname.endsWith("/get_collection_health")) return new Promise(() => {});
  const body = u.pathname.endsWith("/get_collection_health") ? healthBody : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "collector-run-history-"));
try {
  fs.mkdirSync(path.join(scratch, "pages"));
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  for (const f of ["util.js", "panels.js", "read-fields.js"]) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  fs.copyFileSync(path.join(jsDir, "pages", "server-tabs.js"), path.join(scratch, "pages", "server-tabs.js"));
  for (const f of ["grid-tools.js", "multi-picker.js", path.join("pages", "analysis-findings.js"), path.join("pages", "plan-viewer.js"), path.join("pages", "pg-plan-viewer.js")]) {
    if (fs.existsSync(path.join(jsDir, f))) fs.copyFileSync(path.join(jsDir, f), path.join(scratch, f));
  }
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport const CATEGORICAL_COLORS = [];\nexport function normalizeColor(c) { return c; }\n' +
      'export function renderLineChart() { return el("div", {}); }\nexport function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const tabs = await import(pathToFileURL(path.join(scratch, "pages", "server-tabs.js")).href);
  const collect = (node, pred, out = []) => {
    if (pred(node)) out.push(node);
    node.children.forEach((c) => collect(c, pred, out));
    return out;
  };
  const settle = () => new Promise((r) => setTimeout(r, 50));
  const logReads = () => fetches.filter((f) => f.startsWith("get_collection_log?")).map((f) => Object.fromEntries(new URL("http://x/?" + f.split("?")[1]).searchParams));
  const tab = tabs.SERVER_TABS.find((t) => t.id === "health");
  const build = async () => {
    fetches.length = 0;
    const panels = tab.build("srv-a", { hours: 24, label: "24h" }).flat();
    await settle();
    return panels;
  };
  const row = (panels, name) => collect({ tag: "root", children: panels }, (n) => n.tag === "tr" && n.textContent.startsWith(name))[0];
  const chips = (panels) => collect({ tag: "root", children: panels }, (n) => n.tag === "button" && n.textContent.includes("\u00d7"));

  const panels = await build();
  const before = logReads();
  const r = row(panels, "query_store");
  const clickable = !!r && r.style.cursor === "pointer" && !!r.getAttribute("title");
  fetches.length = 0;
  for (const h of (r && r.handlers.click) || []) h({});
  await settle();
  const afterClick = logReads();
  const chipAfterClick = chips(panels).map((c) => c.textContent);
  // a second pick aborts the first read; the new one stays live
  const other = row(panels, "wait_stats");
  logSignals.length = 0;
  globalThis.window = { getSelection: () => ({ isCollapsed: true, toString: () => "" }) };
  for (const h of (r && r.handlers.click) || []) h({ type: "click" });
  await settle();
  for (const h of (other && other.handlers.click) || []) h({ type: "click" });
  await settle();
  const pickSignals = logSignals.map((x) => ({ collector: x.collector, aborted: x.signal.aborted }));
  // a click that ends a text selection inside the row picks nothing
  logSignals.length = 0;
  globalThis.window = { getSelection: () => ({ isCollapsed: false, anchorNode: r, toString: () => "query" }) };
  for (const h of (r && r.handlers.click) || []) h({ type: "click" });
  await settle();
  const selectionReads = logSignals.length;
  // Enter on a focused row picks it (the keydown path of makeActivatable)
  globalThis.window = { getSelection: () => ({ isCollapsed: true, toString: () => "" }) };
  let prevented = false;
  for (const h of (r && r.handlers.keydown) || []) h({ key: "Enter", preventDefault: () => { prevented = true; } });
  await settle();
  const enterPick = logSignals.map((x) => x.collector);
  const rowRole = r ? [r.getAttribute("role"), r.getAttribute("tabindex")] : [];
  const chipStyle = chips(panels).map((c) => [c.style.gridColumn, c.style.justifySelf, c.className]);
  // a rebuild (the 60 s poll) keeps the pick
  const rebuilt = await build();
  const afterRebuild = logReads();
  // the chip clears it
  const rebuildChips = chips(rebuilt).length;
  const chip = chips(rebuilt)[0];
  fetches.length = 0;
  for (const h of (chip && chip.handlers.click) || []) h({});
  await settle();
  const afterClear = logReads();
  console.log(JSON.stringify({ before, clickable, afterClick, chipAfterClick, afterRebuild, rebuildChips, afterClear, pickSignals, selectionReads, enterPick, prevented, rowRole, chipStyle }));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
