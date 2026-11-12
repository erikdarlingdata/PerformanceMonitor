/* #7593: the system_health CPU chart and the system_health Entries grid on the server page's System Events tab. Runs the shipped
   wwwroot/js/pages/server-tabs.js with the shipped charts.js and util.js against a scripted /api/read answer, and prints the CPU
   chart's legend entries, the grid's column headers and the chart panel's text. WebSystemHealthCpuLegendTests starts it as
       node system-health-cpu-legend-harness.mjs <path to wwwroot/js> <scenario>
   Scenario "linux" answers as SQL Server on Linux does (no System CPU, a system_cpu_note); "windows" answers with System CPU. */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag) {
    this.style = {};
    this.classList = {
      add: (c) => { this.className = (this.className + " " + c).trim(); },
      remove: (c) => { this.className = this.className.split(" ").filter((x) => x !== c).join(" "); },
      toggle() {},
      contains: () => false,
    };
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.listeners = {};
    this.parent = null;
    this._text = "";
    this.className = "";
  }
  get parentNode() { return this.parent; }
  getBoundingClientRect() { return { left: 0, top: 0, width: 1000, height: 300 }; }
  closest(tag) { for (let n = this; n; n = n.parent) if (n.tag === tag) return n; return null; }
  get firstChild() { return this.children[0] || null; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  set textContent(v) { this._text = String(v); this.children = []; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) {
    if (c.parent) c.parent.children = c.parent.children.filter((x) => x !== c);
    c.parent = this;
    this.children.push(c);
    return c;
  }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parent = null; }
  addEventListener(t, fn) { (this.listeners[t] ||= []).push(fn); }
  fire(t, e = {}) { for (const fn of this.listeners[t] || []) fn({ preventDefault() {}, ...e }); }
}
class FakeText extends FakeNode { constructor(t) { super("#text"); this._text = t; } }
globalThis.Node = FakeNode;
globalThis.document = {
  body: new FakeNode("body"),
  createElement: (t) => new FakeNode(t),
  createElementNS: (ns, t) => new FakeNode(t),
  createTextNode: (t) => new FakeText(t),
};
globalThis.location = { hash: "#/server/a/events" };

const ago = (h) => new Date(Date.now() - h * 3600000).toISOString().replace("Z", "0000");
const NOTE = "System CPU is not shown for this server: SQL Server on Linux does not report host CPU.";
let reply;
globalThis.fetch = async (url) => {
  const u = new URL(String(url), "http://viewer.test");
  const name = u.pathname.replace("/api/read/", "");
  const body = name === "get_health_parser_system_health" ? reply : {};
  return { status: 200, ok: true, text: async () => JSON.stringify(body) };
};
process.on("unhandledRejection", () => {});

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "system-health-cpu-legend-"));
let modules;
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);
  modules = { util: await load("util.js"), tabs: await load(path.join("pages", "server-tabs.js")) };
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}

const all = (n, out = []) => { out.push(n); for (const c of n.children) all(c, out); return out; };
const flush = async () => { for (let i = 0; i < 25; i++) await new Promise((r) => setTimeout(r, 0)); };
const legend = (host) =>
  all(host)
    .filter((n) => /\bitem\b/.test(n.className) && n.children.some((c) => /\bswatch\b/.test(c.className)))
    .map((n) => n.textContent);
const panelTitled = (root, title) => all(root).find((n) => n.className && /\bpanel\b/.test(n.className) && all(n).some((c) => c.tag === "h3" && c.textContent.startsWith(title)));

const linux = scenario === "linux";
const entries = [3, 2, 1].map((h) => ({ event_time: ago(h), sql_cpu_utilization: 12, system_cpu_utilization: linux ? null : 40, non_yielding_tasks_reported: 0 }));
reply = { status: "ok", server: "a", entries, system_cpu_note: linux ? NOTE : null };

const tab = modules.tabs.SERVER_TABS.find((t) => t.id === "events");
const root = new FakeNode("main");
modules.util.mount(root, tab.build("a", { hours: 24, label: "last 24 hours" }));
await flush();
const cpu = panelTitled(root, "system_health CPU");
const grid = panelTitled(root, "system_health Entries");
console.log(JSON.stringify({
  legend: cpu ? legend(cpu) : null,
  headers: grid ? all(grid).filter((n) => n.tag === "th").map((n) => n.textContent) : null,
  cpuText: cpu ? cpu.textContent : null,
}));
