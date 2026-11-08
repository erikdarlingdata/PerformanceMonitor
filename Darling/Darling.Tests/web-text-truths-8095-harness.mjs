/* #8095: four places where a web sentence or a control describes something other than the data beside it. Each scenario runs the
   shipped code under wwwroot/js under Node, on a fake DOM and a fixture fetch where a page is drawn, and prints one line of JSON.
   WebTextTruths8095Tests starts it as
       node web-text-truths-8095-harness.mjs <path to wwwroot/js> <scenario>
   Scenarios:
     rds-line              plain-text.js on an engine-gate answer whose engine is Amazon RDS for SQL Server, beside the Azure and generic ones
     fleet-attention       the Fleet page's "Needs attention only" control, notice and overflow line on a fleet whose Healthy servers carry an advisory tile
     job-history-fleet     job-history.js's sentence for a not_collected answer under All servers, and for one server
     front-door-fleet      one store section of the front door's Fleet Overview: its problem list, overflow line and updated stamp
   Only the DOM and fetch are stand-ins (the same fake DOM as web-front-door-harness.mjs). */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [jsDir, scenario] = process.argv.slice(-2);

class FakeNode {
  constructor(tag, text) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.dataset = {};
    this.style = {};
    this.className = "";
    this.handlers = {};
    this.isConnected = true;
    this.checked = false;
    this.value = "";
    this.text = text == null ? null : String(text);
    this.classList = { add() {}, remove() {}, toggle() {}, contains: () => false };
  }
  get firstChild() { return this.children[0] || null; }
  get parentNode() { return this.parent || null; }
  appendChild(child) { child.parent = this; this.children.push(child); return child; }
  removeChild(child) { const i = this.children.indexOf(child); if (i >= 0) this.children.splice(i, 1); return child; }
  insertBefore(child, ref) { child.parent = this; const i = ref ? this.children.indexOf(ref) : -1; if (i < 0) this.children.push(child); else this.children.splice(i, 0, child); return child; }
  replaceChild(child, old) { const i = this.children.indexOf(old); if (i >= 0) { child.parent = this; this.children[i] = child; } return old; }
  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  addEventListener(type, fn) { (this.handlers[type] ||= []).push(fn); }
  querySelectorAll() { return []; }
  querySelector() { return null; }
  set textContent(value) { this.children = []; this.text = String(value); }
  get textContent() { return (this.text || "") + this.children.map((c) => c.textContent).join(""); }
}

globalThis.Node = FakeNode;
globalThis.document = {
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (ns, tag) => new FakeNode(tag),
  createTextNode: (text) => new FakeNode("#text", text),
  getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], addEventListener() {},
};
globalThis.window = globalThis;
globalThis.location = { hash: "#/fleet", pathname: "/", search: "", href: "http://localhost/", origin: "http://localhost", reload() {} };
globalThis.history = { pushState() {}, replaceState() {}, back() {} };
const mem = new Map();
globalThis.localStorage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, String(v)), removeItem: (k) => mem.delete(k) };
globalThis.sessionStorage = globalThis.localStorage;
globalThis.addEventListener = () => {};
globalThis.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
globalThis.requestAnimationFrame = (fn) => setTimeout(fn, 0);

const all = (node, pred, out = []) => { if (pred(node)) out.push(node); for (const c of node.children) all(c, pred, out); return out; };
const settle = () => new Promise((r) => setTimeout(r, 40));
const reply = (status, body) => ({ status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) });
const fire = (node, type) => { for (const fn of node.handlers[type] || []) fn({ preventDefault() {} }); };

let handler = () => reply(200, {});
globalThis.fetch = async (url, init = {}) => handler(String(url), init);

/* The engine-gate sentences the service writes (CollectorEngineCapability.NotCollectedMessage), one per engine. */
const EPILOGUE = " This is a permanent engine capability gap, not a collection outage: checking collection health, enabling a collector or starting a capture cannot change it.";
const RDS_RUNNING_JOBS =
  "srv-rds runs on Amazon RDS for SQL Server (an AWS RDS instance). The running_jobs collector does not run on Amazon RDS, " +
  "so this server does not collect the SQL Agent running-job snapshot, and never will." + EPILOGUE;
const AZURE_JOB_HISTORY =
  "srv-az runs on Azure SQL Database (EngineEdition 5), which does not have the SQL Agent job history that this server can read, " +
  "so this server does not collect the SQL Agent job history, and never will." + EPILOGUE + " (The collector for it is job_history.)";
const MI_SYSTEM_HEALTH =
  "srv-mi runs on Azure SQL Managed Instance (EngineEdition 8), which does not have the system_health ring buffer that this server can read, " +
  "so this server does not collect the system_health ring buffer, and never will." + EPILOGUE + " (The collector for it is system_health.)";
const PG_JOB_HISTORY =
  "pg-a runs PostgreSQL. get_job_history reads SQL Server signals (CPU, memory, blocking, deadlocks, waits) that this engine does not collect, " +
  "so it has no figures for this server and never will: any zero or Healthy band would be a guess." + EPILOGUE;

/* A fleet of four: one Critical server, two Healthy servers that carry an advisory tile, one plain Healthy server. The service's problem
   list holds the Critical one and counts the two advisory servers past it, the way BuildRollup does. */
const card = (id, name, extra) => ({
  server_id: id, display_name: name, server_name: name, band: "Healthy", band_label: "Healthy", is_online: true, status: "ok",
  last_collection: "2026-10-28T11:59:00Z", tags: [], is_silenced: false, metric_count: 0, measured_metric_count: 0, has_advisory_tile: false, ...extra,
});
const fleet = {
  total_servers: 4, critical_count: 1, warning_count: 0, offline_count: 0, offline_servers: 0, tags: [], generated_at: "2026-10-28T12:00:00Z",
  cards: [
    card(1, "alpha", { band: "Critical", band_label: "Critical" }),
    card(2, "bravo", { has_advisory_tile: true }),
    card(3, "charlie", { has_advisory_tile: true }),
    card(4, "golf"),
  ],
  worst_servers: [{ display_name: "alpha", band: "Critical", band_label: "Critical", reason: "CPU 95%", advisory_tiles: [] }],
  additional_problem_count: 2,
};

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "web-text-truths-8095-"));
try {
  fs.cpSync(jsDir, scratch, { recursive: true });
  fs.writeFileSync(path.join(scratch, "package.json"), '{ "type": "module" }');
  fs.writeFileSync(
    path.join(scratch, "charts.js"),
    'import { el } from "./util.js";\nexport const SERIES_COLORS = [];\nexport function normalizeColor(c) { return c; }\nexport function renderLineChart() { return el("div", {}); }\n' +
      'export function zoomableLineChart() { return el("div", {}); }\nexport function chartZoomScope() { return ""; }\n'
  );
  const load = (rel) => import(pathToFileURL(path.join(scratch, rel)).href);

  const scenarios = {
    "rds-line": async () => {
      const mod = await load("plain-text.js");
      return { rds: mod.plainText(RDS_RUNNING_JOBS), azure: mod.plainText(AZURE_JOB_HISTORY), other: mod.plainText(MI_SYSTEM_HEALTH), generic: mod.NOT_COLLECTED_LINE };
    },
    "fleet-attention": async () => {
      handler = (url) => (url === "/api/session" ? reply(200, { can_edit: false }) : url === "/api/fleet" ? reply(200, fleet) : reply(200, {}));
      const page = await load("pages/fleet.js");
      const main = new FakeNode("main");
      await page.renderFleet(main);
      await settle();
      const control = all(main, (n) => n.className === "attention-control")[0];
      const toggle = all(main, (n) => String(n.className).split(" ").includes("attention-toggle"))[0];
      const moreBefore = all(main, (n) => n.className === "attention-link" && n.textContent.startsWith("+")).map((n) => n.textContent);
      toggle.checked = true;
      fire(toggle, "change");
      await settle();
      const notice = all(main, (n) => String(n.className).startsWith("attention-note")).map((n) => n.children[0].textContent);
      const shown = all(main, (n) => String(n.className).startsWith("server-card")).length;
      return { title: control ? control.attrs.title : null, more: moreBefore, notice, shown };
    },
    "job-history-fleet": async () => {
      const jh = await load("pages/job-history.js");
      const pg = { kind: "empty", status: "not_collected", message: PG_JOB_HISTORY };
      return {
        allServers: jh.noAgentLine(pg, ""),
        oneServer: jh.noAgentLine(pg, "pg-a"),
        azureAllServers: jh.noAgentLine({ kind: "empty", status: "not_collected", message: AZURE_JOB_HISTORY }, ""),
        emptyAllServers: jh.noAgentLine({ kind: "empty", status: "empty", message: "No job runs matched in the requested time range." }, ""),
      };
    },
    "front-door-fleet": async () => {
      const util = await load("util.js");
      const picker = await load("store-picker.js");
      util.setFrontDoor(true);
      util.setStore("use1");
      handler = (url) => {
        if (url === "/api/front-door") return reply(200, { front_door: true, stores: [{ name: "use1", health: "ok", server_count: 4, last_ok_utc: null }] });
        if (url === "/api/session") return reply(200, { can_edit: false, front_door: true });
        if (url === "/api/fleet") return reply(200, fleet);
        return reply(200, {});
      };
      await picker.refreshStores();
      const page = await load("pages/fleet.js");
      const main = new FakeNode("main");
      await page.renderFleet(main);
      await settle();
      const section = all(main, (n) => n.tag === "section")[0];
      const list = all(section, (n) => n.className === "bandlist")[0];
      const rows = list ? list.children.map((r) => r.textContent) : [];
      if (list && list.children[0]) fire(list.children[0], "click");
      return {
        titles: all(section, (n) => n.tag === "h3").map((n) => n.textContent),
        rows,
        more: all(section, (n) => String(n.className).split(" ").includes("attention-more")).map((n) => n.textContent),
        updated: all(section, (n) => String(n.className).split(" ").includes("meta")).map((n) => n.textContent),
        cards: all(section, (n) => String(n.className).startsWith("server-card")).length,
        hash: location.hash,
      };
    },
  };

  const chosen = scenarios[scenario];
  if (!chosen) throw new Error("unknown scenario " + scenario);
  console.log(JSON.stringify(await chosen()));
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
