/* Runs the web dashboard's Admin page (wwwroot/js/pages/admin.js) on a scenario and prints every piece of text it
   drew as one line of JSON. AdminPageBehaviourTests starts it as
       node admin-page-harness.mjs <path to admin.js> <scenario>
   The import lines become stand-ins (plain-object DOM, a table that flattens each row through the descriptor's
   columns) and the `export` keywords are dropped; everything else is admin.js as shipped. */
import fs from "node:fs";
import vm from "node:vm";

const [adminPath, scenario] = process.argv.slice(-2);
const source = fs.readFileSync(adminPath, "utf8").replace(/^import .*;[ \t]*\r?$/gm, "").replace(/^export /gm, "");
if (/^import /m.test(source)) throw new Error("admin.js layout changed: the harness cannot strip a multi-line import");

const flat = (x) => (Array.isArray(x) ? x.flatMap(flat) : x == null ? [] : [x]);
const el = (tag, attrs, kids) => ({ tag, attrs: attrs || {}, text: attrs && attrs.text, kids: flat(kids) });
const mount = (parent, nodes) => { parent.kids = flat(nodes); };
const strip = (cls) => (m) => el("div", { class: cls, text: m });

const SECRETS = {
  route: { route_id: 7, metric_match: "Blocking Detected", match_kind: "exact_metric", family: "performance",
    configured_channels: ["slack", "pagerduty"], smtp_recipients: ["ops@example.test"], enabled: true,
    modified_at_utc: "2026-01-01T00:00:00Z", webhook_url: "https://hooks.example.test/SECRET-HOOK", password: "SECRET-PW",
    routing_key: "SECRET-KEY", slack_url: "https://hooks.example.test/SECRET-SLACKURL", pagerduty_routing_key: "SECRET-PDKEY", slack_webhook_url: "https://hooks.example.test/SECRET-SLACK" },
};
const reads = [];
const payloads = {
  servers: { server_count: 2, servers: [
    { server_name: "alpha", display_name: "Alpha", engine: "sqlserver", version: "SQL Server 2022", freshness: "Online",
      status: "Enabled", auth: "Windows", monthly_cost: "$1,234", monthly_cost_usd: 1234, added: "2025-12-31T00:00:00.0000000",
      read_only: false, last_collected: "2026-01-01T00:00:00Z", password: "SECRET-PW" },
    { server_name: "bravo", display_name: "Bravo", engine: "postgres", version: "PostgreSQL 18", freshness: "AwaitingFirstCollection",
      status: "Disabled", auth: "SQL Server", monthly_cost: null, monthly_cost_usd: 0, added: "2026-01-02T00:00:00.0000000",
      read_only: false, last_collected: null }] },
  routes: { routes: [SECRETS.route] },
  settings: { alerts_enabled: true, cooldown_minutes: 15, cpu: { enabled: true, threshold_percent: 90, webhook_url: "SECRET-HOOK", slack_url: "SECRET-SLACKURL", pagerduty_routing_key: "SECRET-PDKEY", connection_string: "SECRET-CS", auth_token: "SECRET-AUTH", future_knob: "UNLISTED-VALUE" },
    long_running_query: { enabled: true, threshold_minutes: 30, max_results: 10, exclude_backups: true, excluded_logins: ["svc"] },
    health_bands: { deadlock_warn_per_hour: 1, deadlock_critical_per_hour: 5 }, excluded_databases: ["master"],
    analysis: { enabled: true, interval_minutes: 60, smtp_password: "SECRET-PW" }, fleet_sweep: { enabled: true, interval_minutes: 60 },
    smtp_password: "SECRET-PW" },
};
const toolFor = { servers: "list_servers", routes: "get_notification_routes", settings: "get_alert_settings" };
const pathFor = { servers: "/api/admin/servers" };

const context = vm.createContext({
  console, el, mount,
  loadingStrip: strip("strip loading"), errorStrip: strip("strip error"), emptyStrip: strip("strip empty"), noticeStrip: strip("strip notice"),
  apiGet: async (path) => {
    reads.push({ tool: path, params: null });
    const key = Object.keys(pathFor).find((k) => pathFor[k] === path);
    return { kind: "data", data: payloads[key] };
  },
  readTool: async (tool, params) => {
    reads.push({ tool, params });
    const key = Object.keys(toolFor).find((k) => toolFor[k] === tool);
    return { kind: "data", data: payloads[key] };
  },
  VIZ: { table: (data, desc) => el("table", {}, flat((data[desc.rowsKey] || []).map((row) => [
    el("tr", { text: "rowclass:" + (typeof desc.rowClass === "function" ? desc.rowClass(row) : "") }),
    ...desc.columns.map((c) => el("td", { text: String(row[c.key] ?? "—") }))]))) },
});
vm.runInContext(source, context);

const main = { kids: [] };
const tab = scenario.replace(/^repaint-/, "");
vm.runInContext(`renderAdmin(main, ${JSON.stringify(tab)})`, Object.assign(context, { main }));
await new Promise((r) => setTimeout(r, 20));
let repaint = null;
if (scenario.startsWith("repaint-")) {
  /* The 60 s poll: render the same tab again and look at the page BEFORE the read lands. */
  const before = main.kids[2];
  const textsBefore = [];
  vm.runInContext(`renderAdmin(main, ${JSON.stringify(tab)})`, context);
  const walkB = (n) => { if (!n) return; if (n.text != null) textsBefore.push(String(n.text)); (n.kids || []).forEach(walkB); };
  walkB(main.kids[2]);
  repaint = { sameBody: main.kids[2] === before, loadingShown: textsBefore.some((t) => t.startsWith("Loading")), rowsKept: textsBefore.length > 1 };
  await new Promise((r) => setTimeout(r, 20));
}

const texts = [];
const walk = (n) => { if (!n) return; if (n.text != null) texts.push(String(n.text)); (n.kids || []).forEach(walk); };
walk(main);
console.log(JSON.stringify({ texts, reads, tabs: main.kids.length, repaint }));
