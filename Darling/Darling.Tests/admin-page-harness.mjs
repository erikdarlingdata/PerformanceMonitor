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
    routing_key: "SECRET-KEY", slack_webhook_url: "https://hooks.example.test/SECRET-SLACK" },
};
const reads = [];
const payloads = {
  servers: { server_count: 1, servers: [{ server_name: "alpha", display_name: "Alpha", engine_kind: "sqlserver",
    engine_version: "SQL Server 2022", status: "Online", read_only: false, last_collection: "2026-01-01T00:00:00Z",
    password: "SECRET-PW" }] },
  routes: { routes: [SECRETS.route] },
  settings: { alerts_enabled: true, cooldown_minutes: 15, cpu: { enabled: true, threshold_percent: 90, webhook_url: "SECRET-HOOK" },
    health_bands: { deadlock_warn_per_hour: 1, deadlock_critical_per_hour: 5 }, excluded_databases: ["master"],
    analysis: { enabled: true, interval_minutes: 60, smtp_password: "SECRET-PW" }, fleet_sweep: { enabled: true, interval_minutes: 60 },
    smtp_password: "SECRET-PW" },
};
const toolFor = { servers: "list_servers", routes: "get_notification_routes", settings: "get_alert_settings" };

const context = vm.createContext({
  console, el, mount,
  loadingStrip: strip("strip loading"), errorStrip: strip("strip error"), emptyStrip: strip("strip empty"), noticeStrip: strip("strip notice"),
  readTool: async (tool, params) => {
    reads.push({ tool, params });
    const key = Object.keys(toolFor).find((k) => toolFor[k] === tool);
    return { kind: "data", data: payloads[key] };
  },
  VIZ: { table: (data, desc) => el("table", {}, flat((data[desc.rowsKey] || []).map((row) => desc.columns.map((c) => el("td", { text: String(row[c.key] ?? "—") })))) ) },
});
vm.runInContext(source, context);

const main = { kids: [] };
const tab = scenario;
vm.runInContext(`renderAdmin(main, ${JSON.stringify(tab)})`, Object.assign(context, { main }));
await new Promise((r) => setTimeout(r, 20));

const texts = [];
const walk = (n) => { if (!n) return; if (n.text != null) texts.push(String(n.text)); (n.kids || []).forEach(walk); };
walk(main);
console.log(JSON.stringify({ texts, reads, tabs: main.kids.length }));
