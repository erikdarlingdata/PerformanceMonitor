/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin: a read-only window onto what the desktop's Manage Servers, Notification Routes and Settings windows show.
   Three tabs, each one existing read, no controls that change anything:
     Servers             list_servers
     Notification Routes get_notification_routes
     Alert Settings      get_alert_settings
   The active tab rides in the hash (#/admin/<tab>) and in module scope, so the 60 s repaint keeps it.
   Every cell is an allow-listed column or a key the settings read emits; a route row's destinations are never
   read (the tool reports channel names only), and a settings key that looks like a credential is dropped before
   it reaches a row. */

import { VIZ } from "../panels.js";
import { el, mount, loadingStrip, errorStrip, emptyStrip, noticeStrip, readTool } from "../util.js";

export const SERVER_COLUMNS = [
  { key: "display_name", label: "Display Name" },
  { key: "server_name", label: "Server" },
  { key: "engine_kind", label: "Engine" },
  { key: "engine_version", label: "Version" },
  { key: "status", label: "Freshness", statusSev: true },
  { key: "read_only", label: "Read only", format: "bool" },
  { key: "last_collection", label: "Last Collected", format: "time" },
];

export const ROUTE_COLUMNS = [
  { key: "route_id", label: "#", format: "int" },
  { key: "enabled", label: "Enabled", format: "bool" },
  { key: "metric_match", label: "Matches" },
  { key: "match_kind", label: "Kind" },
  { key: "family", label: "Family" },
  { key: "channels", label: "Channels set", wrap: true },
  { key: "smtp_recipients", label: "Email to", wrap: true },
  { key: "modified_at_utc", label: "Modified", format: "time" },
];

export const SETTING_COLUMNS = [
  { key: "setting", label: "Setting" },
  { key: "value", label: "Value", wrap: true },
];

/* The groups, in the Settings window's order. A key listed here is read from the payload's top level or from the
   named group; a group the read does not carry is left out rather than shown empty. */
export const SETTING_SECTIONS = [
  {
    title: "Alert thresholds",
    groups: ["cpu", "blocking", "deadlocks", "poison_wait", "long_running_query", "tempdb_space", "low_disk",
      "self_alerts", "pvs", "file_growth", "long_running_job", "failed_job", "database_state", "ag", "delivery"],
    top: ["alerts_enabled", "notify_connection_changes", "notify_connection_down_at_startup", "connection_refire_minutes",
      "cooldown_minutes"],
    groupFields: { long_running_query: ["enabled", "threshold_minutes", "max_results"] },
  },
  {
    title: "Long Running Query Filters",
    groups: ["long_running_query"],
    top: [],
    groupFields: { long_running_query: ["exclude_sp_server_diagnostics", "exclude_wait_for", "exclude_backups",
      "exclude_misc_waits", "exclude_cdc", "excluded_program_name_prefixes", "excluded_logins"] },
  },
  { title: "Health bands", groups: ["health_bands"], top: [] },
  { title: "Global alert filters", groups: [], top: ["excluded_databases"] },
  { title: "Automated analysis", groups: ["analysis"], top: [] },
  { title: "Fleet sweep reports", groups: ["fleet_sweep"], top: [] },
];

/* The fields the page shows for each group. A key the read carries that is not listed here is not drawn, so a field
   added to the service later cannot reach the page until it is listed (and reviewed) here. */
export const GROUP_FIELDS = {
  cpu: ["enabled", "threshold_percent", "mode"],
  blocking: ["enabled", "count_threshold", "wait_threshold_seconds", "pg_count_threshold"],
  deadlocks: ["enabled", "count_threshold", "pg_count_threshold"],
  health_bands: ["deadlock_warn_per_hour", "deadlock_critical_per_hour"],
  poison_wait: ["enabled", "threshold_ms"],
  long_running_query: ["enabled", "threshold_minutes", "max_results", "exclude_sp_server_diagnostics", "exclude_wait_for",
    "exclude_backups", "exclude_misc_waits", "exclude_cdc", "excluded_program_name_prefixes", "excluded_logins"],
  tempdb_space: ["enabled", "threshold_percent"],
  low_disk: ["enabled", "threshold_percent", "threshold_gb", "critical_free_percent", "critical_free_gb"],
  self_alerts: ["disk_free_warn_percent", "disk_free_warn_gb", "collection_stale_minutes", "collection_failure_threshold",
    "store_job_cadence_warn_percent", "retention_hold_warn_ratio", "retention_hold_critical_ratio"],
  pvs: ["enabled", "threshold_percent", "floor_gb"],
  file_growth: ["enabled", "rise_mb", "volume_percent", "lookback_minutes"],
  long_running_job: ["enabled", "multiplier"],
  failed_job: ["enabled", "lookback_minutes"],
  database_state: ["enabled"],
  ag: ["enabled", "lag_threshold_seconds", "redo_queue_threshold_kb", "disconnect_refire_minutes"],
  delivery: ["mode", "per_event_max", "cooldown_minutes"],
  analysis: ["enabled", "interval_minutes", "notifications_enabled", "notify_severity", "notify_cooldown_minutes",
    "uncorroborated_route", "uncorroborated_route_source"],
  fleet_sweep: ["enabled", "interval_minutes"],
};

/* A key that names a credential or a destination is never shown, whatever the read carries. */
const SECRET_KEY = /(url|password|passwd|secret|token|credential|api_?key|routing_key|smtp_user|webhook|connection_?string|auth|pwd)/i;

export function isSecretKey(key) {
  return SECRET_KEY.test(String(key));
}

function valueText(v) {
  if (v == null) return "—";
  if (Array.isArray(v)) return v.length ? v.map(String).join(", ") : "(none)";
  if (typeof v === "boolean") return v ? "yes" : "no";
  if (typeof v === "object") return "—";
  return String(v);
}

/** Setting rows for one group: [{ setting, value }], only the fields the group allow-list names (or `fields`, when a
    section shows part of a group) that the group carries; a secret-shaped key is dropped even if it were listed. */
export function groupRows(prefix, group, fields) {
  if (!group || typeof group !== "object" || Array.isArray(group)) return [];
  const allowed = fields || GROUP_FIELDS[prefix] || [];
  return allowed
    .filter((k) => k in group && !isSecretKey(k))
    .map((k) => ({ setting: prefix + "." + k, value: valueText(group[k]) }));
}

/** The sections of the settings payload as [{ title, rows }]; a section with no rows is dropped. */
export function settingSections(data) {
  const d = data && typeof data === "object" ? data : {};
  return SETTING_SECTIONS.map((s) => {
    const rows = [];
    for (const k of s.top) if (k in d && !isSecretKey(k)) rows.push({ setting: k, value: valueText(d[k]) });
    for (const g of s.groups) rows.push(...groupRows(g, d[g], s.groupFields && s.groupFields[g]));
    return { title: s.title, rows };
  }).filter((s) => s.rows.length);
}

/** Route rows: only the columns above, with the channel names joined. A destination field is never copied. */
export function routeRows(data) {
  const routes = data && Array.isArray(data.routes) ? data.routes : [];
  return routes.map((r) => ({
    route_id: r.route_id,
    enabled: r.enabled,
    metric_match: r.metric_match,
    match_kind: r.match_kind,
    family: r.family,
    channels: Array.isArray(r.configured_channels) ? r.configured_channels.filter((c) => !isSecretKey(c)).join(", ") : null,
    smtp_recipients: Array.isArray(r.smtp_recipients) ? r.smtp_recipients.join(", ") : r.smtp_recipients ?? null,
    modified_at_utc: r.modified_at_utc,
  }));
}

export function serverRows(data) {
  if (Array.isArray(data)) return data;
  return data && Array.isArray(data.servers) ? data.servers : [];
}

const TABS = [
  { id: "servers", label: "Servers" },
  { id: "routes", label: "Notification Routes" },
  { id: "settings", label: "Alert Settings" },
];

/* The tab last shown, so a repaint with a bare #/admin returns to it. */
let activeTab = "servers";
let renderGeneration = 0;
let shownBody = null;
let shownTab = null;

function findTab(id) {
  return TABS.find((t) => t.id === id) || TABS.find((t) => t.id === activeTab) || TABS[0];
}

function tabBar(active) {
  return el("nav", { class: "subtabs", "aria-label": "Admin sections" },
    TABS.map((t) => el("a", {
      class: "subtab" + (t.id === active.id ? " active" : ""),
      href: "#/admin/" + t.id,
      "aria-current": t.id === active.id ? "page" : null,
      text: t.label,
    })));
}

/* A read result as a node, or null once the read is a usable payload (the caller then builds from res.data). */
function failure(res, emptyPrefix) {
  if (res.kind === "aborted" || res.kind === "auth") return emptyStrip("");
  if (res.kind === "error") {
    return emptyPrefix && res.message && res.message.startsWith(emptyPrefix) ? emptyStrip(res.message) : errorStrip(res.message);
  }
  if (res.kind === "empty") return emptyStrip(res.message);
  return null;
}

const SERVER_NOT_SHOWN =
  "Not shown here: whether collection is enabled or disabled, authentication, installed version, monthly cost and Added date. The read does not return them; " +
  "add, edit and remove stay in the desktop Manage Servers window.";

async function buildServers(body, generation) {
  const res = await readTool("list_servers", {});
  if (generation !== renderGeneration) return;
  const bad = failure(res, "No servers are registered");
  if (bad) return mount(body, bad);
  const rows = serverRows(res.data);
  mount(body, [
    noticeStrip(rows.length + (rows.length === 1 ? " server. " : " servers. ") + SERVER_NOT_SHOWN),
    VIZ.table({ servers: rows }, { rowsKey: "servers", columns: SERVER_COLUMNS, emptyText: "No servers are registered yet." }),
  ]);
}

const ROUTES_NOTE =
  "Read only. Destinations (webhook URLs, keys) are never reported by the service; only the names of the channels a " +
  "route sets are shown. Add, edit and test stay in the desktop Settings window.";

async function buildRoutes(body, generation) {
  const res = await readTool("get_notification_routes", {});
  if (generation !== renderGeneration) return;
  const bad = failure(res);
  if (bad) return mount(body, bad);
  const rows = routeRows(res.data);
  mount(body, [
    noticeStrip(ROUTES_NOTE),
    VIZ.table({ routes: rows }, {
      rowsKey: "routes",
      columns: ROUTE_COLUMNS,
      emptyText: "No routes. Every alert goes to every channel configured in Settings.",
    }),
  ]);
}

const SETTINGS_NOTE =
  "Read only. Delivery credentials (SMTP, webhooks) are not reported by this read. The data-collection flags " +
  "(plan capture and similar) are not carried by it either; they stay in the desktop Settings window.";

async function buildSettings(body, generation) {
  const res = await readTool("get_alert_settings", {});
  if (generation !== renderGeneration) return;
  const bad = failure(res);
  if (bad) return mount(body, bad);
  const sections = settingSections(res.data);
  if (!sections.length) return mount(body, emptyStrip("The service reported no alert settings."));
  const nodes = [noticeStrip(SETTINGS_NOTE)];
  for (const s of sections) {
    nodes.push(el("h3", { class: "section-title", text: s.title }));
    nodes.push(VIZ.table({ rows: s.rows }, { rowsKey: "rows", columns: SETTING_COLUMNS }));
  }
  mount(body, nodes);
}

const BUILDERS = { servers: buildServers, routes: buildRoutes, settings: buildSettings };

export function renderAdmin(main, tabId) {
  const tab = findTab(tabId);
  activeTab = tab.id;
  const generation = ++renderGeneration;
  /* A repaint of the tab already on screen keeps the body it drew, so the page does not collapse and lose its scroll
     position while the read is in flight; the loading strip is only for the first draw of a tab. */
  const body = shownBody && shownTab === tab.id ? shownBody : el("div", { class: "admin-body" }, [loadingStrip("Loading " + tab.label.toLowerCase())]);
  shownBody = body;
  shownTab = tab.id;
  mount(main, [el("div", { class: "page-head" }, [el("h2", { text: "Admin" })]), tabBar(tab), body]);
  BUILDERS[tab.id](body, generation).catch((e) => {
    if (generation === renderGeneration) mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
  });
}
