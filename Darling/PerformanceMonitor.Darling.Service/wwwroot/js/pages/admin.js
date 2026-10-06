/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin: a window onto what the desktop's Manage Servers, Notification Routes and Settings windows show.
   Three tabs, each one existing read:
     Servers             GET /api/admin/servers (every configured server, enabled or not)
     Notification Routes get_notification_routes
     Alert Settings      get_alert_settings
   Routes and Settings change nothing. The Servers tab also lets a sign-in that can make changes edit one server in place
   (#5240): an Edit button on each row opens a form above the grid, filled from GET /api/admin/servers/{id}.
   The active tab rides in the hash (#/admin/<tab>) and in module scope, so the 60 s repaint keeps it.
   Every cell is an allow-listed column or a key the settings read emits; a route row's destinations are never
   read (the tool reports channel names only), and a settings key that looks like a credential is dropped before
   it reaches a row. */

import { VIZ } from "../panels.js";
import { el, mount, loadingStrip, errorStrip, emptyStrip, noticeStrip, readTool, apiGet, apiWrite, onSessionExpired } from "../util.js";
import { getSession } from "../views-api.js";

export const SERVER_COLUMNS = [
  { key: "display_name", label: "Display Name" },
  { key: "server_name", label: "Server" },
  { key: "auth", label: "Auth" },
  { key: "engine", label: "Engine" },
  { key: "version", label: "Version" },
  { key: "status", label: "Status" },
  { key: "freshness", label: "Freshness", statusSev: true },
  { key: "monthly_cost", label: "Monthly Cost ($)", align: "right", sortValue: (r) => r.monthly_cost_usd },
  { key: "read_only", label: "Read only", format: "bool" },
  { key: "added", label: "Added", format: "time" },
  { key: "last_collected", label: "Last Collected", format: "time" },
];

/* A disabled server's row is drawn with the grey row class the grid already uses for a canceled job run. */
export function serverRowClass(row) {
  return row && row.status === "Disabled" ? "band-Offline" : "";
}

/* The Edit button of one row (#5240): a row the list reports without a numeric server_id gets none, because the by-id
   read and the edit are keyed on it. The label names the server for a screen reader. A button drawn while a save is in
   flight (the poll or the re-read repaints the grid) is born disabled, so no other row opens mid-save; `disabled` is set
   as a property because a `disabled="false"` attribute would still disable it. */
function editCell(row) {
  if (!row || !Number.isInteger(row.server_id)) return null;
  const name = row.display_name || row.server_name || "server " + row.server_id;
  const button = el("button", {
    class: "btn",
    type: "button",
    text: "Edit",
    "aria-label": "Edit " + name,
    "data-server-id": String(row.server_id),
    onClick: () => openEdit(row.server_id),
  });
  button.disabled = busy;
  return button;
}

/* The trailing Edit column. Like the Mute column on Alert History it is the web's per-row write affordance: a link-only
   cell, so the CSV and the copied text leave it out. */
export const EDIT_COLUMN = { key: "edit", label: "Edit", csv: false, copy: false, render: editCell };

/** The Servers grid's columns: SERVER_COLUMNS, plus the Edit column for a seat whose session reports can_edit. */
export function serverColumns(canEdit) {
  return canEdit ? SERVER_COLUMNS.concat([EDIT_COLUMN]) : SERVER_COLUMNS;
}

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

/* Edit a server (#5240): the pure rules behind the Servers tab's edit form. They decide what the form shows, what it
   checks before it sends anything, the body it sends and what each answer means. None of them touches the page or the
   network, so the form built on top of them can be tested rule by rule.
   They mirror the service's edit (PlanEdit and ParseEditChanges in Mcp/DarlingMcpServerAdminTools.Edit.cs):
     - only the fields the form changed are sent, plus the read's opaque modified_at text as expected_modified_at;
     - a SQL Server edit never sends port (the service refuses it, even 0) and a PostgreSQL edit never sends auth;
     - a password is required when the connection of a SQL or ServicePrincipal server changes, because the stored one
       cannot be read back; and the service probes the new connection on ANY connection change (Windows and managed
       identity included) or whenever a password is sent.
   `original` below is editFormValues(the by-id read) as the form opened; it carries the engine. `values` is the
   form's current text and booleans, shaped the same way. */

/** The editable fields in form order. The body is built only from these keys, so no other column can be sent. */
export const EDIT_FIELDS = [
  { key: "host", label: "Server Name / Address" },
  { key: "display_name", label: "Display Name" },
  { key: "port", label: "Port" },
  { key: "auth", label: "Authentication" },
  { key: "username", label: "Username" },
  { key: "encrypt_mode", label: "Encryption" },
  { key: "trust_server_certificate", label: "Trust server certificate" },
  { key: "database", label: "Database" },
  { key: "read_only_intent", label: "Read-only intent" },
  { key: "multi_subnet_failover", label: "Multi-subnet failover" },
  { key: "monthly_cost_usd", label: "Monthly Cost ($)" },
];
export const EDIT_AUTHS = ["Windows", "SQL", "ServicePrincipal", "ManagedIdentity"];
export const EDIT_ENCRYPT_MODES = ["Optional", "Mandatory", "Strict"];

/* The service treats any engine that is not "sqlserver" (ignoring case) as PostgreSQL. */
const isPostgres = (engine) => String(engine == null ? "" : engine).toLowerCase() !== "sqlserver";
const asText = (x) => (x == null ? "" : String(x));
/* The list entry that matches `raw` ignoring case and outer spaces, or undefined. */
const pickWord = (list, raw) => list.find((w) => w.toLowerCase() === asText(raw).trim().toLowerCase());
const authWord = (raw) => pickWord(EDIT_AUTHS, raw) || "Windows";
const storesSecret = (auth) => auth === "SQL" || auth === "ServicePrincipal";
/* The service compares the encryption mode ignoring case and every other field exactly. */
const sameValue = (key, a, b) => (key === "encrypt_mode" ? asText(a).toLowerCase() === asText(b).toLowerCase() : a === b);

/** The by-id read (or a conflict answer's `current`) as the form's values. PostgreSQL always shows auth "SQL", an auth
    the list does not know shows "Windows" as the service's own word for it does, port 0 (the default) shows blank, a
    boolean is true only when it is exactly true, and an encryption mode the list does not know stays as stored text. */
export function editFormValues(row) {
  const r = row && typeof row === "object" ? row : {};
  const engine = isPostgres(r.engine) ? "postgres" : "sqlserver";
  const port = Number(r.port);
  const cost = Number(r.monthly_cost_usd);
  const stored = asText(r.encrypt_mode);
  return {
    engine,
    host: asText(r.host),
    display_name: asText(r.display_name),
    port: Number.isFinite(port) && port > 0 ? String(port) : "",
    auth: engine === "postgres" ? "SQL" : authWord(r.auth),
    username: asText(r.username),
    encrypt_mode: pickWord(EDIT_ENCRYPT_MODES, stored) || stored,
    trust_server_certificate: r.trust_server_certificate === true,
    database: asText(r.database),
    read_only_intent: r.read_only_intent === true,
    multi_subnet_failover: r.multi_subnet_failover === true,
    monthly_cost_usd: Number.isFinite(cost) ? String(cost) : "0",
  };
}

/** Form values as the service reads them: text trimmed, a blank database or username null, the username null for
    Windows, port a number for PostgreSQL (blank is 0, the default; a typed one must be digits from 1 to 65535, else NaN)
    and null for SQL Server (its port goes in the host), auth "SQL" for PostgreSQL, cost a number (blank is 0, a typed
    one must be digits with an optional decimal point, else NaN) and booleans only when exactly true. */
export function normalizeEdit(values, engine) {
  const v = values && typeof values === "object" ? values : {};
  const postgres = isPostgres(engine);
  const trim = (x) => asText(x).trim();
  const auth = postgres ? "SQL" : authWord(v.auth);
  const port = trim(v.port);
  const cost = trim(v.monthly_cost_usd);
  let portValue = null;
  if (postgres) portValue = port === "" ? 0 : /^\d+$/.test(port) && Number(port) >= 1 && Number(port) <= 65535 ? Number(port) : NaN;
  return {
    host: trim(v.host),
    display_name: trim(v.display_name),
    port: portValue,
    auth,
    username: auth === "Windows" ? null : trim(v.username) || null,
    encrypt_mode: pickWord(EDIT_ENCRYPT_MODES, v.encrypt_mode) || trim(v.encrypt_mode),
    trust_server_certificate: v.trust_server_certificate === true,
    database: trim(v.database) || null,
    read_only_intent: v.read_only_intent === true,
    multi_subnet_failover: v.multi_subnet_failover === true,
    monthly_cost_usd: cost === "" ? 0 : /^(\d+\.?\d*|\.\d+)$/.test(cost) ? Number(cost) : NaN,
  };
}

/* The service's own test for "how this server is reached changed", over two normalized sets of values (connectionChanged
   in PlanEdit). NaN never equals NaN, so a port typed wrong counts as a change; the form refuses it before this matters. */
function connectionDiffers(o, v) {
  return o.host !== v.host
    || o.port !== v.port
    || o.database !== v.database
    || o.read_only_intent !== v.read_only_intent
    || o.auth !== v.auth
    || o.username !== v.username
    || o.encrypt_mode.toLowerCase() !== v.encrypt_mode.toLowerCase()
    || o.trust_server_certificate !== v.trust_server_certificate
    || o.multi_subnet_failover !== v.multi_subnet_failover;
}

/** True when the service will ask for the password again: the effective auth (PostgreSQL is always "SQL") stores a
    secret AND the connection changed, switching auth included. Windows and managed identity store none, so never. */
export function passwordRequired(original, values) {
  const engine = original && original.engine;
  const next = normalizeEdit(values, engine);
  return storesSecret(next.auth) && connectionDiffers(normalizeEdit(original, engine), next);
}

/** True when the service will probe the new connection before saving: a password is sent, or the connection changed
    whatever the auth (the same field test as passwordRequired, without its auth condition). */
export function probeExpected(original, values, passwordSent) {
  const engine = original && original.engine;
  return !!passwordSent || connectionDiffers(normalizeEdit(original, engine), normalizeEdit(values, engine));
}

/** The first sentence that stops a save before any request, or null. Each is the desktop's or the service's own
    sentence, in the order the form's fields run. `password` is what was typed (empty when nothing was). */
export function validateEdit(original, values, password) {
  const engine = original && original.engine;
  const was = normalizeEdit(original, engine);
  const next = normalizeEdit(values, engine);
  if (!next.host) return "Server name is required.";
  if (isPostgres(engine) && Number.isNaN(next.port)) return "Port must be between 1 and 65535, or blank for the default (5432).";
  if (next.auth === "SQL" && !next.username) return "Username is required for SQL Server authentication.";
  if (next.auth === "ServicePrincipal" && !next.username) return "The Application (client) ID is required for service-principal authentication.";
  if (!password && passwordRequired(original, values)) {
    if (was.auth !== next.auth) {
      return next.auth === "ServicePrincipal"
        ? "Switching to ServicePrincipal authentication needs the client secret as password."
        : "Switching to SQL authentication needs the password.";
    }
    return "Changing how this server is reached needs its password again: it is stored encrypted and this surface cannot read it back.";
  }
  if (!Number.isFinite(next.monthly_cost_usd)) return "Monthly cost must be a number, zero or more.";
  return null;
}

/** The edit's request body, or null when nothing differs and no password is sent. Only EDIT_FIELDS keys that differ
    between the normalized `original` and `values` (a number that is not finite is never sent); a SQL Server body never
    carries port and a PostgreSQL body never carries auth; a switch of auth always carries the username for SQL,
    ServicePrincipal and managed identity (the service drops the old one on a switch) and Windows never carries one;
    password only when typed AND the effective auth stores a secret; `expected_modified_at` is `token` exactly as the
    read gave it, always last. */
export function buildEditBody(original, values, token, password) {
  const engine = original && original.engine;
  const postgres = isPostgres(engine);
  const was = normalizeEdit(original, engine);
  const next = normalizeEdit(values, engine);
  const switched = was.auth !== next.auth;
  const body = {};
  for (const { key } of EDIT_FIELDS) {
    if (key === (postgres ? "auth" : "port")) continue;
    const value = next[key];
    if (typeof value === "number" && !Number.isFinite(value)) continue;
    if (key === "username" && next.auth === "Windows") continue;
    if ((key === "username" && switched) || !sameValue(key, was[key], value)) body[key] = value;
  }
  if (typeof password === "string" && password !== "" && storesSecret(next.auth)) body.password = password;
  if (!Object.keys(body).length) return null;
  /* A missing token is sent as null, which the service refuses by name, rather than dropped, which would save with no
     stale-edit check at all. */
  body.expected_modified_at = token == null ? null : token;
  return body;
}

/** What one answer to the edit means, from the HTTP status, the parsed body and (for status 0) the transport's own
    message: { kind, close, reread, banner?, notice?, current? }. `close` drops the form, `reread` reads the list again,
    `banner` is the sentence for the form's error strip and `notice` the sentence for the page. It branches on the
    status and the body's status word, never on message text. The sentence is the body's message, else its error, else
    "Request failed (HTTP n).". A 2xx whose body is not a JSON object is the sign-in page of an expired session. */
export function interpretEdit(status, body, message) {
  const b = body !== null && typeof body === "object" && !Array.isArray(body) ? body : null;
  const word = b && typeof b.status === "string" ? b.status : "";
  const sentence = (b && [b.message, b.error].find((s) => typeof s === "string" && s !== "")) || "Request failed (HTTP " + status + ").";
  if (status === 0) return { kind: "network", close: false, reread: false, banner: asText(message) || "Network error." };
  if (status === 401 || (status >= 200 && status < 300 && !b)) return { kind: "expired", close: true, reread: false };
  if (status === 403) return { kind: "readonly", close: true, reread: false, notice: "This account has read-only access. Nothing was saved." };
  if (status === 404) return { kind: "notfound", close: true, reread: true, notice: sentence };
  if (status === 409 && word === "conflict" && b.current && typeof b.current === "object") {
    return { kind: "conflict", close: false, reread: false, banner: "This server was changed since you opened it. Nothing was saved.", current: b.current };
  }
  if (status === 503) return { kind: "timeout", close: false, reread: true, banner: sentence };
  if (status === 200 && word === "updated") {
    const name = asText(b.display_name) || asText(b.server);
    const note = typeof b.note === "string" && b.note ? " " + b.note : "";
    const tested = b.tested === true ? " The connection was tested before saving." : "";
    return { kind: "updated", close: true, reread: true, notice: (name ? 'Saved "' + name + '".' : "Saved.") + note + tested };
  }
  if (status === 200 && word === "unchanged") return { kind: "unchanged", close: true, reread: false, notice: "No change was needed; nothing was written." };
  return { kind: "failed", close: false, reread: false, banner: sentence };
}

/* A value as a conflict line shows it: yes or no, "(default)" for port 0, "(blank)" for none, and what the user typed
   for a number that did not parse. */
function shownEdit(key, value, typed) {
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (key === "port" && value === 0) return "(default)";
  if (typeof value === "number" && !Number.isFinite(value)) return asText(typed).trim() || "(blank)";
  return value == null || value === "" ? "(blank)" : String(value);
}

/** The fields a 409 conflict says changed under the form: one { key, label, was, now, yours, text } per EDIT_FIELDS
    key whose value in `current` (the answer's current values, run through editFormValues) differs from `original`.
    `yours` is what the user entered when they changed that field too, else null. SQL Server skips port and PostgreSQL
    skips auth, as the body does. text is "<Label>: was <old>, now <new>" plus " (you entered <yours>)". */
export function conflictChanges(original, current, values) {
  const engine = original && original.engine;
  const postgres = isPostgres(engine);
  const was = normalizeEdit(original, engine);
  const now = normalizeEdit(editFormValues(current), engine);
  const mine = normalizeEdit(values, engine);
  const typed = values && typeof values === "object" ? values : {};
  const changes = [];
  for (const { key, label } of EDIT_FIELDS) {
    if (key === (postgres ? "auth" : "port") || sameValue(key, was[key], now[key])) continue;
    const shownWas = shownEdit(key, was[key]);
    const shownNow = shownEdit(key, now[key]);
    const yours = sameValue(key, was[key], mine[key]) ? null : shownEdit(key, mine[key], typed[key]);
    changes.push({ key, label, was: shownWas, now: shownNow, yours, text: label + ": was " + shownWas + ", now " + shownNow + (yours === null ? "" : " (you entered " + yours + ")") });
  }
  return changes;
}

/** The form's values after "Reapply my changes" on a 409: `current` (the answer's current values, run through
    editFormValues) with every field the user changed since the form opened laid over it, so the user's own edits
    survive and every other box shows what the service holds now. A field counts as the user's by the test the body
    uses (normalized `original` against normalized `values`), so a box only padded with spaces takes the current value,
    and one that holds text which does not parse (a bad port or cost) keeps it for the next Save to refuse. The username
    goes with the authentication: when the user chose the authentication, the username box they left with it is theirs
    too, so another edit's client id never turns up inside a login they picked. */
export function reapplyValues(original, values, current) {
  const engine = original && original.engine;
  const was = normalizeEdit(original, engine);
  const mine = normalizeEdit(values, engine);
  const typed = values && typeof values === "object" ? values : {};
  const merged = { ...current };
  for (const { key } of EDIT_FIELDS) {
    if (!sameValue(key, was[key], mine[key])) merged[key] = typed[key];
  }
  if (!sameValue("auth", was.auth, mine.auth)) merged.username = typed.username;
  return merged;
}

/** `text` with every copy of the typed password replaced by "[redacted]" (plain split and join, no pattern); with no
    password the text is returned as it is. Applied to any service sentence before it is shown. */
export function redactPassword(text, password) {
  if (typeof password !== "string" || password === "") return text;
  return asText(text).split(password).join("[redacted]");
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

/* The Servers tab's note, by what the session probe said (#5240). A probe that failed is not the same as a read-only
   seat: reloading fixes the first and never the second, so each gets its own words. */
const NOTE_READ_ONLY = "Every configured server, enabled or disabled. This sign-in is read-only. Add, edit and remove stay in the desktop Manage Servers window.";
const NOTE_EDITING = "Every configured server, enabled or disabled. Edit changes a server in place. Add, remove, enable or disable, and excluded databases stay in the desktop Manage Servers window.";
const NOTE_PROBE_FAILED = "Every configured server, enabled or disabled. Could not check whether this sign-in can make changes. Reload the page to try again.";

function serversNote(session) {
  if (session.probe_failed === true) return NOTE_PROBE_FAILED;
  return session.can_edit === true ? NOTE_EDITING : NOTE_READ_ONLY;
}

/* The Servers tab's page state, in module scope so the 60 s repaint keeps it (#5240).
   `layout` is the three boxes the tab draws into, { body, noticeBox, formBox, tableBox }. They are mounted into the body
   ONCE, on the first draw into it, and every later draw (the poll, the re-read after a change, a read that failed, the
   render catch) refreshes only noticeBox and tableBox. A node taken out of the document loses the focus and the keystrokes
   that follow it, and the same node put back does not get the focus back, so while a form is open no code passes formBox
   or anything that holds it to mount() or clear() (review finding 1; manage-tags.js keeps its form the same way).
   `editForm` is the open edit form, or null. `opening` counts every open and every close, so a by-id read that lands after
   its form was discarded is dropped. `notice` is the page-level sentence, { message, isError } or null: a refused or
   failed open now, a save's answer later. `summary` is the count-and-note text of the last good list read.
   `busy` is true while a save runs (review finding 4): Save, Cancel and every Edit button are disabled and a second Save does
   nothing. `held` is a save's answer that landed while the Servers tab was not on screen, shown the next time it is.
   `hashWatched` is true once the one hashchange listener is registered. */
let layout = null;
let editForm = null;
let opening = 0;
let notice = null;
let summary = null;
let busy = false;
let held = null;
let hashWatched = false;

function serverBoxes(body) {
  if (layout && layout.body === body) return layout;
  layout = {
    body,
    noticeBox: el("div", { class: "admin-notice", "data-box": "notice" }),
    formBox: el("div", { class: "admin-form-box", "data-box": "form" }),
    tableBox: el("div", { class: "admin-table-box", "data-box": "table" }),
  };
  mount(body, [layout.noticeBox, layout.formBox, layout.tableBox]);
  return layout;
}

/* The page-level sentence (when there is one) above the count and note of the last good read. */
function drawNotice() {
  if (!layout) return;
  const nodes = [];
  if (notice) {
    nodes.push(el("div", { class: "strip " + (notice.isError ? "error" : "notice"), role: notice.isError ? "alert" : "status", text: notice.message }));
  }
  if (summary) nodes.push(noticeStrip(summary));
  mount(layout.noticeBox, nodes);
}

/** Show `message` above the grid, as an error when `isError`; null or "" takes it away. */
function setNotice(message, isError) {
  notice = message ? { message, isError: isError === true } : null;
  drawNotice();
}

/** A save's answer as a page sentence. With the Servers tab on screen it is the notice; with it off screen (a tab switch
    while the save ran) it is held, and the next arrival at the Servers tab shows it, so the outcome is never lost. */
function showOutcome(message, isError) {
  if (layout) setNotice(message, isError);
  else held = { message, isError: isError === true };
}

/* The Edit buttons now in the grid, for the code that locks them while a request runs. */
function editButtons() {
  return layout ? layout.tableBox.querySelectorAll("button[data-server-id]") : [];
}

/* Lock or unlock what a save in flight must not race: Save and Cancel of the open form and every Edit button in the grid
   (review finding 4). A grid drawn while locked gets its buttons already disabled (editCell), so this reaches only the
   buttons on screen now. */
function setBusy(value) {
  busy = value;
  if (editForm && editForm.saveButton) {
    editForm.saveButton.disabled = value;
    editForm.cancelButton.disabled = value;
  }
  for (const button of editButtons()) button.disabled = value;
}

/* Read the list again under a new render generation (a poll read still in flight is dropped), into the boxes already on
   the page, so an open form is not touched. Resolves when the read has been drawn. */
function reloadServers() {
  if (shownTab !== "servers" || !layout || layout.body !== shownBody) return Promise.resolve();
  return startBuilder("servers", shownBody, ++renderGeneration);
}

async function buildServers(body, generation) {
  const [session, res] = await Promise.all([getSession(), apiGet("/api/admin/servers")]);
  if (generation !== renderGeneration) return;
  const boxes = serverBoxes(body);
  const bad = failure(res, "No servers are registered");
  if (bad) {
    summary = null;
    drawNotice();
    return mount(boxes.tableBox, bad);
  }
  const rows = serverRows(res.data);
  summary = rows.length + (rows.length === 1 ? " server. " : " servers. ") + serversNote(session);
  drawNotice();
  mount(boxes.tableBox, VIZ.table({ servers: rows }, {
    rowsKey: "servers",
    columns: serverColumns(session.can_edit === true),
    rowClass: serverRowClass,
    emptyText: "No servers are registered yet.",
  }));
}

/* ─────────────────────────── the edit form (#5240) ─────────────────────────── */

/* The desktop dialog's sentences (Darling.Viewer AddServerDialog.xaml and .xaml.cs), with the dashes and arrows of
   the originals written out as plain punctuation. */
const ENGINE_TEXT = { sqlserver: "SQL Server", postgres: "PostgreSQL" };
const ENGINE_LOCKED = "The engine is part of what this server is: its collected history is keyed to it, so an edit cannot change it. To move a host between engines, add it as a new server and remove this one.";
const POSTGRES_NOTE = "PostgreSQL targets connect with username and password authentication. The Database box below is optional: blank connects to the maintenance database ('postgres'), which the cluster-wide collectors read from. Encryption maps to sslmode: Optional is prefer, and Mandatory or Strict is verify-full, or require when 'Trust server certificate' is checked (typically needed for Aurora).";
const MANAGED_IDENTITY_NOTE = "Managed Identity only works when the Darling service runs on an Azure VM / resource that has a managed identity assigned.";
const AUTH_LABELS = {
  Windows: "Windows Authentication",
  SQL: "SQL Server Authentication",
  ServicePrincipal: "Azure Service Principal",
  ManagedIdentity: "Azure Managed Identity",
};
const USERNAME_LABELS = { SQL: "Username", ServicePrincipal: "Client (Application) ID", ManagedIdentity: "User-Assigned Identity Client ID (optional)" };
const PASSWORD_LABELS = { SQL: "Password", ServicePrincipal: "Client Secret" };
const PASSWORD_REQUIRED = "(required for this change)";
const PASSWORD_KEPT = "(leave blank to keep the stored one)";

/* The username and password boxes hold the SERVER's login, not this site's. autocomplete="new-password" (and "off" for the
   username) stops a browser filling in the saved sign-in, and these data attributes ask the common password managers (1Password,
   LastPass, Bitwarden and the ones that read data-form-type) not to fill, save, update or generate anything here. The two boxes
   carry no name or id and sit in no form element, so a browser has no sign-in form to take them for (#5240). */
const KEEP_FROM_PASSWORD_MANAGERS = { "data-1p-ignore": "", "data-lpignore": "true", "data-bwignore": "", "data-form-type": "other" };

/* One labelled text box. The label wraps its control, so it names it without an id or a name attribute. */
function textRow(f, key, label, props) {
  const caption = el("span", { class: "mute-label", text: label });
  const input = el("input", { type: "text", class: "tag-input", "data-field": key, ...props });
  input.value = f.values[key];
  input.addEventListener("input", () => {
    f.values[key] = input.value;
    f.refresh();
  });
  return { input, caption, row: el("label", { class: "tag-field", "data-row": key }, [caption, input]) };
}

function checkRow(f, key, label) {
  const input = el("input", { type: "checkbox", "data-field": key });
  input.checked = f.values[key] === true;
  input.addEventListener("change", () => {
    f.values[key] = input.checked;
    f.refresh();
  });
  return el("label", { class: "tag-field", "data-row": key }, [input, " " + label]);
}

/* The encryption choice. The stored word is matched to an option ignoring case by editFormValues; a word the page does not
   know is added as one more option and stays selected, so a form nobody touched sends no change (D15). */
function encryptRow(f) {
  const stored = f.values.encrypt_mode;
  const words = EDIT_ENCRYPT_MODES.includes(stored) ? EDIT_ENCRYPT_MODES : EDIT_ENCRYPT_MODES.concat([stored]);
  const select = el("select", { class: "tag-input", "data-field": "encrypt_mode" }, words.map((w) => el("option", { value: w, text: w || "(blank)" })));
  select.value = stored;
  select.addEventListener("change", () => {
    f.values.encrypt_mode = select.value;
    f.refresh();
  });
  return el("label", { class: "tag-field", "data-row": "encrypt_mode" }, [el("span", { class: "mute-label", text: "Encryption:" }), select]);
}

/* The four authentication choices of a SQL Server form. A PostgreSQL form has none: its auth is always SQL. */
function authRows(f) {
  const choices = EDIT_AUTHS.map((word) => {
    const input = el("input", { type: "radio", name: "admin-edit-auth", value: word, "data-field": "auth" });
    input.addEventListener("change", () => {
      if (!input.checked) return;
      f.values.auth = word;
      f.refresh();
    });
    return { input, label: el("label", { class: "tag-field" }, [input, " " + AUTH_LABELS[word]]) };
  });
  f.radios = choices.map((c) => c.input);
  return el("fieldset", { class: "tag-field", "data-row": "auth" }, [
    el("legend", { class: "mute-label", text: "Authentication" }),
    choices.map((c) => c.label),
  ]);
}

/* Show or hide a row. The author rule `.tag-field { display: block }` outranks the browser's own [hidden] rule (as app.css
   notes for the nav), so the attribute alone would leave the row on screen; the inline display is what hides it. */
function setShown(node, shown) {
  node.hidden = !shown;
  node.style.display = shown ? "" : "none";
}

/* The form for the open server. It is built once per open and kept up to date in place by f.refresh(), so typing never
   replaces a node. Values live in f.values (text and booleans, the shape editFormValues returns); the password is never
   copied into it: the Save step reads the password box once, when it is pressed. The username is kept per authentication
   mode in f.usernames, as the desktop has one box per mode, so a SQL login never turns into a client id by a click. */
function formNode(f) {
  const postgres = f.original.engine === "postgres";
  const heading = el("h3", { text: "Edit " + (postgres ? "PostgreSQL Server" : "SQL Server") + " Connection", tabindex: "-1", "data-role": "heading" });
  const host = textRow(f, "host", "Server Name / Address");
  const name = textRow(f, "display_name", "Display Name (optional)");
  const port = postgres ? textRow(f, "port", "Port:", { inputmode: "numeric" }) : null;
  const username = textRow(f, "username", USERNAME_LABELS.SQL, { autocomplete: "off", ...KEEP_FROM_PASSWORD_MANAGERS });
  const managedNote = el("div", { class: "muted", "data-row": "managed-identity-note", text: MANAGED_IDENTITY_NOTE });
  const secret = el("input", { type: "password", class: "tag-input", "data-field": "password", autocomplete: "new-password", ...KEEP_FROM_PASSWORD_MANAGERS });
  const secretCaption = el("span", { class: "mute-label", text: PASSWORD_LABELS.SQL });
  const secretRow = el("label", { class: "tag-field", "data-row": "password" }, [secretCaption, secret]);
  f.heading = heading;
  f.passwordInput = secret;
  f.bannerBox = el("div", { "data-box": "banner" });
  f.conflictBox = el("div", { "data-box": "conflict" });
  f.statusBox = el("div", { "data-box": "status", role: "status" });
  f.saveButton = el("button", { class: "btn primary", type: "button", text: "Save", "data-action": "save", onClick: () => submitEdit() });
  f.cancelButton = el("button", { class: "btn", type: "button", text: "Cancel", "data-action": "cancel", onClick: () => closeEdit() });
  f.refresh = () => {
    const auth = f.values.auth;
    for (const input of f.radios || []) input.checked = input.value === auth;
    if (f.shownAuth !== auth) {
      f.shownAuth = auth;
      username.input.value = f.usernames[auth] || "";
    }
    f.usernames[auth] = username.input.value;
    f.values.username = auth === "Windows" ? "" : username.input.value;
    username.caption.textContent = USERNAME_LABELS[auth] || USERNAME_LABELS.SQL;
    setShown(username.row, auth !== "Windows");
    setShown(managedNote, auth === "ManagedIdentity");
    setShown(secretRow, auth === "SQL" || auth === "ServicePrincipal");
    secretCaption.textContent = (PASSWORD_LABELS[auth] || PASSWORD_LABELS.SQL) + " " + (passwordRequired(f.original, f.values) ? PASSWORD_REQUIRED : PASSWORD_KEPT);
  };
  const rows = [
    el("div", { class: "tag-field", "data-row": "engine" }, [
      el("span", { class: "mute-label", text: "Database Engine" }),
      el("div", { "data-field": "engine", text: ENGINE_TEXT[f.original.engine] }),
      el("div", { class: "muted", text: ENGINE_LOCKED }),
    ]),
    host.row,
    name.row,
  ];
  if (postgres) rows.push(port.row, el("div", { class: "muted", text: "blank = 5432, the default" }), el("div", { class: "muted", text: POSTGRES_NOTE }));
  else rows.push(authRows(f));
  rows.push(
    username.row,
    managedNote,
    secretRow,
    el("h4", { class: "section-title", text: "Connection Options" }),
    encryptRow(f),
    checkRow(f, "trust_server_certificate", "Trust server certificate (skip certificate validation)"),
    textRow(f, "database", "Database:").row,
  );
  if (!postgres) {
    rows.push(
      checkRow(f, "read_only_intent", "Read-only intent (for AG listeners and readable replicas)"),
      checkRow(f, "multi_subnet_failover", "Multi-subnet failover (for AG listeners and FCIs)"),
    );
  }
  rows.push(
    textRow(f, "monthly_cost_usd", "Monthly Cost ($):", { inputmode: "decimal" }).row,
    f.statusBox,
    f.bannerBox,
    f.conflictBox,
    el("div", { class: "form-actions" }, [f.saveButton, f.cancelButton]),
  );
  f.refresh();
  return el("div", { class: "card tag-form admin-edit-form", role: "group", "aria-label": heading.textContent, "data-edit-form": String(f.id) }, [heading, ...rows]);
}

/* Open the edit form for server `id` (#5240). One form at a time: opening another row discards the open one. The by-id
   read answers every way it can, and each way is handled (review finding 9): data fills the form; a 404 means the
   server was removed meanwhile, so its sentence is shown and the list is read again; a 401 hands over to the shell; any
   other answer (403, 500, a network failure, a body that is not a server's settings) shows its sentence and opens no
   form. The password box is created only after a good read. */
async function openEdit(id) {
  /* A save is running: the Edit buttons are disabled, and this guard holds for any other caller. It comes before
     closeEdit() so that nothing discards the form that is saving. */
  if (busy) return;
  closeEdit();
  const mine = opening;
  setNotice(null);
  mount(layout.formBox, loadingStrip("Loading server settings"));
  const res = await apiGet("/api/admin/servers/" + id);
  if (mine !== opening) return;
  const data = res.kind === "data" && res.data && typeof res.data === "object" && !Array.isArray(res.data) && typeof res.data.modified_at === "string" ? res.data : null;
  if (!data) {
    closeEdit();
    if (res.kind === "auth") return;
    const sentence = typeof res.message === "string" && res.message !== "" ? res.message : "Could not read this server's settings.";
    setNotice(sentence, res.status !== 404);
    if (res.status === 404) await reloadServers();
    return;
  }
  const original = editFormValues(data);
  const f = { id, original, values: { ...original }, usernames: usernamesOf(original), token: data.modified_at };
  editForm = f;
  showForm(f);
}

/* The username boxes' contents by mode as a form is first drawn: the username fills the mode it belongs to, the others start
   blank. */
function usernamesOf(values) {
  const usernames = { SQL: "", ServicePrincipal: "", ManagedIdentity: "" };
  if (values.auth in usernames) usernames[values.auth] = values.username;
  return usernames;
}

/* Draw the form of `f` into its box and put the focus on its heading. Mounting INTO formBox is allowed while a form is open;
   only formBox itself and what holds it are never passed to mount() or clear(). */
function showForm(f) {
  mount(layout.formBox, formNode(f));
  f.heading.focus();
}

/* The one way the form goes away: Cancel, a save that finished, an answer that closes it, another row's Edit and a tab
   switch all come here (review finding 3). The typed password is cleared FIRST, while its box is still in the page, so no
   detached node, closure or state object keeps it; then the state is dropped and the box emptied. Counting a close also
   drops a by-id read that is still in flight. */
function closeEdit() {
  opening++;
  if (editForm && editForm.passwordInput) editForm.passwordInput.value = "";
  editForm = null;
  if (layout) mount(layout.formBox, []);
}

/* A session that expired under ANY read of the page (the poll's list read, another tab's read) makes the shell take the page
   over, which leaves the open form detached but still held by this module. Closing it here, once, at module load, clears the
   typed password first, so a secret never outlives the page it was typed on (#5240). The save's own expiry comes here too. */
onSessionExpired(() => closeEdit());

const STATUS_TESTING = "Testing the connection, then saving. This can take up to a minute.";
const STATUS_SAVING = "Saving.";

/* The form's one error sentence, above its buttons (D11), as an alert strip. */
function showBanner(f, sentence) {
  mount(f.bannerBox, el("div", { class: "strip error", role: "alert", text: sentence }));
}

/* The status line while a save runs; "" takes it away. */
function showStatus(f, sentence) {
  mount(f.statusBox, sentence ? loadingStrip(sentence) : []);
}

const CONFLICT_NONE = "None of the fields on this form changed. Another setting, such as the enabled state, changed.";
const CONFLICT_REAPPLY = "Reapply my changes";
const CONFLICT_RELOAD = "Reload current values";

/* The 409 panel (D10). `out` is interpretEdit's `conflict` answer, { banner, current }; `f` is the open form, kept, with its
   password already cleared and nothing saved. Under the banner: what the other edit changed in the fields of this form (and
   what the user entered for the same field), or the sentence that none of them did, and the two ways forward. Both only
   refill the form: a stale save is never sent again for the user, so the next Save is the user's own. `say` takes the
   submit-time password out of any sentence shown. An answer with no usable modified_at has nothing to reapply onto, so it
   stays the plain banner. */
function showConflict(f, out, say) {
  showBanner(f, say(out.banner));
  const current = out.current;
  if (typeof current.modified_at !== "string") return;
  const changes = conflictChanges(f.original, current, f.values);
  const list = changes.length
    ? el("ul", { class: "admin-conflict-list", "data-role": "conflict-list", "aria-label": "Changed since you opened this server" },
      changes.map((c) => el("li", { "data-conflict-field": c.key, text: say(c.text) })))
    : el("div", { class: "muted", "data-role": "conflict-none", text: CONFLICT_NONE });
  mount(f.conflictBox, el("div", { class: "admin-conflict" }, [
    list,
    el("div", { class: "form-actions" }, [
      el("button", { class: "btn", type: "button", text: CONFLICT_REAPPLY, "data-action": "reapply", onClick: () => refillEdit(f, current, true) }),
      el("button", { class: "btn", type: "button", text: CONFLICT_RELOAD, "data-action": "reload", onClick: () => refillEdit(f, current, false) }),
    ]),
  ]));
}

/* Point the open form at what the service holds now (the `current` of a 409): its values become the baseline and its
   modified_at the token the next Save carries. With `keepMine` the user's own changes are laid over it (reapplyValues);
   without, the form shows the current values and the user's changes are gone. NOTHING is sent: the form is only drawn again
   (a fresh password box, nothing typed) and the user presses Save. Does nothing for a form that is no longer the open one,
   or while a save runs. */
function refillEdit(f, current, keepMine) {
  if (editForm !== f || busy) return;
  const now = editFormValues(current);
  f.values = keepMine ? reapplyValues(f.original, f.values, now) : { ...now };
  f.original = now;
  f.token = current.modified_at;
  f.usernames = usernamesOf(f.values);
  f.shownAuth = undefined;
  showForm(f);
}

/* What one answer to the save does (D9, and review finding 4 for an answer that is late). `here` is true while the form that
   sent the save is still the open one; `password` is what was typed when Save was pressed, kept only in the caller's local
   variable and used to redact every sentence shown.
   - An answer that closes the form closes it through closeEdit (the password goes first) and shows its sentence as the
     notice, an error when the sign-in is read-only.
   - An answer that keeps the form clears the password and shows its sentence in the banner, or hands a conflict to showConflict.
   - A late answer (the form was discarded while the save ran: a tab switch, another page, the hash leaving the tab) touches
     no form. Its page sentence still shows, and a sentence that had only a banner shows as an error notice, so the user
     learns what the save did. */
function landEdit(f, out, password) {
  const here = editForm === f;
  const say = (sentence) => redactPassword(sentence, password);
  if (out.close) {
    if (here) closeEdit();
    if (out.notice) showOutcome(say(out.notice), out.kind === "readonly");
    return;
  }
  if (!here) {
    showOutcome(say(out.banner), true);
    return;
  }
  f.passwordInput.value = "";
  if (out.kind === "conflict") showConflict(f, out, say);
  else showBanner(f, say(out.banner));
}

/* Save (#5240). The form's Save button calls it. The checks that need no request run first, each with the desktop's or the
   service's own sentence in the banner and the password cleared. A form that differs from what was read in nothing, and
   has no password to send, closes with "No change." and sends nothing. Otherwise ONE PATCH goes out through apiWrite, the
   status line says whether the service will test the connection first (probeExpected, whatever the auth: review finding
   5), and the answer is read by interpretEdit from its status and status word, never from message text.
   `busy` is set before the request and cleared in a finally, so it clears whatever became of the form meanwhile (review
   finding 4); the list re-read an answer asks for runs after the lock is released. A late answer, one that arrives after
   the form was discarded, also re-reads the list when it was an `unchanged` one, so the page shows the state the save left. */
async function submitEdit() {
  const f = editForm;
  if (busy || !f) return;
  const password = f.passwordInput.value;
  mount(f.bannerBox, []);
  mount(f.conflictBox, []);
  const problem = validateEdit(f.original, f.values, password);
  if (problem) {
    f.passwordInput.value = "";
    showBanner(f, redactPassword(problem, password));
    return;
  }
  const body = buildEditBody(f.original, f.values, f.token, password);
  if (!body) {
    closeEdit();
    setNotice("No change.", false);
    return;
  }
  setBusy(true);
  showStatus(f, probeExpected(f.original, f.values, typeof body.password === "string") ? STATUS_TESTING : STATUS_SAVING);
  let out = null;
  let late = false;
  try {
    const res = await apiWrite("PATCH", "/api/servers/" + f.id, body);
    out = interpretEdit(res.status, res.body, res.message);
    late = editForm !== f;
    landEdit(f, out, password);
  } finally {
    if (editForm === f) showStatus(f, "");
    setBusy(false);
  }
  if (out.reread || (late && out.kind === "unchanged")) await reloadServers();
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

/* Run a tab's builder. A builder that throws leaves an error strip: in the Servers tab's table area when its boxes are on the
   page (the form above it is never replaced), else in the body. */
function startBuilder(tabId, body, generation) {
  return BUILDERS[tabId](body, generation).catch((e) => {
    if (generation !== renderGeneration) return;
    const strip = errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e)));
    mount(tabId === "servers" && layout && layout.body === body ? layout.tableBox : body, strip);
  });
}

/* True when `hash` shows the Servers tab: #/admin or #/admin/servers, or a tab name the page does not know while the
   Servers tab is the one on screen (renderAdmin falls back the same way). */
function showsServers(hash) {
  if (hash !== "#/admin" && !hash.startsWith("#/admin/")) return false;
  return findTab(hash.slice("#/admin".length).replace(/^\//, "")).id === "servers";
}

/* The one hashchange listener (D6, review finding 6). Another Admin tab is seen by renderAdmin, but leaving the page for
   another one is seen by nothing else, because no render of this page follows it. A hash that no longer shows the Servers
   tab discards the open form, password first, and drops a by-id read still in flight. Discarding twice is harmless. */
function onHashChange() {
  if (!showsServers(String(window.location.hash || ""))) closeEdit();
}

export function renderAdmin(main, tabId) {
  if (!hashWatched) {
    hashWatched = true;
    window.addEventListener("hashchange", onHashChange);
  }
  const tab = findTab(tabId);
  activeTab = tab.id;
  const generation = ++renderGeneration;
  /* A repaint of the tab already on screen keeps the body it drew, so the page does not collapse and lose its scroll
     position while the read is in flight; the loading strip is only for the first draw of a tab. */
  const sameTab = !!shownBody && shownTab === tab.id;
  if (!sameTab) {
    /* Arriving at a tab other than the one on screen discards an open edit form (its password is cleared first) and the
       Servers tab's page state. The one thing kept is the answer of a save that landed while the Servers tab was off screen:
       arriving at that tab shows it, once. */
    closeEdit();
    layout = null;
    summary = null;
    notice = tab.id === "servers" ? held : null;
    if (tab.id === "servers") held = null;
  }
  const body = sameTab ? shownBody : el("div", { class: "admin-body" }, [loadingStrip("Loading " + tab.label.toLowerCase())]);
  shownBody = body;
  shownTab = tab.id;
  /* Stable layout (review finding 1): when the tab is unchanged and its body is still attached to `main`, nothing above or
     around the body changes, so main is NOT mounted again. Mounting main takes the body, and the open form inside it, out of
     the document, and a focused box inside a node taken out loses its focus. Only the builder runs. */
  if (!(sameTab && body.parentNode === main)) {
    mount(main, [el("div", { class: "page-head" }, [el("h2", { text: "Admin" })]), tabBar(tab), body]);
  }
  startBuilder(tab.id, body, generation);
}
