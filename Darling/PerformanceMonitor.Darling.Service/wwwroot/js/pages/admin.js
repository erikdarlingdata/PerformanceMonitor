/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin: a window onto what the desktop's Manage Servers, Notification Routes and Settings windows show.
   Ten tabs (the first three each one existing read):
     Servers             GET /api/admin/servers (every configured server, enabled or not)
     Notification Routes get_notification_routes
     Alert Settings      get_alert_settings
     Report Feedback     GET /api/investigation-feedback (#5913: the feedback people gave investigation reports, the false-positive
                         counts per alert family, and a form to record feedback for a sign-in that can make changes)
     Practice Scores     GET /api/investigation-practice-scores (#5918: how each agent did on practice requests), and Start (#6265)
     Pending Notes       GET /api/investigation-notes/pending, with Approve and Reject (#5903), and Write a note (#6265)
     Threshold Overrides GET /api/threshold-overrides, with Save, Edit and Remove under an admin store login (#5916)
     Agent changes       what agents changed and were refused, with Undo and the switch that turns agent changes off (#5917)
     Server actions      GET /api/server-actions, with the per-server action switches an admin store login can change (#6269)
     Environment Profiles GET /api/environment-profiles, edited as in the Viewer (#6264)
   Alert Settings changes nothing; Notification Routes edits the agent channel for a sign-in that can make changes (#6264). The Servers tab also lets a sign-in that can make changes edit one server in place
   (#5240): an Edit button on each row opens a form above the grid, filled from GET /api/admin/servers/{id}.
   The active tab rides in the hash (#/admin/<tab>) and in module scope, so the 60 s repaint keeps it.
   Every cell is an allow-listed column or a key the settings read emits; a route row's destinations are never
   read (the tool reports channel names only), and a settings key that looks like a credential is dropped before
   it reaches a row. */

import { VIZ } from "../panels.js";
import { routeHash, el, mount, loadingStrip, errorStrip, emptyStrip, noticeStrip, listOnlyNote, readTool, apiGet, apiGetFleet, apiWrite, onSessionExpired, localizeIsoText, storeHash } from "../util.js";
import { getSession } from "../views-api.js";
import { writeNote, startPractice, writeAccess, SIGN_IN_SENTENCE, NOTE_MAX_LENGTH, NOTE_MAX_DAYS, NOTE_DEFAULT_DAYS } from "../investigation-writes.js";
import { buildAgentChanges, resetAgentChangesStatus } from "./admin-agent-changes.js";
import { buildServerActions, resetServerActionsStatus } from "./admin-server-actions.js";
import { buildProfiles, readAgentSetupAccess, routeColumns, routeEditorNodes, routeListOnlyNodes, routeUseText } from "./admin-agent-setup.js";

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
  /* #6351: the other enabled registrations that last reported this machine's identity, worked out when the list is read (an address
     can start reaching another machine, e.g. after an availability group listener fails over). An agent-scoped notification route
     on one of them does not cover the other. The service sends the names already joined; blank when there is no overlap. */
  { key: "same_machine_as", label: "Same machine as", wrap: true },
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
  { key: "applies_to", label: "Applies to" },
  { key: "status", label: "Status" },
  { key: "channels", label: "Channels set", wrap: true },
  { key: "agent", label: "Agent" },
  { key: "resolutions", label: "Send resolutions" },
  { key: "smtp_recipients", label: "Email to", wrap: true },
  { key: "modified_at_utc", label: "Modified", format: "time" },
];

/* The Setting column shows a readable label; the key stays as the cell's tooltip (and the row's `setting`), so a person
   matching a setting to the MCP tool or the config still finds it (click-through 4d). */
export const SETTING_COLUMNS = [
  { key: "setting", label: "Setting", render: (row) => el("span", { title: row.setting, text: row.label || row.setting }) },
  { key: "value", label: "Value", wrap: true },
];

const GROUP_LABELS = {
  cpu: "CPU",
  blocking: "Blocking",
  deadlocks: "Deadlocks",
  poison_wait: "Poison wait",
  runnable_queue: "Runnable queue",
  client_timeouts: "Client timeouts",
  identity_capacity: "Identity capacity",
  long_running_query: "Long-running query",
  tempdb_space: "TempDB space",
  low_disk: "Low disk",
  self_alerts: "Monitor self-alerts",
  pvs: "Version store (PVS)",
  file_growth: "File growth",
  long_running_job: "Long-running job",
  failed_job: "Failed job",
  database_state: "Database state",
  backup_overdue: "Backup overdue",
  log_backup_overdue: "Log backup overdue",
  checkdb_overdue: "CHECKDB overdue",
  agent_alert_gaps: "Agent alert gaps",
  ag: "Availability Groups",
  delivery: "Delivery",
  health_bands: "Health bands",
  analysis: "Automated analysis",
  fleet_sweep: "Fleet sweep reports",
};

/* A unit at the end of a key reads as a bracketed unit: wait_threshold_seconds -> "wait threshold (seconds)". */
const KEY_UNITS = [
  ["_seconds", "seconds"],
  ["_minutes", "minutes"],
  ["_percent", "%"],
  ["_ms", "ms"],
  ["_gb", "GB"],
  ["_mb", "MB"],
  ["_kb", "KB"],
  ["_per_hour", "per hour"],
];

/** The readable label of one setting: "cpu.threshold_percent" -> "CPU: threshold (%)"; "connection_refire_minutes" ->
    "Connection refire (minutes)". A key it has no rule for is its words with the underscores spaced out. */
export function settingLabel(prefix, key) {
  let words = String(key);
  let unit = "";
  for (const [suffix, text] of KEY_UNITS) {
    if (words.endsWith(suffix) && words.length > suffix.length) {
      words = words.slice(0, -suffix.length);
      unit = " (" + text + ")";
      break;
    }
  }
  words = words.replace(/_/g, " ").replace(/\bpg\b/g, "PostgreSQL");
  words = words.charAt(0).toUpperCase() + words.slice(1);
  if (!prefix) return words + unit;
  const group = GROUP_LABELS[prefix] || prefix.replace(/_/g, " ");
  return group + ": " + (/^[A-Z][A-Z]/.test(words) || words.startsWith("PostgreSQL") ? words : words.charAt(0).toLowerCase() + words.slice(1)) + unit;
}

/* The groups, in the Settings window's order. A key listed here is read from the payload's top level or from the
   named group; a group the read does not carry is left out rather than shown empty. */
export const SETTING_SECTIONS = [
  {
    title: "Alert thresholds",
    groups: ["cpu", "blocking", "deadlocks", "poison_wait", "runnable_queue", "client_timeouts", "long_running_query", "tempdb_space", "low_disk",
      "self_alerts", "pvs", "file_growth", "long_running_job", "failed_job", "database_state", "agent_alert_gaps", "backup_overdue", "log_backup_overdue", "checkdb_overdue", "identity_capacity", "ag", "delivery"],
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
  { title: "Global alert filters", groups: [], top: ["excluded_databases", "excluded_volumes"] },
  { title: "Automated analysis", groups: ["analysis"], top: [] },
  { title: "Fleet sweep reports", groups: ["fleet_sweep"], top: [] },
];

/* The fields the page shows for each group. A key the read carries that is not listed here is not drawn, so a field
   added to the service later cannot reach the page until it is listed (and reviewed) here. */
export const GROUP_FIELDS = {
  cpu: ["enabled", "threshold_percent", "mode"],
  blocking: ["enabled", "count_threshold", "wait_threshold_seconds", "pg_count_threshold"],
  deadlocks: ["enabled", "count_threshold", "pg_count_threshold"],
  health_bands: ["deadlock_warn_per_hour", "deadlock_critical_per_hour", "blocking_warn_per_hour", "blocking_critical_per_hour",
    "blocking_warn_wait_seconds", "blocking_critical_wait_seconds"],
  poison_wait: ["enabled", "threshold_ms"],
  runnable_queue: ["enabled", "warning_per_scheduler", "critical_per_scheduler"],
  client_timeouts: ["enabled", "warning_per_minute", "critical_per_minute", "warning_multiple", "critical_multiple"],
  identity_capacity: ["enabled", "warning_percent", "critical_percent"],
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
  backup_overdue: ["enabled"],
  log_backup_overdue: ["enabled"],
  checkdb_overdue: ["enabled"],
  agent_alert_gaps: ["enabled"],
  ag: ["enabled", "lag_threshold_seconds", "redo_queue_threshold_kb", "disconnect_refire_minutes"],
  delivery: ["mode", "per_event_max", "cooldown_minutes", "send_resolutions"],
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

/* #5825: delivery.send_resolutions is one object of per-channel flags, in the Settings window's channel words. Only a
   true or false is drawn, so nothing but yes/no can reach the cell; a channel the read does not carry is left out. */
const RESOLUTION_CHANNELS = [["slack", "Slack"], ["teams", "Teams"], ["webhook", "Generic webhook"], ["email", "Email"]];

export function sendResolutionsText(v) {
  if (!v || typeof v !== "object" || Array.isArray(v)) return "—";
  const parts = RESOLUTION_CHANNELS.filter(([k]) => typeof v[k] === "boolean").map(([k, label]) => label + ": " + (v[k] ? "yes" : "no"));
  return parts.length ? parts.join(", ") : "—";
}

/* A listed field whose value is an object, drawn by its own formatter (valueText draws any object as "—"). */
const FIELD_TEXT = { "delivery.send_resolutions": sendResolutionsText };

/** Setting rows for one group: [{ setting, value }], only the fields the group allow-list names (or `fields`, when a
    section shows part of a group) that the group carries; a secret-shaped key is dropped even if it were listed. */
export function groupRows(prefix, group, fields) {
  if (!group || typeof group !== "object" || Array.isArray(group)) return [];
  const allowed = fields || GROUP_FIELDS[prefix] || [];
  return allowed
    .filter((k) => k in group && !isSecretKey(k))
    .map((k) => ({ setting: prefix + "." + k, label: settingLabel(prefix, k), value: (FIELD_TEXT[prefix + "." + k] || valueText)(group[k]) }));
}

/** The sections of the settings payload as [{ title, rows }]; a section with no rows is dropped. */
export function settingSections(data) {
  const d = data && typeof data === "object" ? data : {};
  return SETTING_SECTIONS.map((s) => {
    const rows = [];
    for (const k of s.top) if (k in d && !isSecretKey(k)) rows.push({ setting: k, label: settingLabel("", k), value: valueText(d[k]) });
    for (const g of s.groups) rows.push(...groupRows(g, d[g], s.groupFields && s.groupFields[g]));
    return { title: s.title, rows };
  }).filter((s) => s.rows.length);
}

/* #5750: the words the desktop viewer's routes window uses (ViewerNotificationRouteScope in the Viewer; a test pins these
   against it). The Applies to cell names the scope: "All servers", the server's name, or the tag's name. */
export const ROUTE_ALL_SERVERS = "All servers";
export const ROUTE_STATUS_OK = "OK";
export const ROUTE_STATUS_DISABLED = "Disabled";
export const ROUTE_STATUS_ORPHANED_SERVER = "Orphaned: server removed";
export const ROUTE_STATUS_ORPHANED_TAG = "Orphaned: tag deleted";
export const ROUTE_STATUS_TAG_UNREADABLE = "Not active: tag data unreadable";
export const ROUTE_STATUS_SCOPE_UNREADABLE = "Not active: scope data unreadable";

/** Where a route applies, from get_notification_routes' scope fields. A scope whose server or tag is gone shows the id it still names. */
export function routeScopeText(r) {
  if (r.scope_server_id != null) return r.scope_server_name || `(server id ${r.scope_server_id})`;
  if (r.scope_tag_id != null) return r.scope_tag_name || `(tag id ${r.scope_tag_id})`;
  return ROUTE_ALL_SERVERS;
}

/** A route's status: a scope that cannot be read or no longer exists matches NOTHING, so it never reads as OK. */
export function routeStatusText(r) {
  if (r.scope_unreadable === true) return r.scope_tag_id != null ? ROUTE_STATUS_TAG_UNREADABLE : ROUTE_STATUS_SCOPE_UNREADABLE;
  if (r.orphaned === true) return r.scope_server_id != null ? ROUTE_STATUS_ORPHANED_SERVER : ROUTE_STATUS_ORPHANED_TAG;
  return r.enabled === false ? ROUTE_STATUS_DISABLED : ROUTE_STATUS_OK;
}

/** The Agent column (#5632): whether a matching alert opens an investigation request for an agent. agent_level null
    inherits from a broader route, 0 is off (and stops the lookup), 1 investigates and 2 acts (#5912), both within the route's hourly and daily
    limits. A read with no agent_level (Lite's shape, or an older service) reads as Inherit. */
export function routeAgentText(r) {
  if (!r || r.agent_level === null || r.agent_level === undefined) return "Inherit";
  if (r.agent_level === 1) return `Investigate (${r.agent_max_per_hour}/h, ${r.agent_max_per_day}/day)`;
  if (r.agent_level === 2) return `Act (${r.agent_max_per_hour}/h, ${r.agent_max_per_day}/day)`;
  if (r.agent_level === 3) return `Act on servers (${r.agent_max_per_hour}/h, ${r.agent_max_per_day}/day)`;
  return "Off";
}

/** The Send resolutions column (#5825), in the route dialog's words: send_resolutions null follows each channel's own
    "Send resolutions" setting, true always sends the cleared message to this route's destinations, false never does. A
    route read without the field reads as the channel setting, the same as null. */
export function routeResolutionsText(r) {
  if (!r || r.send_resolutions === null || r.send_resolutions === undefined) return "Use channel setting";
  return r.send_resolutions === true ? "Always" : "Never";
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
    applies_to: routeScopeText(r),
    status: routeStatusText(r),
    channels: Array.isArray(r.configured_channels) ? r.configured_channels.filter((c) => !isSecretKey(c)).join(", ") : null,
    agent: routeAgentText(r),
    resolutions: routeResolutionsText(r),
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
     - a SQL Server edit never sends port (the service refuses a non-zero one; the page has no way to clear a stored one) and a PostgreSQL edit never sends auth;
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
  /* #6254: SQL Server only. The host (or listener) a Query Store read may use instead of the primary; blank = off. It is part
     of how the server is reached: the saved password is bound to it, so changing it needs the password again. */
  { key: "query_store_replica", label: "QS replica" },
  { key: "monthly_cost_usd", label: "Monthly Cost ($)" },
  /* #5452: PostgreSQL only, like port. A blank box keeps the stored role; "Remove AWS role" sends null. The external ID is
     not here: it is write-only, read from its own box at Save, never held in the form's values. */
  { key: "aws_role_arn", label: "AWS role ARN" },
];

/* The service's own sentences for the AWS role (AwsRoleSettings in the Storage project), so the form refuses what the
   service would refuse, in the same words. A test pins that these match the C# constants. */
export const AWS_INVALID_ROLE = "The AWS role must be an IAM role ARN, such as arn:aws:iam::123456789012:role/darling-monitor.";
export const AWS_INVALID_EXTERNAL_ID = "The external ID must be 2 to 1224 characters: letters, digits and _ + = , . @ : / - with no spaces.";
export const AWS_EXTERNAL_ID_NEEDS_ROLE = "An external ID needs an AWS role ARN. Set the role, or clear the external ID.";
export const AWS_ROLE_CHANGE_NEEDS_EXTERNAL_ID = "Changing the AWS role needs the external ID with it: send the external ID again, or clear it.";

const AWS_ROLE_PATTERN = /^arn:aws(?:-[a-z]+)*:iam::[0-9]{12}:role\/(.+)$/;
const AWS_ROLE_NAME_PATTERN = /^[A-Za-z0-9_+=,.@-]{1,64}$/;
const AWS_ROLE_PATH_SEGMENT_PATTERN = /^[A-Za-z0-9_+=,.@-]+$/;
const AWS_EXTERNAL_ID_PATTERN = /^[A-Za-z0-9_+=,.@:/-]{2,1224}$/;

/* True when `role` (trimmed, not blank) is an IAM role ARN the service accepts. */
function validAwsRole(role) {
  if (role.length < 20 || role.length > 2048) return false;
  const m = AWS_ROLE_PATTERN.exec(role);
  if (!m) return false;
  const parts = m[1].split("/");
  if (!AWS_ROLE_NAME_PATTERN.test(parts[parts.length - 1])) return false;
  let pathLength = 0;
  for (let i = 0; i < parts.length - 1; i++) {
    if (!AWS_ROLE_PATH_SEGMENT_PATTERN.test(parts[i])) return false;
    pathLength += parts[i].length + 1;
  }
  return !(pathLength > 0 && pathLength + 1 > 512);
}
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
    query_store_replica: asText(r.query_store_replica),
    monthly_cost_usd: Number.isFinite(cost) ? String(cost) : "0",
    aws_role_arn: asText(r.aws_role_arn),
    aws_external_id_set: r.aws_external_id_set === true,
    aws_remove_role: false,
    aws_clear_external_id: false,
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
    /* Trimmed; blank is "no replica", which the service reads as a removal. A PostgreSQL target has none. */
    query_store_replica: postgres ? "" : trim(v.query_store_replica),
    monthly_cost_usd: cost === "" ? 0 : /^(\d+\.?\d*|\.\d+)$/.test(cost) ? Number(cost) : NaN,
    /* A blank box is "no role typed" (null): it keeps the stored role, it never removes it. */
    aws_role_arn: postgres ? trim(v.aws_role_arn) || null : null,
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
    || o.multi_subnet_failover !== v.multi_subnet_failover
    || o.query_store_replica !== v.query_store_replica;
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
export function validateEdit(original, values, password, externalId) {
  const engine = original && original.engine;
  const was = normalizeEdit(original, engine);
  const next = normalizeEdit(values, engine);
  if (!next.host) return "Server name is required.";
  if (isPostgres(engine) && Number.isNaN(next.port)) return "Port must be between 1 and 65535, or blank for the default (5432).";
  if (isPostgres(engine)) {
    /* #5452: the AWS role checks, in the service's order: each format, an ID with no role, a role change that would keep the
       stored ID unseen. Whether the role is on darling.json's list is the service's to say. */
    const idTyped = asText(externalId).trim();
    const removeRole = !!values && values.aws_remove_role === true;
    if (!removeRole && next.aws_role_arn !== null && !validAwsRole(next.aws_role_arn)) return AWS_INVALID_ROLE;
    if (idTyped !== "" && !AWS_EXTERNAL_ID_PATTERN.test(idTyped)) return AWS_INVALID_EXTERNAL_ID;
    const effectiveRole = removeRole ? null : next.aws_role_arn !== null ? next.aws_role_arn : was.aws_role_arn;
    if (idTyped !== "" && effectiveRole === null) return AWS_EXTERNAL_ID_NEEDS_ROLE;
    const clearingId = !!values && values.aws_clear_external_id === true;
    if (effectiveRole !== null && effectiveRole !== was.aws_role_arn && !!original && original.aws_external_id_set === true && idTyped === "" && !clearingId) {
      return AWS_ROLE_CHANGE_NEEDS_EXTERNAL_ID;
    }
  }
  if (next.auth === "SQL" && !next.username) return "Username is required for SQL Server authentication.";
  if (next.auth === "ServicePrincipal" && !next.username) return "The Application (client) ID is required for service-principal authentication.";
  if (!password && passwordRequired(original, values)) {
    if (was.auth !== next.auth) {
      return next.auth === "ServicePrincipal"
        ? "Switching to ServicePrincipal authentication needs the client secret as password."
        : "Switching to SQL authentication needs the password.";
    }
    return "Changing the host, port or any other connection setting of this server needs its password sent with the change, because a saved password is only ever sent to the server it was saved for. Send the password together with the new settings.";
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
export function buildEditBody(original, values, token, password, externalId) {
  const engine = original && original.engine;
  const postgres = isPostgres(engine);
  const was = normalizeEdit(original, engine);
  const next = normalizeEdit(values, engine);
  const switched = was.auth !== next.auth;
  const body = {};
  for (const { key } of EDIT_FIELDS) {
    if (key === (postgres ? "auth" : "port")) continue;
    if (key === "aws_role_arn") continue;
    const value = next[key];
    if (typeof value === "number" && !Number.isFinite(value)) continue;
    if (key === "username" && next.auth === "Windows") continue;
    if ((key === "username" && switched) || !sameValue(key, was[key], value)) body[key] = value;
  }
  if (typeof password === "string" && password !== "" && storesSecret(next.auth)) body.password = password;
  if (postgres) {
    /* #5452: a typed role that differs is sent; a blank box keeps the stored one; "Remove AWS role" sends null, which takes
       the external ID with it. The external ID: typed text replaces, "Clear external ID" sends null, otherwise it is left out. */
    const removeRole = !!values && values.aws_remove_role === true;
    const idTyped = asText(externalId).trim();
    if (removeRole) {
      if (was.aws_role_arn !== null) body.aws_role_arn = null;
    } else {
      if (next.aws_role_arn !== null && next.aws_role_arn !== was.aws_role_arn) body.aws_role_arn = next.aws_role_arn;
      if (idTyped !== "") body.aws_external_id = idTyped;
      else if (!!values && values.aws_clear_external_id === true && !!original && original.aws_external_id_set === true) body.aws_external_id = null;
    }
  }
  if (!Object.keys(body).length) return null;
  /* A missing token is sent as null, which the service refuses by name, rather than dropped, which would save with no
     stale-edit check at all. */
  body.expected_modified_at = token == null ? null : token;
  return body;
}

const QS_REPLICA_NOTE = "Optional. A readable secondary (host or host,port) or an availability group listener to read Query Store from instead of this server. Same login and password; blank = off. Not used on Azure SQL Database. Changing it needs the password again.";

const UNEXPECTED_ANSWER = "The service gave an answer this page does not understand, so it is not known whether anything was saved. Your entries are kept; check the server list before saving again.";

/** What one answer to the edit means, from the HTTP status, the parsed body and (for status 0) the transport's own
    message: { kind, close, reread, banner?, notice?, current? }. `close` drops the form, `reread` reads the list again,
    `banner` is the sentence for the form's error strip and `notice` the sentence for the page. It branches on the
    status and the body's status word, never on message text. The sentence is the body's message, else its error, else
    "Request failed (HTTP n).". The session is gone only when the transport says so (`expired`: a 401, or a 2xx whose text is
    not JSON, which is a sign-in page); a 2xx whose body is JSON but not an object is an unexpected answer that keeps the
    form and its typed values, with a sentence (#5356). */
export function interpretEdit(status, body, message, expired) {
  const b = body !== null && typeof body === "object" && !Array.isArray(body) ? body : null;
  const word = b && typeof b.status === "string" ? b.status : "";
  const sentence = (b && [b.message, b.error].find((s) => typeof s === "string" && s !== "")) || "Request failed (HTTP " + status + ").";
  if (status === 0) return { kind: "network", close: false, reread: false, banner: asText(message) || "Network error." };
  if (status === 401 || expired === true) return { kind: "expired", close: true, reread: false };
  if (status >= 200 && status < 300 && !b) return { kind: "unexpected", close: false, reread: false, banner: UNEXPECTED_ANSWER };
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
    /* #6254: the Query Store replica's own connection result, said beside the primary's. A replica that would not open is a
       warning in the same notice, not a failed save: the service reads Query Store from the primary until it can. */
    const replica = typeof b.query_store_replica_test === "string" && b.query_store_replica_test ? " " + b.query_store_replica_test : "";
    return { kind: "updated", close: true, reread: true, notice: (name ? 'Saved "' + name + '".' : "Saved.") + note + tested + replica };
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
  /* #5452: the external ID is never read back; a conflict shows only whether one is stored. */
  if (postgres && !!original && !!current && (original.aws_external_id_set === true) !== (editFormValues(current).aws_external_id_set === true)) {
    const idState = (set) => (set ? "set" : "not set");
    const shownWas = idState(original.aws_external_id_set === true);
    const shownNow = idState(editFormValues(current).aws_external_id_set === true);
    changes.push({ key: "aws_external_id_set", label: "AWS external ID", was: shownWas, now: shownNow, yours: null, text: "AWS external ID: was " + shownWas + ", now " + shownNow });
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
  /* #5452: the two AWS boxes are the user's own intent, kept over whatever the other edit left. */
  merged.aws_remove_role = typed.aws_remove_role === true;
  merged.aws_clear_external_id = typed.aws_clear_external_id === true;
  return merged;
}

/** `text` with every copy of the typed password replaced by "[redacted]" (plain split and join, no pattern); with no
    password the text is returned as it is. Applied to any service sentence before it is shown. */
export function redactPassword(text, ...secrets) {
  let out = text;
  for (const secret of secrets) {
    if (typeof secret !== "string" || secret === "") continue;
    out = asText(out).split(secret).join("[redacted]");
    const trimmed = secret.trim();
    if (trimmed !== "" && trimmed !== secret) out = asText(out).split(trimmed).join("[redacted]");
  }
  return out;
}

const TABS = [
  { id: "servers", label: "Servers" },
  { id: "routes", label: "Notification Routes" },
  { id: "settings", label: "Alert Settings" },
  { id: "feedback", label: "Report Feedback" },
  { id: "practice", label: "Practice Scores" },
  { id: "pending", label: "Pending Notes" },
  { id: "overrides", label: "Threshold Overrides" },
  { id: "agent-changes", label: "Agent changes" },
  { id: "server-actions", label: "Server actions" },
  { id: "profiles", label: "Environment Profiles" },
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
      href: storeHash("#/admin/") + t.id,
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
  /* #5452: the AWS external ID box, write-only like the password: read once when Save is pressed, never copied into f.values. */
  const externalIdInput = el("input", { type: "password", class: "tag-input", "data-field": "aws_external_id", autocomplete: "new-password", ...KEEP_FROM_PASSWORD_MANAGERS });
  const externalIdCaption = el("span", { class: "mute-label", text: "AWS external ID (write-only):" });
  const externalIdRow = el("label", { class: "tag-field", "data-row": "aws_external_id" }, [externalIdCaption, externalIdInput]);
  f.heading = heading;
  f.passwordInput = secret;
  f.externalIdInput = externalIdInput;
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
    externalIdCaption.textContent = "AWS external ID (write-only): " + (f.original.aws_external_id_set ? "one is stored; leave blank to keep it." : "none is stored.");
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
      textRow(f, "query_store_replica", "QS replica:", { autocomplete: "off", ...KEEP_FROM_PASSWORD_MANAGERS }).row,
      el("div", { class: "muted", text: QS_REPLICA_NOTE }),
    );
  }
  rows.push(
    textRow(f, "monthly_cost_usd", "Monthly Cost ($):", { inputmode: "decimal" }).row,
  );
  if (postgres) {
    rows.push(
      el("h4", { class: "section-title", text: "Amazon RDS or Aurora (optional)" }),
      textRow(f, "aws_role_arn", "AWS role ARN: blank keeps the stored role", { autocomplete: "off", ...KEEP_FROM_PASSWORD_MANAGERS }).row,
      checkRow(f, "aws_remove_role", "Remove AWS role"),
      externalIdRow,
      checkRow(f, "aws_clear_external_id", "Clear external ID"),
    );
  }
  rows.push(
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
  if (editForm && editForm.externalIdInput) editForm.externalIdInput.value = "";
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
  /* The redraw detaches the old password box; whatever was typed into it since the 409 goes first (#5356), as closeEdit does. */
  if (f.passwordInput) f.passwordInput.value = "";
  if (f.externalIdInput) f.externalIdInput.value = "";
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
function landEdit(f, out, password, externalId) {
  const here = editForm === f;
  const say = (sentence) => redactPassword(sentence, password, externalId);
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
  f.externalIdInput.value = "";
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
  const externalId = f.externalIdInput.value;
  mount(f.bannerBox, []);
  mount(f.conflictBox, []);
  const problem = validateEdit(f.original, f.values, password, externalId);
  if (problem) {
    f.passwordInput.value = "";
    f.externalIdInput.value = "";
    showBanner(f, redactPassword(problem, password, externalId));
    return;
  }
  const body = buildEditBody(f.original, f.values, f.token, password, externalId);
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
    out = interpretEdit(res.status, res.body, res.message, res.expired === true);
    late = editForm !== f;
    landEdit(f, out, password, externalId);
  } finally {
    if (editForm === f) showStatus(f, "");
    setBusy(false);
  }
  if (out.reread || (late && out.kind === "unchanged")) await reloadServers();
}

const ROUTES_NOTE =
  "Destinations (webhook URLs, keys) are never reported by the service; only the names of the channels a " +
  "route sets are shown. Testing a destination stays in the desktop Settings window.";

/* #5998: the signing status of the generic webhook, in sentences. GET /api/webhook-signing answers presence and dates only (never a
   key): whether a key is set, whether a rotation is in its window and when it ends, and when the last rotation began. The store
   keeps no time for a first key, so none is shown for one. The key is made, rotated and removed in the desktop Viewer. */
export function signingStatusLines(data) {
  if (!data || data.status !== "ok") return [];
  const one = (label, d) => {
    if (d && d.open_failed === true) return label + ": the signing key could not be opened, so posts to this destination are not sent. Generate a new key in the desktop Settings window, or remove it.";
    if (!d || d.key_set !== true) return label + ": no signing key. Posts are not signed.";
    let t = label + ": signing key set. Posts are signed.";
    if (d.rotating === true && d.rotation_ends_utc) t += " A rotation is in its window: both keys sign until " + d.rotation_ends_utc + ".";
    else if (d.last_rotated_utc) t += " The last rotation began " + d.last_rotated_utc + ".";
    return t;
  };
  const lines = [one("Generic webhook (Settings)", data.settings)];
  for (const r of Array.isArray(data.routes) ? data.routes : []) {
    lines.push(one("Route " + r.route_id + (r.metric_match ? " (" + r.metric_match + ")" : "") + (r.enabled === false ? " (disabled)" : ""), r));
  }
  return lines;
}

async function buildRoutes(body, generation) {
  const [res, signing, session] = await Promise.all([readTool("get_notification_routes", {}), apiGet("/api/webhook-signing"), getSession()]);
  if (generation !== renderGeneration) return;
  const bad = failure(res);
  if (bad) return mount(body, bad);
  /* #6264: the agent channel is edited here as in the Viewer; the seat edits only when the session may write AND the store login holds the grants. */
  const access = await readAgentSetupAccess(session);
  if (generation !== renderGeneration) return;
  const sourceRoutes = Array.isArray(res.data && res.data.routes) ? res.data.routes : [];
  const rows = routeRows(res.data).map((row, i) => ({ ...row, agent_use: routeUseText(sourceRoutes[i]) }));
  const scopes = access.routes ? await readScopes() : { servers: [], tags: [] };
  if (generation !== renderGeneration) return;
  const options = {
    canEdit: access.routes,
    routes: sourceRoutes,
    servers: scopes.servers,
    tags: scopes.tags,
    repaint: () => (shownTab === "routes" && shownBody ? startBuilder("routes", shownBody, ++renderGeneration) : undefined),
  };
  /* A failed signing read leaves the strip out; the routes below are the page. */
  const signingNodes = signing && signing.kind === "data" ? signingStatusLines(signing.data).map((t) => noticeStrip(t)) : [];
  mount(body, [
    ...(access.routes ? routeEditorNodes(options) : routeListOnlyNodes(access.message, access.managed, access.adminWritesOff)),
    noticeStrip(ROUTES_NOTE),
    ...signingNodes,
    VIZ.table({ routes: rows }, {
      rowsKey: "routes",
      columns: routeColumns(ROUTE_COLUMNS, options),
      emptyText: "No routes. Every alert goes to every channel configured in Settings.",
    }),
  ]);
}

/** The servers and tags the scope pickers offer, from the fleet read: [{id, name}]. */
async function readScopes() {
  const res = await apiGetFleet();
  const d = res && res.kind !== "error" ? res.data || {} : {};
  return {
    servers: (Array.isArray(d.cards) ? d.cards : []).filter((c) => Number.isInteger(c.server_id)).map((c) => ({ id: c.server_id, name: c.display_name || "server " + c.server_id })),
    tags: (Array.isArray(d.tags) ? d.tags : []).filter((t) => Number.isInteger(t.id)).map((t) => ({ id: t.id, name: t.name || "tag " + t.id })),
  };
}

/* Environment Profiles (#6264) lives in admin-agent-setup.js; it gets this file's helpers and repaints through startBuilder. */
const buildProfilesTab = (body, generation) => buildProfiles(body, generation, {
  getSession,
  failure,
  readScopes,
  isCurrent: (g) => g === renderGeneration,
  repaint: () => (shownTab === "profiles" && shownBody ? startBuilder("profiles", shownBody, ++renderGeneration) : undefined),
});

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

/* Report Feedback (#5913): what people say about the investigation reports agents write. GET /api/investigation-feedback lists the
   reported requests of the last week (the newest 200 are listed; the counts cover every one) with the feedback each one holds and
   the false-positive counts per alert family. The page lists for everyone. The form (PUT /api/investigation-feedback/{id}) shows
   only for a sign-in that can make changes AND when the list's own answer says `can_record`: the service asks the store whether
   the web dashboard's login may write the table. Managed and compose stores connect the web as the viewer role, where it may not,
   so the page is list-only there (FEEDBACK_LIST_ONLY says so and names the Viewer); a record that is refused anyway still answers
   403. A second record by the same author replaces the first. */
export const FEEDBACK_VERDICTS = [
  { value: "right", label: "Right" },
  { value: "partly", label: "Partly right" },
  { value: "wrong", label: "Wrong" },
];
export const FEEDBACK_CORRECTIONS = [
  { value: "", label: "Keep the report's verdict" },
  { value: "real", label: "Real" },
  { value: "false_positive", label: "False positive" },
  { value: "needs_person", label: "Needs a person" },
];

export const FEEDBACK_NOTE =
  "Say whether each report was right, partly right or wrong, and what the agent missed. The counts show how often each alert " +
  "family's investigations end as a false positive once your corrections are applied. A request's settled verdict is the corrected " +
  "verdict of the newest feedback that gave one, so a later feedback that gives no corrected verdict does not displace an earlier " +
  "one that did. A second feedback from the same person replaces their first. Reports are listed newest first.";

/* Shown instead of the form when the store says this dashboard's login may not record (can_record false). */
export const feedbackListOnly = (managed, adminWritesOff) => listOnlyNote(managed, "this page is list-only", "To record feedback", adminWritesOff);

/* What the counts and the list are taken over, in words: the answer's own window (hours_back). */
export function feedbackWindowLabel(hours) {
  const h = Number.isInteger(hours) && hours > 0 ? hours : 168;
  const span = h % 24 === 0 ? (h / 24) + (h === 24 ? " day" : " days") : h + " hours";
  return "Counts and reports cover the requests opened in the last " + span + " that have a report.";
}

export const FEEDBACK_FAMILY_COLUMNS = [
  { key: "family", label: "Alert family" },
  { key: "reported", label: "Reported" },
  { key: "with_feedback", label: "With feedback" },
  { key: "marked_wrong", label: "Marked wrong" },
  { key: "agent_false_positives", label: "False positives (agent)" },
  { key: "false_positives", label: "False positives (after feedback)" },
];

/* The body of a feedback PUT from the form's values. Blank text is sent as null; the service refuses what the table would. */
export function buildFeedbackBody(values) {
  const text = (v) => (typeof v === "string" && v.trim() !== "" ? v : null);
  return {
    verdict: typeof values.verdict === "string" ? values.verdict : "",
    corrected_verdict: text(values.corrected_verdict),
    missed_step: text(values.missed_step),
    comment: text(values.comment),
  };
}

/* What the page does with a feedback PUT's answer. `reread` says the list is stale (read it again); a 403 refusal and a bad value
   leave it as it is, because the list is right and the person is told why. */
export function interpretFeedback(status, body, message, expired) {
  const b = body !== null && typeof body === "object" && !Array.isArray(body) ? body : null;
  const sentence = (b && [b.message, b.error].find((s) => typeof s === "string" && s !== "")) || "Request failed (HTTP " + status + ").";
  if (status === 0) return { kind: "network", reread: false, message: typeof message === "string" && message ? message : "Network error." };
  if (status === 401 || expired === true) return { kind: "expired", reread: false, message: "" };
  if (status === 200 && b && (b.status === "recorded" || b.status === "replaced")) {
    return { kind: b.status, reread: true, message: b.status === "replaced" ? "Your earlier feedback on this report was replaced." : "Feedback recorded." };
  }
  if (status === 403) return { kind: "refused", reread: false, message: sentence };
  if (status === 404 || status === 409) return { kind: "stale", reread: true, message: sentence };
  if (status === 400) return { kind: "invalid", reread: false, message: sentence };
  return { kind: "failed", reread: false, message: sentence };
}

/* The page state, in module scope so the 60 s repaint keeps what a person is typing: the status line of each report, by request id. */
const feedbackStatus = new Map();
const feedbackDrafts = new Map();

function feedbackLine(f) {
  const parts = [f.author + ": " + f.verdict];
  if (f.corrected_verdict) parts.push("corrected to " + f.corrected_verdict);
  if (f.missed_step) parts.push("missed: " + f.missed_step);
  if (f.comment) parts.push(f.comment);
  if (f.recorded_at_utc) parts.push("given " + localizeIsoText(f.recorded_at_utc));
  return parts.join(" - ");
}

function selectField(requestId, field, options, draft) {
  const select = el("select", { "data-fb-field": field, "data-request-id": requestId },
    options.map((o) => el("option", { value: o.value, text: o.label })));
  select.value = draft[field] != null ? draft[field] : options[0].value;
  select.addEventListener("change", () => { draft[field] = select.value; });
  return select;
}

function textField(requestId, field, label, draft) {
  const input = el("textarea", { "data-fb-field": field, "data-request-id": requestId, rows: "2", maxlength: "4000", "aria-label": label });
  input.value = draft[field] != null ? draft[field] : "";
  input.addEventListener("input", () => { draft[field] = input.value; });
  return input;
}

function feedbackForm(r, rerender) {
  const id = r.request_id;
  if (!feedbackDrafts.has(id)) feedbackDrafts.set(id, { verdict: "right" });
  const draft = feedbackDrafts.get(id);
  const status = el("div", { class: "tag-status", "data-fb-status": id, role: "status", text: feedbackStatus.get(id) || "" });
  const save = el("button", { type: "button", class: "btn", "data-fb-save": id, text: "Record feedback" });
  save.addEventListener("click", async () => {
    save.disabled = true;
    const out = await apiWrite("PUT", "/api/investigation-feedback/" + encodeURIComponent(id), buildFeedbackBody(draft));
    save.disabled = false;
    const a = interpretFeedback(out.status, out.body, out.message, out.expired);
    if (a.kind === "expired") return;
    feedbackStatus.set(id, a.message);
    status.textContent = a.message;
    if (a.kind === "recorded" || a.kind === "replaced") feedbackDrafts.delete(id);
    if (a.reread) await rerender();
  });
  return el("div", { class: "feedback-form", "data-fb-form": id }, [
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "The report was" }), selectField(id, "verdict", FEEDBACK_VERDICTS, draft)]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "The verdict should be" }), selectField(id, "corrected_verdict", FEEDBACK_CORRECTIONS, draft)]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "What the agent missed" }), textField(id, "missed_step", "What the agent missed", draft)]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Comment" }), textField(id, "comment", "Comment", draft)]),
    save,
    status,
  ]);
}

function feedbackCard(r, canEdit, rerender) {
  const lines = (r.feedback || []).map((f) => el("li", { "data-fb-line": r.request_id, text: feedbackLine(f) }));
  return el("section", { class: "feedback-card", "data-request-id": r.request_id }, [
    el("h4", { text: r.server_name + " - " + r.family + " - " + r.first_metric }),
    el("p", { class: "mute-label", text: localizeIsoText("Reported " + r.reported_at_utc + " by " + r.agent_id + ": " + r.report_verdict) }),
    el("p", { text: r.root_cause }),
    lines.length ? el("ul", {}, lines) : el("p", { class: "mute-label", text: "No feedback yet." }),
    canEdit ? feedbackForm(r, rerender) : null,
  ]);
}

async function buildFeedback(body, generation) {
  const [session, res] = await Promise.all([getSession(), apiGet("/api/investigation-feedback")]);
  if (generation !== renderGeneration) return;
  const bad = failure(res);
  if (bad) return mount(body, bad);
  const data = res.data || {};
  const requests = Array.isArray(data.requests) ? data.requests : [];
  const families = Array.isArray(data.families) ? data.families : [];
  const rerender = () => buildFeedback(body, generation);
  const canRecord = session.can_edit === true && data.can_record === true;
  const nodes = [noticeStrip(FEEDBACK_NOTE)];
  if (data.can_record !== true) nodes.push(noticeStrip(feedbackListOnly(session.managed_store === true, session.admin_writes_off === true)));
  nodes.push(el("h3", { class: "section-title", text: "False positives by alert family" }));
  nodes.push(el("p", { class: "mute-label", "data-fb-window": "1", text: feedbackWindowLabel(data.hours_back) }));
  nodes.push(VIZ.table({ families }, {
    rowsKey: "families",
    columns: FEEDBACK_FAMILY_COLUMNS,
    emptyText: "No investigation reports in the last " + (data.hours_back || 168) + " hours.",
  }));
  nodes.push(el("h3", { class: "section-title", text: "Reports" }));
  if (data.truncated === true) nodes.push(noticeStrip("Showing the newest " + requests.length + " reports; older ones in the window are not listed, and the counts above still cover them."));
  if (!requests.length) nodes.push(emptyStrip("No investigation has reported in this window."));
  for (const r of requests) nodes.push(feedbackCard(r, canRecord, rerender));
  mount(body, nodes);
}


/* ─────────────────────────── practice scores (#5918) ─────────────────────────── */

/* Practice Scores: how each agent did on practice requests, cases an operator already judged and replayed without their answers.
   GET /api/investigation-practice-scores. The score is advisory and turns no route on or off. A seat that can make changes also
   gets the Start form (#6265). */
const PRACTICE_NOTE =
  "A practice request replays a case somebody gave feedback on, without its answers, and scores the agent's report against that " +
  "feedback. A corrected verdict is the answer; otherwise right makes the report's verdict the answer, partly counts half, and wrong " +
  "with no correction has no answer. Scores are advisory and turn no route on or off. A sign-in that can make changes can start a practice " +
  "request here for a judged case, as the desktop Viewer can.";

/* #6265: start a practice request for one judged case (a reported request somebody gave feedback on, from the Report Feedback list).
   The service refuses what the Viewer refuses: an unknown case, a practice request, no report, no feedback, or a case whose practice
   request is still live, each in the store's own words. */
let practiceStatus = "";
let practicePick = "";

export function judgedCases(data) {
  const list = data && Array.isArray(data.requests) ? data.requests : [];
  return list.filter((r) => r && typeof r.request_id === "string" && Array.isArray(r.feedback) && r.feedback.length > 0);
}

function practiceStartForm(cases) {
  const select = el("select", { class: "tag-input", "data-practice-case": "1", "aria-label": "Judged case to replay" },
    [el("option", { value: "", text: "(choose a judged case)" })].concat(cases.map((r) => el("option", {
      value: r.request_id,
      text: (r.server_name || "?") + " - " + (r.family || "?") + " - " + (r.first_metric || "?") + " (" + (r.feedback.length) + " feedback)",
    }))));
  select.value = practicePick;
  select.addEventListener("input", () => { practicePick = select.value; });
  const status = el("div", { class: "tag-status", role: "status", "data-practice-status": "1", text: practiceStatus });
  const start = el("button", { class: "btn primary", type: "button", text: "Start practice request", "data-action": "start-practice" });
  start.addEventListener("click", async () => {
    if (!select.value) {
      practiceStatus = "Select a judged case to replay.";
      status.textContent = practiceStatus;
      return;
    }
    start.disabled = true;
    const out = await startPractice(select.value);
    start.disabled = false;
    if (out.message === null) return;
    practiceStatus = out.message;
    status.textContent = out.message;
  });
  return el("section", { class: "feedback-card", "data-practice-start": "1" }, [
    el("h4", { text: "Start a practice request" }),
    el("p", { class: "mute-label", text: "Replays a judged case without its answers. An agent on the case's route can claim it, and its report is scored against the feedback." }),
    cases.length ? el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Judged case" }), select]) : emptyStrip("No report has feedback in the last week, so there is no case to replay."),
    cases.length ? start : null,
    status,
  ]);
}

export function practiceScoreText(a) {
  if (a.percent === null || a.percent === undefined) return "not scored yet";
  return Number(a.points) + " of " + Number(a.possible) + " (" + a.percent + "%)";
}

export function practiceRows(agents) {
  return (Array.isArray(agents) ? agents : []).map((a) => ({
    agent_id: a.agent_id,
    cases: a.cases,
    scored: a.scored,
    matched: a.matched,
    score: practiceScoreText(a),
    unscored: a.unscored,
    missed_step_cases: a.missed_step_cases,
  }));
}

const PRACTICE_COLUMNS = [
  { key: "agent_id", label: "Agent" },
  { key: "cases", label: "Cases" },
  { key: "scored", label: "Scored" },
  { key: "matched", label: "Matched" },
  { key: "score", label: "Score" },
  { key: "unscored", label: "No answer" },
  { key: "missed_step_cases", label: "Missed step noted" },
];

async function buildPractice(body, generation) {
  const [session, res] = await Promise.all([getSession(), apiGet("/api/investigation-practice-scores")]);
  if (generation !== renderGeneration) return;
  const bad = failure(res);
  if (bad) return mount(body, bad);
  const data = res.data || {};
  const agents = practiceRows(data.agents);
  const nodes = [noticeStrip(PRACTICE_NOTE)];
  const practiceAccess = writeAccess(session);
  if (practiceAccess === "sign-in") nodes.push(noticeStrip(SIGN_IN_SENTENCE));
  if (practiceAccess === "yes") {
    const feedback = await apiGet("/api/investigation-feedback");
    if (generation !== renderGeneration) return;
    nodes.push(practiceStartForm(feedback.kind === "data" ? judgedCases(feedback.data) : []));
  }
  nodes.push(el("p", { class: "mute-label", "data-practice-window": "1", text: "Practice requests opened in the last " + (data.hours_back || 720) + " hours." }));
  if (!agents.length) nodes.push(emptyStrip("No agent has reported on a practice request in this window."));
  else nodes.push(VIZ.table({ agents }, { rowsKey: "agents", columns: PRACTICE_COLUMNS, emptyText: "No practice scores." }));
  mount(body, nodes);
}


/* ─────────────────────────── pending agent notes (#5903) ─────────────────────────── */

/* A Level 1 agent proposes a note with its report. It stays pending, out of every brief, until a person approves it here.
   A rejected note is kept with who rejected it and when, and never reaches a brief. The text is shown as written: it is
   not edited on this tab. */
export const NOTE_COLUMNS = [
  { key: "server_name", label: "Server" },
  { key: "metric_name", label: "Metric" },
  { key: "family", label: "Family" },
  { key: "story", label: "Story" },
  { key: "note_text", label: "Proposed note", wrap: true },
  { key: "author", label: "Proposed by" },
  { key: "request", label: "Request" },
  { key: "proposed_at_utc", label: "Proposed", format: "time" },
  { key: "expires_at_utc", label: "Expires", format: "time" },
];

const NOTES_NOTE_EDITING =
  "Notes an agent proposed that nobody has decided. An approved note goes into the briefs for its server; a rejected note " +
  "is kept, marked rejected, and never goes into a brief. The text cannot be edited here.";
const NOTES_NOTE_READ_ONLY = "Notes an agent proposed that nobody has decided. This sign-in is read-only, so it cannot approve or reject them.";
const NOTES_NOTE_EXPIRY =
  " A note stops reaching briefs when it expires (the Expires column), and an expired note is not listed.";
const NOTE_NOT_PENDING = "That note is not pending: it was already decided, or it is gone.";
const NOTE_PAST = { approve: "approved", reject: "rejected" };

const shortId = (v) => (typeof v === "string" && v.length > 8 ? v.slice(0, 8) : v || null);

/** The rows of the pending-notes grid from the list read's body. A note with no server is a fleet note. */
export function pendingNoteRows(data) {
  const notes = data && Array.isArray(data.notes) ? data.notes : [];
  return notes.map((n) => ({
    note_id: n.note_id,
    server_name: n.server_name || (Number.isInteger(n.server_id) ? "Server " + n.server_id : "Fleet"),
    metric_name: n.metric_name || null,
    family: n.family || null,
    story: shortId(n.story_path_hash),
    note_text: n.note_text,
    author: n.author,
    request: shortId(n.source_request_id),
    proposed_at_utc: n.proposed_at_utc,
    expires_at_utc: n.expires_at_utc,
  }));
}

/** The sentence over a list the service cut at its cap, else null. The list says so itself (truncated), because the count it
    carries is the page's (notes_returned), not the queue's: deciding the oldest notes shows the rest. */
export function pendingNotesTruncatedNotice(data) {
  if (!data || data.truncated !== true) return null;
  const shown = Number.isInteger(data.notes_returned) ? data.notes_returned : Array.isArray(data.notes) ? data.notes.length : 0;
  return "This list shows the oldest " + shown + " pending notes and more are waiting. Approve or reject some and the next ones appear.";
}

/** What a decision's answer says, in plain words: { message, isError, reread }. `verb` is "approve" or "reject". An expired
    session says nothing here (the shell has already told the person). A note that was already decided, or is gone, is a 404:
    the list is stale, so it is read again. */
export function noteDecisionOutcome(verb, noteId, res) {
  const past = NOTE_PAST[verb] || verb;
  if (!res || res.expired) return { message: null, isError: true, reread: false };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true, reread: false };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && body.status === past) return { message: "Note " + noteId + " " + past + ".", isError: false, reread: true };
  if (res.status === 404) return { message: "Note " + noteId + " was not changed. " + ((body && body.error) || NOTE_NOT_PENDING), isError: true, reread: true };
  /* The store refused the write for privilege: the web host's login is not the admin role. Its sentence says how to decide; the list stays. */
  if (res.status === 403 && body && typeof body.error === "string" && body.error.indexOf("Deciding a note needs") === 0) return { message: body.error, isError: true, reread: false };
  if (res.status === 403 && body && body.error === SIGN_IN_SENTENCE) return { message: body.error, isError: true, reread: false };
  if (res.status === 403) return { message: "This sign-in is read-only, so it cannot " + verb + " notes.", isError: true, reread: false };
  const said = body && typeof body.error === "string" && body.error ? " " + body.error : "";
  return { message: "Note " + noteId + " was not " + past + ". The service answered " + res.status + "." + said, isError: true, reread: false };
}

/* The last decision's one-line answer, kept in module scope so the 60 s repaint and the re-read after a decision show it. */
let notesStatus = null;
let notesBusy = false;

function noteButtons(row) {
  if (!row || !Number.isInteger(row.note_id)) return null;
  const make = (verb, label) => {
    const button = el("button", {
      class: "btn" + (verb === "approve" ? " primary" : ""),
      type: "button",
      text: label,
      "aria-label": label + " note " + row.note_id + " for " + row.server_name,
      "data-note-id": String(row.note_id),
      onClick: () => decideNote(verb, row.note_id),
    });
    button.disabled = notesBusy;
    return button;
  };
  return el("span", { class: "row-actions" }, [make("approve", "Approve"), make("reject", "Reject")]);
}

/** The pending-notes grid's columns: NOTE_COLUMNS, plus Approve and Reject for a seat whose session reports can_edit. */
export function noteColumns(canEdit) {
  return canEdit ? NOTE_COLUMNS.concat([{ key: "decide", label: "Decide", csv: false, copy: false, render: noteButtons }]) : NOTE_COLUMNS;
}

function setNoteButtons(disabled) {
  if (!shownBody || !shownBody.querySelectorAll) return;
  for (const b of shownBody.querySelectorAll("button[data-note-id]")) b.disabled = disabled;
}

async function decideNote(verb, noteId) {
  if (notesBusy) return;
  notesBusy = true;
  setNoteButtons(true);
  let out;
  try {
    const res = await apiWrite("PUT", "/api/investigation-notes/" + encodeURIComponent(noteId) + "/" + verb, {});
    out = noteDecisionOutcome(verb, noteId, res);
  } catch (e) {
    out = { message: "Could not " + verb + " note " + noteId + ": " + (e && e.message ? e.message : String(e)), isError: true, reread: false };
  }
  notesBusy = false;
  if (out.message) notesStatus = { message: out.message, isError: out.isError };
  if (shownTab !== "pending" || !shownBody) return;
  if (out.reread || !out.message) {
    await startBuilder("pending", shownBody, ++renderGeneration);
  } else {
    setNoteButtons(false);
    drawNoteStatus(shownBody);
  }
}

function noteStatusNode() {
  if (!notesStatus) return null;
  return notesStatus.isError ? errorStrip(notesStatus.message) : noticeStrip(notesStatus.message);
}

/* A failed decision with no re-read replaces only the status line at the top of the body. */
function drawNoteStatus(body) {
  const old = body.querySelector && body.querySelector("[data-note-status]");
  const node = noteStatusNode();
  if (node) node.setAttribute("data-note-status", "1");
  if (old && node) old.replaceWith(node);
  else if (old) old.remove();
  else if (node) body.insertBefore(node, body.firstChild);
}

/* #6265: write a note as a person, the twin of the desktop Viewer's Write note. A note a person writes is approved as written (no
   second person decides it) and goes into the briefs for its scope: a server, or the whole fleet. An agent's proposed note keeps its
   Approve and Reject above. The form is drawn only for a seat that can make changes; the service checks every value again, and a
   store login without the admin role answers with the sentence that says what to do. */
const noteDraft = { server_id: "", metric_name: "", note_text: "", days: String(NOTE_DEFAULT_DAYS) };

function noteWriteForm(servers) {
  const field = (label, control) => el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), control]);
  const server = el("select", { class: "tag-input", "data-note-field": "server_id", "aria-label": "Server the note is about" },
    [el("option", { value: "", text: "Fleet (every server)" })].concat(
      servers.filter((s) => Number.isInteger(s.server_id)).map((s) => el("option", { value: String(s.server_id), text: s.display_name || s.server_name || "server " + s.server_id }))));
  const metric = el("input", { type: "text", class: "tag-input", "data-note-field": "metric_name", placeholder: "optional, such as Blocking", "aria-label": "Metric the note is about" });
  const text = el("textarea", { "data-note-field": "note_text", rows: "3", maxlength: String(NOTE_MAX_LENGTH), "aria-label": "Note text" });
  const days = el("input", { type: "number", class: "tag-input", "data-note-field": "days", min: "1", max: String(NOTE_MAX_DAYS), "aria-label": "Keep for days" });
  server.value = noteDraft.server_id;
  metric.value = noteDraft.metric_name;
  text.value = noteDraft.note_text;
  days.value = noteDraft.days;
  const sync = () => { noteDraft.server_id = server.value; noteDraft.metric_name = metric.value; noteDraft.note_text = text.value; noteDraft.days = days.value; };
  for (const c of [server, metric, text, days]) c.addEventListener("input", sync);
  const status = el("div", { class: "tag-status", role: "status", "data-note-write-status": "1", text: noteWriteMessage || "" });
  const save = el("button", { class: "btn primary", type: "button", text: "Write note", "data-action": "write-note" });
  save.addEventListener("click", async () => {
    sync();
    save.disabled = true;
    const out = await writeNote(noteDraft);
    save.disabled = false;
    if (out.message === null) return;
    noteWriteMessage = out.message;
    status.textContent = out.message;
    if (out.ok) {
      noteDraft.note_text = "";
      noteDraft.metric_name = "";
      text.value = "";
      metric.value = "";
    }
  });
  return el("section", { class: "feedback-card", "data-note-write": "1" }, [
    el("h4", { text: "Write a note" }),
    el("p", { class: "mute-label", text: "Tell agents something about a server or the fleet. It is approved as written and goes into their briefs until it expires." }),
    field("About", server),
    field("Metric", metric),
    field("Note (" + NOTE_MAX_LENGTH + " characters at most)", text),
    field("Keep for days (1 to " + NOTE_MAX_DAYS + ")", days),
    save,
    status,
  ]);
}

let noteWriteMessage = "";

async function buildPending(body, generation) {
  const [session, res] = await Promise.all([getSession(), apiGet("/api/investigation-notes/pending")]);
  if (generation !== renderGeneration) return;
  const nodes = [];
  const status = noteStatusNode();
  if (status) {
    status.setAttribute("data-note-status", "1");
    nodes.push(status);
  }
  const bad = failure(res);
  if (bad) return mount(body, nodes.concat([bad]));
  const rows = pendingNoteRows(res.data);
  /* #6265: Approve, Reject and Write a note are made as a named person, so they need a signed-in admin; a seat that can edit but is
     not signed in sees the list and the sentence that says what to do. */
  const access = writeAccess(session);
  const canEdit = access === "yes";
  nodes.push(noticeStrip((canEdit ? NOTES_NOTE_EDITING : NOTES_NOTE_READ_ONLY) + NOTES_NOTE_EXPIRY));
  if (access === "sign-in") nodes.push(noticeStrip(SIGN_IN_SENTENCE));
  if (canEdit) {
    const serverList = await apiGet("/api/admin/servers");
    if (generation !== renderGeneration) return;
    nodes.push(noteWriteForm(serverList.kind === "data" && serverList.data && Array.isArray(serverList.data.servers) ? serverList.data.servers : []));
  }
  const cut = pendingNotesTruncatedNotice(res.data);
  if (cut) nodes.push(noticeStrip(cut));
  nodes.push(VIZ.table({ notes: rows }, {
    rowsKey: "notes",
    columns: noteColumns(canEdit),
    emptyText: "No notes are waiting for a decision.",
  }));
  mount(body, nodes);
}

/* ─────────────────────────── threshold overrides (#5916) ─────────────────────────── */

/* A threshold held for one server (or one database on it), or for a server tag (#6926), that differs from the global value. The list is GET
   /api/threshold-overrides; a change is PUT /api/threshold-overrides, accepted only under a store login with the admin role.
   A managed or compose store connects this page as the viewer role, so there the grid is a list: the form and the Remove
   buttons are not drawn, and the note says where to change one. The service checks the key and the bounds again. */
export const OVERRIDE_COLUMNS = [
  { key: "scope_text", label: "Scope" },
  { key: "database_text", label: "Database" },
  { key: "key", label: "Key" },
  { key: "value", label: "Value" },
  { key: "global_value", label: "Global value" },
  { key: "set_by", label: "Set by" },
  { key: "request", label: "Request" },
  { key: "updated_at", label: "Updated", format: "time" },
];

const OVERRIDES_NOTE_EDITING =
  "A threshold held for one server, for one database on it, or for a server tag (every server under the tag and the tags below it), that differs from the global value. " +
  "A database override wins over a server override, then the deepest tag the server is under, then the global value. A change applies within the alert engine's reload interval. To silence an alert, mute it.";
const overridesNoteReadOnly = (managed, adminWritesOff) =>
  "A threshold held for one server, for one database on it, or for a server tag, that differs from the global value. " +
  listOnlyNote(managed, "this page lists them only", "To change an override", adminWritesOff);
const OVERRIDE_DENIED = "Changing a threshold override needs";

/** The rows of the overrides grid from the list read's body. `database` and `server_id` stay on the row for Edit and Remove. */
export function overrideRows(data) {
  const list = data && Array.isArray(data.overrides) ? data.overrides : [];
  return list.map((o) => ({
    server_id: o.server_id == null ? null : o.server_id,
    server_name: o.server_name == null ? null : o.server_name,
    tag_id: o.tag_id == null ? null : o.tag_id,
    tag_name: o.tag_name == null ? null : o.tag_name,
    orphaned: o.orphaned === true,
    scope_text: o.tag_id == null ? o.server_name : (o.orphaned === true ? "Tag (deleted): orphaned, matches nothing" : "Tag: " + (o.tag_name || "")),
    database: o.database || null,
    database_text: o.database || "Whole server",
    key: o.key,
    value: o.value,
    global_value: Number.isFinite(o.global_value) ? o.global_value : null,
    set_by: o.set_by,
    request: shortId(o.request_id),
    updated_at: o.updated_at,
  }));
}

/** Why a form entry is refused before anything is sent, or null: the key must be one the service lists, the value an integer
    inside its bounds, and a database only on a key that takes one. The service applies the same rules again. */
export function validateOverride(keys, form) {
  const list = Array.isArray(keys) ? keys : [];
  const hasServer = !!(form && String(form.server || "").trim());
  const hasTag = !!(form && String(form.tag || "").trim());
  if (hasServer && hasTag) return "Choose a server or a tag, not both.";
  if (!hasServer && !hasTag) return "Choose a server or a tag.";
  const entry = list.find((k) => k.key === String(form.key || "").trim());
  if (!entry) return "Choose a key from the list: only a tunable threshold can be overridden.";
  const text = String(form.value == null ? "" : form.value).trim();
  if (!/^-?\d+$/.test(text)) return "The value must be a whole number.";
  const n = Number(text);
  if (!Number.isSafeInteger(n) || n < entry.min || n > entry.max) {
    return entry.key + " must be a whole number between " + entry.min + " and " + entry.max + ", the same bounds the global setting has.";
  }
  if (hasTag && String(form.database || "").trim()) {
    return "A tag override holds a server-level value, so it takes no database.";
  }
  if (String(form.database || "").trim() && entry.database_scope !== true) {
    return entry.key + " is compared against a server-level figure, so it takes a server override only, not a database.";
  }
  return null;
}

/** What a save or remove answer says, in plain words: { message, isError, reread }. An expired session says nothing here. */
export function overrideOutcome(verb, res) {
  if (!res || res.expired) return { message: null, isError: true, reread: false };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true, reread: false };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && (body.status === "ok" || body.status === "cleared")) {
    return { message: body.status === "cleared" ? "Override removed. The global value applies again." : "Override saved.", isError: false, reread: true };
  }
  if (res.status === 403 && body && typeof body.error === "string" && body.error.indexOf(OVERRIDE_DENIED) === 0) {
    return { message: body.error, isError: true, reread: false };
  }
  if (res.status === 403) return { message: "This sign-in is read-only, so it cannot change overrides.", isError: true, reread: false };
  const said = body && typeof body.error === "string" && body.error ? " " + body.error : "";
  return {
    message: "The override was not " + (verb === "clear" ? "removed" : "saved") + "." + (res.status === 400 ? "" : " The service answered " + res.status + ".") + said,
    isError: true,
    reread: false,
  };
}

/* The form's entries and the last answer, in module scope so the 60 s repaint keeps what was typed. */
const EMPTY_OVERRIDE_FORM = { server: "", tag: "", key: "", database: "", value: "" };
let overrideEntries = { ...EMPTY_OVERRIDE_FORM };
let overrideStatus = null;
let overrideBusy = false;

const overrideStatusNode = () => (overrideStatus ? (overrideStatus.isError ? errorStrip(overrideStatus.message) : noticeStrip(overrideStatus.message)) : null);

function redrawOverrides() {
  return shownTab === "overrides" && shownBody ? startBuilder("overrides", shownBody, ++renderGeneration) : undefined;
}

async function changeOverride(verb, entry) {
  if (overrideBusy) return;
  overrideBusy = true;
  let out;
  try {
    const payload = { key: entry.key, database_name: entry.database || null };
    if (entry.tag) payload.tag = String(entry.tag);
    else payload.server_name = entry.server;
    if (verb === "clear") payload.clear = true;
    else payload.value = Number(String(entry.value).trim());
    out = overrideOutcome(verb, await apiWrite("PUT", "/api/threshold-overrides", payload));
  } catch (e) {
    out = { message: "Could not change the override: " + (e && e.message ? e.message : String(e)), isError: true, reread: false };
  }
  overrideBusy = false;
  if (out.message) overrideStatus = { message: out.message, isError: out.isError };
  if (out.isError === false && verb !== "clear") overrideEntries = { ...EMPTY_OVERRIDE_FORM };
  await redrawOverrides();
}

function overrideFormNode(servers, keys, tags) {
  const field = (label, control) => el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), control]);
  const choose = (items, textOf) => [el("option", { value: "", text: "(choose)" })].concat(items.map((i) => el("option", { value: textOf(i), text: textOf(i) })));
  const server = el("select", { class: "tag-input", "data-field": "server" }, choose(servers, (s) => s.server_name));
  const tag = el("select", { class: "tag-input", "data-field": "tag" },
    [el("option", { value: "", text: "(none)" })].concat((Array.isArray(tags) ? tags : []).map((t) => el("option", { value: String(t.tag_id), text: t.tag_name }))));
  const key = el("select", { class: "tag-input", "data-field": "key" }, choose(keys, (k) => k.key));
  const database = el("input", { type: "text", class: "tag-input", "data-field": "database", placeholder: "blank = whole server" });
  const value = el("input", { type: "text", class: "tag-input", "data-field": "value", inputmode: "numeric" });
  server.value = overrideEntries.server;
  tag.value = overrideEntries.tag;
  key.value = overrideEntries.key;
  database.value = overrideEntries.database;
  value.value = overrideEntries.value;
  const sync = () => { overrideEntries = { server: server.value, tag: tag.value, key: key.value, database: database.value, value: value.value }; };
  for (const c of [server, tag, key, database, value]) c.addEventListener("input", sync);
  const save = el("button", {
    class: "btn primary", type: "button", text: "Save override", "data-action": "save-override",
    onClick: async () => {
      sync();
      const bad = validateOverride(keys, overrideEntries);
      if (bad) {
        overrideStatus = { message: bad, isError: true };
        await redrawOverrides();
        return;
      }
      await changeOverride("set", overrideEntries);
    },
  });
  return el("div", { class: "tag-form", "data-override-form": "1" }, [
    field("Server:", server), field("or Tag:", tag), field("Key:", key), field("Database:", database), field("Value:", value), save,
  ]);
}

function overrideActions(row) {
  const edit = el("button", {
    class: "btn", type: "button", text: "Edit", "aria-label": "Edit " + row.key + " for " + row.scope_text,
    onClick: async () => {
      overrideEntries = { server: row.tag_id == null ? row.server_name : "", tag: row.tag_id == null ? "" : String(row.tag_id), key: row.key, database: row.database || "", value: String(row.value) };
      await redrawOverrides();
    },
  });
  const remove = el("button", {
    class: "btn", type: "button", text: "Remove", "aria-label": "Remove " + row.key + " for " + row.scope_text, "data-action": "remove-override",
    onClick: () => changeOverride("clear", { server: row.tag_id == null ? row.server_name : "", tag: row.tag_id == null ? "" : String(row.tag_id), key: row.key, database: row.database }),
  });
  return el("span", { class: "row-actions" }, [edit, remove]);
}

/** The grid's columns: OVERRIDE_COLUMNS, plus Edit and Remove for a seat that can change overrides. */
export function overrideColumns(canEdit) {
  return canEdit ? OVERRIDE_COLUMNS.concat([{ key: "actions", label: "Change", csv: false, copy: false, render: overrideActions }]) : OVERRIDE_COLUMNS;
}

async function buildOverrides(body, generation) {
  const [session, res] = await Promise.all([getSession(), apiGet("/api/threshold-overrides")]);
  if (generation !== renderGeneration) return;
  const nodes = [];
  const status = overrideStatusNode();
  if (status) nodes.push(status);
  const bad = failure(res);
  if (bad) return mount(body, nodes.concat([bad]));
  /* Both must agree: the session says the seat may write at all (a read-only seat is refused every PUT), and the list says
     the store login holds the write grant on this table. */
  const canEdit = session.can_edit === true && !!res.data && res.data.can_edit === true;
  const keys = Array.isArray(res.data && res.data.keys) ? res.data.keys : [];
  nodes.push(noticeStrip(canEdit ? OVERRIDES_NOTE_EDITING : overridesNoteReadOnly(session.managed_store === true, session.admin_writes_off === true)));
  if (res.data && typeof res.data.note === "string" && res.data.note) nodes.push(noticeStrip(res.data.note));
  if (canEdit) nodes.push(overrideFormNode(Array.isArray(res.data.servers) ? res.data.servers : [], keys, res.data.tags));
  nodes.push(VIZ.table({ overrides: overrideRows(res.data) }, {
    rowsKey: "overrides",
    columns: overrideColumns(canEdit),
    emptyText: "No server or tag holds an override: every server uses the global values.",
  }));
  mount(body, nodes);
}

/* Agent changes (#5917) lives in its own file; it gets this file's helpers and repaints through startBuilder. */
const agentChangesContext = {
  getSession,
  failure,
  isCurrent: (generation) => generation === renderGeneration,
  repaint: () => {
    if (shownTab === "agent-changes" && shownBody) return startBuilder("agent-changes", shownBody, ++renderGeneration);
    return null;
  },
};
const buildAgentChangesTab = (body, generation) => buildAgentChanges(body, generation, agentChangesContext);

/* Server actions (#6269) lives in its own file too, with the same helpers. */
const serverActionsContext = {
  ...agentChangesContext,
  repaint: () => {
    if (shownTab === "server-actions" && shownBody) return startBuilder("server-actions", shownBody, ++renderGeneration);
    return null;
  },
};
const buildServerActionsTab = (body, generation) => buildServerActions(body, generation, serverActionsContext);

const BUILDERS = { servers: buildServers, routes: buildRoutes, settings: buildSettings, feedback: buildFeedback, practice: buildPractice, pending: buildPending, overrides: buildOverrides, "agent-changes": buildAgentChangesTab, profiles: buildProfilesTab, "server-actions": buildServerActionsTab };

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
  if (!showsServers(routeHash())) closeEdit();
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
    notesStatus = null;
    resetAgentChangesStatus();
    resetServerActionsStatus();
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
