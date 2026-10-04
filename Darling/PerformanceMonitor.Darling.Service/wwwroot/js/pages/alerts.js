/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Alert History page (#1562) — the alert log from get_alert_history (server omitted = whole fleet, each row naming
 * its server). A range, a row limit, a server and a Show dismissed choice (module-scope, so they survive the 60 s
 * poll) set what is read; a reply the tool cut at the row limit says so above the table.
 * A filter box narrows the already-fetched rows by server name client-side (no
 * re-fetch). Every value is untrusted and reaches the DOM through el()/textContent (R4 — never innerHTML),
 * the expanders included (B2): the channel / sent / muted / delivery-error columns collapse into one compact
 * Status column, and the Detail column shows a truncated one-liner that expands to the full payload (structured
 * "Story:/Severity:/Confidence:" findings render as labeled fields).
 *
 * #4194: the Detail expansion is built lazily, on first open (detailCell/detailBody), not up front for all 200
 * rows. The 60s poll (app.js's refresh loop calls renderAlerts(main) again, not a diff of its own) reconciles
 * the existing table by row key instead of rebuilding it (renderAlerts/refreshAlerts/drawAlerts/reconcileRows) -
 * the header and filter box also survive a poll tick untouched, so typing in the filter is no longer clobbered
 * every 60s. Sort order, filters and every rendered field are unchanged.
 */

import { el, mount, readTool, buildQuery, loadingStrip, errorStrip, emptyStrip, noticeStrip, disclosure,
         ALERT_STATE_LABELS, alertDeliveryState } from "../util.js";
import { VIZ, reapplyGridSort, gridRowOf } from "../panels.js";
import { mutePrefillParams } from "../mute-context.js";
import { getSession } from "../views-api.js";

/* #3169: the state-carrying notification_type values, derived from the one shared map rather than listed a
   second time. The status label already carries each of them, so the channel chip beside it must not repeat
   them - "No channel configured (unconfigured)" is the shape this prevents. */
const STATE_ONLY_CHANNELS = new Set(Object.keys(ALERT_STATE_LABELS));

/* Most urgent first when ascending, the order the fleet bands use (Critical, Warning, then the calm states). */
const SEVERITY_RANK = { critical: 0, warning: 1, info: 2, resolution: 3 };

const ALERT_COLUMNS = [
  { key: "alert_time", label: "Time", format: "time" },
  { key: "server_name", label: "Server" },
  { key: "metric_name", label: "Metric" },
  { key: "severity", label: "Severity", sortValue: (a) => (a.severity == null ? null : (SEVERITY_RANK[a.severity] ?? 9)), render: (a) => severityCell(a) },
  { key: "current_value", label: "Value", format: "num1" },
  { key: "threshold_value", label: "Threshold", format: "num1" },
  { key: "status", label: "Status", render: (a) => statusCell(a) },
  { key: "detail_text", label: "Detail", render: (a) => detailCell(a) },
  { key: "triage", label: "Triage", render: (a) => triageCell(a) },
];

/* The Mute column exists only for a seat whose session reports can_edit (the server enforces the write gate;
   this is only the affordance). Both links open the Mute Rules create form pre-filled from the row. "Mute this
   alert" keys on the row's store id when it has one and otherwise on the stored server spelling, and fills the
   database / wait / job / query dimensions from the detail text (js/mute-context.js); the display name in
   server_name is never what a rule matches. "Mute similar" leaves the server blank (this metric anywhere). */
let canMute = false;
const MUTE_COLUMN = { key: "mute", label: "Mute", render: (a) => muteCell(a) };
function alertColumns() {
  return canMute ? ALERT_COLUMNS.concat([MUTE_COLUMN]) : ALERT_COLUMNS;
}

function muteCell(a) {
  const link = (text, params) => el("a", { href: "#/mute-rules" + buildQuery(params), text });
  return el("span", {}, [
    link("Mute this alert", mutePrefillParams(a)),
    " · ",
    link("Mute similar", { metric_name: a.metric_name }),
  ]);
}

/* #3539 A8e: the tier the alert FIRED at, as the tool reports it — never re-derived here from the metric name
 * (R1). The service reads it off the row's persisted context and falls back to the name only for rows that
 * carry none; severity_source says which arm answered, and it rides as the cell's title because the two are
 * not equal evidence: "critical" from the row is what the operator was paged with, "critical" from the name
 * is what the map says about the name. The tone classes are the status cell's own. */
const SEVERITY_TONE = { critical: "Critical", warning: "Warning", resolution: "Healthy", info: "Unknown" };
const SEVERITY_SOURCE_TITLE = {
  fired: "The tier this alert fired at, read from the row",
  metric_name: "Implied by the metric name; this row carries no fired tier",
};

function severityCell(a) {
  if (a.severity == null) return el("span", { class: "muted", text: "—" });
  return el("span", {
    class: "status-cell sev-" + (SEVERITY_TONE[a.severity] || "Unknown"),
    text: String(a.severity),
    title: SEVERITY_SOURCE_TITLE[a.severity_source] || null,
  });
}

/* Deep-link into the #2710 triage page for this row — the SAME route the alert webhooks link to, anchored at
 * this row's own firing instant, so the in-app path and the delivered link land on an identical page. */
function triageCell(a) {
  return el("a", {
    href: "#/triage" + buildQuery({ server: a.server_name, metric: a.metric_name, at: a.alert_time }),
    text: "Open",
  });
}

/* Sent + Muted + Delivery Error collapse into one glyph+text status cell (B2). The channel is appended only
   when it means something on THIS surface: the store records "tray" (the Lite/Dashboard system-tray toast),
   which the headless web dashboard has no equivalent for, so a bare "tray" is dropped rather than shown as a
   meaningless delivery channel (#2781). A real channel (email / webhook / email+webhook) still renders. */
function statusCell(a) {
  let glyph, label, sev;
  const stateLabel = alertDeliveryState(a);
  if (a.muted) {
    glyph = "⊘";
    label = "Muted";
    sev = "Warning";
  } else if (a.send_error) {
    glyph = "✕";
    label = "Delivery failed";
    sev = "Critical";
  } else if (stateLabel !== null) {
    /* #3169: the row states a delivery STATE rather than naming a channel — no channel applies to it, none
       is configured, it was muted, or a channel was consulted and nothing landed. One neutral glyph and
       severity for all of them: whether an unconfigured deployment is a problem is the operator's call, not
       this page's. The label and the legacy decode both live in util.js, shared with triage.js and pinned
       against the WPF surfaces' constants. Extends #2781/#2814, which established the reading for the
       headless "tray" row that #3169 then stopped the service writing. */
    glyph = "•";
    label = stateLabel;
    sev = "Unknown";
  } else if (a.alert_sent) {
    glyph = "✓";
    label = "Delivered";
    sev = "Healthy";
  } else {
    glyph = "•";
    label = "Not sent";
    sev = "Unknown";
  }
  /* #3712: an analysis finding's routing_reason — why it paged, or why it went to the digest — rides as the
     cell's title the way severity_source rides on the severity cell: "why didn't this page" answered on
     hover, from the row, without opening the detail. A delivery error keeps precedence; it is the rarer and
     costlier fact. */
  return el("span", { class: "status-cell sev-" + sev, title: a.send_error || a.routing_reason || null }, [
    el("span", { class: "glyph", text: glyph }),
    el("span", { text: label }),
    /* The channel chip names a real channel only. The state-carrying values are already in the label
       above, so repeating them would read as "No channel configured (unconfigured)". */
    a.notification_type && !STATE_ONLY_CHANNELS.has(a.notification_type)
      ? el("span", { class: "channel", text: a.notification_type }) : null,
  ]);
}

/* The Detail cell (B2): a truncated one-liner that expands to the full payload. Structured finding detail_text
 * (indented "Story:/Severity:/Confidence:/..." lines) renders as labeled fields; anything else renders as a
 * plain text block. A delivery error is appended as its own labeled field so it is fully readable in the
 * expansion, not merely a hover title on the status glyph.
 *
 * #4194: the expansion used to be built for all 200 rows up front, even though it sits behind a collapsed
 * <details> almost nobody opens - parseDetailFields() plus one DOM row per parsed field, times 200, was most
 * of the page's ~19,500 DOM nodes. It is now built once, on first open (the native "toggle" event, which fires
 * on both open and close - `built` skips the close firing and any open after the first). The placeholder is a
 * bare, unstyled div so the eventual body's markup - div.detail-fields, div.detail-fields.detail-error - lands
 * exactly as it did before this change; only the disc-body -> placeholder -> body nesting is one level deeper. */
function detailCell(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  if (!hasDetail && !a.send_error) return el("span", { class: "muted", text: "—" });

  const summary = hasDetail ? a.detail_text : "Delivery error: " + a.send_error;
  const placeholder = el("div", {});
  const node = disclosure(summary, placeholder, { max: 120 });

  let built = false;
  node.addEventListener("toggle", () => {
    if (built || !node.open) return;
    built = true;
    mount(placeholder, detailBody(a));
  });
  return node;
}

/* The expansion body: unchanged shape from the pre-#4194 eager version (see detailCell above), just built lazily. */
function detailBody(a) {
  const hasDetail = a.detail_text != null && String(a.detail_text).trim().length > 0;
  const fields = hasDetail ? parseDetailFields(a.detail_text) : null;
  const body = [];
  if (fields) {
    body.push(el("div", { class: "detail-fields" }, fields.map(fieldRow)));
  } else if (hasDetail) {
    body.push(el("div", { class: "detail-raw", text: a.detail_text }));
  }
  if (a.send_error) {
    body.push(el("div", { class: "detail-fields detail-error" }, [fieldRow(["Delivery error", a.send_error])]));
  }
  return body;
}

/* One parsed pair -> a grid row of the two-column .detail-fields grid. A section heading (no label) is its own
 * full-width row: a lone value in the label/value grid would take the label column's cell and shift every later
 * label and value over by one, which put the labels in the wide value column's far edge, out of sight, leaving
 * bare values (Alert History's blocking and incident details, where every other row is label: value). */
function fieldRow([k, v]) {
  if (!k) return el("div", { class: "detail-heading", text: v });
  return el("div", { class: "detail-field" }, [
    el("span", { class: "fk", text: k }),
    el("span", { class: "fv", text: v }),
  ]);
}

/* Parse an indented "Key: value" detail block into [key, value] pairs; returns null (=> render as raw) unless
 * at least two lines match, so a plain one-sentence detail stays a plain block. The flattened alert context
 * writes each item as an unindented heading line ("Blocking chain (x2)", "Incident 1 of 2") over its indented
 * "Label: value" lines, with a blank line between items: an unindented non-matching line that opens a block
 * (the first line, or the first after a blank one) is a section heading, kept as its own [null, heading] entry
 * instead of being folded onto the previous value ("24.1s-34.9s Incident 1 of 2"). Any other non-matching line
 * is a continuation and folds into the previous field's value (wrapped story text). */
function parseDetailFields(text) {
  const fields = [];
  let matched = 0;
  let blockStart = true;
  for (const rawLine of String(text).split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line.length === 0) {
      blockStart = true;
      continue;
    }
    const m = /^([A-Za-z][A-Za-z ]{0,28}):\s*(.*)$/.exec(line);
    if (m) {
      fields.push([m[1], m[2]]);
      matched++;
    } else if (blockStart && !/^\s/.test(rawLine)) {
      fields.push([null, line]);
    } else if (fields.length) {
      fields[fields.length - 1][1] += " " + line;
    } else {
      fields.push([null, line]);
    }
    blockStart = false;
  }
  return matched >= 2 ? fields : null;
}

/* A row's identity for the #4194 keyed refresh: the same (server, metric, fired instant) triple triageCell()
 * above already treats as identifying one alert - there is no surrogate id in get_alert_history's payload. */
function alertKey(a) {
  return a.server_name + "\u0000" + a.metric_name + "\u0000" + a.alert_time;
}

/* Builds ONE <tr>, styled identically to VIZ.table's own row (same columns, same cell()), by handing it a
 * single-row page and lifting the <tr> back out - reuses the shared renderer's formatting/severity classes
 * without duplicating them, and without vizTable itself having to know about incremental refresh. */
function alertRowNode(row) {
  const wrap = VIZ.table({ alerts: [row] }, { rowsKey: "alerts", columns: alertColumns() });
  return markDismissed(wrap.querySelector("tbody tr"), row);
}

/* #4194: app.js's route() calls renderAlerts(main) fresh on every 60s poll tick as well as on first navigation
 * (see app.js's refresh loop). Reconciles the existing <tbody> to the new row set by key instead of tearing
 * down and rebuilding every row: unchanged rows keep their DOM node, only added/removed rows touch the DOM.
 * Order follows `rows` (already alert_time_desc from the read), so a row that moves position - it cannot,
 * since alert_time is immutable once written, but this stays correct if that ever changes - would still land
 * in the right place. */
function reconcileRows(tbody, rows, rowMap) {
  const nextKeys = new Set(rows.map(alertKey));
  for (const [key, tr] of rowMap) {
    if (!nextKeys.has(key)) {
      tr.remove();
      rowMap.delete(key);
    }
  }
  let anchor = tbody.firstChild;
  for (const row of rows) {
    const key = alertKey(row);
    let tr = rowMap.get(key);
    if (!tr) {
      tr = alertRowNode(row);
      rowMap.set(key, tr);
    }
    if (tr === anchor) {
      anchor = anchor.nextSibling;
    } else {
      tbody.insertBefore(tr, anchor);
    }
  }
}

/* The four read choices, kept at module scope so the 60 s poll (which calls renderAlerts again) and a visit to
 * another page and back keep them. The windows are the desktop Alert History's, less "All": the tool refuses
 * more than 168 hours, and a choice it would refuse is not offered. The row limits stop at the dispatch
 * layer's 1000-row ceiling. The server is the registry's server_name ("" = the whole fleet). */
const WINDOW_CHOICES = [
  { hours: 1, label: "Last 1 Hour" },
  { hours: 4, label: "Last 4 Hours" },
  { hours: 24, label: "Last 24 Hours" },
  { hours: 168, label: "Last 7 Days" },
];
const LIMIT_CHOICES = [200, 500, 1000];
const choices = { hours: 24, limit: 200, server: "", dismissed: false };

/* The get_alert_history parameters the current choices ask for. server_name and include_dismissed are left out
 * (buildQuery drops empty values) when they are at their defaults. */
function readParams() {
  return {
    hours_back: choices.hours,
    limit: choices.limit,
    server_name: choices.server || null,
    include_dismissed: choices.dismissed ? "true" : null,
  };
}

function windowLabel() {
  return (WINDOW_CHOICES.find((w) => w.hours === choices.hours) || WINDOW_CHOICES[2]).label.toLowerCase();
}

/* The server names list_servers reported on the last successful read; the picker keeps the chosen server in the
 * list even when that read fails or no longer names it, so the choice stays visible. */
let serverNames = [];

function serverRowsOf(data) {
  if (Array.isArray(data)) return data;
  if (data && Array.isArray(data.servers)) return data.servers;
  return [];
}

async function loadServerNames() {
  const res = await readTool("list_servers", {});
  if (res.kind === "error" || res.kind === "empty") return;
  serverNames = serverRowsOf(res.data)
    .map((r) => ({ name: r.server_name, label: r.display_name || r.server_name }))
    .filter((r) => r.name);
}

function pickerOptions(items, chosen) {
  return items.map((i) => el("option", { value: String(i.value), text: i.label }));
}

function picker(label, items, chosen) {
  const sel = el("select", { class: "range-select-inline", "aria-label": label }, pickerOptions(items, chosen));
  sel.value = String(chosen);
  return sel;
}

function control(label, select) {
  return el("label", { class: "range-control" }, [el("span", { text: label }), select]);
}

function serverItems() {
  const items = [{ value: "", label: "All servers" }].concat(serverNames.map((r) => ({ value: r.name, label: r.label })));
  if (choices.server && !serverNames.some((r) => r.name === choices.server)) items.push({ value: choices.server, label: choices.server });
  return items;
}

/* A dismissed row (include_dismissed) renders muted and struck through, so it reads as acknowledged beside the
 * live ones; the class is set where each row node is built, and the title says what it means. */
function markDismissed(tr, row) {
  if (!tr || !row) return tr;
  if (row.dismissed === true) {
    tr.className = ((tr.className || "") + " alert-dismissed").trim();
    tr.setAttribute("title", "Dismissed");
  }
  return tr;
}

/* Remembers the last mount so a poll tick landing on the SAME still-open page can reconcile in place instead of
 * rebuilding (module-level: renderAlerts gets no state of its own from app.js's route(), which just calls it
 * again). `headEl.isConnected` tells a poll tick apart from a fresh navigation: mount() on any OTHER page
 * detaches this page's headEl from the document, so a stale `live` falls through to a full rebuild below. */
let live = null;

export async function renderAlerts(main) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refreshAlerts(live);
    return;
  }

  canMute = !!(await getSession()).can_edit;
  await loadServerNames();
  const filter = el("input", {
    class: "filter-box",
    type: "text",
    placeholder: "Filter by server…",
    "aria-label": "Filter alerts by server",
  });

  const windowSel = picker("Time range", WINDOW_CHOICES.map((w) => ({ value: w.hours, label: w.label })), choices.hours);
  const limitSel = picker("Row limit", LIMIT_CHOICES.map((n) => ({ value: n, label: n + " rows" })), choices.limit);
  const serverSel = picker("Server", serverItems(), choices.server);
  const dismissedBox = el("input", { type: "checkbox", "aria-label": "Show dismissed alerts" });
  dismissedBox.checked = choices.dismissed;

  const meta = el("div", { class: "meta", text: "" });
  const noticeBox = el("div", {});
  const tableBox = el("div", {});
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Alert History" }),
    meta,
    el("div", { class: "spacer" }),
    control("Range", windowSel),
    control("Rows", limitSel),
    control("Server", serverSel),
    el("label", { class: "range-control" }, [dismissedBox, el("span", { text: "Show dismissed" })]),
    filter,
  ]);
  mount(main, [headEl, noticeBox, tableBox]);

  live = { main, headEl, meta, noticeBox, tableBox, filter, alerts: [], tbody: null, rowMap: null, seq: 0 };
  /* A changed choice refetches and reconciles the table that is already drawn, the same path a poll tick takes. */
  const changed = () => refreshAlerts(live);
  windowSel.addEventListener("change", () => { choices.hours = Number(windowSel.value); changed(); });
  limitSel.addEventListener("change", () => { choices.limit = Number(limitSel.value); changed(); });
  serverSel.addEventListener("change", () => { choices.server = serverSel.value; changed(); });
  dismissedBox.addEventListener("change", () => { choices.dismissed = !!dismissedBox.checked; changed(); });
  filter.addEventListener("input", () => drawAlerts(live));

  mount(tableBox, loadingStrip("Loading alerts…"));
  await refreshAlerts(live);
}

/* Re-fetches with the current choices, then hands the new alerts to drawAlerts() to reconcile in place. Unlike
 * the first mount, a poll tick never shows the loading strip - the previous table stays exactly as it is until
 * the new page is ready, so a healthy 60s tick with no new alert produces no visible change and no DOM churn at
 * all. A changed window or server reconciles the same way: rows that left the window are removed, new ones land
 * in order. A reply that arrives after a newer request was made is dropped. */
async function refreshAlerts(state) {
  const seq = ++state.seq;
  const res = await readTool("get_alert_history", readParams());
  if (seq !== state.seq) return;
  state.meta.textContent = (choices.server ? choices.server : "fleet-wide") + " · " + windowLabel();
  mount(state.noticeBox, []);
  if (res.kind === "error" || res.kind === "empty") {
    state.alerts = [];
    state.tbody = null;
    state.rowMap = null;
    mount(state.tableBox, res.kind === "error" ? errorStrip(res.message) : emptyStrip(res.message));
    return;
  }
  state.alerts = res.data.alerts || [];
  if (res.data.truncated === true) {
    mount(state.noticeBox, noticeStrip(
      "More alerts exist than shown: the newest " + state.alerts.length + " are listed. Raise the row limit or narrow the range to see the rest."));
  }
  drawAlerts(state);
}

/* Applies the (client-side, unchanged) server filter to state.alerts and either reconciles the existing table
 * or, the first time / after an empty or error state, builds it fresh and records tbody + rowMap for next time. */
function drawAlerts(state) {
  const q = state.filter.value.trim().toLowerCase();
  const rows = q ? state.alerts.filter((a) => String(a.server_name || "").toLowerCase().includes(q)) : state.alerts;

  if (!rows.length) {
    state.tbody = null;
    state.rowMap = null;
    mount(state.tableBox, emptyStrip(q ? 'No alerts match "' + state.filter.value + '".' : "No alerts in this window."));
    return;
  }

  if (state.tbody) {
    reconcileRows(state.tbody, rows, state.rowMap);
    reapplyGridSort(state.tbody);
    return;
  }

  const table = VIZ.table({ alerts: rows }, { rowsKey: "alerts", columns: alertColumns() });
  mount(state.tableBox, table);
  const tbody = table.querySelector("tbody");
  const rowMap = new Map();
  [...tbody.children].forEach((tr) => {
    const row = gridRowOf(tr);
    markDismissed(tr, row);
    rowMap.set(alertKey(row), tr);
  });
  state.tbody = tbody;
  state.rowMap = rowMap;
}
